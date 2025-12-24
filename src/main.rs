use clap::{Parser, ValueEnum};
use clap::builder::styling::{AnsiColor, Styles};
use crossbeam_channel::{bounded, Receiver, Sender};
use csv::StringRecord;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use serde_json::{Map, Value};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;

#[derive(Copy, Clone, Debug, ValueEnum)]
enum OutputFormat {
    Ndjson,
    Array,
}

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about = "Convert CSV files to JSON with streaming output",
    styles = cli_styles()
)]
struct Args {
    /// Input CSV file path
    #[arg(short, long)]
    input: PathBuf,

    /// Output file path (defaults to stdout)
    #[arg(short, long)]
    output: Option<PathBuf>,

    /// Output format
    #[arg(long, value_enum, default_value = "ndjson")]
    format: OutputFormat,

    /// Number of worker threads
    #[arg(short, long, default_value_t = default_threads())]
    threads: usize,

    /// Bounded queue size for in-flight records
    #[arg(long, default_value_t = 1024)]
    queue_size: usize,

    /// Use when the CSV has no header row
    #[arg(long)]
    no_headers: bool,

    /// Convert all rows in memory for maximum parallelism
    #[arg(long)]
    in_memory: bool,

    /// Allow output records in any order (faster, less buffering)
    #[arg(long)]
    unordered: bool,

    /// CSV delimiter character
    #[arg(long, default_value = ",")]
    delimiter: String,

    /// Verbosity (-v, -vv)
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
}

#[derive(Debug)]
struct Job {
    index: u64,
    record: Vec<String>,
}

#[derive(Debug)]
struct ResultRow {
    index: u64,
    json: String,
}

fn default_threads() -> usize {
    std::cmp::max(1, num_cpus::get())
}

fn cli_styles() -> Styles {
    Styles::styled()
        .header(AnsiColor::Blue.on_default().bold())
        .usage(AnsiColor::Green.on_default().bold())
        .literal(AnsiColor::Cyan.on_default().bold())
        .placeholder(AnsiColor::Yellow.on_default())
}

fn main() -> io::Result<()> {
    let args = Args::parse();
    if args.threads == 0 {
        eprintln!("threads must be at least 1");
        std::process::exit(2);
    }
    if args.queue_size == 0 {
        eprintln!("queue-size must be at least 1");
        std::process::exit(2);
    }
    let delimiter = match parse_delimiter(&args.delimiter) {
        Ok(value) => value,
        Err(message) => {
            eprintln!("{message}");
            std::process::exit(2);
        }
    };

    if args.in_memory {
        run_in_memory(&args, delimiter)?;
    } else {
        run_streaming(&args, delimiter)?;
    }
    Ok(())
}

fn run_in_memory(args: &Args, delimiter: u8) -> io::Result<()> {
    let input = File::open(&args.input)?;
    let reader = BufReader::new(input);
    let mut csv_reader = csv::ReaderBuilder::new()
        .delimiter(delimiter)
        .has_headers(!args.no_headers)
        .flexible(true)
        .from_reader(reader);

    let mut first_record = None;
    let headers = if args.no_headers {
        let mut record = StringRecord::new();
        if !csv_reader.read_record(&mut record).map_err(to_io_err)? {
            write_empty_output(args)?;
            return Ok(());
        }
        let headers = (0..record.len())
            .map(|idx| format!("col{}", idx + 1))
            .collect::<Vec<_>>();
        first_record = Some(record);
        headers
    } else {
        load_headers(&mut csv_reader)?
    };

    let mut rows: Vec<Vec<String>> = Vec::new();
    if let Some(record) = first_record {
        rows.push(record.iter().map(|s| s.to_string()).collect());
    }
    for record in csv_reader.into_records() {
        let record = record?;
        rows.push(record.iter().map(|s| s.to_string()).collect());
    }

    if rows.is_empty() {
        write_empty_output(args)?;
        return Ok(());
    }

    let pool = ThreadPoolBuilder::new()
        .num_threads(args.threads)
        .build()
        .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

    let json_rows: Vec<String> = pool.install(|| {
        rows.par_iter()
            .map(|record| record_to_json(&headers, record))
            .collect()
    });

    write_output_in_memory(args, &json_rows)?;

    if args.verbose > 0 {
        eprintln!("Processed {} records", json_rows.len());
        eprintln!(
            "Output format: {:?}, threads: {}, in-memory: true",
            args.format, args.threads
        );
    }

    Ok(())
}

fn run_streaming(args: &Args, delimiter: u8) -> io::Result<()> {
    let input = File::open(&args.input)?;
    let reader = BufReader::new(input);
    let mut csv_reader = csv::ReaderBuilder::new()
        .delimiter(delimiter)
        .has_headers(!args.no_headers)
        .flexible(true)
        .from_reader(reader);

    let mut first_record = None;
    let headers = if args.no_headers {
        let mut record = StringRecord::new();
        if !csv_reader.read_record(&mut record).map_err(to_io_err)? {
            write_empty_output(args)?;
            return Ok(());
        }
        let headers = (0..record.len())
            .map(|idx| format!("col{}", idx + 1))
            .collect::<Vec<_>>();
        first_record = Some(record);
        Arc::new(headers)
    } else {
        Arc::new(load_headers(&mut csv_reader)?)
    };

    let (job_tx, job_rx) = bounded::<Job>(args.queue_size);
    let (result_tx, result_rx) = bounded::<ResultRow>(args.queue_size);

    let mut total_sent = 0u64;
    spawn_workers(args.threads, job_rx, result_tx, Arc::clone(&headers));

    if let Some(record) = first_record {
        send_record(&job_tx, total_sent, &record)?;
        total_sent += 1;
    }

    let records_iter = csv_reader.into_records();
    for record in records_iter {
        let record = record?;
        send_record(&job_tx, total_sent, &record)?;
        total_sent += 1;
    }
    drop(job_tx);

    write_output(args, result_rx, total_sent)?;
    Ok(())
}

fn load_headers(reader: &mut csv::Reader<BufReader<File>>) -> io::Result<Vec<String>> {
    let headers = reader.headers().map_err(to_io_err)?;
    Ok(headers.iter().map(|h| h.to_string()).collect())
}

fn send_record(tx: &Sender<Job>, index: u64, record: &StringRecord) -> io::Result<()> {
    let data = record.iter().map(|s| s.to_string()).collect::<Vec<_>>();
    tx.send(Job { index, record: data })
        .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "worker channel closed"))?;
    Ok(())
}

fn spawn_workers(
    threads: usize,
    job_rx: Receiver<Job>,
    result_tx: Sender<ResultRow>,
    headers: Arc<Vec<String>>,
) {
    for _ in 0..threads {
        let job_rx = job_rx.clone();
        let result_tx = result_tx.clone();
        let headers = Arc::clone(&headers);
        thread::spawn(move || {
            for job in job_rx.iter() {
                let json = record_to_json(&headers, &job.record);
                let _ = result_tx.send(ResultRow {
                    index: job.index,
                    json,
                });
            }
        });
    }
}

fn record_to_json(headers: &[String], record: &[String]) -> String {
    let mut map = Map::with_capacity(std::cmp::max(headers.len(), record.len()));
    for (idx, header) in headers.iter().enumerate() {
        let value = record.get(idx).cloned().unwrap_or_default();
        map.insert(header.clone(), Value::String(value));
    }
    if record.len() > headers.len() {
        for idx in headers.len()..record.len() {
            let key = format!("extra_{}", idx + 1);
            let value = record.get(idx).cloned().unwrap_or_default();
            map.insert(key, Value::String(value));
        }
    }
    serde_json::to_string(&Value::Object(map)).unwrap_or_else(|_| "{}".to_string())
}

fn write_output(args: &Args, result_rx: Receiver<ResultRow>, total_sent: u64) -> io::Result<()> {
    let output: Box<dyn Write> = match &args.output {
        Some(path) => Box::new(File::create(path)?),
        None => Box::new(io::stdout()),
    };
    let mut writer = BufWriter::new(output);

    if matches!(args.format, OutputFormat::Array) {
        writer.write_all(b"[")?;
    }

    let mut wrote_any = false;
    let mut written = 0u64;

    if args.unordered {
        for result in result_rx.iter() {
            write_json_value(&mut writer, args.format, &result.json, &mut wrote_any)?;
            written += 1;
        }
    } else {
        let mut next_index = 0u64;
        let mut buffered: BTreeMap<u64, String> = BTreeMap::new();
        while let Ok(result) = result_rx.recv() {
            buffered.insert(result.index, result.json);
            while let Some(json) = buffered.remove(&next_index) {
                write_json_value(&mut writer, args.format, &json, &mut wrote_any)?;
                next_index += 1;
                written += 1;
            }
        }
    }

    if written != total_sent {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "failed to write all records",
        ));
    }

    if matches!(args.format, OutputFormat::Array) {
        writer.write_all(b"]")?;
    }
    writer.flush()?;

    if args.verbose > 0 {
        eprintln!("Processed {} records", total_sent);
        eprintln!(
            "Output format: {:?}, threads: {}, queue size: {}, unordered: {}",
            args.format, args.threads, args.queue_size, args.unordered
        );
    }

    Ok(())
}

fn to_io_err(err: csv::Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, err)
}

fn parse_delimiter(value: &str) -> Result<u8, String> {
    if value.is_empty() {
        return Err("delimiter must be a single byte character".to_string());
    }
    let bytes = value.as_bytes();
    if bytes.len() == 1 {
        return Ok(bytes[0]);
    }
    if value.starts_with('\\') && value.len() == 2 {
        return match bytes[1] {
            b't' => Ok(b'\t'),
            b'n' => Ok(b'\n'),
            b'r' => Ok(b'\r'),
            b'\\' => Ok(b'\\'),
            _ => Err("delimiter escape must be one of: \\t, \\n, \\r, \\\\".to_string()),
        };
    }
    Err("delimiter must be a single byte character".to_string())
}

fn write_output_in_memory(args: &Args, rows: &[String]) -> io::Result<()> {
    let output: Box<dyn Write> = match &args.output {
        Some(path) => Box::new(File::create(path)?),
        None => Box::new(io::stdout()),
    };
    let mut writer = BufWriter::new(output);

    if matches!(args.format, OutputFormat::Array) {
        writer.write_all(b"[")?;
    }

    let mut wrote_any = false;
    for json in rows {
        write_json_value(&mut writer, args.format, json, &mut wrote_any)?;
    }

    if matches!(args.format, OutputFormat::Array) {
        writer.write_all(b"]")?;
    }
    writer.flush()?;
    Ok(())
}

fn write_json_value(
    writer: &mut BufWriter<Box<dyn Write>>,
    format: OutputFormat,
    json: &str,
    wrote_any: &mut bool,
) -> io::Result<()> {
    match format {
        OutputFormat::Ndjson => {
            writer.write_all(json.as_bytes())?;
            writer.write_all(b"\n")?;
        }
        OutputFormat::Array => {
            if *wrote_any {
                writer.write_all(b",")?;
            }
            writer.write_all(json.as_bytes())?;
            *wrote_any = true;
        }
    }
    Ok(())
}

fn write_empty_output(args: &Args) -> io::Result<()> {
    let output: Box<dyn Write> = match &args.output {
        Some(path) => Box::new(File::create(path)?),
        None => Box::new(io::stdout()),
    };
    let mut writer = BufWriter::new(output);
    if matches!(args.format, OutputFormat::Array) {
        writer.write_all(b"[]")?;
    }
    writer.flush()?;
    Ok(())
}
