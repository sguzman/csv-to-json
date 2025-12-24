# csv-to-json

Fast CSV to JSON converter with streaming output and optional parallelism. Designed to handle large files without loading everything into memory, with an opt-in in-memory mode for maximum CPU parallelism.

## Features

- Streaming conversion (bounded memory) or full in-memory conversion.
- NDJSON (default) or JSON array output.
- Optional unordered output for faster throughput.
- Configurable worker threads and queue size.
- Optional per-record id generation (UUID v4 or deterministic SHA-256).
- CSV options: delimiter and header handling.
- Colored `--help` output.

## Install

Build from source:

```bash
cargo build --release
```

Binary path:

```bash
./target/release/csv-to-json
```

## Usage

```bash
csv-to-json --input <FILE> [options]
```

Examples:

```bash
# NDJSON to stdout
csv-to-json --input data.csv

# JSON array to file
csv-to-json --input data.csv --format array --output data.json

# Tab-delimited input
csv-to-json --input data.tsv --delimiter '\t'

# CSV without headers
csv-to-json --input data.csv --no-headers

# Streaming unordered output (faster, no ordering guarantees)
csv-to-json --input data.csv --unordered

# Full in-memory parallel conversion
csv-to-json --input data.csv --in-memory --threads 16

# Add per-record id (UUID or deterministic SHA-256)
csv-to-json --input data.csv --id uuid
csv-to-json --input data.csv --id sha256
```

## Options

- `-i, --input <FILE>`: Input CSV file path. (required)
- `-o, --output <FILE>`: Output JSON file path (defaults to stdout).
- `--format <ndjson|array>`: Output format. Default: `ndjson`.
- `-t, --threads <N>`: Number of worker threads. Default: number of CPUs.
- `--queue-size <N>`: Bounded in-flight record queue size. Default: `1024`.
- `--no-headers`: Treat input as headerless; columns become `col1`, `col2`, ...
- `--delimiter <CHAR>`: CSV delimiter character. Supports escapes: `\t`, `\n`, `\r`, `\\`.
- `--in-memory`: Load all rows into memory, then convert in parallel.
- `--unordered`: Allow output in any order (streaming mode only).
- `--id <none|uuid|sha256>`: Add an `id` field to each record. Default: `none`.
- `-v, --verbose`: Verbose output. Use `-vv` for more.

## Output formats

- **NDJSON**: One JSON object per line. Best for streaming and large files.
- **Array**: A single JSON array of objects. Suitable for smaller outputs or systems that require an array.

## Performance notes

- **Streaming mode (default)** uses a bounded queue to keep memory usage low. It parses CSV sequentially, converts records in parallel, and writes output as it becomes available.
- **`--unordered`** avoids the ordering buffer and can improve throughput when you do not need stable ordering.
- **`--in-memory`** reads all rows into RAM and parallelizes conversion fully. This is often faster for CPU-heavy conversion but uses memory proportional to file size.

## ID generation

- `uuid`: Random UUID v4 per record. Fast and collision-resistant.
- `sha256`: Deterministic hash of the row fields (joined with a unit-separator byte). Useful for stable IDs or deduping.

## Exit codes

- `0`: success
- `2`: invalid arguments (e.g., invalid delimiter)

## Examples in this repo

Some sample CSVs are included:

- `examples_small.csv`
- `examples_no_headers.csv`
- `examples_semicolon.csv`
- `examples_wide.csv`

## Notes and limitations

- CSV parsing is sequential; parallelism is applied to JSON conversion and output handling.
- `--unordered` is only meaningful in streaming mode. In `--in-memory` mode output order is deterministic.
- Delimiter must be a single byte or one of the supported escapes.

## Development

```bash
cargo fmt
cargo test
```
