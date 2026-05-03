# csv-to-json

`csv-to-json` is a fast Rust converter for turning CSV-like tabular data into JSON with streaming and optional parallelism.

## Intent

Handle large delimited files efficiently without forcing everything into memory, while still allowing higher-throughput in-memory modes when that trade-off is acceptable.

## Ambition

The CLI surface suggests a practical data-engineering utility focused on speed, flexibility, and predictable output modes rather than a general ETL framework.

## Current Status

The project already documents multiple input/output modes and performance-oriented options. It looks mature for a focused CLI utility.

## Core Capabilities Or Focus Areas

- Streaming CSV-to-JSON conversion.
- JSON array and NDJSON output modes.
- Optional full in-memory parallel conversion.
- Support for alternate delimiters and header policies.
- Optional per-record IDs for output rows.

## Project Layout

- `res/`: bundled resources used by the application.
- `src/`: Rust source for the main crate or application entrypoint.
- `Cargo.toml`: crate or workspace manifest and the first place to check for package structure.

## Setup And Requirements

- Rust toolchain.
- Delimited input data.
- Enough disk or stdout bandwidth for the chosen output format.

## Build / Run / Test Commands

```bash
cargo build
cargo test
cargo run -- --help
```

## Notes, Limitations, Or Known Gaps

- Mode selection matters: streaming and full in-memory modes optimize for different constraints.
- The project is intentionally narrow and does not try to be a full schema-mapping tool.

## Next Steps Or Roadmap Hints

- Keep the output guarantees explicit, especially around ordering and streaming behavior.
- Add regression fixtures for the less common delimiter/header combinations if needed.
