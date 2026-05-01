# My Awesome DB

Rust-based out-of-core query execution engine built for a database systems assignment. The project is designed for datasets that may be much larger than available RAM, communicates through file descriptors instead of ordinary file I/O, and is evaluated on both correctness and I/O-aware performance. It is course infrastructure and experimentation code, not a production database.

## Overview

This engine executes query plans represented as JSON ASTs rather than parsing SQL. The supported operators are:

- `Scan` reads a table from block storage.
- `Filter` applies one or more predicates.
- `Project` selects and optionally renames columns.
- `Sort` orders rows by one or more sort keys.
- `Cross` computes a Cartesian product between two inputs.

SQL appears in this repository only as a testing/reference tool: `tests_gen/` runs SQL against a generated SQLite database to produce expected outputs, while the execution engine itself consumes ASTs defined in `common/src/query.rs`.

## Architecture

```text
Monitor  <->  Database  <->  Disk Simulator
  FD 5/6         FD 3/4
```

- The `monitor` crate is the orchestrator. It launches a fresh `disk` process and a fresh `database` process for each enabled query, remaps file descriptors, enforces the assignment limits, and validates the result rows.
- The `database` crate is the query engine. It reads one JSON query from the monitor, asks for the memory limit, requests table blocks and scratch-space metadata from the disk simulator, executes the plan, and streams rows back for validation.
- The `disk` crate simulates a block device. It serves fixed-size block reads/writes, exposes file layout metadata such as start block and block count, provides an anonymous writable region for spill, and reports simulated I/O metrics.

Disk I/O is central to the design because the simulator measures access cost. Reducing unnecessary reads, writes, random access, and spill traffic is part of the assignment objective, not just an implementation detail.

## Core Concepts

- Queries are trees of AST nodes, not SQL strings. The shared AST and value types live in `common/`.
- The engine runs under a strict memory budget, so operators must stream data when possible and spill intermediate state when necessary.
- Tables are stored as fixed-size disk blocks. Each block ends with a row count, and rows are packed into the block payload without crossing block boundaries.
- The disk simulator exposes a read-only file region plus a writable anonymous region. The anonymous region is the only place the database can use for scratch/intermediate storage.
- The generator produces schema metadata and column statistics in `db_config.json`, which the optimizer can use for plan rewrites and execution-strategy choices.
- This implementation includes scan/filter/project fusion where possible, scratch-backed external sorting, and spill-aware execution for large intermediate results.

## Project Structure

```text
.
+-- common/                   Shared query AST and value/data types
+-- configs/                  Typed config crates for database, disk, and monitor JSON files
+-- database/                 Out-of-core query execution engine
+-- demo_query_printer/       Example AST builder and JSON printer
+-- disk/                     Block-based disk simulator with I/O metrics
+-- fd_wrapper/               Raw file-descriptor wrappers used for process communication
+-- generator/                Dataset compiler and runtime/config generator
+-- monitor/                  Assignment harness and result validator
+-- tests_gen/                Visible test/query generator and expected-output refresher
+-- Cargo.toml                Workspace manifest
+-- README.md                 Project documentation
`-- database_study_guide.tex  Optional internal study guide for the `database/` crate
```

- `database/` contains the execution engine itself: query optimization, operator execution, row decoding/encoding, scan pipelines, and scratch-space management.
- `disk/` contains the block-addressable disk simulator and the simulated I/O accounting logic used to measure reads, writes, seeks, and transfer cost.
- `monitor/` contains the execution harness that spawns processes, wires the required file descriptors, applies resource limits, sends queries, and checks results against expected output files.
- `generator/` converts CSV + `.schema` inputs into block-packed `.bin` table files, generates runtime configs, computes column statistics for `db_config.json`, and creates a reference `sqlite.db`.
- `tests_gen/` defines the visible workload and regenerates expected CSV outputs plus `monitor_config.json` by executing SQL equivalents against the generated SQLite database.
- `configs/` contains the typed config crates for `db_config.json`, `disk_sim_config.json`, and `monitor_config.json`.
- `common/` contains shared data/value definitions and the query AST used across the workspace.

Supporting crates:

- `fd_wrapper/` wraps raw file descriptors as Rust `Read`/`Write` objects.
- `demo_query_printer/` shows the query builder API and prints an example AST as JSON.
- `database_study_guide.tex` is supplementary documentation for understanding the `database/` crate internals; it is not part of the execution pipeline.

## Requirements

- Rust toolchain
- `sqlite3` CLI
- A Unix-like runtime environment such as Linux, macOS, or WSL

The process launcher and FD wiring in `monitor/`, `database/`, and `disk/` use Unix file-descriptor APIs, so a Unix-compatible environment is required.

## Setup and Full Run Order

Download the TPCH scratch archive from [here](https://drive.google.com/file/d/1k68hJikGIaY_YW9eGDz3oKJPyB-7AAf9/view?usp=sharing) and place `tpch_scratch.tar.gz` in the repository root.

Extract the provided scratch files:

```bash
tar -xf tpch_scratch.tar.gz
```

Build the workspace:

```bash
cargo build -r
```

Generate the TPCH compiled dataset, runtime config files, and SQLite reference database:

```bash
cargo run -r --bin generator -- all \
  --dataset-folder scratch/datasets/tpch \
  --compiled-dataset-folder scratch/compiled_datasets/tpch \
  --runtime-folder scratch/runtimes/tpch \
  --build-path target/release \
  --block-size 4096
```

Regenerate the visible expected outputs and refresh `monitor_config.json`:

```bash
cargo run -r --bin tests_gen -- -c scratch/compiled_datasets/tpch -r scratch/runtimes/tpch
```

Run the full visible TPCH suite:

```bash
cargo run -r --bin monitor -- --config scratch/runtimes/tpch/monitor_config.json
```

## How the System Runs End-to-End

1. The dataset archive is extracted into `scratch/datasets/tpch`.
2. `generator` packs the CSV tables into block-formatted `.bin` files, builds `sqlite.db`, and writes the runtime JSON configs consumed by the monitor, database, and disk simulator.
3. `tests_gen` loads the bundled visible queries, runs their SQL equivalents against `sqlite.db`, writes expected CSV outputs, and rewrites `scratch/runtimes/tpch/monitor_config.json`.
4. `monitor` reads that config and, for each enabled query, starts `disk` and `database`, sends the JSON AST to the database, and supplies the memory budget on request.
5. `database` executes the plan over disk blocks, using anonymous scratch blocks when an operator must spill.
6. `monitor` validates the streamed output against the expected CSV, while `disk` reports simulated I/O metrics for the run.

## Design Notes

- Out-of-core processing matters because the engine cannot assume relations fit in RAM; execution has to be organized around block streams and scratch runs.
- Disk I/O optimization matters because the simulator tracks access behavior and cost, so better access patterns translate directly into better assignment performance.
- The single-threaded constraint exists to force algorithmic efficiency under systems-style limits; the monitor sets process/thread limits instead of allowing parallel speedups.
- Direct file I/O is disallowed so that all intermediate storage goes through the simulated disk interface, making spill behavior explicit, measurable, and comparable across solutions.

If you want to inspect the query representation directly, start with `common/src/query.rs`, `tests_gen/src/tests.rs`, and `demo_query_printer/`.
