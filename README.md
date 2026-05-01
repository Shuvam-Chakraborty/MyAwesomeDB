# My Awesome DB

## What We Are Building

We are building a Rust-based out-of-core query engine.

The main idea is simple:

- The dataset can be much larger than RAM.
- Queries should still run correctly under a tight memory budget.
- Disk I/O matters, so we want execution plans that avoid unnecessary reads, writes, and spills.

The engine does not parse SQL directly. It executes query plans represented as JSON ASTs built from these operators:

- `Scan`
- `Filter`
- `Project`
- `Sort`
- `Cross`

At runtime the system looks like this:

```text
Monitor  <->  Database  <->  Disk Simulator
```

- `monitor/` starts the processes, sends queries, and validates results.
- `database/` is the query engine we care about most.
- `disk/` simulates block storage and tracks I/O.

In practice, most assignment work should happen in `database/`.

## How We Plan To Build It

The implementation strategy is:

- Read tables block-by-block from the disk simulator instead of trying to load full tables into memory.
- Keep operators streaming whenever possible, especially `Scan`, `Filter`, and `Project`.
- Use the anonymous writable region exposed by the disk simulator as scratch space for large intermediate results.
- Use external/out-of-core sorting for `Sort` when data does not fit in memory.
- Be careful with `Cross`, because it can explode in size. Push useful work earlier when possible so we move less data.
- Treat statistics in `db_config.json` as optimization hints, not as hard guarantees.

So the project is not "build a full SQL database". The goal is much narrower: build a query execution engine that can run the provided AST plans correctly and efficiently under assignment constraints.

## Repo Layout

- `database/` - query execution engine
- `disk/` - disk simulator
- `monitor/` - harness that runs queries and checks output
- `generator/` - converts CSV datasets into `.bin` files and generates runtime configs
- `tests_gen/` - regenerates visible queries and expected outputs using SQLite
- `common/` - shared AST and data types
- `configs/` - config crates used by the workspace
- `demo_query_printer/` - small helper that prints example query AST JSON

## Requirements

You should run this in a Unix-like environment:

- Linux
- macOS
- WSL on Windows

You also need:

- Rust toolchain
- `sqlite3`
- `tar`

Windows note: the monitor/database/disk setup uses Unix file descriptor APIs, so WSL is the safest way to run this project on Windows.

All commands below assume you are running from the repository root inside a Unix shell such as bash, zsh, or WSL.

## Download The Dataset

The starter dataset used in this repo is the TPCH scratch bundle.

1. Download `tpch_scratch.tar.gz` from the Google Drive folder [here](https://drive.google.com/file/d/1k68hJikGIaY_YW9eGDz3oKJPyB-7AAf9/view).

2. Place `tpch_scratch.tar.gz` in the repository root.
3. From the repository root, extract it:

```bash
tar -xf tpch_scratch.tar.gz
```

After extraction you should have a `scratch/` directory with dataset, compiled output, and runtime folders.

## Build The Project

Run this from the repository root:

```bash
cargo build -r
```

This builds all workspace binaries into `target/release/`.

## Generate The Compiled Dataset And Runtime Files

Once the archive is extracted and the workspace is built, run:

```bash
cargo run -r --bin generator -- all \
  --dataset-folder scratch/datasets/tpch \
  --compiled-dataset-folder scratch/compiled_datasets/tpch \
  --runtime-folder scratch/runtimes/tpch \
  --build-path target/release \
  --block-size 4096
```

This does the setup work for the whole project:

- converts CSV tables into block-formatted `.bin` files
- creates `sqlite.db` for reference checking
- writes `disk_sim_config.json`
- writes `db_config.json`
- writes a starter `monitor_config.json`

## Regenerate The Visible Test Outputs

If you want the provided visible workload and expected outputs, run:

```bash
cargo run -r --bin tests_gen -- \
  --compiled-dataset-folder scratch/compiled_datasets/tpch \
  --runtime-folder scratch/runtimes/tpch
```

This uses `sqlite3` and the generated `sqlite.db` to:

- create the expected output CSV files
- rewrite `scratch/runtimes/tpch/monitor_config.json` with the visible queries

## Run The Monitor

To run the visible suite:

```bash
cargo run -r --bin monitor -- --config scratch/runtimes/tpch/monitor_config.json
```

The monitor will:

- launch the disk simulator
- launch the database process
- send queries
- validate the rows returned by your database

If you change code in `database/`, rebuild before running again:

```bash
cargo build -r
```

## Typical Workflow

From a clean repo, the usual workflow is:

```bash
tar -xf tpch_scratch.tar.gz
cargo build -r
cargo run -r --bin generator -- all \
  --dataset-folder scratch/datasets/tpch \
  --compiled-dataset-folder scratch/compiled_datasets/tpch \
  --runtime-folder scratch/runtimes/tpch \
  --build-path target/release \
  --block-size 4096
cargo run -r --bin tests_gen -- \
  --compiled-dataset-folder scratch/compiled_datasets/tpch \
  --runtime-folder scratch/runtimes/tpch
cargo run -r --bin monitor -- --config scratch/runtimes/tpch/monitor_config.json
```

## If You Want To Try A Custom Query

For local debugging, the easiest flow is:

1. Edit `demo_query_printer/src/main.rs`.
2. Print the AST JSON:

```bash
cargo run -r --bin demo_query_printer
```

3. Copy that JSON into a query entry inside `scratch/runtimes/tpch/monitor_config.json`.
4. Run the monitor again.

If you only want to work on the assignment implementation, you do not need to understand every file in the repo. Start with `database/`, keep the block-based execution model in mind, and use the generator + monitor flow above to test your changes.
