# My Awesome DB

Small Rust database project with TPCH data generation, expected-output generation, and monitor-based testing.

## Full Run Order

Download the dataset from [here](https://drive.google.com/file/d/1k68hJikGIaY_YW9eGDz3oKJPyB-7AAf9/view?usp=sharing). Put it in the root directory.

Extract the provided scratch files:

```bash
tar -xf tpch_scratch.tar.gz
```

Build the workspace(target):

```bash
cargo build -r
```

Generate the TPCH compiled dataset, runtime config files, and sqlite reference database:

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
