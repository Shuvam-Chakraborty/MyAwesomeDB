use anyhow::{Context, Result};
use clap::Parser;
use db_config::DbContext;

use crate::{
    cli::CliOptions,
    executor::execute_query,
    io_setup::{setup_disk_io, setup_monitor_io},
    monitor_client::MonitorClient,
    optimizer::optimize_query,
    storage::{block_allocator::ScratchSpace, disk_client::DiskClient},
};

mod cli;
mod estimation;
mod executor;
mod io_setup;
mod monitor_client;
mod optimizer;
mod query_support;
mod row;
mod scan_pipeline;
mod storage;

fn db_main() -> Result<()> {
    let cli_options = CliOptions::parse();
    let ctx = DbContext::load_from_file(cli_options.config_path())?;
    let (disk_in, disk_out) = setup_disk_io();
    let (monitor_in, monitor_out) = setup_monitor_io();
    let mut disk = DiskClient::new(disk_in, disk_out);
    let mut monitor = MonitorClient::new(monitor_in, monitor_out);
    let query = optimize_query(&monitor.read_query()?, &ctx)?;
    let memory_limit_mb = monitor.get_memory_limit_mb()?;
    let block_size = disk.get_block_size()?;
    let anon_start = disk.get_anon_start_block()?;
    let mut scratch = ScratchSpace::new(block_size, anon_start);
    monitor.begin_validation()?;
    execute_query(
        &query,
        &ctx,
        &mut disk,
        &mut scratch,
        memory_limit_mb as usize * 1024 * 1024,
        |row| monitor.send_row(&row),
    )?;
    monitor.finish_validation()?;
    Ok(())
}

fn main() -> Result<()> {
    db_main().with_context(|| "From Database")
}
