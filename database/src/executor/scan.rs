use anyhow::Result;
use common::query::ScanData;
use db_config::DbContext;
use std::io::{Read, Write};

use super::{find_table_spec, scan_batch_blocks};
use crate::{
    row::{Row, Schema},
    scan_pipeline::{execute_scan_pipeline, try_compile_scan_pipeline},
    storage::{
        block_allocator::ScratchSpace,
        disk_client::DiskClient,
        row_codec::{decode_projected_rows_from_block, decode_rows_from_block},
    },
};

pub fn execute_scan<R, W>(
    scan: &ScanData,
    ctx: &DbContext,
    disk: &mut DiskClient<R, W>,
    _scratch: &mut ScratchSpace,
    memory_limit: usize,
    sink: &mut dyn FnMut(Row) -> Result<()>,
) -> Result<Schema>
where
    R: Read,
    W: Write,
{
    if let Some(plan) = try_compile_scan_pipeline(&common::query::QueryOp::Scan(scan.clone()), ctx)?
    {
        let schema = plan.schema().clone();
        execute_scan_pipeline(&plan, disk, memory_limit, sink)?;
        return Ok(schema);
    }

    let table = find_table_spec(ctx, &scan.table_id)?;
    let schema = Schema::from_table_spec(table);
    scan_table(
        &table.file_id,
        disk,
        memory_limit,
        |block| decode_rows_from_block(block, &schema),
        sink,
    )?;

    Ok(schema)
}

pub fn execute_scan_project<R, W>(
    scan: &ScanData,
    column_name_map: &[(String, String)],
    ctx: &DbContext,
    disk: &mut DiskClient<R, W>,
    _scratch: &mut ScratchSpace,
    memory_limit: usize,
    sink: &mut dyn FnMut(Row) -> Result<()>,
) -> Result<Schema>
where
    R: Read,
    W: Write,
{
    if let Some(plan) = try_compile_scan_pipeline(
        &common::query::QueryOp::Project(common::query::ProjectData {
            column_name_map: column_name_map.to_vec(),
            underlying: Box::new(common::query::QueryOp::Scan(scan.clone())),
        }),
        ctx,
    )? {
        let schema = plan.schema().clone();
        execute_scan_pipeline(&plan, disk, memory_limit, sink)?;
        return Ok(schema);
    }

    let table = find_table_spec(ctx, &scan.table_id)?;
    let input_schema = Schema::from_table_spec(table);
    let (projected_schema, indexes) = input_schema.project(column_name_map)?;

    scan_table(
        &table.file_id,
        disk,
        memory_limit,
        |block| decode_projected_rows_from_block(block, &input_schema, &indexes),
        sink,
    )?;

    Ok(projected_schema)
}

fn scan_table<R, W, F>(
    file_id: &str,
    disk: &mut DiskClient<R, W>,
    memory_limit: usize,
    mut decode_block: F,
    sink: &mut dyn FnMut(Row) -> Result<()>,
) -> Result<()>
where
    R: Read,
    W: Write,
    F: FnMut(&[u8]) -> Result<Vec<Row>>,
{
    let start = disk.get_file_start_block(file_id)?;
    let total = disk.get_file_num_blocks(file_id)?;
    let block_size = disk.get_block_size()? as usize;
    let batch_blocks = scan_batch_blocks(block_size, memory_limit, 64, 1024);

    let mut offset = 0u64;
    while offset < total {
        let batch = (total - offset).min(batch_blocks);
        let data = disk.read_blocks(start + offset, batch)?;
        for i in 0..batch as usize {
            let block = &data[i * block_size..(i + 1) * block_size];
            for row in decode_block(block)? {
                sink(row)?;
            }
        }
        offset += batch;
    }

    Ok(())
}
