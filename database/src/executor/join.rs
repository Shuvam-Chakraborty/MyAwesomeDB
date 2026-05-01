use anyhow::Result;
use common::query::CrossData;
use db_config::DbContext;
use std::io::{Read, Write};

use super::{effective_memory, execute_op, infer_schema};
use crate::{
    row::{Row, Schema},
    storage::{
        block_allocator::{ScratchRun, ScratchRunWriter, ScratchSpace, read_scratch_run_ptr},
        disk_client::DiskClient,
    },
};

pub fn execute_cross<R, W>(
    cross: &CrossData,
    ctx: &DbContext,
    disk: &mut DiskClient<R, W>,
    scratch: &mut ScratchSpace,
    memory_limit: usize,
    sink: &mut dyn FnMut(Row) -> Result<()>,
) -> Result<Schema>
where
    R: Read,
    W: Write,
{
    let right_schema = infer_schema(&cross.right, ctx)?;
    let left_schema = infer_schema(&cross.left, ctx)?;
    let output_schema = Schema::combine(&left_schema, &right_schema);
    let budget = effective_memory(memory_limit) / 2;
    let disk_ptr = disk as *mut DiskClient<R, W>;

    let (right_rows, right_run) =
        materialize(&cross.right, ctx, disk, scratch, memory_limit, budget)?;

    let mut emit = |left_row: Row| -> Result<()> {
        if let Some(run) = right_run.as_ref() {
            let mut combine = |right_row: Row| sink(Row::combine(&left_row, &right_row));
            read_scratch_run_ptr(run, &right_schema, disk_ptr, &mut combine)?;
        } else {
            for right_row in &right_rows {
                sink(Row::combine(&left_row, right_row))?;
            }
        }
        Ok(())
    };

    execute_op(&cross.left, ctx, disk, scratch, memory_limit, &mut emit)?;

    if let Some(run) = right_run {
        scratch.release_run(run);
    }

    Ok(output_schema)
}

fn materialize<R, W>(
    op: &common::query::QueryOp,
    ctx: &DbContext,
    disk: &mut DiskClient<R, W>,
    scratch: &mut ScratchSpace,
    memory_limit: usize,
    budget: usize,
) -> Result<(Vec<Row>, Option<ScratchRun>)>
where
    R: Read,
    W: Write,
{
    let mut rows: Vec<Row> = Vec::new();
    let mut bytes = 0usize;
    let mut writer: Option<ScratchRunWriter<R, W>> = None;
    let disk_ptr = disk as *mut DiskClient<R, W>;
    let scratch_ptr = scratch as *mut ScratchSpace;
    let block_size = scratch.block_size();

    {
        let mut collect = |row: Row| -> Result<()> {
            if let Some(w) = writer.as_mut() {
                return w.push_row(&row);
            }
            let rb = row.estimated_heap_bytes();
            if bytes + rb <= budget {
                bytes += rb;
                rows.push(row);
            } else {
                let mut w = ScratchRunWriter::new(disk_ptr, scratch_ptr, block_size);
                for r in &rows {
                    w.push_row(r)?;
                }
                // Once we spill, release the buffered rows instead of keeping a
                // large empty Vec allocated for the rest of the join.
                rows = Vec::new();
                bytes = 0;
                w.push_row(&row)?;
                writer = Some(w);
            }
            Ok(())
        };
        execute_op(op, ctx, disk, scratch, memory_limit, &mut collect)?;
    }

    let run = if let Some(w) = writer {
        Some(w.finish()?)
    } else {
        None
    };
    Ok((rows, run))
}
