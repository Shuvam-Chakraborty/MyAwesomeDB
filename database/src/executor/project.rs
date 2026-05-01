use anyhow::Result;
use common::query::{ProjectData, QueryOp, SortData};
use db_config::DbContext;
use std::collections::HashSet;
use std::io::{Read, Write};

use super::scan::execute_scan_project;
use super::sort::execute_sort;
use super::{execute_op, infer_schema};
use crate::{
    row::{Row, Schema},
    scan_pipeline::{execute_scan_pipeline, try_compile_scan_pipeline},
    storage::{block_allocator::ScratchSpace, disk_client::DiskClient},
};

pub fn execute_project<R, W>(
    project: &ProjectData,
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
    if let Some(plan) = try_compile_scan_pipeline(&QueryOp::Project(project.clone()), ctx)? {
        let schema = plan.schema().clone();
        execute_scan_pipeline(&plan, disk, memory_limit, sink)?;
        return Ok(schema);
    }

    if let QueryOp::Scan(scan) = project.underlying.as_ref() {
        return execute_scan_project(
            scan,
            &project.column_name_map,
            ctx,
            disk,
            scratch,
            memory_limit,
            sink,
        );
    }

    if let QueryOp::Sort(sort) = project.underlying.as_ref() {
        return project_over_sort(project, sort, ctx, disk, scratch, memory_limit, sink);
    }

    let schema = infer_schema(&project.underlying, ctx)?;
    let (projected, indexes) = schema.project(&project.column_name_map)?;

    let mut project_sink = |row: Row| -> Result<()> {
        sink(row.into_project(&indexes)?)?;
        Ok(())
    };

    execute_op(
        &project.underlying,
        ctx,
        disk,
        scratch,
        memory_limit,
        &mut project_sink,
    )?;
    Ok(projected)
}

fn project_over_sort<R, W>(
    project: &ProjectData,
    sort: &SortData,
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
    let reduced = reduced_projection(project, sort);
    let reduced_sort = SortData {
        sort_specs: sort.sort_specs.clone(),
        underlying: Box::new(QueryOp::Project(reduced)),
    };
    let reduced_schema = infer_schema(&QueryOp::Sort(reduced_sort.clone()), ctx)?;
    let (final_schema, final_indexes) = reduced_schema.project(&project.column_name_map)?;

    let mut final_sink = |row: Row| -> Result<()> {
        sink(row.into_project(&final_indexes)?)?;
        Ok(())
    };

    execute_sort(
        &reduced_sort,
        ctx,
        disk,
        scratch,
        memory_limit,
        &mut final_sink,
    )?;
    Ok(final_schema)
}

fn reduced_projection(project: &ProjectData, sort: &SortData) -> ProjectData {
    let mut seen = HashSet::new();
    let mut map = Vec::new();
    for (from, _) in &project.column_name_map {
        if seen.insert(from.clone()) {
            map.push((from.clone(), from.clone()));
        }
    }
    for spec in &sort.sort_specs {
        if seen.insert(spec.column_name.clone()) {
            map.push((spec.column_name.clone(), spec.column_name.clone()));
        }
    }
    ProjectData {
        column_name_map: map,
        underlying: sort.underlying.clone(),
    }
}
