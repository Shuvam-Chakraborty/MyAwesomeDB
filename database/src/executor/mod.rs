use anyhow::{Result, anyhow};
use common::{
    Data,
    query::{ComparisionOperator, ComparisionValue, Predicate, Query, QueryOp, SortSpec},
};
use db_config::DbContext;
use std::io::{Read, Write};

use crate::{
    row::{Row, Schema},
    storage::{block_allocator::ScratchSpace, disk_client::DiskClient},
};

pub mod filter;
pub mod join;
pub mod project;
pub mod scan;
pub mod sort;
mod value;

pub(crate) use crate::query_support::{
    extract_join_keys, find_table_spec, infer_schema, split_cross_predicates,
};
pub(crate) use value::{
    compare_data, compare_rows, compare_values, exact_integral_i64_from_f64, normalized_f64_bits,
};

pub struct CompiledPredicate {
    pub left_index: usize,
    pub operator: ComparisionOperator,
    pub right: PredicateOperand,
}

pub enum PredicateOperand {
    Column(usize),
    Literal(Data),
}

#[derive(Clone)]
pub struct CompiledSortSpec {
    pub index: usize,
    pub ascending: bool,
}

pub fn execute_query<R, W, F>(
    query: &Query,
    ctx: &DbContext,
    disk: &mut DiskClient<R, W>,
    scratch: &mut ScratchSpace,
    memory_limit: usize,
    mut sink: F,
) -> Result<()>
where
    R: Read,
    W: Write,
    F: FnMut(Row) -> Result<()>,
{
    execute_op(&query.root, ctx, disk, scratch, memory_limit, &mut sink).map(|_| ())
}

pub fn execute_op<R, W>(
    op: &QueryOp,
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
    match op {
        QueryOp::Scan(s) => scan::execute_scan(s, ctx, disk, scratch, memory_limit, sink),
        QueryOp::Filter(f) => filter::execute_filter(f, ctx, disk, scratch, memory_limit, sink),
        QueryOp::Project(p) => project::execute_project(p, ctx, disk, scratch, memory_limit, sink),
        QueryOp::Cross(c) => join::execute_cross(c, ctx, disk, scratch, memory_limit, sink),
        QueryOp::Sort(s) => sort::execute_sort(s, ctx, disk, scratch, memory_limit, sink),
    }
}

pub fn compile_predicates(
    schema: &Schema,
    predicates: &[Predicate],
) -> Result<Vec<CompiledPredicate>> {
    predicates
        .iter()
        .map(|p| {
            let left_index = schema
                .index_of(&p.column_name)
                .ok_or_else(|| anyhow!("unknown column: {}", p.column_name))?;
            let right = match &p.value {
                ComparisionValue::Column(name) => {
                    let idx = schema
                        .index_of(name)
                        .ok_or_else(|| anyhow!("unknown col: {name}"))?;
                    PredicateOperand::Column(idx)
                }
                ComparisionValue::I32(v) => PredicateOperand::Literal(Data::Int32(*v)),
                ComparisionValue::I64(v) => PredicateOperand::Literal(Data::Int64(*v)),
                ComparisionValue::F32(v) => PredicateOperand::Literal(Data::Float32(*v)),
                ComparisionValue::F64(v) => PredicateOperand::Literal(Data::Float64(*v)),
                ComparisionValue::String(v) => PredicateOperand::Literal(Data::String(v.clone())),
            };
            Ok(CompiledPredicate {
                left_index,
                operator: p.operator.clone(),
                right,
            })
        })
        .collect()
}

pub fn compile_sort_specs(schema: &Schema, specs: &[SortSpec]) -> Result<Vec<CompiledSortSpec>> {
    specs
        .iter()
        .map(|s| {
            let index = schema
                .index_of(&s.column_name)
                .ok_or_else(|| anyhow!("unknown sort column: {}", s.column_name))?;
            Ok(CompiledSortSpec {
                index,
                ascending: s.ascending,
            })
        })
        .collect()
}

pub fn row_satisfies_predicates(row: &Row, predicates: &[CompiledPredicate]) -> Result<bool> {
    for p in predicates {
        let left = row
            .get(p.left_index)
            .ok_or_else(|| anyhow!("bad left idx"))?;
        let right = match &p.right {
            PredicateOperand::Column(i) => row.get(*i).ok_or_else(|| anyhow!("bad right idx"))?,
            PredicateOperand::Literal(v) => v,
        };
        if !compare_data(left, &p.operator, right)? {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(crate) fn effective_memory(memory_limit: usize) -> usize {
    const MIN_RESERVE_BYTES: usize = 256 * 1024;

    if memory_limit == 0 {
        return 0;
    }

    let fractional_reserve = if memory_limit >= 64 * 1024 * 1024 {
        memory_limit / 8
    } else if memory_limit >= 16 * 1024 * 1024 {
        memory_limit / 6
    } else {
        memory_limit / 4
    };
    let adaptive_floor = (memory_limit / 16).max(MIN_RESERVE_BYTES);
    let reserve = fractional_reserve.max(adaptive_floor).min(memory_limit / 2);

    memory_limit.saturating_sub(reserve)
}

pub(crate) fn scan_batch_blocks(
    block_size: usize,
    memory_limit: usize,
    min_blocks: usize,
    max_blocks: usize,
) -> u64 {
    if block_size == 0 {
        return 1;
    }

    let effective = effective_memory(memory_limit);
    let scan_buffer_bytes = effective
        .saturating_div(8)
        .clamp(
            block_size.saturating_mul(min_blocks),
            block_size.saturating_mul(max_blocks),
        )
        .max(block_size);
    (scan_buffer_bytes / block_size).max(1) as u64
}
