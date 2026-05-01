use anyhow::{Result, anyhow};
use common::{
    Data, DataType,
    query::{ComparisionValue, FilterData, Predicate, ProjectData, QueryOp},
};
use db_config::DbContext;
use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hash, Hasher};
use std::io::{Read, Write};
use std::mem::size_of;

use super::{
    compare_values, compile_predicates, effective_memory, exact_integral_i64_from_f64, execute_op,
    extract_join_keys, infer_schema, normalized_f64_bits, row_satisfies_predicates,
    split_cross_predicates,
};
use crate::{
    estimation::{
        estimate_distinct_values, estimate_query_rows, estimate_query_rows_with_predicates,
        estimated_schema_row_bytes,
    },
    row::{Row, Schema},
    scan_pipeline::{ScanPipelineCursor, execute_scan_pipeline, try_compile_scan_pipeline},
    storage::{
        block_allocator::{ScratchRun, ScratchRunWriter, ScratchSpace, read_scratch_run_ptr},
        disk_client::DiskClient,
        row_codec::decode_row_from_bytes,
    },
};

const PARTITION_FANOUT_BITS: u32 = 3;
const PARTITION_FANOUT: usize = 1usize << PARTITION_FANOUT_BITS;
const MAX_PARTITION_DEPTH: u32 = 3;
const JOIN_FILTER_MIN_BYTES: usize = 256 * 1024;
const JOIN_FILTER_MAX_BYTES: usize = 4 * 1024 * 1024;
const JOIN_HASH_SEED: u64 = 0x517C_C1B7_2722_0A95;
const BLOOM_HASH_SEED_A: u64 = 0x9E37_79B9_7F4A_7C15;
const BLOOM_HASH_SEED_B: u64 = 0xD6E8_FDCD_DCB1_9B27;

type JoinHashMap<V> = HashMap<JoinKeyDigest, V, BuildHasherDefault<DeterministicHasher>>;

#[derive(Clone, Copy)]
struct FilteredOp<'a> {
    op: &'a QueryOp,
    predicates: &'a [Predicate],
}

impl<'a> FilteredOp<'a> {
    fn new(op: &'a QueryOp, predicates: &'a [Predicate]) -> Self {
        Self { op, predicates }
    }
}

#[derive(Clone, Copy)]
struct JoinInput<'a> {
    op: &'a QueryOp,
    predicates: &'a [Predicate],
    schema: &'a Schema,
    key_indexes: &'a [usize],
    estimated_hash_bytes: u128,
}

impl<'a> JoinInput<'a> {
    fn new(
        op: &'a QueryOp,
        predicates: &'a [Predicate],
        schema: &'a Schema,
        key_indexes: &'a [usize],
        estimated_hash_bytes: u128,
    ) -> Self {
        Self {
            op,
            predicates,
            schema,
            key_indexes,
            estimated_hash_bytes,
        }
    }
}

#[derive(Default)]
struct DeterministicHasher {
    state: u64,
}

impl Hasher for DeterministicHasher {
    fn finish(&self) -> u64 {
        avalanche(self.state)
    }

    fn write(&mut self, bytes: &[u8]) {
        let mut chunks = bytes.chunks_exact(8);
        for chunk in &mut chunks {
            let word = u64::from_le_bytes([
                chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6], chunk[7],
            ]);
            self.write_u64(word);
        }

        let remainder = chunks.remainder();
        if !remainder.is_empty() {
            let mut tail = [0u8; 8];
            tail[..remainder.len()].copy_from_slice(remainder);
            self.write_u64(u64::from_le_bytes(tail) ^ remainder.len() as u64);
        }
    }

    fn write_u8(&mut self, value: u8) {
        self.write_u64(value as u64);
    }

    fn write_u64(&mut self, value: u64) {
        self.state = mix_hash_word(self.state, value);
    }

    fn write_usize(&mut self, value: usize) {
        self.write_u64(value as u64);
    }

    fn write_i64(&mut self, value: i64) {
        self.write_u64(value as u64);
    }
}

fn mix_hash_word(state: u64, value: u64) -> u64 {
    avalanche(
        state
            .wrapping_add(JOIN_HASH_SEED)
            .wrapping_add(value.wrapping_mul(BLOOM_HASH_SEED_A)),
    )
}

fn avalanche(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94D0_49BB_1331_11EB);
    value ^ (value >> 31)
}

pub fn execute_filter<R, W>(
    filter: &FilterData,
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
    if let Some(schema) = try_execute_join_filter(filter, ctx, disk, scratch, memory_limit, sink)? {
        return Ok(schema);
    }

    if let Some(schema) = try_execute_scan_pipeline_op(
        &QueryOp::Filter(filter.clone()),
        ctx,
        disk,
        memory_limit,
        sink,
    )? {
        return Ok(schema);
    }

    let schema = infer_schema(&filter.underlying, ctx)?;
    let predicates = compile_predicates(&schema, &filter.predicates)?;

    let mut filter_sink = |row: Row| -> Result<()> {
        if row_satisfies_predicates(&row, &predicates)? {
            sink(row)?;
        }
        Ok(())
    };

    execute_op(
        &filter.underlying,
        ctx,
        disk,
        scratch,
        memory_limit,
        &mut filter_sink,
    )?;
    Ok(schema)
}

fn try_execute_scan_pipeline_op<R, W>(
    op: &QueryOp,
    ctx: &DbContext,
    disk: &mut DiskClient<R, W>,
    memory_limit: usize,
    sink: &mut dyn FnMut(Row) -> Result<()>,
) -> Result<Option<Schema>>
where
    R: Read,
    W: Write,
{
    let Some(plan) = try_compile_scan_pipeline(op, ctx)? else {
        return Ok(None);
    };

    let schema = plan.schema().clone();
    execute_scan_pipeline(&plan, disk, memory_limit, sink)?;
    Ok(Some(schema))
}

fn try_execute_join_filter<R, W>(
    filter: &FilterData,
    ctx: &DbContext,
    disk: &mut DiskClient<R, W>,
    scratch: &mut ScratchSpace,
    memory_limit: usize,
    sink: &mut dyn FnMut(Row) -> Result<()>,
) -> Result<Option<Schema>>
where
    R: Read,
    W: Write,
{
    try_execute_wrapped_join_filter(
        filter.underlying.as_ref(),
        &filter.predicates,
        ctx,
        disk,
        scratch,
        memory_limit,
        sink,
    )
}

fn try_execute_wrapped_join_filter<R, W>(
    op: &QueryOp,
    predicates: &[Predicate],
    ctx: &DbContext,
    disk: &mut DiskClient<R, W>,
    scratch: &mut ScratchSpace,
    memory_limit: usize,
    sink: &mut dyn FnMut(Row) -> Result<()>,
) -> Result<Option<Schema>>
where
    R: Read,
    W: Write,
{
    match op {
        QueryOp::Cross(_) => Ok(Some(execute_join_subtree(
            op,
            predicates,
            ctx,
            disk,
            scratch,
            memory_limit,
            sink,
        )?)),
        QueryOp::Project(project) => {
            let Some(remapped_predicates) = remap_project_predicates(predicates, project) else {
                return Ok(None);
            };

            let child_schema = infer_schema(&project.underlying, ctx)?;
            let (projected_schema, indexes) = child_schema.project(&project.column_name_map)?;
            let mut project_sink = |row: Row| -> Result<()> {
                sink(row.into_project(&indexes)?)?;
                Ok(())
            };

            let executed = try_execute_wrapped_join_filter(
                project.underlying.as_ref(),
                &remapped_predicates,
                ctx,
                disk,
                scratch,
                memory_limit,
                &mut project_sink,
            )?;

            Ok(executed.map(|_| projected_schema))
        }
        _ => Ok(None),
    }
}

fn execute_join_subtree<R, W>(
    op: &QueryOp,
    predicates: &[Predicate],
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
        QueryOp::Cross(cross) => {
            execute_cross_subtree(cross, predicates, ctx, disk, scratch, memory_limit, sink)
        }
        other => {
            let filtered = wrap_filter_if_needed(predicates.to_vec(), other.clone());
            execute_op(&filtered, ctx, disk, scratch, memory_limit, sink)
        }
    }
}

fn execute_cross_subtree<R, W>(
    cross: &common::query::CrossData,
    predicates: &[Predicate],
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
    let left_schema = infer_schema(&cross.left, ctx)?;
    let right_schema = infer_schema(&cross.right, ctx)?;
    let (left_preds, right_preds, cross_preds) =
        split_cross_predicates(predicates, &left_schema, &right_schema);
    let output_schema = Schema::combine(&left_schema, &right_schema);
    let compiled_cross = compile_predicates(&output_schema, &cross_preds)?;

    if let Some((left_keys, right_keys)) =
        extract_join_keys(&cross_preds, &left_schema, &right_schema)?
        && try_execute_ordered_unique_merge_join(
            cross.left.as_ref(),
            &left_preds,
            &left_schema,
            &left_keys,
            cross.right.as_ref(),
            &right_preds,
            &right_schema,
            &right_keys,
            &compiled_cross,
            ctx,
            disk,
            memory_limit,
            sink,
        )?
    {
        return Ok(output_schema);
    }

    let left_cost = estimated_build_cost(cross.left.as_ref(), &left_schema, &left_preds, ctx);
    let right_cost = estimated_build_cost(cross.right.as_ref(), &right_schema, &right_preds, ctx);
    let left_hash_cost =
        estimated_direct_hash_build_cost(cross.left.as_ref(), &left_schema, &left_preds, ctx);
    let right_hash_cost =
        estimated_direct_hash_build_cost(cross.right.as_ref(), &right_schema, &right_preds, ctx);
    let build_left = left_cost <= right_cost;

    if let Some((left_keys, right_keys)) =
        extract_join_keys(&cross_preds, &left_schema, &right_schema)?
    {
        let left_input = JoinInput::new(
            cross.left.as_ref(),
            &left_preds,
            &left_schema,
            &left_keys,
            left_hash_cost,
        );
        let right_input = JoinInput::new(
            cross.right.as_ref(),
            &right_preds,
            &right_schema,
            &right_keys,
            right_hash_cost,
        );
        if build_left {
            execute_equality_join(
                left_input,
                right_input,
                &compiled_cross,
                ctx,
                disk,
                scratch,
                memory_limit,
                true,
                sink,
            )?;
        } else {
            execute_equality_join(
                right_input,
                left_input,
                &compiled_cross,
                ctx,
                disk,
                scratch,
                memory_limit,
                false,
                sink,
            )?;
        }
    } else {
        let left_input = FilteredOp::new(cross.left.as_ref(), &left_preds);
        let right_input = FilteredOp::new(cross.right.as_ref(), &right_preds);
        if build_left {
            execute_materialized_join(
                left_input,
                right_input,
                &compiled_cross,
                ctx,
                disk,
                scratch,
                memory_limit,
                true,
                sink,
            )?;
        } else {
            execute_materialized_join(
                right_input,
                left_input,
                &compiled_cross,
                ctx,
                disk,
                scratch,
                memory_limit,
                false,
                sink,
            )?;
        }
    }

    Ok(output_schema)
}

#[allow(clippy::too_many_arguments)]
fn try_execute_ordered_unique_merge_join<R, W>(
    left_op: &QueryOp,
    left_preds: &[Predicate],
    left_schema: &Schema,
    left_keys: &[usize],
    right_op: &QueryOp,
    right_preds: &[Predicate],
    right_schema: &Schema,
    right_keys: &[usize],
    compiled_cross: &[super::CompiledPredicate],
    ctx: &DbContext,
    disk: &mut DiskClient<R, W>,
    memory_limit: usize,
    sink: &mut dyn FnMut(Row) -> Result<()>,
) -> Result<bool>
where
    R: Read,
    W: Write,
{
    if left_keys.len() != 1 || right_keys.len() != 1 {
        return Ok(false);
    }

    let left_wrapped = wrap_filter_if_needed(left_preds.to_vec(), left_op.clone());
    let right_wrapped = wrap_filter_if_needed(right_preds.to_vec(), right_op.clone());
    let Some(left_plan) = try_compile_scan_pipeline(&left_wrapped, ctx)? else {
        return Ok(false);
    };
    let Some(right_plan) = try_compile_scan_pipeline(&right_wrapped, ctx)? else {
        return Ok(false);
    };

    let left_key = left_keys[0];
    let right_key = right_keys[0];
    if !left_plan.output_physically_ordered(left_key)
        || !right_plan.output_physically_ordered(right_key)
    {
        return Ok(false);
    }

    let left_unique = is_estimated_unique_key(&left_wrapped, left_schema, left_key, ctx);
    let right_unique = is_estimated_unique_key(&right_wrapped, right_schema, right_key, ctx);
    if !left_unique && !right_unique {
        return Ok(false);
    }

    execute_ordered_merge_join(
        ScanPipelineCursor::new(left_plan, disk, memory_limit)?,
        left_key,
        ScanPipelineCursor::new(right_plan, disk, memory_limit)?,
        right_key,
        compiled_cross,
        disk,
        sink,
    )?;

    Ok(true)
}

fn is_estimated_unique_key(
    op: &QueryOp,
    schema: &Schema,
    key_index: usize,
    ctx: &DbContext,
) -> bool {
    let Some(column) = schema.column_at(key_index) else {
        return false;
    };
    let rows = estimate_query_rows(op, ctx).max(1);
    let Some(distinct) = estimate_distinct_values(op, &column.name, ctx) else {
        return false;
    };
    distinct >= rows
}

fn execute_ordered_merge_join<R, W>(
    mut left_cursor: ScanPipelineCursor,
    left_key: usize,
    mut right_cursor: ScanPipelineCursor,
    right_key: usize,
    compiled_cross: &[super::CompiledPredicate],
    disk: &mut DiskClient<R, W>,
    sink: &mut dyn FnMut(Row) -> Result<()>,
) -> Result<()>
where
    R: Read,
    W: Write,
{
    let mut left_row = left_cursor.next_row(disk)?;
    let mut right_row = right_cursor.next_row(disk)?;

    while left_row.is_some() && right_row.is_some() {
        let ordering = {
            let left = left_row
                .as_ref()
                .and_then(|row| row.get(left_key))
                .ok_or_else(|| anyhow!("missing ordered merge left key"))?;
            let right = right_row
                .as_ref()
                .and_then(|row| row.get(right_key))
                .ok_or_else(|| anyhow!("missing ordered merge right key"))?;
            compare_values(left, right)?
        };

        match ordering {
            std::cmp::Ordering::Less => {
                left_row = left_cursor.next_row(disk)?;
            }
            std::cmp::Ordering::Greater => {
                right_row = right_cursor.next_row(disk)?;
            }
            std::cmp::Ordering::Equal => {
                let join_key = left_row
                    .as_ref()
                    .and_then(|row| row.get(left_key))
                    .ok_or_else(|| anyhow!("missing ordered merge join key"))?
                    .clone();
                let mut left_group = Vec::new();
                loop {
                    let Some(current_left) = left_row.take() else {
                        break;
                    };
                    left_group.push(current_left);
                    left_row = left_cursor.next_row(disk)?;
                    let Some(next_left) = left_row.as_ref() else {
                        break;
                    };
                    let next_key = next_left
                        .get(left_key)
                        .ok_or_else(|| anyhow!("missing ordered merge left key"))?;
                    if compare_values(next_key, &join_key)? != std::cmp::Ordering::Equal {
                        break;
                    }
                }

                let mut right_group = Vec::new();
                loop {
                    let Some(current_right) = right_row.take() else {
                        break;
                    };
                    right_group.push(current_right);
                    right_row = right_cursor.next_row(disk)?;
                    let Some(next_right) = right_row.as_ref() else {
                        break;
                    };
                    let next_key = next_right
                        .get(right_key)
                        .ok_or_else(|| anyhow!("missing ordered merge right key"))?;
                    if compare_values(next_key, &join_key)? != std::cmp::Ordering::Equal {
                        break;
                    }
                }

                for left_match in &left_group {
                    for right_match in &right_group {
                        let joined = Row::combine(left_match, right_match);
                        if row_satisfies_predicates(&joined, compiled_cross)? {
                            sink(joined)?;
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

fn execute_equality_join<R, W>(
    build: JoinInput<'_>,
    probe: JoinInput<'_>,
    compiled_cross: &[super::CompiledPredicate],
    ctx: &DbContext,
    disk: &mut DiskClient<R, W>,
    scratch: &mut ScratchSpace,
    memory_limit: usize,
    build_is_left: bool,
    sink: &mut dyn FnMut(Row) -> Result<()>,
) -> Result<()>
where
    R: Read,
    W: Write,
{
    if should_try_direct_hash_build(build.estimated_hash_bytes, memory_limit)
        && let Some(build_rows) = try_build_hash_rows(
            build.op,
            build.predicates,
            build.key_indexes,
            ctx,
            disk,
            scratch,
            memory_limit,
            hash_build_budget(memory_limit),
        )?
    {
        execute_probe_subtree_against_hash(
            probe.op,
            probe.predicates,
            probe.key_indexes,
            &build_rows,
            compiled_cross,
            ctx,
            disk,
            scratch,
            memory_limit,
            build_is_left,
            sink,
        )?;
        return Ok(());
    }

    if should_try_direct_hash_build(probe.estimated_hash_bytes, memory_limit)
        && let Some(build_rows) = try_build_hash_rows(
            probe.op,
            probe.predicates,
            probe.key_indexes,
            ctx,
            disk,
            scratch,
            memory_limit,
            hash_build_budget(memory_limit),
        )?
    {
        execute_probe_subtree_against_hash(
            build.op,
            build.predicates,
            build.key_indexes,
            &build_rows,
            compiled_cross,
            ctx,
            disk,
            scratch,
            memory_limit,
            !build_is_left,
            sink,
        )?;
        return Ok(());
    }

    execute_partitioned_hash_join_from_subtrees(
        build,
        probe,
        compiled_cross,
        ctx,
        disk,
        scratch,
        memory_limit,
        build_is_left,
        sink,
    )
}

fn estimated_build_cost(
    op: &QueryOp,
    schema: &Schema,
    predicates: &[Predicate],
    ctx: &DbContext,
) -> u128 {
    (estimate_query_rows_with_predicates(op, predicates, ctx) as u128)
        .saturating_mul(estimated_schema_row_bytes(schema) as u128)
}

fn estimated_direct_hash_build_cost(
    op: &QueryOp,
    schema: &Schema,
    predicates: &[Predicate],
    ctx: &DbContext,
) -> u128 {
    (estimate_query_rows_with_predicates(op, predicates, ctx) as u128)
        .saturating_mul(estimated_hash_row_bytes(schema) as u128)
}

fn estimated_hash_row_bytes(schema: &Schema) -> usize {
    schema
        .columns()
        .iter()
        .map(|column| match column.data_type {
            DataType::Int32 | DataType::Float32 => 4,
            DataType::Int64 | DataType::Float64 => 8,
            // Hash joins store compact encoded rows instead of full Row/Data
            // allocations, so a serialized-width estimate is closer than the
            // generic in-memory row estimate used for plan costing.
            DataType::String => 64,
        })
        .sum::<usize>()
        .saturating_add(size_of::<Box<[u8]>>())
        .saturating_add(16)
        .max(1)
}

fn should_try_direct_hash_build(estimated_build_bytes: u128, memory_limit: usize) -> bool {
    let budget = hash_build_budget(memory_limit) as u128;
    // The runtime builder enforces this budget while collecting rows. Using
    // the full budget here avoids partitioning joins that could have stayed
    // in memory.
    estimated_build_bytes <= budget
}

fn execute_partitioned_hash_join_from_subtrees<R, W>(
    build: JoinInput<'_>,
    probe: JoinInput<'_>,
    compiled_cross: &[super::CompiledPredicate],
    ctx: &DbContext,
    disk: &mut DiskClient<R, W>,
    scratch: &mut ScratchSpace,
    memory_limit: usize,
    build_is_left: bool,
    sink: &mut dyn FnMut(Row) -> Result<()>,
) -> Result<()>
where
    R: Read,
    W: Write,
{
    if !supports_hybrid_partitioning(build.op) || !supports_hybrid_partitioning(probe.op) {
        let mut join_filter = JoinBloomFilter::new(join_filter_budget(memory_limit));
        let mut build_partitions = partition_subtree_rows(
            build.op,
            build.predicates,
            build.key_indexes,
            build.schema,
            ctx,
            disk,
            scratch,
            memory_limit,
            0,
            Some(&mut join_filter),
            None,
        )?;
        let mut probe_partitions = partition_subtree_rows(
            probe.op,
            probe.predicates,
            probe.key_indexes,
            probe.schema,
            ctx,
            disk,
            scratch,
            memory_limit,
            0,
            None,
            Some(&join_filter),
        )?;

        let join_result = execute_partitioned_hash_join_from_runs(
            &mut build_partitions,
            build.key_indexes,
            &mut probe_partitions,
            probe.key_indexes,
            compiled_cross,
            disk,
            scratch,
            memory_limit,
            build_is_left,
            0,
            sink,
        );

        build_partitions.release(scratch);
        probe_partitions.release(scratch);
        return join_result;
    }

    let disk_ptr = disk as *mut DiskClient<R, W>;
    let scratch_ptr = scratch as *mut ScratchSpace;
    let block_size = scratch.block_size();
    let resident_budget = hybrid_hash_resident_budget(memory_limit);
    let mut join_filter = JoinBloomFilter::new(join_filter_budget(memory_limit));
    let mut resident_build_partitions = std::iter::repeat_with(HybridBuildPartition::new)
        .take(PARTITION_FANOUT)
        .collect::<Vec<_>>();
    let mut build_writers: Vec<Option<ScratchRunWriter<R, W>>> = std::iter::repeat_with(|| None)
        .take(PARTITION_FANOUT)
        .collect();
    let mut total_resident_bytes = 0usize;

    {
        let mut collect_build = |row: Row| -> Result<()> {
            let key = join_key_digest_for_row(&row, build.key_indexes)?;
            join_filter.insert(&key);

            let partition_index = partition_index_for_digest(key, 0);
            let partition = &mut resident_build_partitions[partition_index];
            if partition.spilled {
                let writer = build_writers[partition_index].get_or_insert_with(|| {
                    ScratchRunWriter::new(disk_ptr, scratch_ptr, block_size)
                });
                return writer.push_row(&row);
            }

            let encoded = encode_row_to_boxed(&row)?;
            let previous_used = partition.used_bytes;
            let partition_budget = resident_budget
                .saturating_sub(total_resident_bytes.saturating_sub(previous_used))
                .max(previous_used);
            match insert_encoded_build_row(
                &mut partition.rows_by_key,
                key,
                encoded,
                &mut partition.used_bytes,
                partition_budget,
            ) {
                Ok(()) => {
                    let delta = partition.used_bytes.saturating_sub(previous_used);
                    if total_resident_bytes.saturating_add(delta) > resident_budget {
                        let writer = build_writers[partition_index].get_or_insert_with(|| {
                            ScratchRunWriter::new(disk_ptr, scratch_ptr, block_size)
                        });
                        flush_hybrid_build_partition(partition, writer)?;
                        partition.spilled = true;
                        total_resident_bytes = total_resident_bytes.saturating_sub(previous_used);
                    } else {
                        total_resident_bytes = total_resident_bytes.saturating_add(delta);
                    }
                    Ok(())
                }
                Err(err) if err.downcast_ref::<HashBuildBudgetExceeded>().is_some() => {
                    if previous_used > 0 {
                        let writer = build_writers[partition_index].get_or_insert_with(|| {
                            ScratchRunWriter::new(disk_ptr, scratch_ptr, block_size)
                        });
                        flush_hybrid_build_partition(partition, writer)?;
                        total_resident_bytes = total_resident_bytes.saturating_sub(previous_used);
                    }
                    partition.spilled = true;
                    let writer = build_writers[partition_index].get_or_insert_with(|| {
                        ScratchRunWriter::new(disk_ptr, scratch_ptr, block_size)
                    });
                    writer.push_row(&row)
                }
                Err(err) => Err(err),
            }
        };
        execute_join_subtree(
            build.op,
            build.predicates,
            ctx,
            disk,
            scratch,
            memory_limit,
            &mut collect_build,
        )?;
    }

    let mut probe_writers: Vec<Option<ScratchRunWriter<R, W>>> = std::iter::repeat_with(|| None)
        .take(PARTITION_FANOUT)
        .collect();

    {
        let mut collect_probe = |probe_row: Row| -> Result<()> {
            let key = join_key_digest_for_row(&probe_row, probe.key_indexes)?;
            let partition_index = partition_index_for_digest(key, 0);
            let partition = &resident_build_partitions[partition_index];

            if partition.spilled {
                if !join_filter.may_contain(&key) {
                    return Ok(());
                }

                let writer = probe_writers[partition_index].get_or_insert_with(|| {
                    ScratchRunWriter::new(disk_ptr, scratch_ptr, block_size)
                });
                return writer.push_row(&probe_row);
            }

            emit_hash_join_matches_for_digest(
                key,
                &probe_row,
                &partition.rows_by_key,
                build.schema,
                compiled_cross,
                build_is_left,
                sink,
            )
        };
        execute_join_subtree(
            probe.op,
            probe.predicates,
            ctx,
            disk,
            scratch,
            memory_limit,
            &mut collect_probe,
        )?;
    }

    drop(join_filter);
    drop(resident_build_partitions);

    let mut build_partitions = finish_partition_writers(build.schema.clone(), &mut build_writers)?;
    let mut probe_partitions = finish_partition_writers(probe.schema.clone(), &mut probe_writers)?;

    let join_result = execute_partitioned_hash_join_from_runs(
        &mut build_partitions,
        build.key_indexes,
        &mut probe_partitions,
        probe.key_indexes,
        compiled_cross,
        disk,
        scratch,
        memory_limit,
        build_is_left,
        0,
        sink,
    );

    build_partitions.release(scratch);
    probe_partitions.release(scratch);
    join_result
}

fn supports_hybrid_partitioning(op: &QueryOp) -> bool {
    match op {
        QueryOp::Scan(_) => true,
        QueryOp::Filter(filter) => supports_hybrid_partitioning(filter.underlying.as_ref()),
        QueryOp::Project(project) => supports_hybrid_partitioning(project.underlying.as_ref()),
        QueryOp::Cross(_) | QueryOp::Sort(_) => false,
    }
}

#[allow(clippy::too_many_arguments)]
fn execute_partitioned_hash_join_from_runs<R, W>(
    build_partitions: &mut PartitionedRuns,
    build_keys: &[usize],
    probe_partitions: &mut PartitionedRuns,
    probe_keys: &[usize],
    compiled_cross: &[super::CompiledPredicate],
    disk: &mut DiskClient<R, W>,
    scratch: &mut ScratchSpace,
    memory_limit: usize,
    build_is_left: bool,
    depth: u32,
    sink: &mut dyn FnMut(Row) -> Result<()>,
) -> Result<()>
where
    R: Read,
    W: Write,
{
    for partition_index in 0..PARTITION_FANOUT {
        let build_run = &build_partitions.runs[partition_index];
        let probe_run = &probe_partitions.runs[partition_index];
        if build_run.block_ids().is_empty() || probe_run.block_ids().is_empty() {
            continue;
        }

        let hash_budget = hash_build_budget(memory_limit);
        let build_blocks = build_run.block_ids().len();
        let probe_blocks = probe_run.block_ids().len();

        if build_blocks <= probe_blocks {
            if let Some(build_rows) = try_build_hash_run(
                build_run,
                &build_partitions.schema,
                build_keys,
                disk,
                hash_budget,
            )? {
                execute_probe_run_against_hash(
                    probe_run,
                    &probe_partitions.schema,
                    probe_keys,
                    &build_rows,
                    compiled_cross,
                    disk,
                    build_is_left,
                    sink,
                )?;
                continue;
            }
        } else if let Some(build_rows) = try_build_hash_run(
            probe_run,
            &probe_partitions.schema,
            probe_keys,
            disk,
            hash_budget,
        )? {
            execute_probe_run_against_hash(
                build_run,
                &build_partitions.schema,
                build_keys,
                &build_rows,
                compiled_cross,
                disk,
                !build_is_left,
                sink,
            )?;
            continue;
        }

        if depth < MAX_PARTITION_DEPTH {
            let mut join_filter = JoinBloomFilter::new(join_filter_budget(memory_limit));
            let mut next_build = partition_run_rows(
                build_run,
                &build_partitions.schema,
                build_keys,
                disk,
                scratch,
                depth + 1,
                Some(&mut join_filter),
                None,
            )?;
            let mut next_probe = partition_run_rows(
                probe_run,
                &probe_partitions.schema,
                probe_keys,
                disk,
                scratch,
                depth + 1,
                None,
                Some(&join_filter),
            )?;

            if next_build.non_empty_partitions > 1 || next_probe.non_empty_partitions > 1 {
                let result = execute_partitioned_hash_join_from_runs(
                    &mut next_build,
                    build_keys,
                    &mut next_probe,
                    probe_keys,
                    compiled_cross,
                    disk,
                    scratch,
                    memory_limit,
                    build_is_left,
                    depth + 1,
                    sink,
                );
                next_build.release(scratch);
                next_probe.release(scratch);
                result?;
                continue;
            }

            next_build.release(scratch);
            next_probe.release(scratch);
        }

        execute_nested_loop_join_on_runs(
            build_run,
            &build_partitions.schema,
            probe_run,
            &probe_partitions.schema,
            compiled_cross,
            disk,
            build_is_left,
            sink,
        )?;
    }

    Ok(())
}

fn execute_materialized_join<R, W>(
    build: FilteredOp<'_>,
    probe: FilteredOp<'_>,
    compiled_cross: &[super::CompiledPredicate],
    ctx: &DbContext,
    disk: &mut DiskClient<R, W>,
    scratch: &mut ScratchSpace,
    memory_limit: usize,
    build_is_left: bool,
    sink: &mut dyn FnMut(Row) -> Result<()>,
) -> Result<()>
where
    R: Read,
    W: Write,
{
    let budget = materialization_budget(memory_limit, scratch.block_size());
    let mut build_rows = materialize_subtree_rows(
        build.op,
        build.predicates,
        ctx,
        disk,
        scratch,
        memory_limit,
        budget,
    )?;
    let disk_ptr = disk as *mut DiskClient<R, W>;

    {
        let mut probe_sink = |probe_row: Row| -> Result<()> {
            let mut emit_match = |build_row: &Row| -> Result<()> {
                let joined = if build_is_left {
                    Row::combine(build_row, &probe_row)
                } else {
                    Row::combine(&probe_row, build_row)
                };
                if row_satisfies_predicates(&joined, compiled_cross)? {
                    sink(joined)?;
                }
                Ok(())
            };

            build_rows.for_each(disk_ptr, &mut emit_match)
        };

        execute_join_subtree(
            probe.op,
            probe.predicates,
            ctx,
            disk,
            scratch,
            memory_limit,
            &mut probe_sink,
        )?;
    }

    build_rows.release(scratch);
    Ok(())
}

fn materialize_subtree_rows<R, W>(
    op: &QueryOp,
    predicates: &[Predicate],
    ctx: &DbContext,
    disk: &mut DiskClient<R, W>,
    scratch: &mut ScratchSpace,
    memory_limit: usize,
    budget: usize,
) -> Result<MaterializedRows>
where
    R: Read,
    W: Write,
{
    let schema = infer_schema(op, ctx)?;
    let mut rows = Vec::new();
    let mut bytes = 0usize;
    let mut writer: Option<ScratchRunWriter<R, W>> = None;
    let disk_ptr = disk as *mut DiskClient<R, W>;
    let scratch_ptr = scratch as *mut ScratchSpace;
    let block_size = scratch.block_size();

    {
        let mut collect = |row: Row| -> Result<()> {
            if let Some(writer) = writer.as_mut() {
                return writer.push_row(&row);
            }

            let row_bytes = row.estimated_heap_bytes();
            if bytes.saturating_add(row_bytes) <= budget {
                bytes = bytes.saturating_add(row_bytes);
                rows.push(row);
            } else {
                let mut spill_writer = ScratchRunWriter::new(disk_ptr, scratch_ptr, block_size);
                for buffered in &rows {
                    spill_writer.push_row(buffered)?;
                }
                // Once we spill, drop the buffered rows instead of keeping an
                // oversized empty Vec alive during the probe phase.
                rows = Vec::new();
                bytes = 0;
                spill_writer.push_row(&row)?;
                writer = Some(spill_writer);
            }
            Ok(())
        };
        execute_join_subtree(
            op,
            predicates,
            ctx,
            disk,
            scratch,
            memory_limit,
            &mut collect,
        )?;
    }

    let run = if let Some(writer) = writer {
        Some(writer.finish()?)
    } else {
        None
    };

    Ok(MaterializedRows { schema, rows, run })
}

fn try_build_hash_rows<R, W>(
    op: &QueryOp,
    predicates: &[Predicate],
    key_indexes: &[usize],
    ctx: &DbContext,
    disk: &mut DiskClient<R, W>,
    scratch: &mut ScratchSpace,
    memory_limit: usize,
    budget: usize,
) -> Result<Option<CompactHashRows>>
where
    R: Read,
    W: Write,
{
    let schema = infer_schema(op, ctx)?;
    let mut build_rows: JoinHashMap<Vec<Box<[u8]>>> = JoinHashMap::default();
    let mut used_bytes = 0usize;

    let result = {
        let mut collect = |row: Row| -> Result<()> {
            let key = join_key_digest_for_row(&row, key_indexes)?;
            insert_build_row(&mut build_rows, key, row, &mut used_bytes, budget)
        };

        execute_join_subtree(
            op,
            predicates,
            ctx,
            disk,
            scratch,
            memory_limit,
            &mut collect,
        )
    };

    match result {
        Ok(_) => Ok(Some(CompactHashRows {
            schema,
            rows_by_key: build_rows,
        })),
        Err(err) if err.downcast_ref::<HashBuildBudgetExceeded>().is_some() => Ok(None),
        Err(err) => Err(err),
    }
}

fn try_build_hash_run<R, W>(
    run: &ScratchRun,
    schema: &Schema,
    key_indexes: &[usize],
    disk: &mut DiskClient<R, W>,
    budget: usize,
) -> Result<Option<CompactHashRows>>
where
    R: Read,
    W: Write,
{
    let mut build_rows: JoinHashMap<Vec<Box<[u8]>>> = JoinHashMap::default();
    let mut used_bytes = 0usize;
    let disk_ptr = disk as *mut DiskClient<R, W>;

    let result = {
        let mut collect = |row: Row| -> Result<()> {
            let key = join_key_digest_for_row(&row, key_indexes)?;
            insert_build_row(&mut build_rows, key, row, &mut used_bytes, budget)
        };
        read_scratch_run_ptr(run, schema, disk_ptr, &mut collect)
    };

    match result {
        Ok(_) => Ok(Some(CompactHashRows {
            schema: schema.clone(),
            rows_by_key: build_rows,
        })),
        Err(err) if err.downcast_ref::<HashBuildBudgetExceeded>().is_some() => Ok(None),
        Err(err) => Err(err),
    }
}

struct CompactHashRows {
    schema: Schema,
    rows_by_key: JoinHashMap<Vec<Box<[u8]>>>,
}

struct MaterializedRows {
    schema: Schema,
    rows: Vec<Row>,
    run: Option<ScratchRun>,
}

struct PartitionedRuns {
    schema: Schema,
    runs: Vec<ScratchRun>,
    non_empty_partitions: usize,
}

struct HybridBuildPartition {
    rows_by_key: JoinHashMap<Vec<Box<[u8]>>>,
    used_bytes: usize,
    spilled: bool,
}

impl MaterializedRows {
    fn for_each<R, W>(
        &self,
        disk: *mut DiskClient<R, W>,
        sink: &mut dyn FnMut(&Row) -> Result<()>,
    ) -> Result<()>
    where
        R: Read,
        W: Write,
    {
        if let Some(run) = &self.run {
            let mut emit = |row: Row| sink(&row);
            read_scratch_run_ptr(run, &self.schema, disk, &mut emit)
        } else {
            for row in &self.rows {
                sink(row)?;
            }
            Ok(())
        }
    }

    fn release(&mut self, scratch: &mut ScratchSpace) {
        if let Some(run) = self.run.take() {
            scratch.release_run(run);
        }
    }
}

impl PartitionedRuns {
    fn release(&mut self, scratch: &mut ScratchSpace) {
        let runs = std::mem::take(&mut self.runs);
        for run in runs {
            scratch.release_run(run);
        }
    }
}

impl HybridBuildPartition {
    fn new() -> Self {
        Self {
            rows_by_key: JoinHashMap::default(),
            used_bytes: 0,
            spilled: false,
        }
    }
}

fn hash_build_budget(memory_limit: usize) -> usize {
    effective_memory(memory_limit).saturating_div(2).max(1)
}

fn hybrid_hash_resident_budget(memory_limit: usize) -> usize {
    hash_build_budget(memory_limit).saturating_div(2).max(1)
}

fn materialization_budget(memory_limit: usize, block_size: usize) -> usize {
    effective_memory(memory_limit)
        .saturating_div(2)
        .max(block_size.max(1))
}

fn flush_hybrid_build_partition<R, W>(
    partition: &mut HybridBuildPartition,
    writer: &mut ScratchRunWriter<R, W>,
) -> Result<()>
where
    R: Read,
    W: Write,
{
    let rows_by_key = std::mem::take(&mut partition.rows_by_key);
    for encoded_rows in rows_by_key.into_values() {
        for encoded in encoded_rows {
            writer.push_encoded_row(&encoded)?;
        }
    }
    partition.used_bytes = 0;
    Ok(())
}

fn execute_probe_subtree_against_hash<R, W>(
    probe_op: &QueryOp,
    probe_preds: &[Predicate],
    probe_keys: &[usize],
    build_rows: &CompactHashRows,
    compiled_cross: &[super::CompiledPredicate],
    ctx: &DbContext,
    disk: &mut DiskClient<R, W>,
    scratch: &mut ScratchSpace,
    memory_limit: usize,
    build_is_left: bool,
    sink: &mut dyn FnMut(Row) -> Result<()>,
) -> Result<()>
where
    R: Read,
    W: Write,
{
    let mut probe_sink = |probe_row: Row| -> Result<()> {
        emit_hash_join_matches(
            &probe_row,
            probe_keys,
            build_rows,
            compiled_cross,
            build_is_left,
            sink,
        )
    };

    execute_join_subtree(
        probe_op,
        probe_preds,
        ctx,
        disk,
        scratch,
        memory_limit,
        &mut probe_sink,
    )?;
    Ok(())
}

fn execute_probe_run_against_hash<R, W>(
    probe_run: &ScratchRun,
    probe_schema: &Schema,
    probe_keys: &[usize],
    build_rows: &CompactHashRows,
    compiled_cross: &[super::CompiledPredicate],
    disk: &mut DiskClient<R, W>,
    build_is_left: bool,
    sink: &mut dyn FnMut(Row) -> Result<()>,
) -> Result<()>
where
    R: Read,
    W: Write,
{
    let disk_ptr = disk as *mut DiskClient<R, W>;
    let mut probe_sink = |probe_row: Row| -> Result<()> {
        emit_hash_join_matches(
            &probe_row,
            probe_keys,
            build_rows,
            compiled_cross,
            build_is_left,
            sink,
        )
    };
    read_scratch_run_ptr(probe_run, probe_schema, disk_ptr, &mut probe_sink)
}

fn emit_hash_join_matches(
    probe_row: &Row,
    probe_keys: &[usize],
    build_rows: &CompactHashRows,
    compiled_cross: &[super::CompiledPredicate],
    build_is_left: bool,
    sink: &mut dyn FnMut(Row) -> Result<()>,
) -> Result<()> {
    let key = join_key_digest_for_row(probe_row, probe_keys)?;
    emit_hash_join_matches_for_digest(
        key,
        probe_row,
        &build_rows.rows_by_key,
        &build_rows.schema,
        compiled_cross,
        build_is_left,
        sink,
    )
}

fn emit_hash_join_matches_for_digest(
    key: JoinKeyDigest,
    probe_row: &Row,
    build_rows: &JoinHashMap<Vec<Box<[u8]>>>,
    build_schema: &Schema,
    compiled_cross: &[super::CompiledPredicate],
    build_is_left: bool,
    sink: &mut dyn FnMut(Row) -> Result<()>,
) -> Result<()> {
    if let Some(matches) = build_rows.get(&key) {
        for build_row in matches {
            let build_row = decode_row_from_bytes(build_row, build_schema)?;
            let joined = if build_is_left {
                Row::combine(&build_row, probe_row)
            } else {
                Row::combine(probe_row, &build_row)
            };
            if row_satisfies_predicates(&joined, compiled_cross)? {
                sink(joined)?;
            }
        }
    }
    Ok(())
}

fn execute_nested_loop_join_on_runs<R, W>(
    build_run: &ScratchRun,
    build_schema: &Schema,
    probe_run: &ScratchRun,
    probe_schema: &Schema,
    compiled_cross: &[super::CompiledPredicate],
    disk: &mut DiskClient<R, W>,
    build_is_left: bool,
    sink: &mut dyn FnMut(Row) -> Result<()>,
) -> Result<()>
where
    R: Read,
    W: Write,
{
    let disk_ptr = disk as *mut DiskClient<R, W>;
    let mut probe_sink = |probe_row: Row| -> Result<()> {
        let mut emit_match = |build_row: Row| -> Result<()> {
            let joined = if build_is_left {
                Row::combine(&build_row, &probe_row)
            } else {
                Row::combine(&probe_row, &build_row)
            };
            if row_satisfies_predicates(&joined, compiled_cross)? {
                sink(joined)?;
            }
            Ok(())
        };
        read_scratch_run_ptr(build_run, build_schema, disk_ptr, &mut emit_match)
    };

    read_scratch_run_ptr(probe_run, probe_schema, disk_ptr, &mut probe_sink)
}

fn partition_subtree_rows<R, W>(
    op: &QueryOp,
    predicates: &[Predicate],
    key_indexes: &[usize],
    schema: &Schema,
    ctx: &DbContext,
    disk: &mut DiskClient<R, W>,
    scratch: &mut ScratchSpace,
    memory_limit: usize,
    depth: u32,
    mut build_filter: Option<&mut JoinBloomFilter>,
    probe_filter: Option<&JoinBloomFilter>,
) -> Result<PartitionedRuns>
where
    R: Read,
    W: Write,
{
    let disk_ptr = disk as *mut DiskClient<R, W>;
    let scratch_ptr = scratch as *mut ScratchSpace;
    let block_size = scratch.block_size();
    let mut writers: Vec<Option<ScratchRunWriter<R, W>>> = std::iter::repeat_with(|| None)
        .take(PARTITION_FANOUT)
        .collect();

    {
        let mut collect = |row: Row| -> Result<()> {
            let key = join_key_digest_for_row(&row, key_indexes)?;
            if let Some(filter) = build_filter.as_deref_mut() {
                filter.insert(&key);
            }
            if let Some(filter) = probe_filter
                && !filter.may_contain(&key)
            {
                return Ok(());
            }
            let partition = partition_index_for_digest(key, depth);
            let writer = writers[partition]
                .get_or_insert_with(|| ScratchRunWriter::new(disk_ptr, scratch_ptr, block_size));
            writer.push_row(&row)
        };
        execute_join_subtree(
            op,
            predicates,
            ctx,
            disk,
            scratch,
            memory_limit,
            &mut collect,
        )?;
    }

    finish_partition_writers(schema.clone(), &mut writers)
}

fn partition_run_rows<R, W>(
    run: &ScratchRun,
    schema: &Schema,
    key_indexes: &[usize],
    disk: &mut DiskClient<R, W>,
    scratch: &mut ScratchSpace,
    depth: u32,
    mut build_filter: Option<&mut JoinBloomFilter>,
    probe_filter: Option<&JoinBloomFilter>,
) -> Result<PartitionedRuns>
where
    R: Read,
    W: Write,
{
    let disk_ptr = disk as *mut DiskClient<R, W>;
    let scratch_ptr = scratch as *mut ScratchSpace;
    let block_size = scratch.block_size();
    let mut writers: Vec<Option<ScratchRunWriter<R, W>>> = std::iter::repeat_with(|| None)
        .take(PARTITION_FANOUT)
        .collect();

    {
        let mut collect = |row: Row| -> Result<()> {
            let key = join_key_digest_for_row(&row, key_indexes)?;
            if let Some(filter) = build_filter.as_deref_mut() {
                filter.insert(&key);
            }
            if let Some(filter) = probe_filter
                && !filter.may_contain(&key)
            {
                return Ok(());
            }
            let partition = partition_index_for_digest(key, depth);
            let writer = writers[partition]
                .get_or_insert_with(|| ScratchRunWriter::new(disk_ptr, scratch_ptr, block_size));
            writer.push_row(&row)
        };
        read_scratch_run_ptr(run, schema, disk_ptr, &mut collect)?;
    }

    finish_partition_writers(schema.clone(), &mut writers)
}

fn finish_partition_writers<R, W>(
    schema: Schema,
    writers: &mut [Option<ScratchRunWriter<R, W>>],
) -> Result<PartitionedRuns>
where
    R: Read,
    W: Write,
{
    let mut runs = Vec::with_capacity(PARTITION_FANOUT);
    let mut non_empty_partitions = 0usize;
    for writer in writers.iter_mut() {
        let run = match writer.take() {
            Some(writer) => writer.finish()?,
            None => ScratchRun::empty(),
        };
        if !run.block_ids().is_empty() {
            non_empty_partitions += 1;
        }
        runs.push(run);
    }
    Ok(PartitionedRuns {
        schema,
        runs,
        non_empty_partitions,
    })
}

fn partition_index_for_digest(digest: JoinKeyDigest, depth: u32) -> usize {
    let shifted = digest >> (depth * PARTITION_FANOUT_BITS);
    (shifted as usize) & (PARTITION_FANOUT - 1)
}

fn join_filter_budget(memory_limit: usize) -> usize {
    effective_memory(memory_limit)
        .saturating_div(32)
        .clamp(JOIN_FILTER_MIN_BYTES, JOIN_FILTER_MAX_BYTES)
        .max(JOIN_FILTER_MIN_BYTES)
}

struct JoinBloomFilter {
    words: Vec<u64>,
    bit_mask: usize,
}

impl JoinBloomFilter {
    fn new(target_bytes: usize) -> Self {
        let bit_count = (target_bytes.saturating_mul(8)).next_power_of_two().max(64);
        let word_count = (bit_count / 64).max(1);
        Self {
            words: vec![0; word_count],
            bit_mask: bit_count - 1,
        }
    }

    fn insert(&mut self, digest: &JoinKeyDigest) {
        let (bit_a, bit_b) = bloom_hashes(*digest, self.bit_mask);
        self.set_bit(bit_a);
        self.set_bit(bit_b);
    }

    fn may_contain(&self, digest: &JoinKeyDigest) -> bool {
        let (bit_a, bit_b) = bloom_hashes(*digest, self.bit_mask);
        self.get_bit(bit_a) && self.get_bit(bit_b)
    }

    fn set_bit(&mut self, bit: usize) {
        let word_index = bit / 64;
        let bit_index = bit % 64;
        self.words[word_index] |= 1u64 << bit_index;
    }

    fn get_bit(&self, bit: usize) -> bool {
        let word_index = bit / 64;
        let bit_index = bit % 64;
        (self.words[word_index] & (1u64 << bit_index)) != 0
    }
}

fn bloom_hashes(first: JoinKeyDigest, bit_mask: usize) -> (usize, usize) {
    let second = avalanche(first ^ BLOOM_HASH_SEED_A);
    let third = avalanche(first.wrapping_mul(BLOOM_HASH_SEED_B) ^ JOIN_HASH_SEED);
    ((second as usize) & bit_mask, (third as usize) & bit_mask)
}

fn hash_map_reserved_bytes(map: &JoinHashMap<Vec<Box<[u8]>>>) -> usize {
    let map_slots = map
        .capacity()
        .saturating_mul(size_of::<(JoinKeyDigest, Vec<Box<[u8]>>)>())
        .saturating_mul(2);
    let bucket_slots = map
        .values()
        .map(|rows| rows.capacity().saturating_mul(size_of::<Box<[u8]>>()))
        .sum::<usize>();
    map_slots.saturating_add(bucket_slots)
}

fn encode_row_to_boxed(row: &Row) -> Result<Box<[u8]>> {
    let row_size = row.serialized_size();
    let mut encoded = vec![0u8; row_size];
    row.encode_into(&mut encoded)?;
    Ok(encoded.into_boxed_slice())
}

fn insert_build_row(
    build_rows: &mut JoinHashMap<Vec<Box<[u8]>>>,
    key: JoinKeyDigest,
    row: Row,
    used_bytes: &mut usize,
    budget: usize,
) -> Result<()> {
    insert_encoded_build_row(
        build_rows,
        key,
        encode_row_to_boxed(&row)?,
        used_bytes,
        budget,
    )
}

fn insert_encoded_build_row(
    build_rows: &mut JoinHashMap<Vec<Box<[u8]>>>,
    key: JoinKeyDigest,
    encoded: Box<[u8]>,
    used_bytes: &mut usize,
    budget: usize,
) -> Result<()> {
    let is_new_bucket = !build_rows.contains_key(&key);
    if is_new_bucket && build_rows.try_reserve(1).is_err() {
        return Err(HashBuildBudgetExceeded.into());
    }

    let bucket_bytes = if let Some(rows) = build_rows.get_mut(&key) {
        if rows.len() == rows.capacity() && rows.try_reserve(1).is_err() {
            return Err(HashBuildBudgetExceeded.into());
        }
        0
    } else {
        size_of::<JoinKeyDigest>()
            .saturating_add(size_of::<Vec<Box<[u8]>>>())
            .saturating_add(size_of::<Box<[u8]>>())
    };

    let row_size = encoded.len();
    let projected = used_bytes
        .saturating_add(row_size)
        .saturating_add(size_of::<Box<[u8]>>())
        .saturating_add(bucket_bytes)
        .saturating_add(hash_map_reserved_bytes(build_rows));
    if projected > budget {
        return Err(HashBuildBudgetExceeded.into());
    }

    match build_rows.entry(key) {
        std::collections::hash_map::Entry::Occupied(mut entry) => {
            entry.get_mut().push(encoded);
        }
        std::collections::hash_map::Entry::Vacant(entry) => {
            entry.insert(vec![encoded]);
        }
    }

    *used_bytes = used_bytes
        .saturating_add(row_size)
        .saturating_add(size_of::<Box<[u8]>>())
        .saturating_add(bucket_bytes);
    Ok(())
}

#[derive(Debug)]
struct HashBuildBudgetExceeded;

impl std::fmt::Display for HashBuildBudgetExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("hash build exceeded budget")
    }
}

impl std::error::Error for HashBuildBudgetExceeded {}

fn wrap_filter_if_needed(predicates: Vec<Predicate>, underlying: QueryOp) -> QueryOp {
    if predicates.is_empty() {
        underlying
    } else {
        QueryOp::Filter(FilterData {
            predicates,
            underlying: Box::new(underlying),
        })
    }
}

fn join_key_digest_for_row(row: &Row, key_indexes: &[usize]) -> Result<JoinKeyDigest> {
    let mut hasher = DeterministicHasher::default();
    key_indexes.len().hash(&mut hasher);
    for &index in key_indexes {
        let value = row
            .get(index)
            .ok_or_else(|| anyhow!("missing join column at index {index}"))?;
        hash_join_value(value, &mut hasher);
    }
    Ok(hasher.finish())
}

fn hash_join_value<H: Hasher>(value: &Data, hasher: &mut H) {
    match value {
        Data::Int32(v) => {
            0u8.hash(hasher);
            (*v as i64).hash(hasher);
        }
        Data::Int64(v) => {
            0u8.hash(hasher);
            v.hash(hasher);
        }
        Data::Float32(v) => hash_join_numeric_float(*v as f64, hasher),
        Data::Float64(v) => hash_join_numeric_float(*v, hasher),
        Data::String(v) => {
            1u8.hash(hasher);
            v.hash(hasher);
        }
    }
}

fn hash_join_numeric_float<H: Hasher>(value: f64, hasher: &mut H) {
    match join_numeric_float(value) {
        NumericJoinKey::Integer(integer) => {
            0u8.hash(hasher);
            integer.hash(hasher);
        }
        NumericJoinKey::Float(bits) => {
            2u8.hash(hasher);
            bits.hash(hasher);
        }
    }
}

type JoinKeyDigest = u64;

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
enum NumericJoinKey {
    Integer(i64),
    Float(FloatJoinKey),
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
struct FloatJoinKey {
    bits: u64,
}

fn join_numeric_float(value: f64) -> NumericJoinKey {
    if let Some(integer) = exact_integral_i64_from_f64(value) {
        NumericJoinKey::Integer(integer)
    } else {
        NumericJoinKey::Float(FloatJoinKey {
            bits: normalized_f64_bits(value),
        })
    }
}

fn remap_project_predicates(
    predicates: &[Predicate],
    project: &ProjectData,
) -> Option<Vec<Predicate>> {
    predicates
        .iter()
        .map(|predicate| {
            Some(Predicate {
                column_name: remap_project_column(&predicate.column_name, project)?,
                operator: predicate.operator.clone(),
                value: match &predicate.value {
                    ComparisionValue::Column(other) => {
                        ComparisionValue::Column(remap_project_column(other, project)?)
                    }
                    other => other.clone(),
                },
            })
        })
        .collect()
}

fn remap_project_column(column: &str, project: &ProjectData) -> Option<String> {
    project
        .column_name_map
        .iter()
        .find_map(|(from, to)| (to == column).then(|| from.clone()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn join_key_digests_treat_integral_floats_like_matching_ints() -> Result<()> {
        let int_row = Row::new(vec![Data::Int64(42)]);
        let float_row = Row::new(vec![Data::Float64(42.0)]);
        assert_eq!(
            join_key_digest_for_row(&int_row, &[0])?,
            join_key_digest_for_row(&float_row, &[0])?
        );

        let zero_row = Row::new(vec![Data::Int32(0)]);
        let neg_zero_row = Row::new(vec![Data::Float64(-0.0)]);
        assert_eq!(
            join_key_digest_for_row(&zero_row, &[0])?,
            join_key_digest_for_row(&neg_zero_row, &[0])?
        );
        Ok(())
    }

    #[test]
    fn join_key_digests_keep_large_ints_distinct_from_rounded_floats() -> Result<()> {
        let int_row = Row::new(vec![Data::Int64(9_007_199_254_740_993)]);
        let float_row = Row::new(vec![Data::Float64(9_007_199_254_740_992.0)]);
        assert_ne!(
            join_key_digest_for_row(&int_row, &[0])?,
            join_key_digest_for_row(&float_row, &[0])?,
        );
        Ok(())
    }
}
