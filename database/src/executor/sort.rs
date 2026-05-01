use anyhow::Result;
use common::{
    Data, DataType,
    query::{QueryOp, SortData},
};
use db_config::DbContext;
use std::collections::BinaryHeap;
use std::io::{Read, Write};
use std::mem::size_of;

use super::{
    CompiledSortSpec, compare_rows, compare_values, compile_sort_specs, effective_memory,
    execute_op, infer_schema,
};
use crate::{
    estimation::{estimate_query_rows, estimated_schema_row_bytes},
    row::{Row, Schema},
    scan_pipeline::{ScanPipelineCursor, try_compile_scan_pipeline},
    storage::{
        block_allocator::{
            ScratchRun, ScratchRunCursor, ScratchRunWriter, ScratchSpace, write_rows_to_scratch_ptr,
        },
        disk_client::DiskClient,
        row_codec::{decode_row_from_bytes, decode_value_from_bytes},
    },
};

const INLINE_COMPACT_SORT_KEYS: usize = 16;

struct ScratchIo<R: Read, W: Write> {
    disk: *mut DiskClient<R, W>,
    scratch: *mut ScratchSpace,
    block_size: usize,
}

impl<R: Read, W: Write> ScratchIo<R, W> {
    fn new(disk: &mut DiskClient<R, W>, scratch: &mut ScratchSpace) -> Self {
        Self {
            disk,
            scratch,
            block_size: scratch.block_size(),
        }
    }

    fn writer(&self) -> ScratchRunWriter<R, W> {
        ScratchRunWriter::new(self.disk, self.scratch, self.block_size)
    }
}

pub fn execute_sort<R, W>(
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
    let schema = infer_schema(&sort.underlying, ctx)?;
    let specs = compile_sort_specs(&schema, &sort.sort_specs)?;

    if try_execute_segmented_ordered_scan_sort(
        sort,
        ctx,
        disk,
        scratch,
        memory_limit,
        &schema,
        &specs,
        sink,
    )? {
        return Ok(schema);
    }

    let run_budget = sort_run_budget(sort.underlying.as_ref(), &schema, ctx, memory_limit);
    let compact_budget = compact_sort_budget(sort.underlying.as_ref(), &schema, ctx, memory_limit);
    let compact_state_budget = compact_budget;

    let scratch_io = ScratchIo::new(disk, scratch);

    let mut runs: Vec<ScratchRun> = Vec::new();
    let mut current_rows: Vec<Row> = Vec::new();
    let mut current_bytes: usize = 0;
    let mut compact_state =
        (compact_state_budget > 0).then(|| CompactSortState::with_capacity(compact_state_budget));

    {
        let mut collect = |row: Row| -> Result<()> {
            if let Some(compact) = compact_state.as_mut() {
                if compact.try_push_row(&row)? {
                    return Ok(());
                }

                flush_compact_run(
                    compact_state.take().expect("compact state should exist"),
                    &schema,
                    &specs,
                    &scratch_io,
                    &mut runs,
                )?;

                let mut fresh_compact = CompactSortState::with_capacity(compact_state_budget);
                if fresh_compact.try_push_row(&row)? {
                    compact_state = Some(fresh_compact);
                    return Ok(());
                }
            }

            push_standard_row(
                row,
                &specs,
                run_budget,
                &scratch_io,
                &mut current_rows,
                &mut current_bytes,
                &mut runs,
            )?;
            Ok(())
        };
        execute_op(
            &sort.underlying,
            ctx,
            disk,
            scratch,
            memory_limit,
            &mut collect,
        )?;
    }

    if let Some(compact) = compact_state.take() {
        if runs.is_empty() && current_rows.is_empty() {
            emit_compact_rows(compact, &schema, &specs, sink)?;
            return Ok(schema);
        }
        if !compact.rows.is_empty() {
            flush_compact_run(
                compact,
                &schema,
                &specs,
                &scratch_io,
                &mut runs,
            )?;
        }
    }

    if runs.is_empty() {
        // ORDER BY only constrains the key order, so we can use the
        // in-place unstable sort and avoid the large temporary buffer
        // allocated by the stable sort implementation.
        current_rows.sort_unstable_by(|a, b| compare_rows(a, b, &specs));
        for row in current_rows {
            sink(row)?;
        }
        return Ok(schema);
    }

    if !current_rows.is_empty() {
        flush_run(&mut current_rows, &specs, &scratch_io, &mut runs)?;
    }

    merge_runs(&runs, &schema, &specs, disk, sink)?;

    for run in runs {
        scratch.release_run(run);
    }

    Ok(schema)
}

#[allow(clippy::too_many_arguments)]
fn try_execute_segmented_ordered_scan_sort<R, W>(
    sort: &SortData,
    ctx: &DbContext,
    disk: &mut DiskClient<R, W>,
    scratch: &mut ScratchSpace,
    memory_limit: usize,
    schema: &Schema,
    specs: &[CompiledSortSpec],
    sink: &mut dyn FnMut(Row) -> Result<()>,
) -> Result<bool>
where
    R: Read,
    W: Write,
{
    let Some(first_spec) = specs.first() else {
        return Ok(false);
    };
    if !first_spec.ascending {
        return Ok(false);
    }

    let Some(plan) = try_compile_scan_pipeline(&sort.underlying, ctx)? else {
        return Ok(false);
    };
    if !plan.output_physically_ordered(first_spec.index) {
        return Ok(false);
    }

    let run_budget = sort_run_budget(sort.underlying.as_ref(), schema, ctx, memory_limit);
    let scratch_io = ScratchIo::new(disk, scratch);
    let mut cursor = ScanPipelineCursor::new(plan, disk, memory_limit)?;
    let mut current_key: Option<Data> = None;
    let mut current_rows: Vec<Row> = Vec::new();
    let mut current_bytes = 0usize;
    let mut runs: Vec<ScratchRun> = Vec::new();

    while let Some(row) = cursor.next_row(disk)? {
        let key = row
            .get(first_spec.index)
            .ok_or_else(|| anyhow::anyhow!("missing segmented sort key"))?
            .clone();

        if let Some(existing_key) = current_key.as_ref() {
            if compare_values(existing_key, &key)? != std::cmp::Ordering::Equal {
                flush_segment(
                    &mut current_rows,
                    &mut current_bytes,
                    &mut runs,
                    specs,
                    disk,
                    scratch,
                    &scratch_io,
                    schema,
                    sink,
                )?;
                current_key = Some(key.clone());
            }
        } else {
            current_key = Some(key.clone());
        }

        push_standard_row(
            row,
            specs,
            run_budget,
            &scratch_io,
            &mut current_rows,
            &mut current_bytes,
            &mut runs,
        )?;
    }

    flush_segment(
        &mut current_rows,
        &mut current_bytes,
        &mut runs,
        specs,
        disk,
        scratch,
        &scratch_io,
        schema,
        sink,
    )?;

    Ok(true)
}

#[allow(clippy::too_many_arguments)]
fn flush_segment<R, W>(
    current_rows: &mut Vec<Row>,
    current_bytes: &mut usize,
    runs: &mut Vec<ScratchRun>,
    specs: &[CompiledSortSpec],
    disk: &mut DiskClient<R, W>,
    scratch: &mut ScratchSpace,
    scratch_io: &ScratchIo<R, W>,
    schema: &Schema,
    sink: &mut dyn FnMut(Row) -> Result<()>,
) -> Result<()>
where
    R: Read,
    W: Write,
{
    if runs.is_empty() {
        current_rows.sort_unstable_by(|a, b| compare_rows(a, b, specs));
        for row in current_rows.drain(..) {
            sink(row)?;
        }
        *current_bytes = 0;
        return Ok(());
    }

    if !current_rows.is_empty() {
        flush_run(current_rows, specs, scratch_io, runs)?;
    }
    *current_bytes = 0;
    merge_runs(runs, schema, specs, disk, sink)?;
    for run in runs.drain(..) {
        scratch.release_run(run);
    }
    Ok(())
}

fn sort_run_budget(op: &QueryOp, schema: &Schema, ctx: &DbContext, memory_limit: usize) -> usize {
    let effective = effective_memory(memory_limit);
    let join_like = contains_join_like(op);
    let row_width = estimated_schema_row_bytes(schema);
    let reserve = structural_memory_reserve(join_like, row_width, effective);
    let available = effective.saturating_sub(reserve);
    if available == 0 {
        return 256 * 1024;
    }

    let estimated_total = (estimate_query_rows(op, ctx) as u128)
        .saturating_mul(estimated_schema_row_bytes(schema) as u128);

    let target = if estimated_total <= available as u128 {
        available
    } else if estimated_total <= (available as u128).saturating_mul(2) {
        available / 2
    } else {
        available / 3
    };

    let target = if row_width >= 512 {
        target.min(available / 4)
    } else if row_width >= 256 {
        target.min(available / 3)
    } else {
        target
    };

    let min_budget = (available / 8).max(256 * 1024);
    let max_budget = if join_like {
        if row_width >= 512 {
            available / 3
        } else if row_width >= 256 {
            available * 2 / 5
        } else {
            available / 2
        }
    } else if row_width >= 512 {
        available / 2
    } else if row_width >= 256 {
        available * 2 / 3
    } else {
        available * 3 / 4
    };

    let max_budget = max_budget.max(min_budget).min(available);

    target.clamp(min_budget.min(available), max_budget)
}

fn structural_memory_reserve(join_like: bool, row_width: usize, effective: usize) -> usize {
    if join_like && row_width >= 512 {
        effective * 2 / 5
    } else if join_like || row_width >= 512 {
        effective / 4
    } else {
        effective / 8
    }
}

fn contains_join_like(op: &QueryOp) -> bool {
    match op {
        QueryOp::Scan(_) => false,
        QueryOp::Filter(filter) => contains_join_like(&filter.underlying),
        QueryOp::Project(project) => contains_join_like(&project.underlying),
        QueryOp::Sort(sort) => contains_join_like(&sort.underlying),
        QueryOp::Cross(_) => true,
    }
}

fn estimated_sort_row_bytes(row: &Row) -> usize {
    row.estimated_heap_bytes()
        .saturating_add(row.serialized_size())
        .saturating_add(64)
}

fn compact_sort_budget(
    op: &QueryOp,
    schema: &Schema,
    _ctx: &DbContext,
    memory_limit: usize,
) -> usize {
    let effective = effective_memory(memory_limit);
    let join_like = contains_join_like(op);
    let row_width = estimated_schema_row_bytes(schema);
    let reserve = structural_memory_reserve(join_like, row_width, effective);
    let available = effective.saturating_sub(reserve);
    if available == 0 {
        return 256 * 1024;
    }

    let guard_bytes = (available / 16).max(64 * 1024);
    let floor_bytes = (available / 2).max(256 * 1024);
    available
        .saturating_sub(guard_bytes)
        .max(floor_bytes.min(available))
}

#[derive(Clone, Copy)]
struct CompactRowRef {
    start: usize,
    len: usize,
}

struct CompactSortState {
    storage: Vec<u8>,
    rows: Vec<CompactRowRef>,
    budget: usize,
}

struct CompactSortLayout {
    specs: Vec<CompiledSortSpec>,
    spec_position_by_column: Vec<Option<usize>>,
}

impl CompactSortState {
    fn with_capacity(budget: usize) -> Self {
        Self {
            storage: Vec::with_capacity(initial_compact_capacity(budget)),
            rows: Vec::new(),
            budget,
        }
    }

    fn try_push_row(&mut self, row: &Row) -> Result<bool> {
        let row_size = row.serialized_size();
        let needed = self
            .storage
            .len()
            .saturating_add(row_size)
            .saturating_add((self.rows.len() + 1).saturating_mul(size_of::<CompactRowRef>()));
        if needed > self.budget {
            return Ok(false);
        }

        if !self.ensure_storage_capacity(row_size) || !self.ensure_row_capacity() {
            return Ok(false);
        }

        if self.reserved_bytes() > self.budget {
            return Ok(false);
        }

        let start = self.storage.len();
        self.storage.resize(start + row_size, 0);
        row.encode_into(&mut self.storage[start..start + row_size])?;
        self.rows.push(CompactRowRef {
            start,
            len: row_size,
        });
        Ok(true)
    }

    fn ensure_storage_capacity(&mut self, row_size: usize) -> bool {
        if self.storage.len().saturating_add(row_size) <= self.storage.capacity() {
            return true;
        }

        let growth = self
            .storage
            .capacity()
            .saturating_div(4)
            .clamp(256 * 1024, 1024 * 1024)
            .max(row_size);
        let target_capacity = self.storage.len().saturating_add(growth);
        let reserved = target_capacity.saturating_add(
            self.rows
                .capacity()
                .saturating_mul(size_of::<CompactRowRef>()),
        );
        if reserved > self.budget {
            return false;
        }

        self.storage
            .try_reserve_exact(target_capacity.saturating_sub(self.storage.len()))
            .is_ok()
    }

    fn ensure_row_capacity(&mut self) -> bool {
        if self.rows.len() < self.rows.capacity() {
            return true;
        }

        // Grow row metadata in fixed chunks so a large ordered result does not
        // trigger one big allocator jump that defeats the compact-sort budget.
        let growth = self.rows.capacity().saturating_div(4).clamp(4096, 32768);
        let target_capacity = self.rows.len().saturating_add(growth);
        let reserved = self
            .storage
            .capacity()
            .saturating_add(target_capacity.saturating_mul(size_of::<CompactRowRef>()));
        if reserved > self.budget {
            return false;
        }

        self.rows
            .try_reserve_exact(target_capacity.saturating_sub(self.rows.len()))
            .is_ok()
    }

    fn reserved_bytes(&self) -> usize {
        self.storage.capacity().saturating_add(
            self.rows
                .capacity()
                .saturating_mul(size_of::<CompactRowRef>()),
        )
    }
}

fn initial_compact_capacity(budget: usize) -> usize {
    budget.min(128 * 1024)
}

fn flush_compact_run<R, W>(
    mut compact: CompactSortState,
    schema: &Schema,
    specs: &[CompiledSortSpec],
    scratch_io: &ScratchIo<R, W>,
    runs: &mut Vec<ScratchRun>,
) -> Result<()>
where
    R: Read,
    W: Write,
{
    if compact.rows.is_empty() {
        return Ok(());
    }

    let layout = CompactSortLayout::from_specs(schema, specs);
    compact.rows.sort_unstable_by(|left, right| {
        layout.compare(
            &compact.storage[left.start..left.start + left.len],
            &compact.storage[right.start..right.start + right.len],
            schema,
        )
    });

    let mut writer = scratch_io.writer();
    for row_ref in compact.rows {
        let row = decode_compact_row(
            &compact.storage[row_ref.start..row_ref.start + row_ref.len],
            schema,
        )?;
        writer.push_row(&row)?;
    }

    runs.push(writer.finish()?);
    Ok(())
}

fn emit_compact_rows(
    mut compact: CompactSortState,
    schema: &Schema,
    specs: &[CompiledSortSpec],
    sink: &mut dyn FnMut(Row) -> Result<()>,
) -> Result<()> {
    let layout = CompactSortLayout::from_specs(schema, specs);
    compact.rows.sort_unstable_by(|left, right| {
        layout.compare(
            &compact.storage[left.start..left.start + left.len],
            &compact.storage[right.start..right.start + right.len],
            schema,
        )
    });

    for row_ref in compact.rows {
        let row = decode_compact_row(
            &compact.storage[row_ref.start..row_ref.start + row_ref.len],
            schema,
        )?;
        sink(row)?;
    }

    Ok(())
}

fn push_standard_row<R, W>(
    row: Row,
    specs: &[CompiledSortSpec],
    run_budget: usize,
    scratch_io: &ScratchIo<R, W>,
    current_rows: &mut Vec<Row>,
    current_bytes: &mut usize,
    runs: &mut Vec<ScratchRun>,
) -> Result<()>
where
    R: Read,
    W: Write,
{
    let rb = estimated_sort_row_bytes(&row);
    if !current_rows.is_empty() && current_bytes.saturating_add(rb) > run_budget {
        flush_run(current_rows, specs, scratch_io, runs)?;
        *current_bytes = 0;
    }
    *current_bytes = current_bytes.saturating_add(rb);
    current_rows.push(row);
    Ok(())
}

impl CompactSortLayout {
    fn from_specs(schema: &Schema, specs: &[CompiledSortSpec]) -> Self {
        let mut spec_position_by_column = vec![None; schema.len()];
        for (position, spec) in specs.iter().enumerate() {
            spec_position_by_column[spec.index] = Some(position);
        }
        Self {
            specs: specs.to_vec(),
            spec_position_by_column,
        }
    }

    fn compare(&self, left: &[u8], right: &[u8], schema: &Schema) -> std::cmp::Ordering {
        if self.specs.len() <= INLINE_COMPACT_SORT_KEYS {
            return self.compare_inline(left, right, schema);
        }

        for spec in &self.specs {
            let left_value = decode_sort_value_at_column(left, schema, spec.index)
                .expect("encoded row should remain valid");
            let right_value = decode_sort_value_at_column(right, schema, spec.index)
                .expect("encoded row should remain valid");
            let ord =
                compare_values(&left_value, &right_value).unwrap_or(std::cmp::Ordering::Equal);
            let ord = if spec.ascending { ord } else { ord.reverse() };
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }

        std::cmp::Ordering::Equal
    }

    fn compare_inline(&self, left: &[u8], right: &[u8], schema: &Schema) -> std::cmp::Ordering {
        let mut left_spans = [CompactValueSpan::default(); INLINE_COMPACT_SORT_KEYS];
        let mut right_spans = [CompactValueSpan::default(); INLINE_COMPACT_SORT_KEYS];
        let mut left_offset = 0usize;
        let mut right_offset = 0usize;

        for (column_index, column) in schema.columns().iter().enumerate() {
            let left_len = encoded_value_size(&left[left_offset..], &column.data_type)
                .expect("encoded row should remain valid");
            let right_len = encoded_value_size(&right[right_offset..], &column.data_type)
                .expect("encoded row should remain valid");

            if let Some(spec_position) = self.spec_position_by_column[column_index] {
                left_spans[spec_position] = CompactValueSpan {
                    start: left_offset,
                    len: left_len,
                };
                right_spans[spec_position] = CompactValueSpan {
                    start: right_offset,
                    len: right_len,
                };
            }

            left_offset += left_len;
            right_offset += right_len;
        }

        for (position, spec) in self.specs.iter().enumerate() {
            let column = schema
                .column_at(spec.index)
                .expect("sort column should exist in compact row schema");
            let left_span = left_spans[position];
            let right_span = right_spans[position];
            let (left_value, _) = decode_value_from_bytes(
                &left[left_span.start..left_span.start + left_span.len],
                &column.data_type,
            )
            .expect("encoded row should remain valid");
            let (right_value, _) = decode_value_from_bytes(
                &right[right_span.start..right_span.start + right_span.len],
                &column.data_type,
            )
            .expect("encoded row should remain valid");

            let ord =
                compare_values(&left_value, &right_value).unwrap_or(std::cmp::Ordering::Equal);
            let ord = if spec.ascending { ord } else { ord.reverse() };
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }

        std::cmp::Ordering::Equal
    }
}

#[derive(Clone, Copy, Default)]
struct CompactValueSpan {
    start: usize,
    len: usize,
}

fn decode_sort_value_at_column(buf: &[u8], schema: &Schema, target_index: usize) -> Result<Data> {
    let mut offset = 0usize;
    for (column_index, column) in schema.columns().iter().enumerate() {
        if column_index == target_index {
            let (value, _) = decode_value_from_bytes(&buf[offset..], &column.data_type)?;
            return Ok(value);
        }
        offset += encoded_value_size(&buf[offset..], &column.data_type)?;
    }

    anyhow::bail!("sort column index {target_index} out of bounds")
}

fn decode_compact_row(buf: &[u8], schema: &Schema) -> Result<Row> {
    decode_row_from_bytes(buf, schema)
}

fn encoded_value_size(buf: &[u8], data_type: &DataType) -> Result<usize> {
    Ok(match data_type {
        DataType::Int32 | DataType::Float32 => 4,
        DataType::Int64 | DataType::Float64 => 8,
        DataType::String => {
            buf.iter()
                .position(|byte| *byte == 0)
                .expect("string must be null-terminated")
                + 1
        }
    })
}

fn flush_run<R, W>(
    rows: &mut Vec<Row>,
    specs: &[CompiledSortSpec],
    scratch_io: &ScratchIo<R, W>,
    runs: &mut Vec<ScratchRun>,
) -> Result<()>
where
    R: Read,
    W: Write,
{
    rows.sort_unstable_by(|a, b| compare_rows(a, b, specs));
    let run = write_rows_to_scratch_ptr(rows, scratch_io.disk, scratch_io.scratch)?;
    runs.push(run);
    rows.clear();
    Ok(())
}

fn merge_runs<R, W>(
    runs: &[ScratchRun],
    schema: &Schema,
    specs: &[CompiledSortSpec],
    disk: &mut DiskClient<R, W>,
    sink: &mut dyn FnMut(Row) -> Result<()>,
) -> Result<()>
where
    R: Read,
    W: Write,
{
    if runs.is_empty() {
        return Ok(());
    }

    if runs.len() == 1 {
        let mut cursor = ScratchRunCursor::new(runs[0].clone());
        while let Some(row) = cursor.next_row(schema, disk)? {
            sink(row)?;
        }
        return Ok(());
    }

    let mut cursors: Vec<ScratchRunCursor> =
        runs.iter().cloned().map(ScratchRunCursor::new).collect();
    let mut heap: BinaryHeap<HeapItem<'_>> = BinaryHeap::new();

    for (i, cursor) in cursors.iter_mut().enumerate() {
        if let Some(row) = cursor.next_row(schema, disk)? {
            heap.push(HeapItem {
                row,
                run_index: i,
                specs,
            });
        }
    }

    while let Some(item) = heap.pop() {
        let i = item.run_index;
        sink(item.row)?;
        if let Some(row) = cursors[i].next_row(schema, disk)? {
            heap.push(HeapItem {
                row,
                run_index: i,
                specs,
            });
        }
    }

    Ok(())
}

struct HeapItem<'a> {
    row: Row,
    run_index: usize,
    specs: &'a [CompiledSortSpec],
}

impl PartialEq for HeapItem<'_> {
    fn eq(&self, other: &Self) -> bool {
        compare_rows(&self.row, &other.row, &self.specs) == std::cmp::Ordering::Equal
            && self.run_index == other.run_index
    }
}
impl Eq for HeapItem<'_> {}

impl PartialOrd for HeapItem<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapItem<'_> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        compare_rows(&self.row, &other.row, &self.specs)
            .reverse()
            .then_with(|| self.run_index.cmp(&other.run_index).reverse())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use db_config::table::{ColumnSpec, TableSpec};

    fn schema(columns: &[(&str, DataType)]) -> Schema {
        Schema::from_table_spec(&TableSpec {
            name: "t".to_string(),
            file_id: "t".to_string(),
            column_specs: columns
                .iter()
                .map(|(name, data_type)| ColumnSpec {
                    column_name: (*name).to_string(),
                    data_type: data_type.clone(),
                    stats: None,
                })
                .collect(),
        })
    }

    #[test]
    fn emit_compact_rows_orders_simple_rows() -> Result<()> {
        let schema = schema(&[
            ("n_name", DataType::String),
            ("n_nationkey", DataType::Int32),
        ]);
        let specs = vec![
            CompiledSortSpec {
                index: 0,
                ascending: true,
            },
            CompiledSortSpec {
                index: 1,
                ascending: true,
            },
        ];

        let mut compact = CompactSortState::with_capacity(1024 * 1024);
        assert!(compact.try_push_row(&Row::new(vec![
            Data::String("BRAZIL".to_string()),
            Data::Int32(2),
        ]))?);
        assert!(compact.try_push_row(&Row::new(vec![
            Data::String("ALGERIA".to_string()),
            Data::Int32(0),
        ]))?);
        assert!(compact.try_push_row(&Row::new(vec![
            Data::String("ARGENTINA".to_string()),
            Data::Int32(1),
        ]))?);

        let mut rows = Vec::new();
        emit_compact_rows(compact, &schema, &specs, &mut |row| {
            rows.push(row);
            Ok(())
        })?;

        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].get(0), Some(&Data::String("ALGERIA".to_string())));
        assert_eq!(rows[1].get(0), Some(&Data::String("ARGENTINA".to_string())));
        assert_eq!(rows[2].get(0), Some(&Data::String("BRAZIL".to_string())));
        Ok(())
    }

    #[test]
    fn emit_compact_rows_respects_sort_spec_order_not_schema_order() -> Result<()> {
        let schema = schema(&[
            ("a", DataType::Int32),
            ("b", DataType::Int32),
            ("c", DataType::Int32),
        ]);
        let specs = vec![
            CompiledSortSpec {
                index: 2,
                ascending: true,
            },
            CompiledSortSpec {
                index: 0,
                ascending: true,
            },
        ];

        let mut compact = CompactSortState::with_capacity(1024 * 1024);
        assert!(compact.try_push_row(&Row::new(vec![
            Data::Int32(2),
            Data::Int32(0),
            Data::Int32(1),
        ]))?);
        assert!(compact.try_push_row(&Row::new(vec![
            Data::Int32(1),
            Data::Int32(0),
            Data::Int32(2),
        ]))?);

        let mut rows = Vec::new();
        emit_compact_rows(compact, &schema, &specs, &mut |row| {
            rows.push(row);
            Ok(())
        })?;

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get(2), Some(&Data::Int32(1)));
        assert_eq!(rows[1].get(2), Some(&Data::Int32(2)));
        Ok(())
    }
}
