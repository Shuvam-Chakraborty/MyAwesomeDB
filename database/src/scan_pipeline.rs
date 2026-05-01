use anyhow::{Context, Result, anyhow, bail};
use common::{
    Data, DataType,
    query::{ComparisionOperator, ComparisionValue, Predicate, QueryOp},
};
use db_config::{DbContext, statistics::ColumnStat};
use std::collections::BTreeSet;
use std::io::{Read, Write};

use crate::{
    executor::{compare_data, find_table_spec, scan_batch_blocks},
    row::{Row, Schema},
    storage::{
        disk_client::DiskClient,
        row_codec::{decode_value_from_bytes, skip_value_from_bytes},
    },
};
const STREAM_SCAN_MIN_BATCH_BLOCKS: usize = 64;
const STREAM_SCAN_MAX_BATCH_BLOCKS: usize = 1024;

#[derive(Clone)]
pub struct ScanPipelinePlan {
    file_id: String,
    base_columns: Vec<BaseColumnMeta>,
    required_lookup: Vec<Option<usize>>,
    output_columns: Vec<OutputColumnMeta>,
    predicates: Vec<CompiledPipelinePredicate>,
    schema: Schema,
}

#[derive(Clone)]
struct BaseColumnMeta {
    name: String,
    data_type: DataType,
    physically_ordered: bool,
}

#[derive(Clone)]
struct OutputColumnMeta {
    name: String,
    base_index: usize,
    required_index: usize,
}

#[derive(Clone)]
struct VisibleColumn {
    name: String,
    base_index: usize,
}

#[derive(Clone)]
struct LogicalScanPipeline {
    file_id: String,
    base_columns: Vec<BaseColumnMeta>,
    visible_columns: Vec<VisibleColumn>,
    predicates: Vec<LogicalPredicate>,
    base_schema: Schema,
}

#[derive(Clone)]
struct LogicalPredicate {
    left_base_index: usize,
    operator: ComparisionOperator,
    right: LogicalPredicateOperand,
}

#[derive(Clone)]
enum LogicalPredicateOperand {
    Column(usize),
    Literal(Data),
}

#[derive(Clone)]
struct CompiledPipelinePredicate {
    left_required_index: usize,
    operator: ComparisionOperator,
    right: CompiledPipelinePredicateOperand,
}

#[derive(Clone)]
enum CompiledPipelinePredicateOperand {
    Column(usize),
    Literal(Data),
}

pub struct ScanPipelineCursor {
    plan: ScanPipelinePlan,
    block_size: usize,
    start_block: u64,
    total_blocks: u64,
    batch_blocks: u64,
    next_block_offset: u64,
    batch_data: Vec<u8>,
    batch_block_count: usize,
    next_batch_block_index: usize,
    current_block_start: usize,
    current_block_data_limit: usize,
    current_block_row_count: usize,
    current_block_row_index: usize,
    current_block_offset: usize,
    decoded_values: Vec<Data>,
}

pub fn try_compile_scan_pipeline(
    op: &QueryOp,
    ctx: &DbContext,
) -> Result<Option<ScanPipelinePlan>> {
    let Some(logical) = compile_logical_scan_pipeline(op, ctx)? else {
        return Ok(None);
    };

    let mut required_base_indexes = BTreeSet::new();
    for column in &logical.visible_columns {
        required_base_indexes.insert(column.base_index);
    }
    for predicate in &logical.predicates {
        required_base_indexes.insert(predicate.left_base_index);
        if let LogicalPredicateOperand::Column(index) = predicate.right {
            required_base_indexes.insert(index);
        }
    }

    let required_base_indexes: Vec<usize> = required_base_indexes.into_iter().collect();
    let mut required_lookup = vec![None; logical.base_columns.len()];
    for (required_index, &base_index) in required_base_indexes.iter().enumerate() {
        required_lookup[base_index] = Some(required_index);
    }

    let mut output_map = Vec::with_capacity(logical.visible_columns.len());
    let mut output_columns = Vec::with_capacity(logical.visible_columns.len());
    for visible in &logical.visible_columns {
        let required_index = required_lookup[visible.base_index]
            .ok_or_else(|| anyhow!("missing required index for visible column"))?;
        let base_name = logical.base_columns[visible.base_index].name.clone();
        output_map.push((base_name, visible.name.clone()));
        output_columns.push(OutputColumnMeta {
            name: visible.name.clone(),
            base_index: visible.base_index,
            required_index,
        });
    }

    let (schema, _) = logical.base_schema.project(&output_map)?;
    let predicates = logical
        .predicates
        .into_iter()
        .map(|predicate| {
            let left_required_index = required_lookup[predicate.left_base_index]
                .ok_or_else(|| anyhow!("missing left predicate column"))?;
            let right = match predicate.right {
                LogicalPredicateOperand::Column(base_index) => {
                    let required_index = required_lookup[base_index]
                        .ok_or_else(|| anyhow!("missing right predicate column"))?;
                    CompiledPipelinePredicateOperand::Column(required_index)
                }
                LogicalPredicateOperand::Literal(value) => {
                    CompiledPipelinePredicateOperand::Literal(value)
                }
            };
            Ok(CompiledPipelinePredicate {
                left_required_index,
                operator: predicate.operator,
                right,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(Some(ScanPipelinePlan {
        file_id: logical.file_id,
        base_columns: logical.base_columns,
        required_lookup,
        output_columns,
        predicates,
        schema,
    }))
}

impl ScanPipelinePlan {
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn output_physically_ordered(&self, output_index: usize) -> bool {
        self.output_columns
            .get(output_index)
            .map(|column| self.base_columns[column.base_index].physically_ordered)
            .unwrap_or(false)
    }
}

impl ScanPipelineCursor {
    pub fn new<R: Read, W: Write>(
        plan: ScanPipelinePlan,
        disk: &mut DiskClient<R, W>,
        memory_limit: usize,
    ) -> Result<Self> {
        let block_size = disk.get_block_size()? as usize;
        let start_block = disk.get_file_start_block(&plan.file_id)?;
        let total_blocks = disk.get_file_num_blocks(&plan.file_id)?;
        let batch_blocks = scan_batch_blocks(
            block_size,
            memory_limit,
            STREAM_SCAN_MIN_BATCH_BLOCKS,
            STREAM_SCAN_MAX_BATCH_BLOCKS,
        );

        Ok(Self {
            decoded_values: Vec::with_capacity(
                plan.required_lookup
                    .iter()
                    .filter(|required| required.is_some())
                    .count(),
            ),
            plan,
            block_size,
            start_block,
            total_blocks,
            batch_blocks,
            next_block_offset: 0,
            batch_data: Vec::new(),
            batch_block_count: 0,
            next_batch_block_index: 0,
            current_block_start: 0,
            current_block_data_limit: 0,
            current_block_row_count: 0,
            current_block_row_index: 0,
            current_block_offset: 0,
        })
    }

    pub fn next_row<R: Read, W: Write>(
        &mut self,
        disk: &mut DiskClient<R, W>,
    ) -> Result<Option<Row>> {
        loop {
            if self.current_block_row_index >= self.current_block_row_count {
                if !self.advance_block(disk)? {
                    return Ok(None);
                }
            }

            let row = self.decode_current_row()?;
            self.current_block_row_index += 1;
            if let Some(row) = row {
                return Ok(Some(row));
            }
        }
    }

    fn advance_block<R: Read, W: Write>(&mut self, disk: &mut DiskClient<R, W>) -> Result<bool> {
        if self.next_batch_block_index >= self.batch_block_count {
            if !self.load_next_batch(disk)? {
                return Ok(false);
            }
        }

        self.current_block_start = self.next_batch_block_index * self.block_size;
        self.next_batch_block_index += 1;

        let block =
            &self.batch_data[self.current_block_start..self.current_block_start + self.block_size];
        if block.len() < 2 {
            bail!("block too short");
        }
        self.current_block_data_limit = block.len() - 2;
        self.current_block_row_count = u16::from_le_bytes([
            block[self.current_block_data_limit],
            block[self.current_block_data_limit + 1],
        ]) as usize;
        self.current_block_row_index = 0;
        self.current_block_offset = 0;
        Ok(true)
    }

    fn load_next_batch<R: Read, W: Write>(&mut self, disk: &mut DiskClient<R, W>) -> Result<bool> {
        if self.next_block_offset >= self.total_blocks {
            return Ok(false);
        }

        let batch = (self.total_blocks - self.next_block_offset).min(self.batch_blocks);
        let start_block = self.start_block + self.next_block_offset;
        self.batch_data = disk.read_blocks(start_block, batch)?;
        self.batch_block_count = batch as usize;
        self.next_batch_block_index = 0;
        self.next_block_offset += batch;
        Ok(true)
    }

    fn decode_current_row(&mut self) -> Result<Option<Row>> {
        self.decoded_values.clear();
        let block =
            &self.batch_data[self.current_block_start..self.current_block_start + self.block_size];

        for (base_index, column) in self.plan.base_columns.iter().enumerate() {
            let slice = block
                .get(self.current_block_offset..self.current_block_data_limit)
                .ok_or_else(|| anyhow!("row offset exceeded block payload"))?;

            if self.plan.required_lookup[base_index].is_some() {
                let (value, consumed) = decode_value_from_bytes(slice, &column.data_type)
                    .with_context(|| format!("decode scan pipeline column {}", column.name))?;
                self.current_block_offset += consumed;
                self.decoded_values.push(value);
            } else {
                self.current_block_offset += skip_value_from_bytes(slice, &column.data_type)
                    .with_context(|| format!("skip scan pipeline column {}", column.name))?;
            }
        }

        for predicate in &self.plan.predicates {
            let left = self
                .decoded_values
                .get(predicate.left_required_index)
                .ok_or_else(|| anyhow!("missing left predicate value"))?;
            let right = match &predicate.right {
                CompiledPipelinePredicateOperand::Column(required_index) => self
                    .decoded_values
                    .get(*required_index)
                    .ok_or_else(|| anyhow!("missing right predicate value"))?,
                CompiledPipelinePredicateOperand::Literal(value) => value,
            };

            if !compare_data(left, &predicate.operator, right)? {
                return Ok(None);
            }
        }

        let mut values = Vec::with_capacity(self.plan.output_columns.len());
        for output in &self.plan.output_columns {
            values.push(
                self.decoded_values
                    .get(output.required_index)
                    .ok_or_else(|| anyhow!("missing output column value {}", output.name))?
                    .clone(),
            );
        }
        Ok(Some(Row::new(values)))
    }
}

pub fn execute_scan_pipeline<R, W>(
    plan: &ScanPipelinePlan,
    disk: &mut DiskClient<R, W>,
    memory_limit: usize,
    sink: &mut dyn FnMut(Row) -> Result<()>,
) -> Result<()>
where
    R: Read,
    W: Write,
{
    let mut cursor = ScanPipelineCursor::new(plan.clone(), disk, memory_limit)?;
    while let Some(row) = cursor.next_row(disk)? {
        sink(row)?;
    }
    Ok(())
}

fn compile_logical_scan_pipeline(
    op: &QueryOp,
    ctx: &DbContext,
) -> Result<Option<LogicalScanPipeline>> {
    match op {
        QueryOp::Scan(scan) => {
            let table = find_table_spec(ctx, &scan.table_id)?;
            let base_columns = table
                .column_specs
                .iter()
                .map(|column| BaseColumnMeta {
                    name: column.column_name.clone(),
                    data_type: column.data_type.clone(),
                    physically_ordered: column
                        .stats
                        .as_deref()
                        .map(|stats| {
                            stats
                                .iter()
                                .any(|stat| matches!(stat, ColumnStat::IsPhysicallyOrdered))
                        })
                        .unwrap_or(false),
                })
                .collect::<Vec<_>>();
            let visible_columns = base_columns
                .iter()
                .enumerate()
                .map(|(index, column)| VisibleColumn {
                    name: column.name.clone(),
                    base_index: index,
                })
                .collect();

            Ok(Some(LogicalScanPipeline {
                file_id: table.file_id.clone(),
                base_columns,
                visible_columns,
                predicates: Vec::new(),
                base_schema: Schema::from_table_spec(table),
            }))
        }
        QueryOp::Filter(filter) => {
            let Some(mut logical) = compile_logical_scan_pipeline(&filter.underlying, ctx)? else {
                return Ok(None);
            };

            for predicate in &filter.predicates {
                logical.predicates.push(compile_pipeline_predicate(
                    predicate,
                    &logical.visible_columns,
                )?);
            }
            Ok(Some(logical))
        }
        QueryOp::Project(project) => {
            let Some(mut logical) = compile_logical_scan_pipeline(&project.underlying, ctx)? else {
                return Ok(None);
            };

            logical.visible_columns = project
                .column_name_map
                .iter()
                .map(|(from, to)| {
                    let source = find_visible_column(&logical.visible_columns, from)?;
                    Ok(VisibleColumn {
                        name: to.clone(),
                        base_index: source.base_index,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Some(logical))
        }
        QueryOp::Sort(_) | QueryOp::Cross(_) => Ok(None),
    }
}

fn compile_pipeline_predicate(
    predicate: &Predicate,
    visible_columns: &[VisibleColumn],
) -> Result<LogicalPredicate> {
    let left = find_visible_column(visible_columns, &predicate.column_name)?;
    let right = match &predicate.value {
        ComparisionValue::Column(other) => {
            LogicalPredicateOperand::Column(find_visible_column(visible_columns, other)?.base_index)
        }
        ComparisionValue::I32(value) => LogicalPredicateOperand::Literal(Data::Int32(*value)),
        ComparisionValue::I64(value) => LogicalPredicateOperand::Literal(Data::Int64(*value)),
        ComparisionValue::F32(value) => LogicalPredicateOperand::Literal(Data::Float32(*value)),
        ComparisionValue::F64(value) => LogicalPredicateOperand::Literal(Data::Float64(*value)),
        ComparisionValue::String(value) => {
            LogicalPredicateOperand::Literal(Data::String(value.clone()))
        }
    };

    Ok(LogicalPredicate {
        left_base_index: left.base_index,
        operator: predicate.operator.clone(),
        right,
    })
}

fn find_visible_column<'a>(
    visible_columns: &'a [VisibleColumn],
    name: &str,
) -> Result<&'a VisibleColumn> {
    visible_columns
        .iter()
        .find(|column| column.name == name)
        .ok_or_else(|| anyhow!("unknown visible column {name}"))
}
