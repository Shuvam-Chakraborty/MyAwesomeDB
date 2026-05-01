use common::{
    Data,
    query::{ComparisionOperator, ComparisionValue, Predicate, ProjectData, QueryOp},
};
use db_config::{
    DbContext,
    statistics::ColumnStat,
    table::{ColumnSpec, TableSpec},
};
use std::mem::size_of;

use crate::{
    query_support::{
        extract_join_keys, find_table_spec, infer_schema, split_cross_predicates,
    },
    row::{Row, Schema},
};

pub fn estimate_query_rows(op: &QueryOp, ctx: &DbContext) -> u64 {
    match op {
        QueryOp::Scan(scan) => estimate_scan_rows(&scan.table_id, ctx),
        QueryOp::Filter(filter) => {
            estimate_query_rows_with_predicates(&filter.underlying, &filter.predicates, ctx)
        }
        QueryOp::Project(project) => estimate_query_rows(&project.underlying, ctx),
        QueryOp::Sort(sort) => estimate_query_rows(&sort.underlying, ctx),
        QueryOp::Cross(cross) => estimate_cross_rows(cross, &[], ctx),
    }
}

pub fn estimate_query_rows_with_predicates(
    op: &QueryOp,
    predicates: &[Predicate],
    ctx: &DbContext,
) -> u64 {
    match op {
        QueryOp::Scan(scan) => estimate_scan_rows_with_predicates(&scan.table_id, predicates, ctx),
        QueryOp::Filter(filter) => {
            let mut merged = filter.predicates.clone();
            merged.extend_from_slice(predicates);
            estimate_query_rows_with_predicates(&filter.underlying, &merged, ctx)
        }
        QueryOp::Project(project) => {
            let remapped = remap_project_predicates(predicates, project);
            estimate_query_rows_with_predicates(&project.underlying, &remapped, ctx)
        }
        QueryOp::Sort(sort) => {
            estimate_query_rows_with_predicates(&sort.underlying, predicates, ctx)
        }
        QueryOp::Cross(cross) => estimate_cross_rows(cross, predicates, ctx),
    }
}

pub fn estimate_distinct_values(op: &QueryOp, column_name: &str, ctx: &DbContext) -> Option<u64> {
    match op {
        QueryOp::Scan(scan) => {
            let table = find_table_spec(ctx, &scan.table_id).ok()?;
            let row_count = estimate_scan_rows(&scan.table_id, ctx).max(1);
            let column = table
                .column_specs
                .iter()
                .find(|column| column.column_name == column_name)?;
            Some(
                column_distinct_values(column)
                    .unwrap_or(row_count)
                    .min(row_count)
                    .max(1),
            )
        }
        QueryOp::Filter(filter) => {
            let underlying_distinct =
                estimate_distinct_values(&filter.underlying, column_name, ctx)?;
            let underlying_rows = estimate_query_rows(&filter.underlying, ctx).max(1);
            let filtered_rows =
                estimate_query_rows_with_predicates(&filter.underlying, &filter.predicates, ctx)
                    .max(1);
            let scaled = ((underlying_distinct as u128)
                .saturating_mul(filtered_rows as u128)
                .saturating_add(underlying_rows as u128 - 1)
                / underlying_rows as u128) as u64;
            Some(scaled.min(underlying_distinct).min(filtered_rows).max(1))
        }
        QueryOp::Project(project) => {
            let source = project
                .column_name_map
                .iter()
                .find_map(|(from, to)| (to == column_name).then_some(from.as_str()))?;
            estimate_distinct_values(&project.underlying, source, ctx)
        }
        QueryOp::Sort(sort) => estimate_distinct_values(&sort.underlying, column_name, ctx),
        QueryOp::Cross(cross) => {
            let left_schema = infer_schema(cross.left.as_ref(), ctx).ok()?;
            if left_schema.index_of(column_name).is_some() {
                estimate_distinct_values(cross.left.as_ref(), column_name, ctx)
            } else {
                estimate_distinct_values(cross.right.as_ref(), column_name, ctx)
            }
        }
    }
}

pub fn estimated_schema_row_bytes(schema: &Schema) -> usize {
    size_of::<Row>()
        .saturating_add(schema.len().saturating_mul(size_of::<Data>()))
        .saturating_add(
            schema
                .columns()
                .iter()
                .map(|column| match &column.data_type {
                    common::DataType::Int32 => 4,
                    common::DataType::Int64 => 8,
                    common::DataType::Float32 => 4,
                    common::DataType::Float64 => 8,
                    common::DataType::String => 64,
                })
                .sum::<usize>(),
        )
        .max(1)
}

fn estimate_cross_rows(
    cross: &common::query::CrossData,
    predicates: &[Predicate],
    ctx: &DbContext,
) -> u64 {
    let Ok(left_schema) = infer_schema(cross.left.as_ref(), ctx) else {
        return estimate_query_rows(cross.left.as_ref(), ctx)
            .saturating_mul(estimate_query_rows(cross.right.as_ref(), ctx))
            .max(1);
    };
    let Ok(right_schema) = infer_schema(cross.right.as_ref(), ctx) else {
        return estimate_query_rows(cross.left.as_ref(), ctx)
            .saturating_mul(estimate_query_rows(cross.right.as_ref(), ctx))
            .max(1);
    };

    let (left_preds, right_preds, cross_preds) =
        split_cross_predicates(predicates, &left_schema, &right_schema);
    let left_rows =
        estimate_query_rows_with_predicates(cross.left.as_ref(), &left_preds, ctx);
    let right_rows =
        estimate_query_rows_with_predicates(cross.right.as_ref(), &right_preds, ctx);

    if let Ok(Some((left_keys, right_keys))) =
        extract_join_keys(&cross_preds, &left_schema, &right_schema)
    {
        let mut rows = estimate_join_rows(
            cross.left.as_ref(),
            cross.right.as_ref(),
            &left_schema,
            &right_schema,
            left_rows,
            right_rows,
            &left_keys,
            &right_keys,
            ctx,
        );
        let extra_predicates = cross_preds.len().saturating_sub(left_keys.len());
        if extra_predicates > 0 {
            rows = apply_generic_predicate_penalty(rows, extra_predicates, 0.25);
        }
        rows.max(1)
    } else {
        apply_generic_predicate_penalty(
            left_rows.saturating_mul(right_rows),
            cross_preds.len(),
            0.25,
        )
    }
}

fn estimate_join_rows(
    left_op: &QueryOp,
    right_op: &QueryOp,
    left_schema: &Schema,
    right_schema: &Schema,
    left_rows: u64,
    right_rows: u64,
    left_keys: &[usize],
    right_keys: &[usize],
    ctx: &DbContext,
) -> u64 {
    let mut rows = left_rows.saturating_mul(right_rows);

    for (&left_key, &right_key) in left_keys.iter().zip(right_keys.iter()) {
        let Some(left_column) = left_schema.column_at(left_key) else {
            continue;
        };
        let Some(right_column) = right_schema.column_at(right_key) else {
            continue;
        };

        let left_distinct = estimate_distinct_values(left_op, &left_column.name, ctx)
            .unwrap_or(left_rows)
            .min(left_rows.max(1));
        let right_distinct = estimate_distinct_values(right_op, &right_column.name, ctx)
            .unwrap_or(right_rows)
            .min(right_rows.max(1));
        let divisor = left_distinct.max(right_distinct).max(1);
        rows = rows.saturating_div(divisor);
    }

    rows.max(1)
}

fn estimate_scan_rows_with_predicates(
    table_id: &str,
    predicates: &[Predicate],
    ctx: &DbContext,
) -> u64 {
    let base_rows = estimate_scan_rows(table_id, ctx);
    let Ok(table) = find_table_spec(ctx, table_id) else {
        return apply_generic_predicate_penalty(base_rows, predicates.len(), 0.25);
    };

    let mut selectivity = 1.0f64;
    for predicate in predicates {
        selectivity *= estimate_scan_predicate_selectivity(table, predicate);
    }

    apply_selectivity(base_rows, selectivity)
}

fn estimate_scan_rows(table_id: &str, ctx: &DbContext) -> u64 {
    let Ok(table) = find_table_spec(ctx, table_id) else {
        return u64::MAX / 4;
    };

    table_row_count(table)
        .or_else(|| {
            table
                .column_specs
                .iter()
                .filter_map(column_distinct_values)
                .max()
        })
        .unwrap_or(u64::MAX / 4)
}

fn table_row_count(table: &TableSpec) -> Option<u64> {
    table
        .column_specs
        .iter()
        .filter_map(|column| column.stats.as_deref())
        .filter_map(stats_row_count)
        .max()
}

fn stats_row_count(stats: &[ColumnStat]) -> Option<u64> {
    histogram_row_count(stats)
        .or_else(|| {
            let cardinality = stats_cardinality(stats)?;
            let density = stats_density(stats)?;
            if density > 0.0 {
                Some(((cardinality as f64) / (density as f64)).round() as u64)
            } else {
                None
            }
        })
        .or_else(|| stats_cardinality(stats))
}

fn histogram_row_count(stats: &[ColumnStat]) -> Option<u64> {
    stats.iter().find_map(|stat| {
        let ColumnStat::HistogramStat(histogram) = stat else {
            return None;
        };
        Some(
            histogram
                .frequency_points
                .iter()
                .map(|(_, frequency)| frequency.0)
                .sum(),
        )
    })
}

fn column_distinct_values(column: &ColumnSpec) -> Option<u64> {
    column.stats.as_deref().and_then(stats_cardinality)
}

fn stats_cardinality(stats: &[ColumnStat]) -> Option<u64> {
    stats.iter().find_map(|stat| {
        let ColumnStat::CardinalityStat(cardinality) = stat else {
            return None;
        };
        Some(cardinality.0)
    })
}

fn stats_density(stats: &[ColumnStat]) -> Option<f32> {
    stats.iter().find_map(|stat| {
        let ColumnStat::DensityStat(density) = stat else {
            return None;
        };
        Some(density.0)
    })
}

fn estimate_scan_predicate_selectivity(table: &TableSpec, predicate: &Predicate) -> f64 {
    let default = default_selectivity(&predicate.operator);
    let Some(column) = table
        .column_specs
        .iter()
        .find(|column| column.column_name == predicate.column_name)
    else {
        return default;
    };
    let Some(stats) = column.stats.as_deref() else {
        return default;
    };

    let selectivity = match &predicate.value {
        ComparisionValue::Column(_) => default,
        _ => match predicate.operator {
            ComparisionOperator::EQ => equality_selectivity(stats).unwrap_or(default),
            ComparisionOperator::NE => 1.0 - equality_selectivity(stats).unwrap_or(1.0 - default),
            ComparisionOperator::GT
            | ComparisionOperator::GTE
            | ComparisionOperator::LT
            | ComparisionOperator::LTE => {
                inequality_selectivity(stats, &predicate.value, &predicate.operator)
                    .unwrap_or(default)
            }
        },
    };

    let minimum = 1.0 / table_row_count(table).unwrap_or(1).max(1) as f64;
    selectivity.clamp(minimum, 1.0)
}

fn equality_selectivity(stats: &[ColumnStat]) -> Option<f64> {
    let cardinality = stats_cardinality(stats)?.max(1);
    Some(1.0 / cardinality as f64)
}

fn inequality_selectivity(
    stats: &[ColumnStat],
    value: &ComparisionValue,
    operator: &ComparisionOperator,
) -> Option<f64> {
    let literal = comparison_value_to_data(value)?;
    let (lower, upper) = stats_range(stats)?;

    if let (Data::String(lower), Data::String(upper), Data::String(literal)) =
        (lower, upper, &literal)
    {
        return Some(match operator {
            ComparisionOperator::GT | ComparisionOperator::GTE => {
                string_inequality_selectivity(lower, upper, literal, true)
            }
            ComparisionOperator::LT | ComparisionOperator::LTE => {
                string_inequality_selectivity(lower, upper, literal, false)
            }
            ComparisionOperator::EQ | ComparisionOperator::NE => return None,
        });
    }

    let lower = numeric_data_value(lower)?;
    let upper = numeric_data_value(upper)?;
    let literal = numeric_data_value(&literal)?;

    if upper <= lower {
        return Some(0.5);
    }

    let position = ((literal - lower) / (upper - lower)).clamp(0.0, 1.0);
    Some(match operator {
        ComparisionOperator::GT | ComparisionOperator::GTE => (1.0 - position).clamp(0.05, 0.95),
        ComparisionOperator::LT | ComparisionOperator::LTE => position.clamp(0.05, 0.95),
        ComparisionOperator::EQ | ComparisionOperator::NE => return None,
    })
}

fn string_inequality_selectivity(
    lower: &str,
    upper: &str,
    literal: &str,
    greater_than: bool,
) -> f64 {
    if upper <= lower {
        return 0.5;
    }

    if literal <= lower {
        return if greater_than { 0.95 } else { 0.05 };
    }
    if literal >= upper {
        return if greater_than { 0.05 } else { 0.95 };
    }

    let lower_ord = lower.as_bytes();
    let upper_ord = upper.as_bytes();
    let literal_ord = literal.as_bytes();
    let max_len = lower_ord.len().max(upper_ord.len()).max(literal_ord.len());

    let mut lower_score = 0u128;
    let mut upper_score = 0u128;
    let mut literal_score = 0u128;
    for index in 0..max_len.min(8) {
        let shift = ((max_len.min(8) - index - 1) * 8) as u32;
        lower_score |= (u128::from(*lower_ord.get(index).unwrap_or(&0))) << shift;
        upper_score |= (u128::from(*upper_ord.get(index).unwrap_or(&0))) << shift;
        literal_score |= (u128::from(*literal_ord.get(index).unwrap_or(&0))) << shift;
    }

    let span = upper_score.saturating_sub(lower_score).max(1);
    let position = (literal_score.saturating_sub(lower_score) as f64 / span as f64).clamp(0.0, 1.0);
    if greater_than {
        (1.0 - position).clamp(0.05, 0.95)
    } else {
        position.clamp(0.05, 0.95)
    }
}

fn stats_range(stats: &[ColumnStat]) -> Option<(&Data, &Data)> {
    stats.iter().find_map(|stat| {
        let ColumnStat::RangeStat(range) = stat else {
            return None;
        };
        Some((&range.lower_bound, &range.upper_bound))
    })
}

fn comparison_value_to_data(value: &ComparisionValue) -> Option<Data> {
    match value {
        ComparisionValue::Column(_) => None,
        ComparisionValue::I32(value) => Some(Data::Int32(*value)),
        ComparisionValue::I64(value) => Some(Data::Int64(*value)),
        ComparisionValue::F32(value) => Some(Data::Float32(*value)),
        ComparisionValue::F64(value) => Some(Data::Float64(*value)),
        ComparisionValue::String(value) => Some(Data::String(value.clone())),
    }
}

fn numeric_data_value(value: &Data) -> Option<f64> {
    match value {
        Data::Int32(value) => Some(*value as f64),
        Data::Int64(value) => Some(*value as f64),
        Data::Float32(value) => Some(*value as f64),
        Data::Float64(value) => Some(*value),
        Data::String(_) => None,
    }
}

fn default_selectivity(operator: &ComparisionOperator) -> f64 {
    match operator {
        ComparisionOperator::EQ => 0.1,
        ComparisionOperator::NE => 0.9,
        ComparisionOperator::GT
        | ComparisionOperator::GTE
        | ComparisionOperator::LT
        | ComparisionOperator::LTE => 0.5,
    }
}

fn apply_selectivity(base_rows: u64, selectivity: f64) -> u64 {
    ((base_rows as f64) * selectivity)
        .round()
        .max(1.0)
        .min(u64::MAX as f64) as u64
}

fn apply_generic_predicate_penalty(
    rows: u64,
    predicate_count: usize,
    per_predicate_selectivity: f64,
) -> u64 {
    let mut selectivity = 1.0f64;
    for _ in 0..predicate_count.min(4) {
        selectivity *= per_predicate_selectivity;
    }
    apply_selectivity(rows.max(1), selectivity)
}

fn remap_project_predicates(predicates: &[Predicate], project: &ProjectData) -> Vec<Predicate> {
    predicates
        .iter()
        .filter_map(|predicate| {
            let column_name = project
                .column_name_map
                .iter()
                .find_map(|(from, to)| (to == &predicate.column_name).then_some(from.clone()))?;
            let value = match &predicate.value {
                ComparisionValue::Column(other) => ComparisionValue::Column(
                    project
                        .column_name_map
                        .iter()
                        .find_map(|(from, to)| (to == other).then_some(from.clone()))?,
                ),
                other => other.clone(),
            };
            Some(Predicate {
                column_name,
                operator: predicate.operator.clone(),
                value,
            })
        })
        .collect()
}
