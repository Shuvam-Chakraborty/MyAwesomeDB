use anyhow::{Result, anyhow};
use common::query::{ComparisionOperator, ComparisionValue, Predicate, QueryOp};
use db_config::{DbContext, table::TableSpec};

use crate::row::Schema;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum PredicateSide {
    Left,
    Right,
    Cross,
}

pub fn infer_schema(op: &QueryOp, ctx: &DbContext) -> Result<Schema> {
    match op {
        QueryOp::Scan(scan) => Ok(Schema::from_table_spec(find_table_spec(ctx, &scan.table_id)?)),
        QueryOp::Filter(filter) => infer_schema(&filter.underlying, ctx),
        QueryOp::Project(project) => {
            let child_schema = infer_schema(&project.underlying, ctx)?;
            let (projected_schema, _) = child_schema.project(&project.column_name_map)?;
            Ok(projected_schema)
        }
        QueryOp::Cross(cross) => {
            let left_schema = infer_schema(&cross.left, ctx)?;
            let right_schema = infer_schema(&cross.right, ctx)?;
            Ok(Schema::combine(&left_schema, &right_schema))
        }
        QueryOp::Sort(sort) => infer_schema(&sort.underlying, ctx),
    }
}

pub fn find_table_spec<'a>(ctx: &'a DbContext, table_id: &str) -> Result<&'a TableSpec> {
    ctx.table_specs()
        .iter()
        .find(|table| table.name == table_id || table.file_id == table_id)
        .ok_or_else(|| anyhow!("unknown table: {table_id}"))
}

pub fn split_cross_predicates(
    predicates: &[Predicate],
    left_schema: &Schema,
    right_schema: &Schema,
) -> (Vec<Predicate>, Vec<Predicate>, Vec<Predicate>) {
    let mut left_predicates = Vec::new();
    let mut right_predicates = Vec::new();
    let mut cross_predicates = Vec::new();

    for predicate in predicates {
        match classify_predicate(predicate, left_schema, right_schema) {
            PredicateSide::Left => left_predicates.push(predicate.clone()),
            PredicateSide::Right => right_predicates.push(predicate.clone()),
            PredicateSide::Cross => cross_predicates.push(predicate.clone()),
        }
    }

    (left_predicates, right_predicates, cross_predicates)
}

pub fn extract_join_keys(
    predicates: &[Predicate],
    left_schema: &Schema,
    right_schema: &Schema,
) -> Result<Option<(Vec<usize>, Vec<usize>)>> {
    let mut left_keys = Vec::new();
    let mut right_keys = Vec::new();

    for predicate in predicates {
        if !matches!(predicate.operator, ComparisionOperator::EQ) {
            continue;
        }

        let ComparisionValue::Column(other_column) = &predicate.value else {
            continue;
        };

        let left_column = left_schema.index_of(&predicate.column_name);
        let right_column = right_schema.index_of(&predicate.column_name);
        let left_other_column = left_schema.index_of(other_column);
        let right_other_column = right_schema.index_of(other_column);

        match (
            left_column,
            right_column,
            left_other_column,
            right_other_column,
        ) {
            (Some(left_index), None, None, Some(right_index)) => {
                left_keys.push(left_index);
                right_keys.push(right_index);
            }
            (None, Some(right_index), Some(left_index), None) => {
                left_keys.push(left_index);
                right_keys.push(right_index);
            }
            _ => {}
        }
    }

    if left_keys.is_empty() {
        Ok(None)
    } else {
        Ok(Some((left_keys, right_keys)))
    }
}

pub fn classify_predicate(
    predicate: &Predicate,
    left_schema: &Schema,
    right_schema: &Schema,
) -> PredicateSide {
    let mut references_left = false;
    let mut references_right = false;

    for column_name in predicate_columns(predicate) {
        match (
            left_schema.index_of(column_name).is_some(),
            right_schema.index_of(column_name).is_some(),
        ) {
            (true, false) => references_left = true,
            (false, true) => references_right = true,
            _ => return PredicateSide::Cross,
        }
    }

    match (references_left, references_right) {
        (true, false) => PredicateSide::Left,
        (false, true) => PredicateSide::Right,
        _ => PredicateSide::Cross,
    }
}

pub fn predicate_columns(predicate: &Predicate) -> Vec<&str> {
    let mut columns = vec![predicate.column_name.as_str()];
    if let ComparisionValue::Column(other_column) = &predicate.value {
        columns.push(other_column.as_str());
    }
    columns
}
