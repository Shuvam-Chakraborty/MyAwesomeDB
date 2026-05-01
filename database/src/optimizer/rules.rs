use anyhow::{Result, anyhow};
use common::query::{
    ComparisionValue, CrossData, FilterData, Predicate, ProjectData, QueryOp, SortData,
};
use db_config::{DbContext, statistics::ColumnStat};
use std::collections::HashSet;

use crate::{
    estimation::{
        estimate_distinct_values, estimate_query_rows, estimate_query_rows_with_predicates,
        estimated_schema_row_bytes,
    },
    query_support::{
        PredicateSide, classify_predicate, extract_join_keys, find_table_spec, infer_schema,
        predicate_columns,
    },
    row::Schema,
    scan_pipeline::try_compile_scan_pipeline,
};

const DIRECT_HASH_BUILD_BUDGET_BYTES: u128 = 24 * 1024 * 1024;
const ORDERED_MERGE_JOIN_BONUS_FACTOR: u128 = 8;

pub fn optimize_op(op: &QueryOp, ctx: &DbContext) -> Result<QueryOp> {
    match op {
        QueryOp::Scan(s) => Ok(QueryOp::Scan(s.clone())),
        QueryOp::Project(p) => optimize_project(p, ctx),
        QueryOp::Sort(s) => optimize_sort(s, ctx),
        QueryOp::Cross(c) => optimize_cross(c, ctx),
        QueryOp::Filter(f) => optimize_filter(f, ctx),
    }
}

fn optimize_project(project: &ProjectData, ctx: &DbContext) -> Result<QueryOp> {
    let underlying = optimize_op(&project.underlying, ctx)?;
    let result = match underlying {
        QueryOp::Filter(f) => push_project_through_filter(project, f, ctx),
        QueryOp::Sort(s) => push_project_through_sort(project, s, ctx),
        QueryOp::Cross(c) => push_project_through_cross(project, c, ctx),
        QueryOp::Project(inner) => compose_projects(project, inner),
        other => Ok(QueryOp::Project(ProjectData {
            column_name_map: project.column_name_map.clone(),
            underlying: Box::new(other),
        })),
    }?;
    simplify_project(result, ctx)
}

fn optimize_sort(sort: &SortData, ctx: &DbContext) -> Result<QueryOp> {
    let underlying = optimize_op(&sort.underlying, ctx)?;
    let specs = dedup_sort_specs(&sort.sort_specs);
    if specs.is_empty() {
        return Ok(underlying);
    }
    if sort_redundant(&specs, &underlying, ctx)? {
        return Ok(underlying);
    }
    Ok(QueryOp::Sort(SortData {
        sort_specs: specs,
        underlying: Box::new(underlying),
    }))
}

fn optimize_cross(cross: &CrossData, ctx: &DbContext) -> Result<QueryOp> {
    let left = optimize_op(&cross.left, ctx)?;
    let right = optimize_op(&cross.right, ctx)?;
    let left_card = estimate_query_rows(&left, ctx);
    let right_card = estimate_query_rows(&right, ctx);
    if left_card < right_card {
        Ok(QueryOp::Cross(CrossData {
            left: Box::new(right),
            right: Box::new(left),
        }))
    } else {
        Ok(QueryOp::Cross(CrossData {
            left: Box::new(left),
            right: Box::new(right),
        }))
    }
}

fn optimize_filter(filter: &FilterData, ctx: &DbContext) -> Result<QueryOp> {
    let predicates = dedup_predicates(&filter.predicates);
    let underlying = optimize_op(&filter.underlying, ctx)?;
    match underlying {
        QueryOp::Filter(inner) => {
            let mut merged = inner.predicates;
            merged.extend(predicates);
            optimize_filter(
                &FilterData {
                    predicates: merged,
                    underlying: inner.underlying,
                },
                ctx,
            )
        }
        QueryOp::Project(p) => push_filter_through_project(&predicates, p, ctx),
        QueryOp::Sort(s) => {
            let pushed = FilterData {
                predicates,
                underlying: s.underlying,
            };
            Ok(QueryOp::Sort(SortData {
                sort_specs: s.sort_specs,
                underlying: Box::new(optimize_filter(&pushed, ctx)?),
            }))
        }
        QueryOp::Cross(c) => {
            let reordered = reorder_cross_tree(QueryOp::Cross(c), &predicates, ctx)?;
            match reordered {
                QueryOp::Cross(reordered_cross) => {
                    push_filter_through_cross(&predicates, reordered_cross, ctx)
                }
                other => wrap_filter(predicates, other),
            }
        }
        other => wrap_filter(predicates, other),
    }
}

fn push_filter_through_cross(
    predicates: &[Predicate],
    cross: CrossData,
    ctx: &DbContext,
) -> Result<QueryOp> {
    let left_schema = infer_schema(&cross.left, ctx)?;
    let right_schema = infer_schema(&cross.right, ctx)?;
    let mut left_preds = Vec::new();
    let mut right_preds = Vec::new();
    let mut remaining = Vec::new();
    for p in predicates {
        match classify_predicate(p, &left_schema, &right_schema) {
            PredicateSide::Left => left_preds.push(p.clone()),
            PredicateSide::Right => right_preds.push(p.clone()),
            PredicateSide::Cross => remaining.push(p.clone()),
        }
    }
    let left = optimize_op(&wrap_filter(left_preds, *cross.left)?, ctx)?;
    let right = optimize_op(&wrap_filter(right_preds, *cross.right)?, ctx)?;
    let left_card = estimate_query_rows(&left, ctx);
    let right_card = estimate_query_rows(&right, ctx);
    let (left, right) = if left_card < right_card {
        (right, left)
    } else {
        (left, right)
    };
    wrap_filter(
        remaining,
        QueryOp::Cross(CrossData {
            left: Box::new(left),
            right: Box::new(right),
        }),
    )
}

fn push_filter_through_project(
    predicates: &[Predicate],
    project: ProjectData,
    ctx: &DbContext,
) -> Result<QueryOp> {
    let remapped = remap_predicates(predicates, &project)?;
    let pushed = optimize_filter(
        &FilterData {
            predicates: remapped,
            underlying: project.underlying,
        },
        ctx,
    )?;
    optimize_project(
        &ProjectData {
            column_name_map: project.column_name_map,
            underlying: Box::new(pushed),
        },
        ctx,
    )
}

fn push_project_through_filter(
    project: &ProjectData,
    filter: FilterData,
    ctx: &DbContext,
) -> Result<QueryOp> {
    let reduced = projection_for_filter(project, &filter);
    let pushed = optimize_project(&reduced, ctx)?;
    Ok(QueryOp::Project(ProjectData {
        column_name_map: project.column_name_map.clone(),
        underlying: Box::new(QueryOp::Filter(FilterData {
            predicates: filter.predicates,
            underlying: Box::new(pushed),
        })),
    }))
}

fn push_project_through_sort(
    project: &ProjectData,
    sort: SortData,
    ctx: &DbContext,
) -> Result<QueryOp> {
    let reduced = projection_for_sort(project, &sort);
    let pushed = optimize_project(&reduced, ctx)?;
    Ok(QueryOp::Project(ProjectData {
        column_name_map: project.column_name_map.clone(),
        underlying: Box::new(QueryOp::Sort(SortData {
            sort_specs: sort.sort_specs,
            underlying: Box::new(pushed),
        })),
    }))
}

fn push_project_through_cross(
    project: &ProjectData,
    cross: CrossData,
    ctx: &DbContext,
) -> Result<QueryOp> {
    let left_schema = infer_schema(&cross.left, ctx)?;
    let right_schema = infer_schema(&cross.right, ctx)?;
    let left_map = projection_for_side(project, &left_schema);
    let right_map = projection_for_side(project, &right_schema);
    Ok(QueryOp::Project(ProjectData {
        column_name_map: project.column_name_map.clone(),
        underlying: Box::new(QueryOp::Cross(CrossData {
            left: Box::new(wrap_project_if_needed(left_map, left_schema, *cross.left)),
            right: Box::new(wrap_project_if_needed(
                right_map,
                right_schema,
                *cross.right,
            )),
        })),
    }))
}

fn compose_projects(outer: &ProjectData, inner: ProjectData) -> Result<QueryOp> {
    let mut composed = Vec::with_capacity(outer.column_name_map.len());
    for (outer_from, outer_to) in &outer.column_name_map {
        let inner_from = inner
            .column_name_map
            .iter()
            .find_map(|(f, t)| (t == outer_from).then(|| f.clone()))
            .ok_or_else(|| anyhow!("compose projects failed"))?;
        composed.push((inner_from, outer_to.clone()));
    }
    Ok(QueryOp::Project(ProjectData {
        column_name_map: composed,
        underlying: inner.underlying,
    }))
}

fn simplify_project(op: QueryOp, ctx: &DbContext) -> Result<QueryOp> {
    if let QueryOp::Project(ref p) = op {
        let schema = infer_schema(&p.underlying, ctx)?;
        if is_identity(&p.column_name_map, &schema) {
            return Ok(*p.underlying.clone());
        }
    }
    Ok(op)
}

fn sort_redundant(
    specs: &[common::query::SortSpec],
    op: &QueryOp,
    ctx: &DbContext,
) -> Result<bool> {
    if specs.len() != 1 || !specs[0].ascending {
        return Ok(false);
    }
    let Some((table_id, col)) = trace_to_scan(op, &specs[0].column_name) else {
        return Ok(false);
    };
    let table = find_table_spec(ctx, &table_id)?;
    let Some(col_spec) = table.column_specs.iter().find(|c| c.column_name == col) else {
        return Ok(false);
    };
    Ok(col_spec
        .stats
        .as_ref()
        .map(|s| {
            s.iter()
                .any(|st| matches!(st, ColumnStat::IsPhysicallyOrdered))
        })
        .unwrap_or(false))
}

fn trace_to_scan(op: &QueryOp, col: &str) -> Option<(String, String)> {
    match op {
        QueryOp::Scan(s) => Some((s.table_id.clone(), col.to_string())),
        QueryOp::Filter(f) => trace_to_scan(&f.underlying, col),
        QueryOp::Project(p) => {
            let src = p
                .column_name_map
                .iter()
                .find_map(|(f, t)| (t == col).then(|| f.clone()))?;
            trace_to_scan(&p.underlying, &src)
        }
        _ => None,
    }
}

fn wrap_filter(predicates: Vec<Predicate>, underlying: QueryOp) -> Result<QueryOp> {
    if predicates.is_empty() {
        Ok(underlying)
    } else {
        Ok(QueryOp::Filter(FilterData {
            predicates,
            underlying: Box::new(underlying),
        }))
    }
}

fn wrap_project_if_needed(
    map: Vec<(String, String)>,
    schema: Schema,
    underlying: QueryOp,
) -> QueryOp {
    if is_identity(&map, &schema) {
        return underlying;
    }
    QueryOp::Project(ProjectData {
        column_name_map: map,
        underlying: Box::new(underlying),
    })
}

fn dedup_predicates(predicates: &[Predicate]) -> Vec<Predicate> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for p in predicates {
        let key = predicate_key(p);
        if seen.insert(key) {
            out.push(p.clone());
        }
    }
    out
}

fn predicate_key(p: &Predicate) -> String {
    let val = match &p.value {
        ComparisionValue::Column(n) => format!("C:{n}"),
        ComparisionValue::I32(v) => format!("I32:{v}"),
        ComparisionValue::I64(v) => format!("I64:{v}"),
        ComparisionValue::F32(v) => format!("F32:{v:?}"),
        ComparisionValue::F64(v) => format!("F64:{v:?}"),
        ComparisionValue::String(v) => format!("S:{v}"),
    };
    format!("{}|{:?}|{}", p.column_name, p.operator, val)
}

fn dedup_sort_specs(specs: &[common::query::SortSpec]) -> Vec<common::query::SortSpec> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for s in specs {
        if seen.insert(s.column_name.clone()) {
            out.push(s.clone());
        }
    }
    out
}

fn projection_for_filter(project: &ProjectData, filter: &FilterData) -> ProjectData {
    let mut seen = HashSet::new();
    let mut map = Vec::new();
    for (from, _) in &project.column_name_map {
        if seen.insert(from.clone()) {
            map.push((from.clone(), from.clone()));
        }
    }
    for p in &filter.predicates {
        if seen.insert(p.column_name.clone()) {
            map.push((p.column_name.clone(), p.column_name.clone()));
        }
        if let ComparisionValue::Column(c) = &p.value {
            if seen.insert(c.clone()) {
                map.push((c.clone(), c.clone()));
            }
        }
    }
    ProjectData {
        column_name_map: map,
        underlying: filter.underlying.clone(),
    }
}

fn projection_for_sort(project: &ProjectData, sort: &SortData) -> ProjectData {
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

fn projection_for_side(project: &ProjectData, schema: &Schema) -> Vec<(String, String)> {
    let mut seen = HashSet::new();
    let mut map = Vec::new();
    for (from, _) in &project.column_name_map {
        if schema.index_of(from).is_some() && seen.insert(from.clone()) {
            map.push((from.clone(), from.clone()));
        }
    }
    map
}

fn remap_predicates(predicates: &[Predicate], project: &ProjectData) -> Result<Vec<Predicate>> {
    predicates
        .iter()
        .map(|p| {
            Ok(Predicate {
                column_name: remap_col(&p.column_name, project)?,
                operator: p.operator.clone(),
                value: match &p.value {
                    ComparisionValue::Column(c) => ComparisionValue::Column(remap_col(c, project)?),
                    other => other.clone(),
                },
            })
        })
        .collect()
}

fn remap_col(col: &str, project: &ProjectData) -> Result<String> {
    project
        .column_name_map
        .iter()
        .find_map(|(f, t)| (t == col).then(|| f.clone()))
        .ok_or_else(|| anyhow!("remap failed for {col}"))
}

fn is_identity(map: &[(String, String)], schema: &Schema) -> bool {
    map.len() == schema.columns().len()
        && map
            .iter()
            .zip(schema.columns())
            .all(|((f, t), c)| f == &c.name && t == &c.name)
}

fn reorder_cross_tree(op: QueryOp, predicates: &[Predicate], ctx: &DbContext) -> Result<QueryOp> {
    let mut inputs = Vec::new();
    flatten_cross_inputs(op, &mut inputs);
    if inputs.len() <= 2 {
        return Ok(rebuild_left_deep_cross(inputs));
    }

    let schemas = inputs
        .iter()
        .map(|input| infer_schema(input, ctx))
        .collect::<Result<Vec<_>>>()?;
    let total_masks = 1usize << inputs.len();
    let mut best_plans: Vec<Option<PlanState>> = vec![None; total_masks];

    for (index, input) in inputs.iter().enumerate() {
        let schema = schemas[index].clone();
        let cost = seed_plan_score(index, input, &schema, &inputs, predicates, ctx)? as u128;
        best_plans[1usize << index] = Some(PlanState {
            plan: input.clone(),
            schema,
            cost,
        });
    }

    for mask in 1usize..total_masks {
        let Some(state) = best_plans[mask].clone() else {
            continue;
        };
        let available_connectivity: Vec<(usize, usize)> = (0..inputs.len())
            .filter(|next_index| mask & (1usize << next_index) == 0)
            .map(|next_index| {
                (
                    next_index,
                    count_connecting_predicates(predicates, &state.schema, &schemas[next_index]),
                )
            })
            .collect();
        let has_connected_extension = available_connectivity
            .iter()
            .any(|(_, connectivity)| *connectivity > 0);

        for next_index in 0..inputs.len() {
            if mask & (1usize << next_index) != 0 {
                continue;
            }

            let next_input = &inputs[next_index];
            let next_schema = &schemas[next_index];
            let connectivity = count_connecting_predicates(predicates, &state.schema, next_schema);
            if has_connected_extension && connectivity == 0 {
                continue;
            }
            let extension_cost = if connectivity > 0 {
                estimate_join_extension_score(
                    &state.plan,
                    &state.schema,
                    next_input,
                    next_schema,
                    predicates,
                    ctx,
                )?
            } else {
                let rows =
                    estimate_input_with_local_filters(next_input, next_schema, predicates, ctx)?
                        as u128;
                rows.saturating_mul(estimated_schema_row_bytes(next_schema) as u128)
                    .saturating_mul(100)
            };

            let new_mask = mask | (1usize << next_index);
            let new_state = PlanState {
                plan: QueryOp::Cross(CrossData {
                    left: Box::new(state.plan.clone()),
                    right: Box::new(next_input.clone()),
                }),
                schema: Schema::combine(&state.schema, next_schema),
                cost: state.cost.saturating_add(extension_cost),
            };

            let should_replace = match &best_plans[new_mask] {
                Some(existing) => new_state.cost < existing.cost,
                None => true,
            };
            if should_replace {
                best_plans[new_mask] = Some(new_state);
            }
        }
    }

    let final_mask = total_masks - 1;
    Ok(best_plans[final_mask]
        .clone()
        .map(|state| state.plan)
        .unwrap_or_else(|| rebuild_left_deep_cross(inputs)))
}

#[derive(Clone)]
struct PlanState {
    plan: QueryOp,
    schema: Schema,
    cost: u128,
}

fn seed_plan_score(
    index: usize,
    input: &QueryOp,
    schema: &Schema,
    inputs: &[QueryOp],
    predicates: &[Predicate],
    ctx: &DbContext,
) -> Result<u64> {
    let degree = inputs
        .iter()
        .enumerate()
        .filter(|(other_index, _)| *other_index != index)
        .map(|(_, other)| infer_schema(other, ctx))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .map(|other_schema| count_connecting_predicates(predicates, schema, &other_schema))
        .sum::<usize>()
        .max(1) as u64;
    let rows = estimate_input_with_local_filters(input, schema, predicates, ctx)?;
    let width = estimated_schema_row_bytes(schema) as u64;
    Ok(rows.saturating_mul(width.max(1)).saturating_div(degree))
}

fn flatten_cross_inputs(op: QueryOp, out: &mut Vec<QueryOp>) {
    match op {
        QueryOp::Cross(cross) => {
            flatten_cross_inputs(*cross.left, out);
            flatten_cross_inputs(*cross.right, out);
        }
        other => out.push(other),
    }
}

fn rebuild_left_deep_cross(mut inputs: Vec<QueryOp>) -> QueryOp {
    let mut iter = inputs.drain(..);
    let mut plan = iter.next().expect("cross inputs must not be empty");
    for next in iter {
        plan = QueryOp::Cross(CrossData {
            left: Box::new(plan),
            right: Box::new(next),
        });
    }
    plan
}

fn estimate_input_with_local_filters(
    input: &QueryOp,
    schema: &Schema,
    predicates: &[Predicate],
    ctx: &DbContext,
) -> Result<u64> {
    let local_predicates: Vec<Predicate> = predicates
        .iter()
        .filter(|predicate| predicate_belongs_to_schema(predicate, schema))
        .cloned()
        .collect();

    if local_predicates.is_empty() {
        Ok(estimate_query_rows(input, ctx))
    } else {
        Ok(estimate_query_rows_with_predicates(
            input,
            &local_predicates,
            ctx,
        ))
    }
}

fn estimate_join_extension_score(
    plan: &QueryOp,
    plan_schema: &Schema,
    input: &QueryOp,
    input_schema: &Schema,
    predicates: &[Predicate],
    ctx: &DbContext,
) -> Result<u128> {
    let joined = QueryOp::Cross(CrossData {
        left: Box::new(plan.clone()),
        right: Box::new(input.clone()),
    });
    let combined_schema = Schema::combine(plan_schema, input_schema);
    let relevant_predicates: Vec<Predicate> = predicates
        .iter()
        .filter(|predicate| predicate_belongs_to_schema(predicate, &combined_schema))
        .cloned()
        .collect();
    let joined_rows =
        estimate_query_rows_with_predicates(&joined, &relevant_predicates, ctx) as u128;
    let added_width = estimated_schema_row_bytes(input_schema) as u128;
    let connectivity =
        count_connecting_predicates(predicates, plan_schema, input_schema).max(1) as u128;
    let base_cost = joined_rows
        .saturating_mul(added_width.max(1))
        .saturating_div(connectivity);

    if let Some(merge_cost) =
        estimate_ordered_merge_join_score(plan, plan_schema, input, input_schema, predicates, ctx)?
    {
        return Ok(base_cost.min(merge_cost));
    }

    Ok(base_cost)
}

fn estimate_ordered_merge_join_score(
    plan: &QueryOp,
    plan_schema: &Schema,
    input: &QueryOp,
    input_schema: &Schema,
    predicates: &[Predicate],
    ctx: &DbContext,
) -> Result<Option<u128>> {
    let plan_local_predicates: Vec<Predicate> = predicates
        .iter()
        .filter(|predicate| predicate_belongs_to_schema(predicate, plan_schema))
        .cloned()
        .collect();
    let input_local_predicates: Vec<Predicate> = predicates
        .iter()
        .filter(|predicate| predicate_belongs_to_schema(predicate, input_schema))
        .cloned()
        .collect();
    let join_predicates: Vec<Predicate> = predicates
        .iter()
        .filter(|predicate| predicate_connects_schemas(predicate, plan_schema, input_schema))
        .cloned()
        .collect();

    let Some((plan_keys, input_keys)) =
        extract_join_keys(&join_predicates, plan_schema, input_schema)?
    else {
        return Ok(None);
    };
    if plan_keys.len() != 1 || input_keys.len() != 1 {
        return Ok(None);
    }

    let wrapped_plan = wrap_filter(plan_local_predicates, plan.clone())?;
    let wrapped_input = wrap_filter(input_local_predicates, input.clone())?;
    let Some(plan_pipeline) = try_compile_scan_pipeline(&wrapped_plan, ctx)? else {
        return Ok(None);
    };
    let Some(input_pipeline) = try_compile_scan_pipeline(&wrapped_input, ctx)? else {
        return Ok(None);
    };

    if !plan_pipeline.output_physically_ordered(plan_keys[0])
        || !input_pipeline.output_physically_ordered(input_keys[0])
    {
        return Ok(None);
    }

    let plan_key_name = plan_schema
        .column_at(plan_keys[0])
        .ok_or_else(|| anyhow!("missing plan merge key"))?
        .name
        .clone();
    let input_key_name = input_schema
        .column_at(input_keys[0])
        .ok_or_else(|| anyhow!("missing input merge key"))?
        .name
        .clone();
    let plan_rows = estimate_query_rows(&wrapped_plan, ctx).max(1) as u128;
    let input_rows = estimate_query_rows(&wrapped_input, ctx).max(1) as u128;
    let plan_distinct = estimate_distinct_values(&wrapped_plan, &plan_key_name, ctx).unwrap_or(0);
    let input_distinct =
        estimate_distinct_values(&wrapped_input, &input_key_name, ctx).unwrap_or(0);
    if (plan_distinct as u128) < plan_rows && (input_distinct as u128) < input_rows {
        return Ok(None);
    }

    let plan_width = estimated_schema_row_bytes(plan_schema) as u128;
    let input_width = estimated_schema_row_bytes(input_schema) as u128;
    let smaller_side_bytes = (plan_rows.saturating_mul(plan_width))
        .min(input_rows.saturating_mul(input_width))
        .max(1);
    let spill_factor = (smaller_side_bytes / DIRECT_HASH_BUILD_BUDGET_BYTES).max(1);
    let merge_cost = plan_rows
        .saturating_add(input_rows)
        .saturating_mul(plan_width.saturating_add(input_width).max(1))
        .saturating_div(
            ORDERED_MERGE_JOIN_BONUS_FACTOR
                .saturating_mul(spill_factor)
                .max(1),
        );

    Ok(Some(merge_cost.max(1)))
}

fn predicate_belongs_to_schema(predicate: &Predicate, schema: &Schema) -> bool {
    predicate_columns(predicate)
        .into_iter()
        .all(|column| schema.index_of(column).is_some())
}

fn predicate_connects_schemas(predicate: &Predicate, left: &Schema, right: &Schema) -> bool {
    let mut saw_left = false;
    let mut saw_right = false;

    for column in predicate_columns(predicate) {
        match (
            left.index_of(column).is_some(),
            right.index_of(column).is_some(),
        ) {
            (true, false) => saw_left = true,
            (false, true) => saw_right = true,
            _ => return false,
        }
    }

    saw_left && saw_right
}

fn count_connecting_predicates(predicates: &[Predicate], left: &Schema, right: &Schema) -> usize {
    predicates
        .iter()
        .filter(|predicate| predicate_connects_schemas(predicate, left, right))
        .count()
}
