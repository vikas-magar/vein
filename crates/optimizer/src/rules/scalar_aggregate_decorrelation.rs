//! Decorrelation for scalar aggregate subqueries.

mod global_projection;
mod correlated_projection;

use error::Result;
use parser::ast::expr::BinaryOp;
use plan::{
    LogicalExpr, LogicalPlan, PlanTraversal, ResolvedColumn, ResolvedColumnRef, VeinDataType,
    VeinSchema,
};
use std::sync::Arc;
use crate::context::{OptimizerContext, OptimizerPhase};
use crate::rule::{OptimizerRule, RuleType};

/// Rewrites a supported correlated scalar aggregate into a grouped aggregate
/// and a left join. This keeps evaluation set based and avoids per-row
/// subquery execution.
pub struct ScalarAggregateDecorrelationRule;

struct Decorrelation {
    right: LogicalPlan,
    correlations: Vec<CorrelationPair>,
    join_kind: ScalarJoinKind,
    value: ResolvedColumnRef,
    aggregate_name: String,
}

struct CorrelationPair {
    outer: ResolvedColumnRef,
    inner: ResolvedColumnRef,
}

#[derive(Clone, Copy)]
enum ScalarJoinKind {
    Cross,
    Left,
    Single,
}

fn distinct_inner_keys(correlations: &[CorrelationPair]) -> Vec<ResolvedColumnRef> {
    let mut seen = std::collections::HashSet::with_capacity(correlations.len());
    correlations
        .iter()
        .filter_map(|pair| {
            seen.insert(pair.inner.id)
                .then(|| ResolvedColumnRef::clone(&pair.inner))
        })
        .collect()
}

impl OptimizerRule for ScalarAggregateDecorrelationRule {
    fn name(&self) -> &str {
        "ScalarAggregateDecorrelationRule"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Heuristic
    }

    fn phase(&self) -> OptimizerPhase {
        OptimizerPhase::Heuristic
    }

    fn apply(&self, plan: &LogicalPlan, _context: &OptimizerContext) -> Result<Option<LogicalPlan>> {
        match plan {
            LogicalPlan::Filter { input, predicate } => {
                let mut decorrelations = Vec::new();
                let (predicate, changed) =
                    rewrite_expression(predicate.clone(), input.schema(), &mut decorrelations)?;
                if !changed {
                    return Ok(None);
                }

                let outer_schema = input.schema().clone();
                let input = attach_scalar_joins((**input).clone(), decorrelations);
                let filtered = LogicalPlan::Filter {
                    input: Box::new(input),
                    predicate,
                };
                let exprs = outer_schema
                    .fields
                    .iter()
                    .cloned()
                    .map(LogicalExpr::Column)
                    .collect();
                Ok(Some(LogicalPlan::Project {
                    input: Box::new(filtered),
                    exprs,
                    schema: Box::new(outer_schema),
                }))
            }
            LogicalPlan::Project {
                input,
                exprs,
                schema,
            } => {
                let mut decorrelations = Vec::new();
                let mut changed = false;
                let exprs = exprs
                    .iter()
                    .map(|expr| {
                        let (expr, expr_changed) =
                            rewrite_expression(expr.clone(), input.schema(), &mut decorrelations)?;
                        changed |= expr_changed;
                        Ok(expr)
                    })
                    .collect::<Result<Vec<_>>>()?;
                if !changed {
                    return Ok(None);
                }

                // A scalar subquery may produce NULL when its correlated input
                // has no match. Preserve that fact in the restored projection
                // schema for every expression that consumes its joined value.
                let output_schema = VeinSchema::new(
                    schema
                        .fields
                        .iter()
                        .enumerate()
                        .map(|(index, field)| {
                            let consumes_scalar_result = exprs.get(index).is_some_and(|expr| {
                                decorrelations.iter().any(|decorrelation| {
                                    references_column(expr, decorrelation.value.id)
                                })
                            });
                            if consumes_scalar_result {
                                nullable_column(field)
                            } else {
                                ResolvedColumnRef::clone(field)
                            }
                        })
                        .collect(),
                );
                let input = attach_scalar_joins((**input).clone(), decorrelations);
                Ok(Some(LogicalPlan::Project {
                    input: Box::new(input),
                    exprs,
                    schema: Box::new(output_schema),
                }))
            }
            LogicalPlan::Sort {
                input,
                sort_keys,
                schema,
            } => {
                let mut decorrelations = Vec::new();
                let mut changed = false;
                let sort_keys = sort_keys
                    .iter()
                    .map(|key| {
                        let (expr, expr_changed) = rewrite_expression(
                            key.expr.clone(),
                            input.schema(),
                            &mut decorrelations,
                        )?;
                        changed |= expr_changed;
                        Ok(plan::SortKey {
                            expr,
                            asc: key.asc,
                            nulls_first: key.nulls_first,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                if !changed {
                    return Ok(None);
                }

                let output_schema = schema.clone();
                let input = attach_scalar_joins((**input).clone(), decorrelations);
                let sort_schema = Box::new(input.schema().clone());
                let sorted = LogicalPlan::Sort {
                    input: Box::new(input),
                    sort_keys,
                    schema: sort_schema,
                };
                let exprs = output_schema
                    .fields
                    .iter()
                    .cloned()
                    .map(LogicalExpr::Column)
                    .collect();
                Ok(Some(LogicalPlan::Project {
                    input: Box::new(sorted),
                    exprs,
                    schema: output_schema,
                }))
            }
            _ => Ok(None),
        }
    }
}

/// Adds each decorrelated scalar input beneath the expression that consumes it.
/// The join schema includes only outer columns and the scalar helper columns;
/// the caller restores its original output schema above this node.
fn attach_scalar_joins(mut input: LogicalPlan, decorrelations: Vec<Decorrelation>) -> LogicalPlan {
    for decorrelation in decorrelations {
        let mut fields = input.schema().fields.clone();
        fields.extend(decorrelation.right.schema().fields.iter().map(|field| {
            if !matches!(decorrelation.join_kind, ScalarJoinKind::Cross) {
                nullable_column(field)
            } else {
                ResolvedColumnRef::clone(field)
            }
        }));
        let schema = VeinSchema::new(fields);
        let join_type = match decorrelation.join_kind {
            ScalarJoinKind::Cross => parser::ast::pipe_op::JoinType::Cross,
            ScalarJoinKind::Left => parser::ast::pipe_op::JoinType::Left,
            ScalarJoinKind::Single => parser::ast::pipe_op::JoinType::Single,
        };
        let on = conjunction(
            decorrelation
                .correlations
                .into_iter()
                .map(|pair| equality_expr(pair.outer, pair.inner))
                .collect(),
        );
        tracing::debug!(
            target: "vein.audit",
            event = "scalar_subquery_decorrelated",
            kind = %decorrelation.aggregate_name,
            "scalar_subquery_decorrelated"
        );
        input = LogicalPlan::Join {
            join_type,
            left: Box::new(input),
            right: Box::new(decorrelation.right),
            on,
            using: Vec::new(),
            schema: Box::new(schema),
        };
    }
    input
}

fn rewrite_expression(
    expr: LogicalExpr,
    outer_schema: &VeinSchema,
    decorrelations: &mut Vec<Decorrelation>,
) -> Result<(LogicalExpr, bool)> {
    if let LogicalExpr::ScalarSubquery(subquery) = expr {
        if let Some(decorrelation) = decorrelate_scalar_aggregate(&subquery, outer_schema) {
            let value = LogicalExpr::Column(ResolvedColumnRef::clone(&decorrelation.value));
            decorrelations.push(decorrelation);
            return Ok((value, true));
        }
        if let Some(decorrelation) = decorrelate_uncorrelated_scalar(&subquery) {
            let value = LogicalExpr::Column(ResolvedColumnRef::clone(&decorrelation.value));
            decorrelations.push(decorrelation);
            return Ok((value, true));
        }
        if let Some(decorrelation) = decorrelate_correlated_scalar_row(&subquery, outer_schema) {
            let value = LogicalExpr::Column(ResolvedColumnRef::clone(&decorrelation.value));
            decorrelations.push(decorrelation);
            return Ok((value, true));
        }
        return Ok((LogicalExpr::ScalarSubquery(subquery), false));
    }

    let mut changed = false;
    let rewritten = expr.map_children(&mut |child| {
        let (child, child_changed) = rewrite_expression(child, outer_schema, decorrelations)?;
        changed |= child_changed;
        Ok(child)
    })?;
    Ok((rewritten, changed))
}

/// Converts a one-column, uncorrelated row subquery into a one-row global
/// aggregate. SINGLE_VALUE preserves scalar cardinality errors while letting
/// the normal relational planner execute the subquery once for the whole query.
fn decorrelate_uncorrelated_scalar(subquery: &LogicalPlan) -> Option<Decorrelation> {
    if !supports_uncorrelated_scalar_input(subquery) || subquery.schema().fields.len() != 1 {
        return None;
    }
    let source_value = ResolvedColumnRef::clone(subquery.schema().fields.first()?);
    let value = nullable_column(&source_value);
    let aggregate = LogicalPlan::Aggregate {
        input: Box::new(subquery.clone()),
        grouping_sets: vec![Vec::new()],
        aggs: vec![LogicalExpr::FunctionCall {
            name: "single_value".to_string(),
            args: vec![LogicalExpr::Column(source_value)],
            is_aggregate: true,
            distinct: false,
            return_type: value.data_type.clone(),
        }],
        schema: Box::new(VeinSchema::new(vec![Arc::clone(&value)])),
    };
    Some(Decorrelation {
        right: aggregate,
        correlations: Vec::new(),
        join_kind: ScalarJoinKind::Cross,
        value,
        aggregate_name: "SINGLE_VALUE".to_string(),
    })
}

/// Only traverse relational shapes whose expressions and schemas are preserved
/// by the scalar rewrite. Unknown operators stay untouched for a clear planner
/// boundary instead of risking an incorrect correlation rewrite.
fn supports_uncorrelated_scalar_input(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Scan { filters, .. } => filters.iter().all(is_local_scalar_input_expr),
        LogicalPlan::ExternalScan { .. } => true,
        LogicalPlan::Filter { input, predicate } => {
            is_local_scalar_input_expr(predicate) && supports_uncorrelated_scalar_input(input)
        }
        LogicalPlan::Project { input, exprs, .. } => {
            exprs.iter().all(is_local_scalar_input_expr)
                && supports_uncorrelated_scalar_input(input)
        }
        LogicalPlan::SubqueryAlias { input, .. } => supports_uncorrelated_scalar_input(input),
        _ => false,
    }
}

fn is_local_scalar_input_expr(expr: &LogicalExpr) -> bool {
    !contains_outer_column(expr) && expr.subquery_plans().is_empty()
}

fn decorrelate_scalar_aggregate(
    subquery: &LogicalPlan,
    outer_schema: &VeinSchema,
) -> Option<Decorrelation> {
    if let Some(decorrelation) = global_projection::decorrelate(subquery) {
        return Some(decorrelation);
    }
    if let Some(decorrelation) = correlated_projection::decorrelate(subquery, outer_schema) {
        return Some(decorrelation);
    }

    let LogicalPlan::Aggregate {
        input,
        grouping_sets,
        aggs,
        schema,
    } = subquery
    else {
        return None;
    };
    if grouping_sets.len() != 1
        || !grouping_sets[0].is_empty()
        || aggs.len() != 1
        || schema.fields.len() != 1
    {
        return None;
    }
    let mut aggregate_calls = Vec::new();
    collect_aggregate_calls(&aggs[0], &mut aggregate_calls);
    let [LogicalExpr::FunctionCall {
        name,
        is_aggregate: true,
        distinct: false,
        return_type,
        ..
    }] = aggregate_calls.as_slice()
    else {
        return None;
    };
    if !matches!(name.to_ascii_uppercase().as_str(), "COUNT" | "MIN" | "MAX" | "SUM" | "AVG") {
        return None;
    }
    let value = ResolvedColumnRef::clone(schema.fields.first()?);

    let mut base = input.as_ref();
    let mut terms = Vec::new();
    while let LogicalPlan::Filter { input, predicate } = base {
        split_conjunction(predicate, &mut terms);
        base = input;
    }

    let mut correlations = Vec::new();
    let mut residual = Vec::new();
    for term in terms {
        if let Some((inner, outer)) = equality_correlation(&term) {
            if base.schema().position_of_id(inner.id).is_none()
                || outer_schema.position_of_id(outer.id).is_none()
            {
                return None;
            }
            if !correlations
                .iter()
                .any(|pair: &CorrelationPair| pair.inner.id == inner.id && pair.outer.id == outer.id)
            {
                correlations.push(CorrelationPair { outer, inner });
            }
        } else if contains_outer_column(&term) {
            return None;
        } else {
            residual.push(term);
        }
    }
    if name.eq_ignore_ascii_case("count") && !correlations.is_empty() {
        // A missing grouped COUNT is zero, but a LEFT JOIN represents it as
        // NULL. Keep correlated COUNT unchanged until that default is modeled.
        return None;
    }
    if contains_outer_column(&aggs[0]) {
        return None;
    }

    let mut grouped_input = base.clone();
    if let Some(predicate) = conjunction(residual) {
        grouped_input = LogicalPlan::Filter {
            input: Box::new(grouped_input),
            predicate,
        };
    }

    let aggregate_value = Arc::new(
        ResolvedColumn::new(value.id, value.name.clone(), value.qualifier.clone(), return_type.clone())
            .with_nullable(!name.eq_ignore_ascii_case("count")),
    );
    let mut grouped_aggs = Vec::with_capacity(1);
    let mut replacement_count = 0;
    let rewritten_value = replace_aggregate_calls(
        aggs[0].clone(),
        &aggregate_value,
        &mut replacement_count,
        &mut grouped_aggs,
    )
        .ok()?;
    if replacement_count != 1 || grouped_aggs.len() != 1 {
        return None;
    }

    let inner_keys = distinct_inner_keys(&correlations);
    let grouping_sets = if inner_keys.is_empty() {
        vec![Vec::new()]
    } else {
        vec![inner_keys.iter().cloned().map(LogicalExpr::Column).collect()]
    };
    let mut aggregate_fields = inner_keys;
    aggregate_fields.push(Arc::clone(&aggregate_value));
    let aggregate = LogicalPlan::Aggregate {
        input: Box::new(grouped_input),
        grouping_sets,
        aggs: grouped_aggs,
        schema: Box::new(VeinSchema::new(aggregate_fields)),
    };
    let (right, projected_value) = if rewritten_value == LogicalExpr::Column(Arc::clone(&aggregate_value)) {
        (aggregate, aggregate_value)
    } else {
        let mut exprs = Vec::with_capacity(2);
        let mut fields = Vec::with_capacity(2);
        for inner_key in distinct_inner_keys(&correlations) {
            exprs.push(LogicalExpr::Column(ResolvedColumnRef::clone(&inner_key)));
            fields.push(inner_key);
        }
        exprs.push(rewritten_value);
        fields.push(ResolvedColumnRef::clone(&value));
        let projected = LogicalPlan::Project {
            input: Box::new(aggregate),
            exprs,
            schema: Box::new(VeinSchema::new(fields)),
        };
        (projected, value)
    };

    Some(Decorrelation {
        right,
        join_kind: if correlations.is_empty() {
            ScalarJoinKind::Cross
        } else {
            ScalarJoinKind::Left
        },
        correlations,
        value: projected_value,
        aggregate_name: name.clone(),
    })
}

/// Rewrites a one-column correlated row subquery to a single-match join.
/// The right projection carries its value and the required key columns as
/// Arrow array references; it does not materialize joined row values.
fn decorrelate_correlated_scalar_row(
    subquery: &LogicalPlan,
    outer_schema: &VeinSchema,
) -> Option<Decorrelation> {
    let LogicalPlan::Project { input, exprs, schema } = subquery else {
        return None;
    };
    if exprs.len() != 1 || schema.fields.len() != 1 {
        return None;
    }
    let value_expression = &exprs[0];
    if contains_outer_column(value_expression)
        || value_expression.is_volatile()
        || !value_expression.subquery_plans().is_empty()
        || value_expression.data_type() != schema.fields[0].data_type
        || contains_missing_local_column(value_expression, input.schema())
    {
        return None;
    }
    let mut value = nullable_column(&schema.fields[0]);

    let mut predicates = Vec::new();
    let mut base = input.as_ref();
    while let LogicalPlan::Filter { input, predicate } = base {
        split_conjunction(predicate, &mut predicates);
        base = input;
    }
    let LogicalPlan::Scan { filters, .. } = base else {
        return None;
    };
    for filter in filters {
        split_conjunction(filter, &mut predicates);
    }
    let mut correlations = Vec::new();
    let mut local_filters = Vec::new();
    for predicate in predicates {
        if let Some((inner, outer)) = equality_correlation(&predicate) {
            if base.schema().position_of_id(inner.id).is_none()
                || outer_schema.position_of_id(outer.id).is_none()
            {
                return None;
            }
            if !correlations.iter().any(|(existing_outer, existing_inner): &(ResolvedColumnRef, ResolvedColumnRef)| {
                existing_outer.id == outer.id && existing_inner.id == inner.id
            }) {
                correlations.push((outer, inner));
            }
        } else if contains_outer_column(&predicate)
            || predicate.is_volatile()
            || !predicate.subquery_plans().is_empty()
        {
            return None;
        } else {
            local_filters.push(predicate);
        }
    }
    if correlations.is_empty() {
        return None;
    }
    if let LogicalExpr::Column(source_value) = value_expression
        && let Some((_, key)) = correlations.iter().find(|(_, key)| key.id == source_value.id) {
        // The joined relation already carries this array as its key column.
        // Reuse that column ID for the scalar reference instead of emitting a
        // duplicate schema field for an aliased projection.
        value = nullable_column(key);
    }

    // Reapply deterministic local filters once above the scan, while removing
    // correlation predicates that cannot be evaluated in the shared build.
    let mut right_base = base.clone();
    if let LogicalPlan::Scan { filters, .. } = &mut right_base {
        filters.clear();
    }
    let mut right = right_base;
    if let Some(predicate) = conjunction(local_filters) {
        right = LogicalPlan::Filter {
            input: Box::new(right),
            predicate,
        };
    }

    let mut right_exprs = Vec::with_capacity(correlations.len() + 1);
    let mut right_fields = Vec::with_capacity(correlations.len() + 1);
    for (_, inner) in &correlations {
        if right_fields.iter().any(|field: &ResolvedColumnRef| field.id == inner.id) {
            continue;
        }
        right_exprs.push(LogicalExpr::Column(ResolvedColumnRef::clone(inner)));
        right_fields.push(ResolvedColumnRef::clone(inner));
    }
    let projected_key = match value_expression {
        LogicalExpr::Column(source_value) => {
            right_fields.iter().any(|field| field.id == source_value.id)
        }
        _ => false,
    };
    if !projected_key {
        right_exprs.push(value_expression.clone());
        right_fields.push(ResolvedColumnRef::clone(&schema.fields[0]));
    }
    right = LogicalPlan::Project {
        input: Box::new(right),
        exprs: right_exprs,
        schema: Box::new(VeinSchema::new(right_fields)),
    };

    Some(Decorrelation {
        right,
        correlations: correlations
            .into_iter()
            .map(|(outer, inner)| CorrelationPair { outer, inner })
            .collect(),
        join_kind: ScalarJoinKind::Single,
        value,
        aggregate_name: "SINGLE_ROW".to_string(),
    })
}

/// Returns true when an expression references a column outside the local input
/// schema. Outer references and lambda-local references are conservatively
/// rejected from this rewrite because their binding scopes cannot be preserved
/// by the scalar build-side projection.
fn contains_missing_local_column(expr: &LogicalExpr, schema: &VeinSchema) -> bool {
    match expr {
        LogicalExpr::Column(column) => schema.position_of_id(column.id).is_none(),
        LogicalExpr::OuterColumn(_) => true,
        _ => expr
            .children()
            .into_iter()
            .any(|child| contains_missing_local_column(child, schema)),
    }
}

/// Returns true when an expression reads a column produced by one of the
/// decorrelated scalar joins. This allows the enclosing projection to carry
/// nullable metadata through casts and other computed expressions.
fn references_column(expr: &LogicalExpr, id: plan::ColumnId) -> bool {
    match expr {
        LogicalExpr::Column(column) | LogicalExpr::OuterColumn(column) => column.id == id,
        _ => expr.children().into_iter().any(|child| references_column(child, id)),
    }
}

fn equality_expr(left: ResolvedColumnRef, right: ResolvedColumnRef) -> LogicalExpr {
    LogicalExpr::BinaryOp {
        left: Box::new(LogicalExpr::Column(left)),
        op: BinaryOp::Eq,
        right: Box::new(LogicalExpr::Column(right)),
        return_type: VeinDataType::Boolean,
    }
}

fn collect_aggregate_calls(expr: &LogicalExpr, output: &mut Vec<LogicalExpr>) {
    if matches!(expr, LogicalExpr::FunctionCall { is_aggregate: true, .. }) {
        output.push(expr.clone());
        return;
    }
    for child in expr.children() {
        collect_aggregate_calls(child, output);
    }
}

fn replace_aggregate_calls(
    expr: LogicalExpr,
    value: &ResolvedColumnRef,
    replacements: &mut usize,
    aggregate_calls: &mut Vec<LogicalExpr>,
) -> Result<LogicalExpr> {
    if matches!(expr, LogicalExpr::FunctionCall { is_aggregate: true, .. }) {
        *replacements += 1;
        aggregate_calls.push(expr);
        return Ok(LogicalExpr::Column(ResolvedColumnRef::clone(value)));
    }
    expr.map_children(&mut |child| {
        replace_aggregate_calls(child, value, replacements, aggregate_calls)
    })
}

fn equality_correlation(term: &LogicalExpr) -> Option<(ResolvedColumnRef, ResolvedColumnRef)> {
    let LogicalExpr::BinaryOp {
        left,
        op: BinaryOp::Eq,
        right,
        ..
    } = term
    else {
        return None;
    };
    match (left.as_ref(), right.as_ref()) {
        (LogicalExpr::Column(inner), LogicalExpr::OuterColumn(outer))
        | (LogicalExpr::OuterColumn(outer), LogicalExpr::Column(inner)) => {
            Some((ResolvedColumnRef::clone(inner), ResolvedColumnRef::clone(outer)))
        }
        _ => None,
    }
}

fn split_conjunction(expr: &LogicalExpr, terms: &mut Vec<LogicalExpr>) {
    if let LogicalExpr::BinaryOp {
        left,
        op: BinaryOp::And,
        right,
        ..
    } = expr
    {
        split_conjunction(left, terms);
        split_conjunction(right, terms);
    } else {
        terms.push(expr.clone());
    }
}

fn conjunction(terms: Vec<LogicalExpr>) -> Option<LogicalExpr> {
    let mut terms = terms.into_iter();
    let mut expr = terms.next()?;
    for right in terms {
        expr = LogicalExpr::BinaryOp {
            left: Box::new(expr),
            op: BinaryOp::And,
            right: Box::new(right),
            return_type: VeinDataType::Boolean,
        };
    }
    Some(expr)
}

fn contains_outer_column(expr: &LogicalExpr) -> bool {
    matches!(expr, LogicalExpr::OuterColumn(_))
        || expr.children().into_iter().any(contains_outer_column)
}

fn nullable_column(column: &ResolvedColumnRef) -> ResolvedColumnRef {
    Arc::new(ResolvedColumn {
        nullable: true,
        ..column.as_ref().clone()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Optimizer, OptimizerPipeline};
    use plan::{ColumnId, LogicalLiteral, ResolvedColumn, TableMetadata};
    use std::sync::Arc;

    fn field(id: usize, name: &str, qualifier: &str) -> ResolvedColumnRef {
        Arc::new(ResolvedColumn::new(
            ColumnId(id),
            name,
            Some(qualifier.to_string()),
            VeinDataType::Int64,
        ))
    }

    pub(super) fn scan(name: &str, fields: Vec<ResolvedColumnRef>) -> LogicalPlan {
        let schema = VeinSchema::new(fields);
        LogicalPlan::Scan {
            table: Arc::new(TableMetadata::new(name, Arc::new(schema.clone()))),
            projection: None,
            filters: Vec::new(),
            schema: Box::new(schema),
        }
    }

    fn correlated_min_subquery(
        inner_key: &ResolvedColumnRef,
        measure: &ResolvedColumnRef,
        outer_key: &ResolvedColumnRef,
        output: &ResolvedColumnRef,
    ) -> LogicalPlan {
        let input = scan(
            "inner_fixture",
            vec![Arc::clone(inner_key), Arc::clone(measure)],
        );
        let filtered = LogicalPlan::Filter {
            input: Box::new(input),
            predicate: LogicalExpr::BinaryOp {
                left: Box::new(LogicalExpr::Column(Arc::clone(inner_key))),
                op: BinaryOp::Eq,
                right: Box::new(LogicalExpr::OuterColumn(Arc::clone(outer_key))),
                return_type: VeinDataType::Boolean,
            },
        };
        LogicalPlan::Aggregate {
            input: Box::new(filtered),
            grouping_sets: vec![Vec::new()],
            aggs: vec![LogicalExpr::FunctionCall {
                name: "min".to_string(),
                args: vec![LogicalExpr::Column(Arc::clone(measure))],
                is_aggregate: true,
                distinct: false,
                return_type: VeinDataType::Int64,
            }],
            schema: Box::new(VeinSchema::new(vec![Arc::clone(output)])),
        }
    }

    #[test]
    fn decorrelates_scalar_min_to_grouped_left_join() {
        let outer_key = field(1, "outer_key", "outer_table");
        let outer_value = field(2, "outer_value", "outer_table");
        let inner_key = field(3, "inner_key", "inner_table");
        let measure = field(4, "measure", "inner_table");
        let min_value = field(5, "min_value", "inner_table");
        let input = scan(
            "outer_fixture",
            vec![Arc::clone(&outer_key), Arc::clone(&outer_value)],
        );
        let subquery = correlated_min_subquery(&inner_key, &measure, &outer_key, &min_value);
        let filter = LogicalPlan::Filter {
            input: Box::new(input),
            predicate: LogicalExpr::BinaryOp {
                left: Box::new(LogicalExpr::Column(Arc::clone(&outer_value))),
                op: BinaryOp::Eq,
                right: Box::new(LogicalExpr::ScalarSubquery(Box::new(subquery))),
                return_type: VeinDataType::Boolean,
            },
        };

        let rewritten = ScalarAggregateDecorrelationRule
            .apply(&filter, &OptimizerContext::default())
            .expect("decorrelation should not fail")
            .expect("supported scalar aggregate should decorrelate");
        let LogicalPlan::Project { input, schema: output_schema, .. } = rewritten else {
            panic!("rewrite must hide the aggregate helper columns");
        };
        assert_eq!(output_schema.fields, vec![Arc::clone(&outer_key), Arc::clone(&outer_value)]);
        let LogicalPlan::Filter { input, predicate } = input.as_ref() else {
            panic!("rewrite should preserve the filter");
        };
        let LogicalPlan::Join {
            join_type,
            schema,
            right,
            on,
            ..
        } = input.as_ref()
        else {
            panic!("correlated aggregate should become a join");
        };
        assert_eq!(*join_type, parser::ast::pipe_op::JoinType::Left);
        assert!(matches!(right.as_ref(), LogicalPlan::Aggregate { grouping_sets, schema, .. }
            if grouping_sets == &vec![vec![LogicalExpr::Column(Arc::clone(&inner_key))]]
                && schema.fields[1].nullable));
        assert!(schema.fields[2].nullable);
        assert!(matches!(on, Some(LogicalExpr::BinaryOp { op: BinaryOp::Eq, .. })));
        assert!(matches!(predicate, LogicalExpr::BinaryOp { right, .. }
            if matches!(right.as_ref(), LogicalExpr::Column(column) if column.id == min_value.id)));
    }

    #[test]
    fn decorrelates_uncorrelated_global_sum_arithmetic_to_cross_join() {
        let outer = field(21, "outer_value", "outer_fixture");
        let measure = field(22, "measure", "inner_fixture");
        let scalar = field(23, "scalar_value", "inner_fixture");
        let input = scan("outer_fixture", vec![Arc::clone(&outer)]);
        let sum = LogicalExpr::FunctionCall {
            name: "sum".to_string(),
            args: vec![LogicalExpr::Column(Arc::clone(&measure))],
            is_aggregate: true,
            distinct: false,
            return_type: VeinDataType::Int64,
        };
        let expression = LogicalExpr::BinaryOp {
            left: Box::new(sum),
            op: BinaryOp::Multiply,
            right: Box::new(LogicalExpr::Literal(LogicalLiteral::Int64(10))),
            return_type: VeinDataType::Int64,
        };
        let subquery = LogicalPlan::Aggregate {
            input: Box::new(scan("inner_fixture", vec![Arc::clone(&measure)])),
            grouping_sets: vec![Vec::new()],
            aggs: vec![expression],
            schema: Box::new(VeinSchema::new(vec![Arc::clone(&scalar)])),
        };
        let filter = LogicalPlan::Filter {
            input: Box::new(input),
            predicate: LogicalExpr::BinaryOp {
                left: Box::new(LogicalExpr::Column(Arc::clone(&outer))),
                op: BinaryOp::Gt,
                right: Box::new(LogicalExpr::ScalarSubquery(Box::new(subquery))),
                return_type: VeinDataType::Boolean,
            },
        };

        let rewritten = ScalarAggregateDecorrelationRule
            .apply(&filter, &OptimizerContext::default())
            .expect("uncorrelated aggregate should rewrite")
            .expect("global scalar aggregate should decorrelate");
        let LogicalPlan::Project { input, exprs, schema } = rewritten else {
            panic!("scalar join column must be removed after filtering");
        };
        assert_eq!(schema.fields, vec![Arc::clone(&outer)]);
        assert_eq!(exprs, vec![LogicalExpr::Column(Arc::clone(&outer))]);
        let LogicalPlan::Filter { input, predicate } = input.as_ref() else {
            panic!("filter must remain above the scalar join");
        };
        assert!(matches!(predicate, LogicalExpr::BinaryOp { right, .. }
            if matches!(right.as_ref(), LogicalExpr::Column(column) if column.id == scalar.id)));
        let LogicalPlan::Join { join_type, on, right, .. } = input.as_ref() else {
            panic!("uncorrelated scalar aggregate must become a join");
        };
        assert_eq!(*join_type, parser::ast::pipe_op::JoinType::Cross);
        assert!(on.is_none(), "CROSS JOIN carries no predicate");
        let LogicalPlan::Project { input, exprs, .. } = right.as_ref() else {
            panic!("aggregate arithmetic must be projected after reduction");
        };
        assert!(matches!(input.as_ref(), LogicalPlan::Aggregate { grouping_sets, aggs, .. }
            if grouping_sets == &vec![Vec::new()]
                && matches!(aggs.as_slice(), [LogicalExpr::FunctionCall { name, .. }] if name.eq_ignore_ascii_case("sum"))));
        assert!(matches!(exprs.as_slice(), [LogicalExpr::BinaryOp { left, op: BinaryOp::Multiply, .. }]
            if matches!(left.as_ref(), LogicalExpr::Column(column) if column.id == scalar.id)));
    }

    #[test]
    fn leaves_potentially_multirow_scalar_aggregate_unchanged() {
        let outer = field(31, "outer_value", "outer_fixture");
        let measure = field(32, "measure", "inner_fixture");
        let output = field(33, "group_value", "inner_fixture");
        let subquery = LogicalPlan::Aggregate {
            input: Box::new(scan("inner_fixture", vec![Arc::clone(&measure)])),
            grouping_sets: vec![vec![LogicalExpr::Column(Arc::clone(&measure))]],
            aggs: vec![LogicalExpr::FunctionCall {
                name: "sum".to_string(),
                args: vec![LogicalExpr::Column(Arc::clone(&measure))],
                is_aggregate: true,
                distinct: false,
                return_type: VeinDataType::Int64,
            }],
            schema: Box::new(VeinSchema::new(vec![Arc::clone(&measure), output])),
        };
        let filter = LogicalPlan::Filter {
            input: Box::new(scan("outer_fixture", vec![Arc::clone(&outer)])),
            predicate: LogicalExpr::BinaryOp {
                left: Box::new(LogicalExpr::Column(outer)),
                op: BinaryOp::Eq,
                right: Box::new(LogicalExpr::ScalarSubquery(Box::new(subquery))),
                return_type: VeinDataType::Boolean,
            },
        };

        assert!(ScalarAggregateDecorrelationRule
            .apply(&filter, &OptimizerContext::default())
            .expect("unsupported scalar cardinality must not be rewritten")
            .is_none());
    }

    #[test]
    fn leaves_count_scalar_subqueries_unchanged_to_preserve_empty_input_semantics() {
        let outer_key = field(11, "outer_key", "outer_table");
        let inner_key = field(12, "inner_key", "inner_table");
        let count_value = field(13, "count_value", "inner_table");
        let input = scan("outer_fixture", vec![Arc::clone(&outer_key)]);
        let correlated = correlated_min_subquery(
            &inner_key,
            &field(14, "measure", "inner_table"),
            &outer_key,
            &count_value,
        );
        let LogicalPlan::Aggregate { input: correlated_input, schema, .. } = correlated else {
            unreachable!();
        };
        let count = LogicalPlan::Aggregate {
            input: correlated_input,
            grouping_sets: vec![Vec::new()],
            aggs: vec![LogicalExpr::FunctionCall {
                name: "count".to_string(),
                args: vec![LogicalExpr::Literal(LogicalLiteral::Int64(1))],
                is_aggregate: true,
                distinct: false,
                return_type: VeinDataType::Int64,
            }],
            schema,
        };
        let filter = LogicalPlan::Filter {
            input: Box::new(input),
            predicate: LogicalExpr::ScalarSubquery(Box::new(count)),
        };

        assert!(ScalarAggregateDecorrelationRule
            .apply(&filter, &OptimizerContext::default())
            .expect("unsupported forms should be left to another implementation")
            .is_none());
    }

    #[test]
    fn decorrelates_uncorrelated_row_subquery_with_single_value_cardinality_check() {
        let outer = field(71, "outer_value", "outer_fixture");
        let scalar = field(72, "scalar_value", "inner_fixture");
        let inner = scan("inner_fixture", vec![Arc::clone(&scalar)]);
        let subquery = LogicalPlan::Project {
            input: Box::new(inner),
            exprs: vec![LogicalExpr::Column(Arc::clone(&scalar))],
            schema: Box::new(VeinSchema::new(vec![Arc::clone(&scalar)])),
        };
        let filter = LogicalPlan::Filter {
            input: Box::new(scan("outer_fixture", vec![Arc::clone(&outer)])),
            predicate: LogicalExpr::BinaryOp {
                left: Box::new(LogicalExpr::Column(Arc::clone(&outer))),
                op: BinaryOp::Eq,
                right: Box::new(LogicalExpr::ScalarSubquery(Box::new(subquery))),
                return_type: VeinDataType::Boolean,
            },
        };

        let rewritten = ScalarAggregateDecorrelationRule
            .apply(&filter, &OptimizerContext::default())
            .expect("uncorrelated scalar row query should be supported")
            .expect("scalar row query must be rewritten");
        let LogicalPlan::Project { input, .. } = rewritten else {
            panic!("cross-joined scalar output must be projected away after filtering");
        };
        let LogicalPlan::Filter { input, predicate } = input.as_ref() else {
            panic!("outer predicate must remain above the scalar join");
        };
        assert!(matches!(predicate, LogicalExpr::BinaryOp { right, .. }
            if matches!(right.as_ref(), LogicalExpr::Column(column) if column.id == scalar.id)));
        let LogicalPlan::Join { join_type, right, .. } = input.as_ref() else {
            panic!("uncorrelated scalar result must join once with outer input");
        };
        assert_eq!(*join_type, parser::ast::pipe_op::JoinType::Cross);
        assert!(matches!(right.as_ref(), LogicalPlan::Aggregate { grouping_sets, aggs, schema, .. }
            if grouping_sets == &vec![Vec::new()]
                && schema.fields.len() == 1
                && schema.fields[0].nullable
                && matches!(aggs.as_slice(), [LogicalExpr::FunctionCall { name, .. }] if name.eq_ignore_ascii_case("single_value"))));
    }

    #[test]
    fn decorrelates_correlated_row_scalar_with_single_join_and_local_filter() {
        let outer_key = field(91, "outer_key", "outer_fixture");
        let outer_group = field(92, "outer_group", "outer_fixture");
        let outer_value = field(93, "outer_value", "outer_fixture");
        let inner_key = field(94, "inner_key", "inner_fixture");
        let inner_group = field(95, "inner_group", "inner_fixture");
        let inner_value = field(96, "inner_value", "inner_fixture");
        let scalar = field(97, "scalar_value", "inner_fixture");
        let local_filter = LogicalExpr::BinaryOp {
            left: Box::new(LogicalExpr::Column(Arc::clone(&inner_value))),
            op: BinaryOp::Gt,
            right: Box::new(LogicalExpr::Literal(LogicalLiteral::Int64(0))),
            return_type: VeinDataType::Boolean,
        };
        let correlations = LogicalExpr::BinaryOp {
            left: Box::new(LogicalExpr::BinaryOp {
                left: Box::new(LogicalExpr::Column(Arc::clone(&inner_key))),
                op: BinaryOp::Eq,
                right: Box::new(LogicalExpr::OuterColumn(Arc::clone(&outer_key))),
                return_type: VeinDataType::Boolean,
            }),
            op: BinaryOp::And,
            right: Box::new(LogicalExpr::BinaryOp {
                left: Box::new(LogicalExpr::BinaryOp {
                    left: Box::new(LogicalExpr::OuterColumn(Arc::clone(&outer_group))),
                    op: BinaryOp::Eq,
                    right: Box::new(LogicalExpr::Column(Arc::clone(&inner_group))),
                    return_type: VeinDataType::Boolean,
                }),
                op: BinaryOp::And,
                right: Box::new(LogicalExpr::BinaryOp {
                    left: Box::new(LogicalExpr::OuterColumn(Arc::clone(&outer_key))),
                    op: BinaryOp::Eq,
                    right: Box::new(LogicalExpr::Column(Arc::clone(&inner_key))),
                    return_type: VeinDataType::Boolean,
                }),
                return_type: VeinDataType::Boolean,
            }),
            return_type: VeinDataType::Boolean,
        };
        let inner = scan(
            "inner_fixture",
            vec![Arc::clone(&inner_key), Arc::clone(&inner_group), Arc::clone(&inner_value)],
        );
        let subquery = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Filter {
                input: Box::new(LogicalPlan::Filter {
                    input: Box::new(inner),
                    predicate: local_filter,
                }),
                predicate: correlations,
            }),
            exprs: vec![LogicalExpr::Column(Arc::clone(&inner_value))],
            schema: Box::new(VeinSchema::new(vec![Arc::clone(&scalar)])),
        };
        let outer = scan(
            "outer_fixture",
            vec![Arc::clone(&outer_key), Arc::clone(&outer_group), Arc::clone(&outer_value)],
        );
        let filter = LogicalPlan::Filter {
            input: Box::new(outer),
            predicate: LogicalExpr::BinaryOp {
                left: Box::new(LogicalExpr::Column(Arc::clone(&outer_value))),
                op: BinaryOp::Gt,
                right: Box::new(LogicalExpr::ScalarSubquery(Box::new(subquery))),
                return_type: VeinDataType::Boolean,
            },
        };

        let rewritten = ScalarAggregateDecorrelationRule
            .apply(&filter, &OptimizerContext::default())
            .expect("supported correlated scalar should rewrite")
            .expect("correlated one-column row scalar should be decorrelated");
        let LogicalPlan::Project { input, schema, .. } = rewritten else {
            panic!("scalar and key helpers must be projected away");
        };
        assert_eq!(schema.fields, vec![Arc::clone(&outer_key), Arc::clone(&outer_group), Arc::clone(&outer_value)]);
        let LogicalPlan::Filter { input, predicate } = input.as_ref() else {
            panic!("the outer predicate must remain above the scalar join");
        };
        assert!(matches!(predicate, LogicalExpr::BinaryOp { right, .. }
            if matches!(right.as_ref(), LogicalExpr::Column(column) if column.id == scalar.id && column.nullable)));
        let LogicalPlan::Join { join_type, on, schema, right, .. } = input.as_ref() else {
            panic!("correlated scalar must become a join");
        };
        assert_eq!(*join_type, parser::ast::pipe_op::JoinType::Single);
        assert_eq!(schema.fields.len(), 6, "outer columns plus two keys and scalar value");
        assert!(schema.fields[3..].iter().all(|field| field.nullable));
        let Some(on) = on else { panic!("single join must carry its correlation keys"); };
        let mut join_key_terms = Vec::new();
        split_conjunction(on, &mut join_key_terms);
        assert_eq!(join_key_terms.len(), 2, "duplicate equalities must not duplicate hash keys");
        let LogicalPlan::Project { input: right_input, exprs, schema: right_schema } = right.as_ref() else {
            panic!("the build side must project just required keys and scalar value");
        };
        assert_eq!(exprs.len(), 3);
        assert_eq!(right_schema.fields.len(), 3);
        let LogicalPlan::Filter { input: local_input, predicate: local } = right_input.as_ref() else {
            panic!("inner-only filters should remain on the build side");
        };
        assert!(matches!(local, LogicalExpr::BinaryOp { op: BinaryOp::Gt, .. }));
        let LogicalPlan::Scan { filters, .. } = local_input.as_ref() else {
            panic!("the inner base should remain a scan");
        };
        assert!(filters.is_empty(), "moved filters must not be duplicated at the scan");
    }

    #[test]
    fn leaves_correlated_scalar_with_non_equality_reference_unchanged() {
        let outer_key = field(101, "outer_key", "outer_fixture");
        let inner_key = field(102, "inner_key", "inner_fixture");
        let inner_value = field(103, "inner_value", "inner_fixture");
        let scalar = field(104, "scalar_value", "inner_fixture");
        let subquery = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Filter {
                input: Box::new(scan(
                    "inner_fixture",
                    vec![Arc::clone(&inner_key), Arc::clone(&inner_value)],
                )),
                predicate: LogicalExpr::BinaryOp {
                    left: Box::new(LogicalExpr::Column(Arc::clone(&inner_key))),
                    op: BinaryOp::Gt,
                    right: Box::new(LogicalExpr::OuterColumn(Arc::clone(&outer_key))),
                    return_type: VeinDataType::Boolean,
                },
            }),
            exprs: vec![LogicalExpr::Column(Arc::clone(&inner_value))],
            schema: Box::new(VeinSchema::new(vec![scalar])),
        };
        let filter = LogicalPlan::Filter {
            input: Box::new(scan("outer_fixture", vec![outer_key])),
            predicate: LogicalExpr::ScalarSubquery(Box::new(subquery)),
        };
        assert!(ScalarAggregateDecorrelationRule
            .apply(&filter, &OptimizerContext::default())
            .expect("unsupported correlation must not cause an optimizer failure")
            .is_none());
    }

    #[test]
    fn correlated_scalar_projection_reuses_a_value_that_is_also_a_join_key() {
        let outer_key = field(111, "outer_key", "outer_fixture");
        let inner_key = field(112, "inner_key", "inner_fixture");
        let scalar = field(113, "scalar_value", "inner_fixture");
        let subquery = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Filter {
                input: Box::new(scan("inner_fixture", vec![Arc::clone(&inner_key)])),
                predicate: LogicalExpr::BinaryOp {
                    left: Box::new(LogicalExpr::Column(Arc::clone(&inner_key))),
                    op: BinaryOp::Eq,
                    right: Box::new(LogicalExpr::OuterColumn(Arc::clone(&outer_key))),
                    return_type: VeinDataType::Boolean,
                },
            }),
            exprs: vec![LogicalExpr::Column(Arc::clone(&inner_key))],
            schema: Box::new(VeinSchema::new(vec![Arc::clone(&scalar)])),
        };
        let filter = LogicalPlan::Filter {
            input: Box::new(scan("outer_fixture", vec![Arc::clone(&outer_key)])),
            predicate: LogicalExpr::ScalarSubquery(Box::new(subquery)),
        };
        let rewritten = ScalarAggregateDecorrelationRule
            .apply(&filter, &OptimizerContext::default())
            .expect("key/value reuse should be a valid rewrite")
            .expect("correlated scalar key projection should decorrelate");
        let LogicalPlan::Project { input, .. } = rewritten else {
            panic!("original outer schema must be preserved");
        };
        let LogicalPlan::Filter { input, .. } = input.as_ref() else {
            unreachable!();
        };
        let LogicalPlan::Join { right, schema, .. } = input.as_ref() else {
            unreachable!();
        };
        assert_eq!(right.schema().fields.len(), 1, "key and scalar share one physical column");
        assert_eq!(schema.fields.len(), 2);
    }

    #[test]
    fn decorrelates_uncorrelated_scalar_in_project_to_single_value_cross_join() {
        let outer = field(121, "outer_value", "outer_fixture");
        let inner = field(122, "inner_value", "inner_fixture");
        let scalar = field(123, "scalar_value", "inner_fixture");
        let projected_scalar = Arc::new(ResolvedColumn::new(
            plan::ColumnId(124),
            "selected_value",
            None,
            VeinDataType::Int64,
        ));
        let subquery = LogicalPlan::Project {
            input: Box::new(scan("inner_fixture", vec![Arc::clone(&inner)])),
            exprs: vec![LogicalExpr::Column(Arc::clone(&inner))],
            schema: Box::new(VeinSchema::new(vec![Arc::clone(&scalar)])),
        };
        let input = scan("outer_fixture", vec![Arc::clone(&outer)]);
        let output_schema = Box::new(VeinSchema::new(vec![
            Arc::clone(&outer),
            Arc::clone(&projected_scalar),
        ]));
        let project = LogicalPlan::Project {
            input: Box::new(input),
            exprs: vec![
                LogicalExpr::Column(Arc::clone(&outer)),
                LogicalExpr::ScalarSubquery(Box::new(subquery)),
            ],
            schema: output_schema.clone(),
        };

        let rewritten = ScalarAggregateDecorrelationRule
            .apply(&project, &OptimizerContext::default())
            .expect("uncorrelated scalar projection should be supported")
            .expect("scalar projection must be rewritten");
        let LogicalPlan::Project { input, exprs, schema } = rewritten else {
            panic!("the original projection should remain above the scalar join");
        };
        assert_eq!(schema.fields[0], output_schema.fields[0]);
        assert_eq!(schema.fields[1].id, projected_scalar.id);
        assert_eq!(schema.fields[1].data_type, projected_scalar.data_type);
        assert!(schema.fields[1].nullable, "an empty scalar input yields a typed NULL");
        assert!(matches!(
            exprs.get(1),
            Some(LogicalExpr::Column(column)) if column.id == scalar.id
        ));
        let LogicalPlan::Join {
            join_type,
            right,
            ..
        } = input.as_ref()
        else {
            panic!("uncorrelated scalar projection should use a cross join");
        };
        assert_eq!(*join_type, parser::ast::pipe_op::JoinType::Cross);
        assert!(matches!(
            right.as_ref(),
            LogicalPlan::Aggregate { grouping_sets, aggs, .. }
                if grouping_sets == &vec![Vec::new()]
                    && matches!(aggs.as_slice(), [LogicalExpr::FunctionCall { name, .. }] if name.eq_ignore_ascii_case("single_value"))
        ));
    }

    #[test]
    fn decorrelates_correlated_scalar_in_project_to_single_match_join() {
        let outer_key = field(131, "outer_key", "outer_fixture");
        let inner_key = field(132, "inner_key", "inner_fixture");
        let inner_value = field(133, "inner_value", "inner_fixture");
        let scalar = field(134, "scalar_value", "inner_fixture");
        let subquery = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Filter {
                input: Box::new(scan(
                    "inner_fixture",
                    vec![Arc::clone(&inner_key), Arc::clone(&inner_value)],
                )),
                predicate: LogicalExpr::BinaryOp {
                    left: Box::new(LogicalExpr::Column(Arc::clone(&inner_key))),
                    op: BinaryOp::Eq,
                    right: Box::new(LogicalExpr::OuterColumn(Arc::clone(&outer_key))),
                    return_type: VeinDataType::Boolean,
                },
            }),
            exprs: vec![LogicalExpr::Column(Arc::clone(&inner_value))],
            schema: Box::new(VeinSchema::new(vec![Arc::clone(&scalar)])),
        };
        let input = scan("outer_fixture", vec![Arc::clone(&outer_key)]);
        let project = LogicalPlan::Project {
            input: Box::new(input),
            exprs: vec![LogicalExpr::ScalarSubquery(Box::new(subquery))],
            schema: Box::new(VeinSchema::new(vec![Arc::clone(&scalar)])),
        };

        let rewritten = ScalarAggregateDecorrelationRule
            .apply(&project, &OptimizerContext::default())
            .expect("correlated scalar projection should be supported")
            .expect("correlated scalar projection must be rewritten");
        let LogicalPlan::Project { input, exprs, schema } = rewritten else {
            panic!("the original projection should remain above the scalar join");
        };
        assert_eq!(schema.fields.len(), 1);
        assert!(matches!(
            exprs.as_slice(),
            [LogicalExpr::Column(column)] if column.id == scalar.id && column.nullable
        ));
        let LogicalPlan::Join {
            join_type,
            on,
            right,
            schema: join_schema,
            ..
        } = input.as_ref()
        else {
            panic!("correlated scalar projection should use a single-match join");
        };
        assert_eq!(*join_type, parser::ast::pipe_op::JoinType::Single);
        assert!(on.is_some(), "single-match join must retain its correlation key");
        assert!(join_schema.fields[1..].iter().all(|field| field.nullable));
        assert_eq!(right.schema().fields.len(), 2, "build side needs only its key and scalar value");
    }

    #[test]
    fn decorrelates_deterministic_computed_correlated_scalar_projection() {
        let outer_key = field(151, "outer_key", "outer_fixture");
        let inner_key = field(152, "inner_key", "inner_fixture");
        let inner_value = field(153, "inner_value", "inner_fixture");
        let scalar = field(154, "computed_value", "inner_fixture");
        let subquery = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Filter {
                input: Box::new(scan(
                    "inner_fixture",
                    vec![Arc::clone(&inner_key), Arc::clone(&inner_value)],
                )),
                predicate: LogicalExpr::BinaryOp {
                    left: Box::new(LogicalExpr::Column(Arc::clone(&inner_key))),
                    op: BinaryOp::Eq,
                    right: Box::new(LogicalExpr::OuterColumn(Arc::clone(&outer_key))),
                    return_type: VeinDataType::Boolean,
                },
            }),
            exprs: vec![LogicalExpr::BinaryOp {
                left: Box::new(LogicalExpr::Column(Arc::clone(&inner_value))),
                op: BinaryOp::Plus,
                right: Box::new(LogicalExpr::Literal(LogicalLiteral::Int64(7))),
                return_type: VeinDataType::Int64,
            }],
            schema: Box::new(VeinSchema::new(vec![Arc::clone(&scalar)])),
        };
        let project = LogicalPlan::Project {
            input: Box::new(scan("outer_fixture", vec![Arc::clone(&outer_key)])),
            exprs: vec![LogicalExpr::ScalarSubquery(Box::new(subquery))],
            schema: Box::new(VeinSchema::new(vec![Arc::clone(&scalar)])),
        };

        let rewritten = ScalarAggregateDecorrelationRule
            .apply(&project, &OptimizerContext::default())
            .expect("deterministic computed scalar projection should be analyzed")
            .expect("computed correlated scalar projection should use the single-match join");
        let LogicalPlan::Project { input, exprs, schema } = rewritten else {
            panic!("the outer projection should remain above the scalar join");
        };
        assert!(schema.fields[0].nullable, "a no-match scalar value must be nullable");
        assert_eq!(schema.fields[0].data_type, VeinDataType::Int64);
        assert!(matches!(
            exprs.as_slice(),
            [LogicalExpr::Column(column)] if column.id == scalar.id && column.nullable
        ));
        let LogicalPlan::Join { join_type, right, .. } = input.as_ref() else {
            panic!("computed scalar projection should be attached by a join");
        };
        assert_eq!(*join_type, parser::ast::pipe_op::JoinType::Single);
        let LogicalPlan::Project { exprs, schema, .. } = right.as_ref() else {
            panic!("the computed value should be evaluated once in the build projection");
        };
        assert!(matches!(
            exprs.as_slice(),
            [LogicalExpr::Column(key), LogicalExpr::BinaryOp { op: BinaryOp::Plus, .. }]
                if key.id == inner_key.id
        ));
        assert_eq!(schema.fields.len(), 2);
        assert_eq!(schema.fields[1].id, scalar.id);
        assert_eq!(schema.fields[1].data_type, VeinDataType::Int64);
    }

    #[test]
    fn preserves_cast_type_and_nullability_for_correlated_scalar_projection() {
        let outer_key = field(171, "outer_key", "outer_fixture");
        let inner_key = field(172, "inner_key", "inner_fixture");
        let inner_value = field(173, "inner_value", "inner_fixture");
        let scalar = field(174, "scalar_value", "inner_fixture");
        let projected = Arc::new(ResolvedColumn::new(
            ColumnId(175),
            "coerced_value",
            None,
            VeinDataType::Float64,
        ));
        let subquery = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Filter {
                input: Box::new(scan(
                    "inner_fixture",
                    vec![Arc::clone(&inner_key), Arc::clone(&inner_value)],
                )),
                predicate: LogicalExpr::BinaryOp {
                    left: Box::new(LogicalExpr::Column(Arc::clone(&inner_key))),
                    op: BinaryOp::Eq,
                    right: Box::new(LogicalExpr::OuterColumn(Arc::clone(&outer_key))),
                    return_type: VeinDataType::Boolean,
                },
            }),
            exprs: vec![LogicalExpr::Column(Arc::clone(&inner_value))],
            schema: Box::new(VeinSchema::new(vec![Arc::clone(&scalar)])),
        };
        let project = LogicalPlan::Project {
            input: Box::new(scan("outer_fixture", vec![Arc::clone(&outer_key)])),
            exprs: vec![LogicalExpr::Cast {
                expr: Box::new(LogicalExpr::ScalarSubquery(Box::new(subquery))),
                target_type: VeinDataType::Float64,
            }],
            schema: Box::new(VeinSchema::new(vec![Arc::clone(&projected)])),
        };

        let rewritten = ScalarAggregateDecorrelationRule
            .apply(&project, &OptimizerContext::default())
            .expect("casted correlated scalar projection should be analyzed")
            .expect("the scalar subquery should be decorrelated");
        let LogicalPlan::Project { exprs, schema, .. } = rewritten else {
            panic!("the outer projection should remain above the scalar join");
        };
        assert!(schema.fields[0].nullable, "cast must preserve scalar NULL possibility");
        assert_eq!(schema.fields[0].data_type, VeinDataType::Float64);
        assert!(matches!(
            exprs.as_slice(),
            [LogicalExpr::Cast { expr, target_type: VeinDataType::Float64 }]
                if matches!(expr.as_ref(), LogicalExpr::Column(column)
                    if column.id == scalar.id
                        && column.data_type == VeinDataType::Int64
                        && column.nullable)
        ));
    }

    #[test]
    fn leaves_volatile_computed_correlated_scalar_projection_for_planner_diagnostic() {
        let outer_key = field(161, "outer_key", "outer_fixture");
        let inner_key = field(162, "inner_key", "inner_fixture");
        let scalar = Arc::new(ResolvedColumn::new(
            ColumnId(163),
            "volatile_value",
            Some("inner_fixture".to_string()),
            VeinDataType::Float64,
        ));
        let subquery = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Filter {
                input: Box::new(scan("inner_fixture", vec![Arc::clone(&inner_key)])),
                predicate: LogicalExpr::BinaryOp {
                    left: Box::new(LogicalExpr::Column(Arc::clone(&inner_key))),
                    op: BinaryOp::Eq,
                    right: Box::new(LogicalExpr::OuterColumn(Arc::clone(&outer_key))),
                    return_type: VeinDataType::Boolean,
                },
            }),
            exprs: vec![LogicalExpr::FunctionCall {
                name: "random".to_string(),
                args: vec![],
                is_aggregate: false,
                distinct: false,
                return_type: VeinDataType::Float64,
            }],
            schema: Box::new(VeinSchema::new(vec![Arc::clone(&scalar)])),
        };
        let project = LogicalPlan::Project {
            input: Box::new(scan("outer_fixture", vec![outer_key])),
            exprs: vec![LogicalExpr::ScalarSubquery(Box::new(subquery))],
            schema: Box::new(VeinSchema::new(vec![scalar])),
        };

        assert!(ScalarAggregateDecorrelationRule
            .apply(&project, &OptimizerContext::default())
            .expect("unsafe volatile projection should be rejected without optimizer failure")
            .is_none());
        let error = OptimizerPipeline::default()
            .optimize(project)
            .expect_err("unsupported volatile scalar should fail before physical compilation");
        assert!(error.to_string().contains("Project.exprs"), "{error}");
    }

    #[test]
    fn decorrelates_correlated_scalar_aggregate_in_sort_key() {
        let outer_key = field(141, "outer_key", "outer_fixture");
        let inner_key = field(142, "inner_key", "inner_fixture");
        let inner_measure = field(143, "inner_measure", "inner_fixture");
        let scalar = field(144, "minimum_value", "inner_fixture");
        let input = scan("outer_fixture", vec![Arc::clone(&outer_key)]);
        let output_schema = Box::new(VeinSchema::new(vec![Arc::clone(&outer_key)]));
        let sort = LogicalPlan::Sort {
            input: Box::new(input),
            sort_keys: vec![plan::SortKey {
                expr: LogicalExpr::ScalarSubquery(Box::new(correlated_min_subquery(
                    &inner_key,
                    &inner_measure,
                    &outer_key,
                    &scalar,
                ))),
                asc: false,
                nulls_first: false,
            }],
            schema: output_schema.clone(),
        };

        let rewritten = ScalarAggregateDecorrelationRule
            .apply(&sort, &OptimizerContext::default())
            .expect("correlated scalar sort key should be supported")
            .expect("scalar subquery in a sort key must be rewritten");
        let LogicalPlan::Project {
            input: sorted,
            exprs,
            schema,
        } = rewritten
        else {
            panic!("original output columns should be restored above the sort");
        };
        assert_eq!(schema.fields, output_schema.fields);
        assert!(matches!(
            exprs.as_slice(),
            [LogicalExpr::Column(column)] if column.id == outer_key.id
        ));
        let LogicalPlan::Sort {
            input: joined,
            sort_keys,
            schema: sort_schema,
        } = sorted.as_ref()
        else {
            panic!("decorrelated input should still sort after its scalar join");
        };
        assert_eq!(
            sort_schema.fields.len(),
            3,
            "sort retains the outer key and the scalar join key/value columns"
        );
        assert!(matches!(
            sort_keys.as_slice(),
            [plan::SortKey { expr: LogicalExpr::Column(column), asc: false, nulls_first: false }]
                if column.id == scalar.id && column.nullable
        ));
        assert!(matches!(
            joined.as_ref(),
            LogicalPlan::Join { join_type: parser::ast::pipe_op::JoinType::Left, .. }
        ));
    }

    #[test]
    fn does_not_mistake_nested_subqueries_for_uncorrelated_scalar_input() {
        let outer = field(81, "outer_value", "outer_fixture");
        let scalar = field(82, "scalar_value", "inner_fixture");
        let nested_value = field(83, "nested_value", "nested_fixture");
        let subquery = LogicalPlan::Project {
            input: Box::new(scan("inner_fixture", vec![Arc::clone(&scalar)])),
            exprs: vec![LogicalExpr::ScalarSubquery(Box::new(scan(
                "nested_fixture",
                vec![Arc::clone(&nested_value)],
            )))],
            schema: Box::new(VeinSchema::new(vec![Arc::clone(&scalar)])),
        };
        let filter = LogicalPlan::Filter {
            input: Box::new(scan("outer_fixture", vec![Arc::clone(&outer)])),
            predicate: LogicalExpr::ScalarSubquery(Box::new(subquery)),
        };

        assert!(ScalarAggregateDecorrelationRule
            .apply(&filter, &OptimizerContext::default())
            .expect("nested unsupported shape is not an optimizer error")
            .is_none());
    }
}

#[cfg(test)]
#[path = "scalar_aggregate_decorrelation/composite_key_tests.rs"]
mod composite_key_tests;
