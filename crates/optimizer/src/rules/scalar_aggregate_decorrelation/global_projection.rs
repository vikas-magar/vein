//! Scalar lowering for the binder's normalized global-aggregate projection.

use super::{contains_missing_local_column, contains_outer_column, nullable_column, Decorrelation};
use plan::{LogicalExpr, LogicalPlan, PlanTraversal, VeinSchema};
use std::sync::Arc;

/// Converts a projection over a global aggregate into a scalar cross-join input.
/// The global aggregate guarantees one row even when its input is empty, while
/// the projection preserves that row and computes the scalar expression once.
pub(super) fn decorrelate(subquery: &LogicalPlan) -> Option<Decorrelation> {
    let LogicalPlan::Project {
        input,
        exprs,
        schema,
    } = subquery
    else {
        return None;
    };
    let LogicalPlan::Aggregate {
        grouping_sets,
        aggs,
        schema: aggregate_schema,
        ..
    } = input.as_ref()
    else {
        return None;
    };

    let [expression] = exprs.as_slice() else {
        return None;
    };
    let [output] = schema.fields.as_slice() else {
        return None;
    };
    if !matches!(grouping_sets.as_slice(), [set] if set.is_empty())
        || aggs.is_empty()
        || expression.data_type() != output.data_type
        || expression.is_volatile()
        || !expression.subquery_plans().is_empty()
        || contains_missing_local_column(expression, aggregate_schema)
        || contains_aggregate_call(expression)
        || !plan_is_uncorrelated_and_deterministic(input)
    {
        return None;
    }

    let value = nullable_column(output);
    let right = LogicalPlan::Project {
        input: Box::new((**input).clone()),
        exprs: exprs.clone(),
        schema: Box::new(VeinSchema::new(vec![Arc::clone(&value)])),
    };
    Some(Decorrelation {
        right,
        correlations: Vec::new(),
        residual: Vec::new(),
        join_kind: super::ScalarJoinKind::Cross,
        value,
        aggregate_name: "GLOBAL_AGGREGATE_PROJECTION".to_string(),
        replacement: None,
    })
}

fn contains_aggregate_call(expression: &LogicalExpr) -> bool {
    matches!(expression, LogicalExpr::FunctionCall { is_aggregate: true, .. })
        || expression
        .children()
        .into_iter()
        .any(contains_aggregate_call)
}

fn plan_is_uncorrelated_and_deterministic(plan: &LogicalPlan) -> bool {
    let mut safe = true;
    plan.for_each_expression(&mut |_, expression| {
        if contains_outer_column(expression)
            || expression.is_volatile()
            || !expression.subquery_plans().is_empty()
        {
            safe = false;
        }
    });
    safe && plan
        .children()
        .into_iter()
        .all(plan_is_uncorrelated_and_deterministic)
}

#[cfg(test)]
#[path = "global_projection_tests.rs"]
mod tests;
