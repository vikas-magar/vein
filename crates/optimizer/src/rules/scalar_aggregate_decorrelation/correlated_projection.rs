//! Reattaches a scalar projection above a decorrelated correlated aggregate.

use super::{
    contains_missing_local_column, contains_outer_column, decorrelate_scalar_aggregate,
    nullable_column, Decorrelation,
};
use plan::{LogicalExpr, LogicalPlan, PlanTraversal, ResolvedColumnRef, VeinSchema};
use std::collections::HashSet;
use std::sync::Arc;

/// Lowers a deterministic one-column projection over a correlated global
/// aggregate while preserving the aggregate's correlation key for its join.
pub(super) fn decorrelate(
    subquery: &LogicalPlan,
    outer_schema: &VeinSchema,
) -> Option<Decorrelation> {
    let LogicalPlan::Project {
        input,
        exprs,
        schema,
    } = subquery
    else {
        return None;
    };
    if !matches!(input.as_ref(), LogicalPlan::Aggregate { .. }) {
        return None;
    }
    let [expression] = exprs.as_slice() else {
        return None;
    };
    let [output] = schema.fields.as_slice() else {
        return None;
    };
    if expression.is_volatile()
        || !expression.subquery_plans().is_empty()
        || contains_outer_column(expression)
        || contains_aggregate_call(expression)
        || expression.data_type() != output.data_type
        || contains_missing_local_column(expression, input.schema())
    {
        return None;
    }

    let mut decorrelation = decorrelate_scalar_aggregate(input, outer_schema)?;
    let keys = correlation_keys(&decorrelation)?;
    let mut seen = HashSet::with_capacity(keys.len());
    if keys.iter().any(|key| {
        !seen.insert(key.id) || decorrelation.right.schema().position_of_id(key.id).is_none()
    }) || contains_missing_local_column(expression, decorrelation.right.schema())
    {
        return None;
    }

    let value = nullable_column(output);
    let mut projected_exprs = Vec::with_capacity(keys.len() + 1);
    let mut projected_fields = Vec::with_capacity(keys.len() + 1);
    for key in keys {
        projected_exprs.push(LogicalExpr::Column(ResolvedColumnRef::clone(&key)));
        projected_fields.push(key);
    }
    projected_exprs.push(expression.clone());
    projected_fields.push(Arc::clone(&value));
    decorrelation.right = LogicalPlan::Project {
        input: Box::new(decorrelation.right),
        exprs: projected_exprs,
        schema: Box::new(VeinSchema::new(projected_fields)),
    };
    decorrelation.value = value;
    Some(decorrelation)
}

fn correlation_keys(decorrelation: &Decorrelation) -> Option<Vec<ResolvedColumnRef>> {
    let keys = super::distinct_inner_keys(&decorrelation.correlations);
    (!keys.is_empty()).then_some(keys)
}

fn contains_aggregate_call(expression: &LogicalExpr) -> bool {
    matches!(expression, LogicalExpr::FunctionCall { is_aggregate: true, .. })
        || expression
        .children()
        .into_iter()
        .any(|child| contains_aggregate_call(&child))
}

#[cfg(test)]
#[path = "correlated_projection_tests.rs"]
mod tests;
