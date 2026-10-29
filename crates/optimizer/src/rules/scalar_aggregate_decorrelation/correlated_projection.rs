//! Scalar projections execute after reattaching the grouped correlation result.
use super::{
    Decorrelation, contains_missing_local_column, contains_outer_column,
    decorrelate_scalar_aggregate, scalar_result,
};
use plan::{LogicalExpr, LogicalPlan, PlanTraversal, VeinSchema};

/// Keep the grouped result on the right, then evaluate the scalar expression
/// above the left join. Evaluating it on the right loses empty-group defaults.
pub(super) fn decorrelate(
    subquery: &LogicalPlan, outer_schema: &VeinSchema,
) -> Option<Decorrelation> {
    let LogicalPlan::Project { input, exprs, schema } = subquery else { return None; };
    if !matches!(input.as_ref(), LogicalPlan::Aggregate { .. }) { return None; }
    let [expression] = exprs.as_slice() else { return None; };
    let [output] = schema.fields.as_slice() else { return None; };
    if expression.is_volatile()
        || !expression.subquery_plans().is_empty()
        || contains_outer_column(expression)
        || contains_aggregate_call(expression)
        || expression.data_type() != output.data_type
        || contains_missing_local_column(expression, input.schema())
    { return None; }

    let mut lowered = decorrelate_scalar_aggregate(input, outer_schema)?;
    if lowered.correlations.is_empty()
        || contains_missing_local_column(expression, lowered.right.schema())
    { return None; }
    lowered.replacement = Some(scalar_result::substitute(
        expression.clone(), lowered.value.id, &scalar_result::expression(&lowered),
    ).ok()?);
    Some(lowered)
}

fn contains_aggregate_call(expression: &LogicalExpr) -> bool {
    matches!(expression, LogicalExpr::FunctionCall { is_aggregate: true, .. })
        || expression.children().into_iter().any(contains_aggregate_call)
}

#[cfg(test)]
#[path = "correlated_projection_tests.rs"]
mod tests;
