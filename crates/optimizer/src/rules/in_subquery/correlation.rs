use plan::{LogicalExpr, LogicalPlan, PlanTraversal, VeinSchema};

/// Detects references to this membership join's input, including references
/// nested inside subqueries. Correlations to an intermediate RHS scope remain
/// local when their column identities do not belong to this input.
pub(super) fn references_outer_input(plan: &LogicalPlan, outer_schema: &VeinSchema) -> bool {
    fn expression_references_outer_input(
        expression: &LogicalExpr,
        outer_schema: &VeinSchema,
    ) -> bool {
        matches!(expression, LogicalExpr::OuterColumn(column)
            if outer_schema.position_of_id(column.id).is_some())
            || expression
            .children()
            .into_iter()
            .any(|child| expression_references_outer_input(child, outer_schema))
    }

    let mut found = false;
    plan.for_each_expression(&mut |_, expression| {
        found |= expression_references_outer_input(expression, outer_schema);
    });
    found
        || plan
        .children()
        .into_iter()
        .any(|child| references_outer_input(child, outer_schema))
        || plan
        .subquery_plans()
        .into_iter()
        .any(|subquery| references_outer_input(subquery, outer_schema))
}
