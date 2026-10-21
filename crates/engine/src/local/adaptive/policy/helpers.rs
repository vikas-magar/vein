//! Policy-specific safety and cardinality helpers.

use plan::{LogicalPlan, PlanTraversal};

pub(super) fn contains_blocking_relation(plan: &LogicalPlan) -> bool {
    matches!(
        plan,
        LogicalPlan::Join { .. } | LogicalPlan::Aggregate { .. } | LogicalPlan::Distinct { .. }
    ) || plan.children().into_iter().any(contains_blocking_relation)
}

/// Only relational, side-effect-free subtrees can be replayed after a failure.
/// Subqueries and semantic/opaque calls require separate dependency handling.
pub(super) fn safe_subtree(plan: &LogicalPlan) -> bool {
    if !matches!(
        plan,
        LogicalPlan::Scan { .. }
            | LogicalPlan::ExternalScan { .. }
            | LogicalPlan::Intermediate { .. }
            | LogicalPlan::Filter { .. }
            | LogicalPlan::Project { .. }
            | LogicalPlan::Join { .. }
            | LogicalPlan::Aggregate { .. }
            | LogicalPlan::Distinct { .. }
            | LogicalPlan::Sort { .. }
            | LogicalPlan::Limit { .. }
            | LogicalPlan::SubqueryAlias { .. }
    ) {
        return false;
    }
    let mut has_unsafe_expression = false;
    plan.for_each_expression(&mut |_, expression| {
        has_unsafe_expression |=
            expression.is_volatile() || !expression.subquery_plans().is_empty();
    });
    !has_unsafe_expression && plan.children().into_iter().all(safe_subtree)
}

pub(super) fn needs_feedback(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Intermediate { .. } => false,
        LogicalPlan::Scan { filters, table, .. } => {
            !filters.is_empty()
                || table
                .statistics
                .as_ref()
                .and_then(|stats| stats.num_rows)
                .is_none()
        }
        LogicalPlan::Join { .. } | LogicalPlan::Aggregate { .. } | LogicalPlan::Distinct { .. } => {
            true
        }
        LogicalPlan::Filter { input, .. } => contains_pending_operator(input),
        _ => plan.children().into_iter().any(needs_feedback),
    }
}

fn contains_pending_operator(plan: &LogicalPlan) -> bool {
    matches!(
        plan,
        LogicalPlan::Scan { .. }
            | LogicalPlan::ExternalScan { .. }
            | LogicalPlan::Join { .. }
            | LogicalPlan::Aggregate { .. }
            | LogicalPlan::Distinct { .. }
    ) || plan.children().into_iter().any(contains_pending_operator)
}

pub(super) fn known_input_rows(plan: &LogicalPlan) -> Option<u64> {
    match plan {
        LogicalPlan::Scan { table, .. } => table.statistics.as_ref()?.num_rows,
        LogicalPlan::Intermediate { row_count, .. } => Some(*row_count),
        _ => {
            let mut rows = 0_u64;
            for child in plan.children() {
                rows = rows.checked_add(known_input_rows(child)?)?;
            }
            Some(rows)
        }
    }
}

pub(super) fn missing_synopses(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Intermediate { .. } => false,
        LogicalPlan::Scan { table, schema, .. } => schema.fields.iter().any(|column| {
            table
                .statistics
                .as_ref()
                .and_then(|stats| stats.column_statistics.get(&column.name))
                .is_none_or(|stats| stats.distinct_count.is_none())
        }),
        _ => plan.children().into_iter().any(missing_synopses),
    }
}
