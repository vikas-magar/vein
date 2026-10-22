//! Compact predicate placement and exact expression correspondence diagnostics.

use plan::{LogicalExpr, LogicalPlan, PlanTraversal};
use crate::rules::predicate_analysis::conjuncts;

mod placement;
pub use placement::{PredicateLocation, PredicatePlacement, PredicateRetention};

/// Input expressions are metadata only. Their ordinal identifiers are local to
/// this optimization attempt, independent of mutable tree positions.
pub(crate) struct PredicateLineage {
    inputs: Vec<LogicalExpr>,
}

impl PredicateLineage {
    pub(crate) fn capture(plan: &LogicalPlan) -> Self {
        let mut inputs = Vec::new();
        visit_predicates(plan, |_, _, _, expression| inputs.push(expression.clone()));
        Self { inputs }
    }

    pub(crate) fn placements(&self, plan: &LogicalPlan) -> Vec<PredicatePlacement> {
        let mut placements = Vec::new();
        visit_predicates(plan, |node_id, owner, location, expression| {
            let matching_input_predicates = self.inputs.iter().enumerate()
                .filter_map(|(id, input)| (input == expression).then_some(id)).collect();
            let placement = PredicatePlacement::new(
                node_id, owner, location, expression, matching_input_predicates);
            tracing::debug!(target: "vein.optimizer", event = "optimizer_predicate_placement",
                node_id, operator = owner.operator_name(), location = ?location,
                expression_kind = expression.kind_name(), columns = ?placement.columns,
                matching_input_predicates = ?placement.matching_input_predicates,
                retention = ?placement.retention, reorder_safe = placement.reorder_safe,
                "predicate placement and exact input expression correspondence");
            placements.push(placement);
        });
        placements
    }
}

fn visit_predicates(
    root: &LogicalPlan,
    mut visit: impl FnMut(usize, &LogicalPlan, PredicateLocation, &LogicalExpr),
) {
    let mut pending = vec![root];
    let mut node_id = 0;
    while let Some(plan) = pending.pop() {
        let mut emit = |location, expression| {
            for term in conjuncts(expression) { visit(node_id, plan, location, term); }
        };
        match plan {
            LogicalPlan::Scan { filters, .. } => {
                for expression in filters { emit(PredicateLocation::Scan, expression); }
            }
            LogicalPlan::Filter { predicate, .. } => emit(PredicateLocation::Filter, predicate),
            LogicalPlan::Join { on: Some(on), .. } => emit(PredicateLocation::Join, on),
            _ => {}
        }
        pending.extend(plan.subquery_plans().into_iter().rev());
        pending.extend(plan.children().into_iter().rev());
        node_id += 1;
    }
}

#[cfg(test)]
mod tests;
