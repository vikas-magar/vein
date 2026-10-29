//! Avoid moving an existence test across a join that already reduces its input.
use super::eligibility::Input;
use crate::context::OptimizerContext;
use crate::rules::join_transitivity::{equi_join_keys, schema_ids};
use crate::rules::predicate_analysis::conjuncts;
use plan::{LogicalPlan, PlanTraversal};

pub(super) fn allows(plan: &LogicalPlan, target: Input, context: &OptimizerContext) -> bool {
    let LogicalPlan::Join { left: probe, right: build, on: Some(on), .. } = plan else { return false; };
    let LogicalPlan::Join { left, .. } = probe.as_ref() else { return false; };
    // Derive both populations in one estimator traversal. Postorder puts the
    // right child's root immediately before the join, and the left child's
    // root at the end of its own subtree.
    let estimates = context.estimate_plan(probe);
    let target_index = match target {
        Input::Left => node_count(left).checked_sub(1),
        Input::Right => estimates.len().checked_sub(2),
    };
    let current = estimates.last().and_then(|estimate| estimate.rows.value());
    let proposed = target_index.and_then(|index| estimates.get(index))
        .and_then(|estimate| estimate.rows.value());
    let equality_only = equi_join_keys(on, &schema_ids(probe.schema()), &schema_ids(build.schema())).len()
        == conjuncts(on).len();
    // Residual joins may traverse multiple matches per key. Without a measured
    // benefit, preserve the filtering already supplied by the inner join.
    let allowed = current.zip(proposed).is_some_and(|(current, proposed)| {
        proposed < current || (equality_only && proposed == current)
    });
    tracing::debug!(target: "vein.optimizer", event = "optimizer_semi_join_placement_cost",
        current_probe_rows = ?current, proposed_probe_rows = ?proposed, allowed,
        equality_only,
        reason = if allowed { "non_increasing_probe_population" } else { "larger_or_unknown_probe_population" },
        "semi-join placement cost guard");
    allowed
}

fn node_count(plan: &LogicalPlan) -> usize {
    let mut pending = vec![plan];
    let mut count = 0;
    while let Some(node) = pending.pop() {
        count += 1;
        pending.extend(node.children());
    }
    count
}
