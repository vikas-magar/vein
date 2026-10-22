use super::*;

pub(super) fn log_decision(decision: &JoinOrderDecision) {
    let original_order_labels = relation_order_labels(
        &decision.original_order,
        &decision.relation_labels,
    );
    let selected_order_labels = relation_order_labels(
        &decision.selected_order,
        &decision.relation_labels,
    );
    if decision.outcome == JoinOrderOutcome::Reordered {
        tracing::info!(
            target: "vein.optimizer",
            event = "optimizer_join_order_selected",
            relation_count = decision.relation_count,
            relation_labels = ?decision.relation_labels,
            original_order = ?decision.original_order,
            original_order_labels = ?original_order_labels,
            selected_order = ?decision.selected_order,
            selected_order_labels = ?selected_order_labels,
            candidate_splits = decision.candidate_splits,
            original_cost = ?decision.original_cost,
            selected_cost = ?decision.selected_cost,
            cardinality_assumption = ?decision.cardinality_assumption,
            alternative_count = decision.alternatives.len(),
            "optimizer_join_order_selected"
        );
    } else {
        tracing::info!(
            target: "vein.optimizer",
            event = "optimizer_join_order_decision",
            relation_count = decision.relation_count,
            relation_labels = ?decision.relation_labels,
            original_order = ?decision.original_order,
            original_order_labels = ?original_order_labels,
            selected_order = ?decision.selected_order,
            selected_order_labels = ?selected_order_labels,
            outcome = ?decision.outcome,
            candidate_splits = decision.candidate_splits,
            original_cost = ?decision.original_cost,
            selected_cost = ?decision.selected_cost,
            cardinality_assumption = ?decision.cardinality_assumption,
            alternative_count = decision.alternatives.len(),
            "optimizer_join_order_decision"
        );
    }
    for alternative in &decision.alternatives {
        tracing::debug!(
            target: "vein.optimizer",
            event = "optimizer_join_order_alternative",
            relation_mask = alternative.relation_mask,
            left_relation_mask = alternative.left_relation_mask,
            right_relation_mask = alternative.right_relation_mask,
            left_rows = alternative.left_rows,
            right_rows = alternative.right_rows,
            output_rows = alternative.output_rows,
            left_subtree_cost = alternative.cost.left_subtree_cost,
            right_subtree_cost = alternative.cost.right_subtree_cost,
            local_cost = ?alternative.cost.local_cost,
            cumulative_cost = alternative.cost.cumulative_cost,
            best_for_subset = alternative.best_for_subset,
            original_tree_split = alternative.original_tree_split,
            "optimizer_join_order_alternative"
        );
    }
}

pub(super) fn relation_order_labels<'a>(order: &[usize], labels: &'a [String]) -> Vec<&'a str> {
    order
        .iter()
        .filter_map(|relation| labels.get(*relation).map(String::as_str))
        .collect()
}
