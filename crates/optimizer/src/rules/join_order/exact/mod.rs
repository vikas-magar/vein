use super::*;

mod subsets;

pub(super) fn enumerate<C: JoinCostModel>(
    graph: &JoinGraph<'_>,
    context: &OptimizerContext,
    config: &JoinOrderConfig,
    cost_model: &C,
) -> SearchResult {
    let mut alternatives = Vec::new();
    let mut states = match search_state::initialize(graph, context) {
        Ok(states) => states,
        Err(outcome) => return search_state::fallback(outcome, 0, None, alternatives),
    };

    let candidate_splits = match subsets::enumerate_subsets(graph, config, cost_model, &mut states, &mut alternatives) {
        Ok(count) => count,
        Err(failure) => return failure,
    };
    let original_cost = match original_tree_cost(graph.full_mask, graph, &states, cost_model) {
        Ok(cost) => cost,
        Err(reason) => {
            tracing::warn!(target: "vein.optimizer", event = "optimizer_join_order_original_cost_unavailable",
            relation_count = graph.leaves.len(), candidate_splits, reason = ?reason,
            "could not reconstruct the original join tree cost from enumerated relation states");
            return SearchResult::Fallback {
                outcome: JoinOrderOutcome::InvalidCost,
                candidate_splits,
                original_cost: None,
                alternatives,
            };
        }
    };
    search_state::finish(graph, &states, candidate_splits, original_cost, alternatives)
}
