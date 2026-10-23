//! Shared cardinality and reconstruction state for exact and approximate search.
use super::*;

pub(super) fn initialize<'a>(graph: &JoinGraph<'a>, context: &'a OptimizerContext)
                             -> std::result::Result<HashMap<usize, SubplanState<'a>>, JoinOrderOutcome> {
    if !is_connected(graph.full_mask, &graph.adjacency) { return Err(JoinOrderOutcome::DisconnectedGraph); }
    let mut states = HashMap::new();
    for (index, leaf) in graph.leaves.iter().enumerate() {
        let relation = context.estimate_relation(leaf);
        if relation.rows.value().is_none() {
            tracing::warn!(target: "vein.optimizer", event = "optimizer_join_order_missing_relation_rows",
                relation_index = index, relation = %graph.labels[index], estimate_source = ?relation.rows.source);
            return Err(match relation.rows.source {
                crate::estimation::EstimateSource::MissingStatistics | crate::estimation::EstimateSource::StaleStatistics => JoinOrderOutcome::InsufficientStatistics,
                _ => JoinOrderOutcome::UnsupportedShape,
            });
        }
        states.insert(1 << index, SubplanState { relation, cumulative_cost: 0.0, split: None, order: vec![index] });
    }
    Ok(states)
}

pub(super) fn candidate<'a, C: JoinCostModel>(
    graph: &'a JoinGraph<'a>, cost_model: &C,
    states: &HashMap<usize, SubplanState<'a>>, left_mask: usize, right_mask: usize,
) -> std::result::Result<SubplanState<'a>, JoinOrderOutcome> {
    let left = &states[&left_mask];
    let right = &states[&right_mask];
    let predicates = crossing_predicates(graph, left_mask, right_mask);
    if predicates.is_empty() { return Err(JoinOrderOutcome::DisconnectedGraph); }
    let relation = inner_join(left.relation.clone(), right.relation.clone(), &predicates, &graph.analysis_schema);
    let output_rows = relation.rows.value().ok_or(JoinOrderOutcome::UnsupportedShape)?;
    let cumulative_cost = checked_cost(cost_model,
                                       JoinCostInput { rows: left.relation.rows.value().ok_or(JoinOrderOutcome::InvalidCost)?, cumulative_cost: left.cumulative_cost },
                                       JoinCostInput { rows: right.relation.rows.value().ok_or(JoinOrderOutcome::InvalidCost)?, cumulative_cost: right.cumulative_cost },
                                       output_rows).ok_or(JoinOrderOutcome::InvalidCost)?;
    let mut order = left.order.clone();
    order.extend_from_slice(&right.order);
    Ok(SubplanState { relation, cumulative_cost, split: Some((left_mask, right_mask)), order })
}

pub(super) fn finish(
    graph: &JoinGraph<'_>, states: &HashMap<usize, SubplanState<'_>>, candidate_splits: usize,
    original_cost: f64, alternatives: Vec<JoinOrderAlternative>,
) -> SearchResult {
    let selected = &states[&graph.full_mask];
    let selected_cost = selected.cumulative_cost;
    if selected_cost >= original_cost || same_tree(graph.full_mask, graph, states) {
        return SearchResult::NoImprovement {
            order: (0..graph.leaves.len()).collect(),
            candidate_splits,
            original_cost,
            selected_cost: original_cost,
            alternatives,
        };
    }
    SearchResult::Selected {
        splits: states.iter().filter_map(|(mask, state)| state.split.map(|split| (*mask, split))).collect(),
        order: selected.order.clone(),
        candidate_splits,
        original_cost,
        selected_cost,
        alternatives,
    }
}

pub(super) fn fallback(outcome: JoinOrderOutcome, candidate_splits: usize,
                       original_cost: Option<f64>, alternatives: Vec<JoinOrderAlternative>) -> SearchResult {
    SearchResult::Fallback { outcome, candidate_splits, original_cost, alternatives }
}
