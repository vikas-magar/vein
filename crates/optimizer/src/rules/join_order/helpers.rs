use super::*;

pub(super) fn checked_cost<C: JoinCostModel>(
    model: &C,
    left: JoinCostInput,
    right: JoinCostInput,
    output_rows: f64,
) -> Option<f64> {
    let cost = model.combine(left, right, output_rows)?;
    (cost.is_finite() && cost >= 0.0).then_some(cost)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum OriginalTreeCostFailure {
    MissingOriginalSplit { relation_mask: usize },
    MissingSubplanState { relation_mask: usize },
    MissingCardinality { relation_mask: usize },
    InvalidCost { relation_mask: usize },
}

pub(super) fn original_tree_cost<C: JoinCostModel>(
    mask: usize,
    graph: &JoinGraph<'_>,
    states: &HashMap<usize, SubplanState<'_>>,
    cost_model: &C,
) -> std::result::Result<f64, OriginalTreeCostFailure> {
    if mask.count_ones() == 1 {
        return Ok(0.0);
    }
    let (left_mask, right_mask) = *graph.original_splits.get(&mask)
        .ok_or(OriginalTreeCostFailure::MissingOriginalSplit { relation_mask: mask })?;
    let left_cost = original_tree_cost(left_mask, graph, states, cost_model)?;
    let right_cost = original_tree_cost(right_mask, graph, states, cost_model)?;
    let left_state = states.get(&left_mask)
        .ok_or(OriginalTreeCostFailure::MissingSubplanState { relation_mask: left_mask })?;
    let right_state = states.get(&right_mask)
        .ok_or(OriginalTreeCostFailure::MissingSubplanState { relation_mask: right_mask })?;
    let output_state = states.get(&mask)
        .ok_or(OriginalTreeCostFailure::MissingSubplanState { relation_mask: mask })?;
    let left_rows = left_state.relation.rows.value()
        .ok_or(OriginalTreeCostFailure::MissingCardinality { relation_mask: left_mask })?;
    let right_rows = right_state.relation.rows.value()
        .ok_or(OriginalTreeCostFailure::MissingCardinality { relation_mask: right_mask })?;
    let output_rows = output_state.relation.rows.value()
        .ok_or(OriginalTreeCostFailure::MissingCardinality { relation_mask: mask })?;
    checked_cost(
        cost_model,
        JoinCostInput {
            rows: left_rows,
            cumulative_cost: left_cost,
        },
        JoinCostInput {
            rows: right_rows,
            cumulative_cost: right_cost,
        },
        output_rows,
    )
        .ok_or(OriginalTreeCostFailure::InvalidCost { relation_mask: mask })
}

pub(super) fn same_tree(
    mask: usize,
    graph: &JoinGraph<'_>,
    states: &HashMap<usize, SubplanState<'_>>,
) -> bool {
    if mask.count_ones() == 1 {
        return true;
    }
    let Some(state) = states.get(&mask) else {
        return false;
    };
    let (Some(selected), Some(original)) = (state.split, graph.original_splits.get(&mask)) else {
        return false;
    };
    selected == *original
        && same_tree(selected.0, graph, states)
        && same_tree(selected.1, graph, states)
}

pub(super) fn crossing_predicates<'a>(
    graph: &'a JoinGraph<'a>,
    left: usize,
    right: usize,
) -> Vec<&'a LogicalExpr> {
    if !predicates::has_edge(left, right, &graph.adjacency) { return Vec::new(); }
    graph
        .predicates
        .iter()
        .filter(|predicate| predicates::crosses(predicate.relations, left, right))
        .map(|predicate| predicate.expression)
        .collect()
}

pub(super) fn is_connected(mask: usize, adjacency: &[usize]) -> bool {
    if mask == 0 {
        return false;
    }
    let first = mask.trailing_zeros() as usize;
    let mut visited = 1usize << first;
    let mut pending = vec![first];
    while let Some(relation) = pending.pop() {
        let mut neighbors = adjacency[relation] & mask & !visited;
        while neighbors != 0 {
            let next = neighbors.trailing_zeros() as usize;
            let bit = 1usize << next;
            visited |= bit;
            neighbors &= !bit;
            pending.push(next);
        }
    }
    visited == mask
}
