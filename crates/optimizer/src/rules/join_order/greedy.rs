//! Bounded connected-pair search when exact subset enumeration is too expensive.
use super::*;

pub(super) fn enumerate<'a, C: JoinCostModel>(
    graph: &'a JoinGraph<'a>, context: &'a OptimizerContext, config: &JoinOrderConfig, model: &C,
) -> SearchResult {
    let mut states = match search_state::initialize(graph, context) {
        Ok(states) => states,
        Err(outcome) => return search_state::fallback(outcome, 0, None, vec![]),
    };
    if let Err(outcome) = seed_original(graph.full_mask, graph, model, &mut states) {
        return search_state::fallback(outcome, 0, None, vec![]);
    }
    let original_cost = states[&graph.full_mask].cumulative_cost;
    let mut forest = (0..graph.leaves.len()).map(|index| 1usize << index).collect::<Vec<_>>();
    let mut alternatives = Vec::new();
    let mut examined = 0usize;
    while forest.len() > 1 {
        let mut best: Option<(usize, usize, SubplanState<'_>, usize)> = None;
        for a in 0..forest.len() {
            for b in a + 1..forest.len() {
                let (left_mask, right_mask) = (forest[a], forest[b]);
                if crossing_predicates(graph, left_mask, right_mask).is_empty() { continue; }
                if examined >= config.max_candidate_splits {
                    return search_state::fallback(JoinOrderOutcome::SearchBudgetExceeded,
                                                  examined, Some(original_cost), alternatives);
                }
                examined += 1;
                for (left_mask, right_mask) in [(left_mask, right_mask), (right_mask, left_mask)] {
                    let candidate = match search_state::candidate(graph, model, &states, left_mask, right_mask) {
                        Ok(candidate) => candidate,
                        Err(outcome) => return search_state::fallback(outcome, examined, Some(original_cost), alternatives),
                    };
                    let index = alternatives.len();
                    alternatives.push(diagnostic(graph, model, &states, &candidate, left_mask, right_mask));
                    let replace = best.as_ref().is_none_or(|(_, _, best, _)| {
                        candidate.cumulative_cost < best.cumulative_cost ||
                            (candidate.cumulative_cost == best.cumulative_cost && candidate.order < best.order)
                    });
                    if replace { best = Some((a, b, candidate, index)); }
                }
            }
        }
        let Some((a, b, candidate, index)) = best else {
            return search_state::fallback(JoinOrderOutcome::DisconnectedGraph, examined, Some(original_cost), alternatives);
        };
        alternatives[index].best_for_subset = true;
        let mask = forest[a] | forest[b];
        states.insert(mask, candidate);
        forest.remove(b);
        forest[a] = mask;
    }
    search_state::finish(graph, &states, examined, original_cost, alternatives)
}

fn seed_original<'a, C: JoinCostModel>(
    mask: usize, graph: &'a JoinGraph<'a>, model: &C, states: &mut HashMap<usize, SubplanState<'a>>,
) -> std::result::Result<(), JoinOrderOutcome> {
    if states.contains_key(&mask) { return Ok(()); }
    let &(left, right) = graph.original_splits.get(&mask).ok_or(JoinOrderOutcome::UnsupportedShape)?;
    seed_original(left, graph, model, states)?;
    seed_original(right, graph, model, states)?;
    states.insert(mask, search_state::candidate(graph, model, states, left, right)?);
    Ok(())
}

fn diagnostic<C: JoinCostModel>(
    graph: &JoinGraph<'_>, model: &C, states: &HashMap<usize, SubplanState<'_>>,
    candidate: &SubplanState<'_>, left_mask: usize, right_mask: usize,
) -> JoinOrderAlternative {
    let left = JoinCostInput { rows: states[&left_mask].relation.rows.value().unwrap(), cumulative_cost: states[&left_mask].cumulative_cost };
    let right = JoinCostInput { rows: states[&right_mask].relation.rows.value().unwrap(), cumulative_cost: states[&right_mask].cumulative_cost };
    let output_rows = candidate.relation.rows.value().unwrap();
    JoinOrderAlternative {
        relation_mask: left_mask | right_mask,
        left_relation_mask: left_mask,
        right_relation_mask: right_mask,
        left_rows: left.rows,
        right_rows: right.rows,
        output_rows,
        cost: JoinCostBreakdown {
            left_subtree_cost: left.cumulative_cost,
            right_subtree_cost: right.cumulative_cost,
            local_cost: model.local_cost(left, right, output_rows, candidate.cumulative_cost).filter(|value| value.is_finite() && *value >= 0.0),
            cumulative_cost: candidate.cumulative_cost,
        },
        best_for_subset: false,
        original_tree_split: graph.original_splits.get(&(left_mask | right_mask)) == Some(&(left_mask, right_mask)),
    }
}
