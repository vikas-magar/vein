use super::*;

pub(super) fn enumerate_subsets<'a, C: JoinCostModel>(
    graph: &'a JoinGraph<'a>,
    config: &JoinOrderConfig,
    cost_model: &C,
    states: &mut HashMap<usize, SubplanState<'a>>,
    alternatives: &mut Vec<JoinOrderAlternative>,
) -> std::result::Result<usize, SearchResult> {
    let mut candidate_splits = 0usize;
    for width in 2..=graph.leaves.len() {
        for mask in 1..=graph.full_mask {
            if mask.count_ones() as usize != width || !is_connected(mask, &graph.adjacency) {
                continue;
            }
            let first_alternative = alternatives.len();
            let mut canonical_relation: Option<RelationEstimate<'_>> = None;
            let mut best: Option<(f64, Vec<usize>, (usize, usize))> = None;
            let mut left_mask = (mask - 1) & mask;
            while left_mask > 0 {
                let right_mask = mask ^ left_mask;
                if left_mask < right_mask {
                    candidate_splits = candidate_splits.saturating_add(1);
                    if candidate_splits > config.max_candidate_splits {
                        return Err(SearchResult::Fallback {
                            outcome: JoinOrderOutcome::SearchBudgetExceeded,
                            candidate_splits,
                            original_cost: None,
                            alternatives: std::mem::take(alternatives),
                        });
                    }
                    let (Some(left), Some(right)) =
                        (states.get(&left_mask), states.get(&right_mask))
                    else {
                        left_mask = (left_mask - 1) & mask;
                        continue;
                    };
                    let predicates = crossing_predicates(graph, left_mask, right_mask);
                    if predicates.is_empty() {
                        left_mask = (left_mask - 1) & mask;
                        continue;
                    }
                    let estimate = inner_join(
                        left.relation.clone(),
                        right.relation.clone(),
                        &predicates,
                        &graph.analysis_schema,
                    );
                    if estimate.rows.value().is_none() {
                        let outcome = match estimate.rows.source {
                            crate::estimation::EstimateSource::MissingStatistics
                            | crate::estimation::EstimateSource::StaleStatistics =>
                                JoinOrderOutcome::InsufficientStatistics,
                            _ => JoinOrderOutcome::UnsupportedShape,
                        };
                        tracing::warn!(target: "vein.optimizer", event = "optimizer_join_order_missing_candidate_rows",
                            left_mask, right_mask, left_rows = ?left.relation.rows.value(),
                            right_rows = ?right.relation.rows.value(),
                            estimate_source = ?estimate.rows.source,
                            confidence = ?estimate.rows.confidence,
                            "join-order candidate has no output cardinality estimate");
                        return Err(SearchResult::Fallback {
                            outcome,
                            candidate_splits,
                            original_cost: None,
                            alternatives: std::mem::take(alternatives),
                        });
                    }
                    if let Some(canonical) = &canonical_relation {
                        if !canonical.has_same_join_summary(&estimate) {
                            return Err(SearchResult::Fallback {
                                outcome: JoinOrderOutcome::AmbiguousEstimates,
                                candidate_splits,
                                original_cost: None,
                                alternatives: std::mem::take(alternatives),
                            });
                        }
                    } else {
                        canonical_relation = Some(estimate);
                    }
                    let output_rows = canonical_relation
                        .as_ref()
                        .and_then(|relation| relation.rows.value())
                        .expect("known canonical cardinality");

                    for (first_mask, second_mask) in [
                        (left_mask, right_mask),
                        (right_mask, left_mask),
                    ] {
                        let first = states.get(&first_mask).expect("split state exists");
                        let second = states.get(&second_mask).expect("split state exists");
                        let left_input = JoinCostInput {
                            rows: first.relation.rows.value().expect("known row estimate"),
                            cumulative_cost: first.cumulative_cost,
                        };
                        let right_input = JoinCostInput {
                            rows: second.relation.rows.value().expect("known row estimate"),
                            cumulative_cost: second.cumulative_cost,
                        };
                        let Some(cost) = checked_cost(cost_model, left_input, right_input, output_rows)
                        else {
                            return Err(SearchResult::Fallback {
                                outcome: JoinOrderOutcome::InvalidCost,
                                candidate_splits,
                                original_cost: None,
                                alternatives: std::mem::take(alternatives),
                            });
                        };
                        let local_cost = cost_model
                            .local_cost(left_input, right_input, output_rows, cost)
                            .filter(|value| value.is_finite() && *value >= 0.0);
                        alternatives.push(JoinOrderAlternative {
                            relation_mask: mask,
                            left_relation_mask: first_mask,
                            right_relation_mask: second_mask,
                            left_rows: left_input.rows,
                            right_rows: right_input.rows,
                            output_rows,
                            cost: JoinCostBreakdown {
                                left_subtree_cost: left_input.cumulative_cost,
                                right_subtree_cost: right_input.cumulative_cost,
                                local_cost,
                                cumulative_cost: cost,
                            },
                            best_for_subset: false,
                            original_tree_split: graph
                                .original_splits
                                .get(&mask)
                                .is_some_and(|split| *split == (first_mask, second_mask)),
                        });
                        let mut order = first.order.clone();
                        order.extend_from_slice(&second.order);
                        let replace = best.as_ref().is_none_or(|(best_cost, best_order, _)| {
                            cost < *best_cost || (cost == *best_cost && order < *best_order)
                        });
                        if replace {
                            best = Some((cost, order, (first_mask, second_mask)));
                        }
                    }
                }
                left_mask = (left_mask - 1) & mask;
            }

            let (Some(relation), Some((cumulative_cost, order, split))) =
                (canonical_relation, best)
            else {
                return Err(SearchResult::Fallback {
                    outcome: JoinOrderOutcome::DisconnectedGraph,
                    candidate_splits,
                    original_cost: None,
                    alternatives: std::mem::take(alternatives),
                });
            };
            for alternative in &mut alternatives[first_alternative..] {
                alternative.best_for_subset =
                    (alternative.left_relation_mask, alternative.right_relation_mask) == split;
            }
            states.insert(
                mask,
                SubplanState {
                    relation,
                    cumulative_cost,
                    split: Some(split),
                    order,
                },
            );
        }
    }
    Ok(candidate_splits)
}
