use super::*;

impl<C: JoinCostModel> JoinOrderRule<C> {
    pub(super) fn attempt<'a>(
        &self,
        plan: &'a LogicalPlan,
        context: &'a OptimizerContext,
    ) -> Option<SearchAttempt> {
        if !is_reorderable_join(plan) {
            return None;
        }

        let graph = match JoinGraph::build(plan) {
            Ok(graph) => graph,
            Err(failure) => {
                if failure.relation_count < 3 {
                    return None;
                }
                return Some(SearchAttempt::Fallback(JoinOrderDecision {
                    relation_count: failure.relation_count,
                    relation_labels: failure.relation_labels,
                    original_order: (0..failure.relation_count).collect(),
                    selected_order: (0..failure.relation_count).collect(),
                    candidate_splits: 0,
                    original_cost: None,
                    selected_cost: None,
                    cardinality_assumption:
                    JoinCardinalityAssumption::UniformDistinctValueOverlap,
                    alternatives: Vec::new(),
                    outcome: failure.outcome,
                }));
            }
        };

        let config = &context.config().join_order;
        if graph.leaves.len() >= usize::BITS as usize {
            return Some(SearchAttempt::Fallback(graph.decision(
                0,
                None,
                None,
                JoinOrderOutcome::RelationLimitExceeded,
            )));
        }

        let search = if graph.leaves.len() > config.max_relations {
            tracing::debug!(target: "vein.optimizer", event = "optimizer_join_search_strategy",
                strategy = "greedy", reason = "exact_relation_limit", relations = graph.leaves.len());
            greedy::enumerate(&graph, context, config, &self.cost_model)
        } else {
            match enumerate(&graph, context, config, &self.cost_model) {
                SearchResult::Fallback { outcome, .. } if matches!(outcome,
                    JoinOrderOutcome::SearchBudgetExceeded | JoinOrderOutcome::AmbiguousEstimates) => {
                    tracing::debug!(target: "vein.optimizer", event = "optimizer_join_search_strategy",
                        strategy = "greedy", reason = ?outcome, relations = graph.leaves.len());
                    greedy::enumerate(&graph, context, config, &self.cost_model)
                }
                result => result,
            }
        };
        match search {
            SearchResult::Selected {
                splits,
                order,
                candidate_splits,
                original_cost,
                selected_cost,
                alternatives,
            } => {
                let mut decision = graph.decision(
                    candidate_splits,
                    Some(original_cost),
                    Some(selected_cost),
                    JoinOrderOutcome::Reordered,
                );
                decision.selected_order = order;
                decision.alternatives = alternatives;
                let predicates = graph
                    .predicates
                    .iter()
                    .map(|predicate| OwnedPredicate {
                        relations: predicate.relations,
                        expression: predicate.expression.clone(),
                    })
                    .collect();
                Some(SearchAttempt::Selected(PreparedJoinOrder {
                    splits,
                    predicates,
                    relation_count: graph.leaves.len(),
                    decision,
                }))
            }
            SearchResult::NoImprovement {
                order,
                candidate_splits,
                original_cost,
                selected_cost,
                alternatives,
            } => {
                let mut decision = graph.decision(
                    candidate_splits,
                    Some(original_cost),
                    Some(selected_cost),
                    JoinOrderOutcome::NoImprovement,
                );
                decision.selected_order = order;
                decision.alternatives = alternatives;
                Some(SearchAttempt::NoChange(decision))
            }
            SearchResult::Fallback {
                outcome,
                candidate_splits,
                original_cost,
                alternatives,
            } => {
                let mut decision = graph.decision(
                    candidate_splits,
                    original_cost,
                    None,
                    outcome,
                );
                decision.alternatives = alternatives;
                Some(SearchAttempt::Fallback(decision))
            }
        }
    }
}
