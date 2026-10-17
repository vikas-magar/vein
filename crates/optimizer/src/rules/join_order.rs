//! Bounded dynamic programming for safe inner equijoins.
//!
//! The rule operates on relation metadata and borrowed estimates only. It
//! moves existing logical subtrees when rebuilding a selected alternative;
//! execution batches and column buffers never enter the optimizer.

use std::collections::HashMap;

use error::{Result, VeinError};
use parser::ast::{expr::BinaryOp, pipe_op::JoinType};
use plan::{LogicalExpr, LogicalPlan, PlanTraversal, VeinDataType, VeinSchema};

use crate::context::{
    JoinCardinalityAssumption, JoinCostBreakdown, JoinOrderAlternative, JoinOrderConfig,
    JoinOrderDecision, JoinOrderOutcome, OptimizerContext,
};
use crate::estimation::{inner_join, RelationEstimate};
use crate::rule::{OptimizerRule, RuleType};

/// Cost information for one input to a join-cost model.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct JoinCostInput {
    /// Estimated rows produced by this input.
    pub rows: f64,
    /// Cumulative cost of joins below this input.
    pub cumulative_cost: f64,
}

/// Extension point for comparing legal join trees.
///
/// Return a finite, non-negative cumulative cost. Returning None declines
/// to make a choice and causes the optimizer to preserve the original order.
pub trait JoinCostModel: Send + Sync {
    /// Combines child costs with the estimated output cardinality.
    fn combine(
        &self,
        left: JoinCostInput,
        right: JoinCostInput,
        output_rows: f64,
    ) -> Option<f64>;

    /// Exposes the local cost term when the model can decompose its result.
    ///
    /// Custom models may leave this unknown while still participating in
    /// planning; diagnostic inputs and the cumulative result remain available.
    fn local_cost(
        &self,
        _left: JoinCostInput,
        _right: JoinCostInput,
        _output_rows: f64,
        _cumulative_cost: f64,
    ) -> Option<f64> {
        None
    }
}

/// DuckDB-style cumulative cardinality cost: sum each intermediate output.
#[derive(Debug, Default, Clone, Copy)]
pub struct CumulativeCardinalityCost;

impl JoinCostModel for CumulativeCardinalityCost {
    fn combine(
        &self,
        left: JoinCostInput,
        right: JoinCostInput,
        output_rows: f64,
    ) -> Option<f64> {
        let cost = left.cumulative_cost + right.cumulative_cost + output_rows;
        (left.rows.is_finite()
            && right.rows.is_finite()
            && output_rows.is_finite()
            && cost.is_finite()
            && left.rows >= 0.0
            && right.rows >= 0.0
            && output_rows >= 0.0
            && cost >= 0.0)
            .then_some(cost)
    }

    fn local_cost(
        &self,
        _left: JoinCostInput,
        _right: JoinCostInput,
        output_rows: f64,
        _cumulative_cost: f64,
    ) -> Option<f64> {
        Some(output_rows)
    }
}

/// Statistics-driven join-order rule using a bounded exact subset search.
pub struct JoinOrderRule<C = CumulativeCardinalityCost> {
    cost_model: C,
}

impl Default for JoinOrderRule<CumulativeCardinalityCost> {
    fn default() -> Self {
        Self {
            cost_model: CumulativeCardinalityCost,
        }
    }
}

impl<C: JoinCostModel> JoinOrderRule<C> {
    /// Creates the exact-search rule with a caller-supplied cost model.
    pub fn with_cost_model(cost_model: C) -> Self {
        Self { cost_model }
    }

    fn rewrite_owned(
        &self,
        plan: LogicalPlan,
        context: &OptimizerContext,
        decisions: &mut Vec<JoinOrderDecision>,
    ) -> Result<(LogicalPlan, bool)> {
        match self.attempt(&plan, context) {
            Some(SearchAttempt::Selected(prepared)) => {
                log_decision(&prepared.decision);
                decisions.push(prepared.decision.clone());
                Ok((rebuild(plan, prepared)?, true))
            }
            Some(SearchAttempt::NoChange(decision)) => {
                log_decision(&decision);
                decisions.push(decision);
                Ok((plan, false))
            }
            Some(SearchAttempt::Fallback(decision)) => {
                log_decision(&decision);
                decisions.push(decision);
                self.rewrite_children(plan, context, decisions)
            }
            None => self.rewrite_children(plan, context, decisions),
        }
    }

    fn rewrite_children(
        &self,
        plan: LogicalPlan,
        context: &OptimizerContext,
        decisions: &mut Vec<JoinOrderDecision>,
    ) -> Result<(LogicalPlan, bool)> {
        let changed = std::cell::Cell::new(false);
        let rewritten = plan.map_children(&mut |child| {
            let (child, child_changed) = self.rewrite_owned(*child, context, decisions)?;
            changed.set(changed.get() || child_changed);
            Ok(Box::new(child))
        })?;
        Ok((rewritten, changed.get()))
    }

    fn attempt<'a>(
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
        if graph.leaves.len() > config.max_relations
            || graph.leaves.len() >= usize::BITS as usize
        {
            return Some(SearchAttempt::Fallback(graph.decision(
                0,
                None,
                None,
                JoinOrderOutcome::RelationLimitExceeded,
            )));
        }

        match enumerate(&graph, context, config, &self.cost_model) {
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
                        left_relation: predicate.left_relation,
                        right_relation: predicate.right_relation,
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

impl<C: JoinCostModel + 'static> OptimizerRule for JoinOrderRule<C> {
    fn name(&self) -> &str {
        "join_order"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::CostBased
    }

    fn is_plan_rule(&self) -> bool {
        true
    }

    fn apply_plan(
        &self,
        _plan: &LogicalPlan,
        _context: &OptimizerContext,
    ) -> Result<Option<LogicalPlan>> {
        Ok(None)
    }

    fn apply_owned_plan(
        &self,
        plan: LogicalPlan,
        context: &OptimizerContext,
    ) -> Result<(LogicalPlan, bool, Vec<JoinOrderDecision>)> {
        let mut decisions = Vec::new();
        let (plan, changed) = self.rewrite_owned(plan, context, &mut decisions)?;
        Ok((plan, changed, decisions))
    }

    fn apply(&self, _plan: &LogicalPlan, _context: &OptimizerContext) -> Result<Option<LogicalPlan>> {
        Ok(None)
    }
}

struct JoinGraph<'a> {
    leaves: Vec<&'a LogicalPlan>,
    predicates: Vec<JoinPredicate<'a>>,
    original_splits: HashMap<usize, (usize, usize)>,
    adjacency: Vec<usize>,
    analysis_schema: VeinSchema,
    full_mask: usize,
    labels: Vec<String>,
}

impl JoinGraph<'_> {
    fn decision(
        &self,
        candidate_splits: usize,
        original_cost: Option<f64>,
        selected_cost: Option<f64>,
        outcome: JoinOrderOutcome,
    ) -> JoinOrderDecision {
        JoinOrderDecision {
            relation_count: self.leaves.len(),
            relation_labels: self.labels.clone(),
            original_order: (0..self.leaves.len()).collect(),
            selected_order: (0..self.leaves.len()).collect(),
            candidate_splits,
            original_cost,
            selected_cost,
            cardinality_assumption: JoinCardinalityAssumption::UniformDistinctValueOverlap,
            alternatives: Vec::new(),
            outcome,
        }
    }

    fn build(root: &'_ LogicalPlan) -> std::result::Result<JoinGraph<'_>, GraphFailure> {
        let mut pending = vec![(root, false)];
        let mut leaves = Vec::new();
        let mut joins = Vec::new();
        let mut original_splits = HashMap::new();
        let mut subtree_masks = Vec::new();
        while let Some((node, visited)) = pending.pop() {
            if is_reorderable_join(node) {
                let LogicalPlan::Join { left, right, .. } = node else {
                    unreachable!("join eligibility checked the plan variant");
                };
                if !visited {
                    pending.push((node, true));
                    pending.push((right, false));
                    pending.push((left, false));
                } else {
                    let right_mask = subtree_masks
                        .pop()
                        .expect("right child mask is produced before its parent");
                    let left_mask = subtree_masks
                        .pop()
                        .expect("left child mask is produced before its parent");
                    let parent_mask = left_mask | right_mask;
                    original_splits.insert(parent_mask, (left_mask, right_mask));
                    joins.push((node, left_mask, right_mask));
                    subtree_masks.push(parent_mask);
                }
            } else {
                let relation_index = leaves.len();
                if relation_index >= usize::BITS as usize {
                    let mut relation_labels = leaves
                        .iter()
                        .map(|leaf: &&LogicalPlan| {
                            leaf.table_name()
                                .unwrap_or_else(|| leaf.operator_name())
                                .to_string()
                        })
                        .collect::<Vec<_>>();
                    relation_labels.push(
                        node.table_name()
                            .unwrap_or_else(|| node.operator_name())
                            .to_string(),
                    );
                    return Err(GraphFailure {
                        relation_count: relation_index + 1,
                        relation_labels,
                        outcome: JoinOrderOutcome::RelationLimitExceeded,
                    });
                }
                leaves.push(node);
                subtree_masks.push(1usize << relation_index);
            }
        }

        let labels = leaves
            .iter()
            .map(|leaf| {
                leaf.table_name()
                    .unwrap_or_else(|| leaf.operator_name())
                    .to_string()
            })
            .collect::<Vec<_>>();
        let relation_count = leaves.len();
        if relation_count >= usize::BITS as usize {
            return Err(GraphFailure {
                relation_count,
                relation_labels: labels,
                outcome: JoinOrderOutcome::RelationLimitExceeded,
            });
        }

        debug_assert_eq!(subtree_masks, vec![(1usize << relation_count) - 1]);

        let mut column_relations = HashMap::new();
        let mut all_fields = Vec::new();
        for (relation, leaf) in leaves.iter().enumerate() {
            for column in &leaf.schema().fields {
                if column_relations.insert(column.id, relation).is_some() {
                    return Err(GraphFailure {
                        relation_count,
                        relation_labels: labels,
                        outcome: JoinOrderOutcome::UnsupportedShape,
                    });
                }
                all_fields.push(std::sync::Arc::clone(column));
            }
        }

        let mut graph_predicates = Vec::new();
        let mut adjacency = vec![0usize; relation_count];
        for (node, left_mask, right_mask) in joins {
            let LogicalPlan::Join { on, .. } = node
            else {
                unreachable!("join eligibility checked the plan variant");
            };
            let Some(on) = on.as_ref() else {
                unreachable!("join eligibility requires a predicate");
            };
            let Some(atoms) = equi_atoms(on) else {
                return Err(GraphFailure {
                    relation_count,
                    relation_labels: labels,
                    outcome: JoinOrderOutcome::UnsupportedShape,
                });
            };
            for expression in atoms {
                let LogicalExpr::BinaryOp { left: a, right: b, .. } = expression else {
                    unreachable!("equi_atoms returns equality expressions");
                };
                let (LogicalExpr::Column(a), LogicalExpr::Column(b)) =
                    (a.as_ref(), b.as_ref())
                else {
                    unreachable!("equi_atoms validates column operands");
                };
                let (Some(&a_relation), Some(&b_relation)) = (
                    column_relations.get(&a.id),
                    column_relations.get(&b.id),
                ) else {
                    return Err(GraphFailure {
                        relation_count,
                        relation_labels: labels,
                        outcome: JoinOrderOutcome::UnsupportedShape,
                    });
                };
                if a_relation == b_relation {
                    return Err(GraphFailure {
                        relation_count,
                        relation_labels: labels,
                        outcome: JoinOrderOutcome::UnsupportedShape,
                    });
                }
                let a_bit = 1usize << a_relation;
                let b_bit = 1usize << b_relation;
                let crosses_children = (left_mask & a_bit != 0 && right_mask & b_bit != 0)
                    || (left_mask & b_bit != 0 && right_mask & a_bit != 0);
                if !crosses_children {
                    return Err(GraphFailure {
                        relation_count,
                        relation_labels: labels,
                        outcome: JoinOrderOutcome::UnsupportedShape,
                    });
                }
                adjacency[a_relation] |= b_bit;
                adjacency[b_relation] |= a_bit;
                graph_predicates.push(JoinPredicate {
                    expression,
                    left_relation: a_relation,
                    right_relation: b_relation,
                });
            }
        }

        Ok(JoinGraph {
            leaves,
            predicates: graph_predicates,
            original_splits,
            adjacency,
            analysis_schema: VeinSchema::new(all_fields),
            full_mask: (1usize << relation_count) - 1,
            labels,
        })
    }
}

struct GraphFailure {
    relation_count: usize,
    relation_labels: Vec<String>,
    outcome: JoinOrderOutcome,
}

struct JoinPredicate<'a> {
    expression: &'a LogicalExpr,
    left_relation: usize,
    right_relation: usize,
}

struct OwnedPredicate {
    expression: LogicalExpr,
    left_relation: usize,
    right_relation: usize,
}

struct PreparedJoinOrder {
    splits: HashMap<usize, (usize, usize)>,
    predicates: Vec<OwnedPredicate>,
    relation_count: usize,
    decision: JoinOrderDecision,
}

enum SearchAttempt {
    Selected(PreparedJoinOrder),
    NoChange(JoinOrderDecision),
    Fallback(JoinOrderDecision),
}

enum SearchResult {
    Selected {
        splits: HashMap<usize, (usize, usize)>,
        order: Vec<usize>,
        candidate_splits: usize,
        original_cost: f64,
        selected_cost: f64,
        alternatives: Vec<JoinOrderAlternative>,
    },
    NoImprovement {
        order: Vec<usize>,
        candidate_splits: usize,
        original_cost: f64,
        selected_cost: f64,
        alternatives: Vec<JoinOrderAlternative>,
    },
    Fallback {
        outcome: JoinOrderOutcome,
        candidate_splits: usize,
        original_cost: Option<f64>,
        alternatives: Vec<JoinOrderAlternative>,
    },
}

struct SubplanState<'a> {
    relation: RelationEstimate<'a>,
    cumulative_cost: f64,
    split: Option<(usize, usize)>,
    order: Vec<usize>,
}

fn enumerate<C: JoinCostModel>(
    graph: &JoinGraph<'_>,
    context: &OptimizerContext,
    config: &JoinOrderConfig,
    cost_model: &C,
) -> SearchResult {
    let mut alternatives = Vec::new();
    if !is_connected(graph.full_mask, &graph.adjacency) {
        return SearchResult::Fallback {
            outcome: JoinOrderOutcome::DisconnectedGraph,
            candidate_splits: 0,
            original_cost: None,
            alternatives,
        };
    }

    let mut states = HashMap::new();
    for (relation_index, leaf) in graph.leaves.iter().enumerate() {
        let relation = context.estimate_relation(leaf);
        if relation.rows.value().is_none() {
            let outcome = match relation.rows.source {
                crate::estimation::EstimateSource::MissingStatistics
                | crate::estimation::EstimateSource::StaleStatistics =>
                    JoinOrderOutcome::InsufficientStatistics,
                _ => JoinOrderOutcome::UnsupportedShape,
            };
            tracing::warn!(target: "vein.optimizer", event = "optimizer_join_order_missing_relation_rows",
                relation_index, relation = %graph.labels[relation_index],
                estimate_source = ?relation.rows.source, confidence = ?relation.rows.confidence,
                "join-order search cannot start because this input has no population estimate");
            return SearchResult::Fallback {
                outcome,
                candidate_splits: 0,
                original_cost: None,
                alternatives,
            };
        }
        states.insert(
            1usize << relation_index,
            SubplanState {
                relation,
                cumulative_cost: 0.0,
                split: None,
                order: vec![relation_index],
            },
        );
    }

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
                        return SearchResult::Fallback {
                            outcome: JoinOrderOutcome::SearchBudgetExceeded,
                            candidate_splits,
                            original_cost: None,
                            alternatives,
                        };
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
                        return SearchResult::Fallback {
                            outcome,
                            candidate_splits,
                            original_cost: None,
                            alternatives,
                        };
                    }
                    if let Some(canonical) = &canonical_relation {
                        if !canonical.has_same_join_summary(&estimate) {
                            return SearchResult::Fallback {
                                outcome: JoinOrderOutcome::AmbiguousEstimates,
                                candidate_splits,
                                original_cost: None,
                                alternatives,
                            };
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
                            return SearchResult::Fallback {
                                outcome: JoinOrderOutcome::InvalidCost,
                                candidate_splits,
                                original_cost: None,
                                alternatives,
                            };
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
                return SearchResult::Fallback {
                    outcome: JoinOrderOutcome::DisconnectedGraph,
                    candidate_splits,
                    original_cost: None,
                    alternatives,
                };
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
    let selected = states.get(&graph.full_mask).expect("full connected state exists");
    let selected_cost = selected.cumulative_cost;
    if selected_cost >= original_cost || same_tree(graph.full_mask, graph, &states) {
        return SearchResult::NoImprovement {
            order: selected.order.clone(),
            candidate_splits,
            original_cost,
            selected_cost,
            alternatives,
        };
    }

    let splits = states
        .iter()
        .filter_map(|(mask, state)| state.split.map(|split| (*mask, split)))
        .collect();
    SearchResult::Selected {
        splits,
        order: selected.order.clone(),
        candidate_splits,
        original_cost,
        selected_cost,
        alternatives,
    }
}

fn checked_cost<C: JoinCostModel>(
    model: &C,
    left: JoinCostInput,
    right: JoinCostInput,
    output_rows: f64,
) -> Option<f64> {
    let cost = model.combine(left, right, output_rows)?;
    (cost.is_finite() && cost >= 0.0).then_some(cost)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OriginalTreeCostFailure {
    MissingOriginalSplit { relation_mask: usize },
    MissingSubplanState { relation_mask: usize },
    MissingCardinality { relation_mask: usize },
    InvalidCost { relation_mask: usize },
}

fn original_tree_cost<C: JoinCostModel>(
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

fn same_tree(
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

fn crossing_predicates<'a>(
    graph: &'a JoinGraph<'a>,
    left: usize,
    right: usize,
) -> Vec<&'a LogicalExpr> {
    graph
        .predicates
        .iter()
        .filter(|predicate| {
            let first = 1usize << predicate.left_relation;
            let second = 1usize << predicate.right_relation;
            (left & first != 0 && right & second != 0)
                || (left & second != 0 && right & first != 0)
        })
        .map(|predicate| predicate.expression)
        .collect()
}

fn is_connected(mask: usize, adjacency: &[usize]) -> bool {
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

fn is_reorderable_join(plan: &LogicalPlan) -> bool {
    let LogicalPlan::Join {
        join_type,
        left,
        right,
        on: Some(on),
        using,
        ..
    } = plan
    else {
        return false;
    };
    if *join_type != JoinType::Inner || !using.is_empty() {
        return false;
    }
    let Some(atoms) = equi_atoms(on) else {
        return false;
    };
    !atoms.is_empty()
        && atoms.iter().all(|expression| {
        let LogicalExpr::BinaryOp { left: a, right: b, .. } = expression else {
            return false;
        };
        let (LogicalExpr::Column(a), LogicalExpr::Column(b)) =
            (a.as_ref(), b.as_ref())
        else {
            return false;
        };
        if a.id == b.id {
            return false;
        }
        let left_a = left.schema().fields.iter().any(|column| column.id == a.id);
        let left_b = left.schema().fields.iter().any(|column| column.id == b.id);
        let right_a = right.schema().fields.iter().any(|column| column.id == a.id);
        let right_b = right.schema().fields.iter().any(|column| column.id == b.id);
        (left_a && !right_a && right_b && !left_b)
            || (left_b && !right_b && right_a && !left_a)
    })
}

fn equi_atoms(expression: &LogicalExpr) -> Option<Vec<&LogicalExpr>> {
    let mut pending = vec![expression];
    let mut atoms = Vec::new();
    while let Some(expression) = pending.pop() {
        match expression {
            LogicalExpr::BinaryOp {
                left,
                op: BinaryOp::And,
                right,
                ..
            } => {
                pending.push(right);
                pending.push(left);
            }
            LogicalExpr::BinaryOp {
                left,
                op: BinaryOp::Eq,
                right,
                ..
            } if matches!(
                (left.as_ref(), right.as_ref()),
                (LogicalExpr::Column(_), LogicalExpr::Column(_))
            ) => atoms.push(expression),
            _ => return None,
        }
    }
    Some(atoms)
}

fn rebuild(plan: LogicalPlan, prepared: PreparedJoinOrder) -> Result<LogicalPlan> {
    let root_schema = plan.schema().clone();
    let mut leaves = Vec::with_capacity(prepared.relation_count);
    take_leaf_plans(plan, &mut leaves);
    if leaves.len() != prepared.relation_count {
        return Err(VeinError::planning(
            "join-order reconstruction found a different relation count than analysis",
            None,
        ));
    }
    let full_mask = (1usize << prepared.relation_count) - 1;
    let mut leaves = leaves.into_iter().map(Some).collect::<Vec<_>>();
    build_subset(
        full_mask,
        full_mask,
        &prepared,
        &mut leaves,
        &root_schema,
    )
}

fn take_leaf_plans(plan: LogicalPlan, leaves: &mut Vec<LogicalPlan>) {
    if is_reorderable_join(&plan) {
        let LogicalPlan::Join { left, right, .. } = plan else {
            unreachable!("join eligibility checked the plan variant");
        };
        take_leaf_plans(*left, leaves);
        take_leaf_plans(*right, leaves);
    } else {
        leaves.push(plan);
    }
}

fn build_subset(
    mask: usize,
    full_mask: usize,
    prepared: &PreparedJoinOrder,
    leaves: &mut [Option<LogicalPlan>],
    root_schema: &VeinSchema,
) -> Result<LogicalPlan> {
    if mask.count_ones() == 1 {
        let index = mask.trailing_zeros() as usize;
        return leaves
            .get_mut(index)
            .and_then(Option::take)
            .ok_or_else(|| VeinError::planning("join-order leaf was already consumed", None));
    }
    let (left_mask, right_mask) = *prepared
        .splits
        .get(&mask)
        .ok_or_else(|| VeinError::planning("join-order split is missing", None))?;
    let left = build_subset(left_mask, full_mask, prepared, leaves, root_schema)?;
    let right = build_subset(right_mask, full_mask, prepared, leaves, root_schema)?;
    let predicates = prepared
        .predicates
        .iter()
        .filter(|predicate| {
            let first = 1usize << predicate.left_relation;
            let second = 1usize << predicate.right_relation;
            (left_mask & first != 0 && right_mask & second != 0)
                || (left_mask & second != 0 && right_mask & first != 0)
        })
        .map(|predicate| predicate.expression.clone())
        .collect::<Vec<_>>();
    let on = predicates
        .into_iter()
        .reduce(|left, right| LogicalExpr::BinaryOp {
            left: Box::new(left),
            op: BinaryOp::And,
            right: Box::new(right),
            return_type: VeinDataType::Boolean,
        })
        .ok_or_else(|| VeinError::planning("join-order split has no connecting predicate", None))?;
    let schema = if mask == full_mask {
        root_schema.clone()
    } else {
        VeinSchema::new(
            left.schema()
                .fields
                .iter()
                .chain(&right.schema().fields)
                .cloned()
                .collect(),
        )
    };
    Ok(LogicalPlan::Join {
        join_type: JoinType::Inner,
        left: Box::new(left),
        right: Box::new(right),
        on: Some(on),
        using: Vec::new(),
        schema: Box::new(schema),
    })
}

fn log_decision(decision: &JoinOrderDecision) {
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

fn relation_order_labels<'a>(order: &[usize], labels: &'a [String]) -> Vec<&'a str> {
    order
        .iter()
        .filter_map(|relation| labels.get(*relation).map(String::as_str))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use parser::ast::{expr::BinaryOp, pipe_op::JoinType};
    use catalog::{DataUnitStatistics, StatisticsUnitKind};
    use plan::{
        ColumnId, ColumnStatistics, LogicalExpr, LogicalPlan, ResolvedColumn,
        TableMetadata, TableStatistics, VeinDataType, VeinSchema,
    };

    use super::*;
    use crate::{OptimizerConfig, OptimizerPipeline};

    #[test]
    fn checkpoint_distribution_reorders_the_remaining_joins_without_changing_population() {
        use common::statistics::QueryStatisticsContext;
        let a = scan("a", 1000, &[(1, "a_key", 1)]);
        let b = scan("b", 1000, &[(2, "b_a", 1), (3, "b_c", 1000)]);
        let c = scan("c", 10000, &[(4, "c_key", 100)]);
        let bc = join(b, c, equality(3, "b_c", 4, "c_key"), JoinType::Inner);
        let original = join(a.clone(), bc.clone(), equality(1, "a_key", 2, "b_a"), JoinType::Inner);
        let context = Arc::new(QueryStatisticsContext::new([]));
        let (_, before) = OptimizerPipeline::default()
            .optimize_with_statistics_context(original, Arc::clone(&context)).unwrap();
        assert!(!before.join_order_decisions.iter().any(|decision| decision.outcome == JoinOrderOutcome::Reordered));

        // Same row count, different observed key distribution. The synopsis
        // belongs only to this materialized population and uses column IDs.
        context.record_intermediate(17, Arc::new(TableStatistics {
            num_rows: Some(1000),
            column_statistics: std::collections::HashMap::from([("1".into(), ColumnStatistics {
                distinct_count: Some(200),
                null_count: Some(0),
                ..Default::default()
            })]),
            ..Default::default()
        })).unwrap();
        let observed = LogicalPlan::Intermediate {
            source_id: 17,
            row_count: 1000,
            schema: Box::new(a.schema().clone()),
        };
        let provider = Arc::new(crate::QueryStatisticsProvider::new(Arc::clone(&context)));
        let estimates = OptimizerContext::with_statistics_provider(OptimizerConfig::default(), provider);
        assert_eq!(estimates.estimate_relation(&observed).columns[&ColumnId(1)].distinct, Some(200.0));
        let remaining = join(observed, bc, equality(1, "a_key", 2, "b_a"), JoinType::Inner);
        let expected_schema = remaining.schema().clone();
        let (optimized, after) = OptimizerPipeline::default()
            .optimize_with_statistics_context(remaining, context).unwrap();
        let decision = after.join_order_decisions.iter()
            .find(|decision| decision.outcome == JoinOrderOutcome::Reordered)
            .expect("observed key distribution must change the remaining join order");
        assert!(decision.selected_cost < decision.original_cost);
        assert_eq!(optimized.schema(), &expected_schema);
    }

    #[test]
    fn mismatched_checkpoint_population_does_not_reuse_its_column_synopses() {
        let source = scan("a", 100, &[(1, "key", 10)]);
        let context = Arc::new(common::statistics::QueryStatisticsContext::new([]));
        context.record_intermediate(17, Arc::new(TableStatistics {
            num_rows: Some(8),
            column_statistics: std::collections::HashMap::from([
                ("1".into(), ColumnStatistics { distinct_count: Some(2), null_count: Some(0), ..Default::default() }),
            ]),
            ..Default::default()
        })).unwrap();
        let provider = Arc::new(crate::QueryStatisticsProvider::new(context));
        let context = OptimizerContext::with_statistics_provider(OptimizerConfig::default(), provider);
        let input = LogicalPlan::Intermediate { source_id: 17, row_count: 9, schema: Box::new(source.schema().clone()) };
        let estimate = context.estimate_relation(&input);
        assert_eq!(estimate.rows.value(), Some(9.0));
        assert_ne!(estimate.columns[&ColumnId(1)].distinct, Some(2.0));
    }

    fn scan(name: &str, rows: u64, columns: &[(usize, &str, u64)]) -> LogicalPlan {
        let fields = columns
            .iter()
            .map(|(id, name, _)| {
                Arc::new(ResolvedColumn::new(
                    ColumnId(*id),
                    *name,
                    None,
                    VeinDataType::Int64,
                ))
            })
            .collect::<Vec<_>>();
        let schema = VeinSchema::new(fields);
        let column_statistics = columns
            .iter()
            .map(|(_, name, distinct)| {
                (
                    (*name).to_string(),
                    ColumnStatistics {
                        distinct_count: Some(*distinct),
                        null_count: Some(0),
                        ..Default::default()
                    },
                )
            })
            .collect();
        let statistics = TableStatistics {
            num_rows: Some(rows),
            column_statistics,
            ..Default::default()
        };
        LogicalPlan::Scan {
            table: Arc::new(
                TableMetadata::new(name, Arc::new(schema.clone())).with_statistics(statistics),
            ),
            projection: None,
            filters: Vec::new(),
            schema: Box::new(schema),
        }
    }

    fn scan_without_statistics(name: &str, columns: &[(usize, &str)]) -> LogicalPlan {
        let fields = columns
            .iter()
            .map(|(id, name)| {
                Arc::new(ResolvedColumn::new(
                    ColumnId(*id),
                    *name,
                    None,
                    VeinDataType::Int64,
                ))
            })
            .collect::<Vec<_>>();
        let schema = VeinSchema::new(fields);
        LogicalPlan::Scan {
            table: Arc::new(TableMetadata::new(name, Arc::new(schema.clone()))),
            projection: None,
            filters: Vec::new(),
            schema: Box::new(schema),
        }
    }

    fn equality(left: usize, left_name: &str, right: usize, right_name: &str) -> LogicalExpr {
        LogicalExpr::BinaryOp {
            left: Box::new(LogicalExpr::Column(Arc::new(ResolvedColumn::new(
                ColumnId(left),
                left_name,
                None,
                VeinDataType::Int64,
            )))),
            op: BinaryOp::Eq,
            right: Box::new(LogicalExpr::Column(Arc::new(ResolvedColumn::new(
                ColumnId(right),
                right_name,
                None,
                VeinDataType::Int64,
            )))),
            return_type: VeinDataType::Boolean,
        }
    }

    fn join(
        left: LogicalPlan,
        right: LogicalPlan,
        on: LogicalExpr,
        kind: JoinType,
    ) -> LogicalPlan {
        let schema = VeinSchema::new(
            left.schema()
                .fields
                .iter()
                .chain(&right.schema().fields)
                .cloned()
                .collect(),
        );
        LogicalPlan::Join {
            join_type: kind,
            left: Box::new(left),
            right: Box::new(right),
            on: Some(on),
            using: Vec::new(),
            schema: Box::new(schema),
        }
    }

    fn selective_chain() -> LogicalPlan {
        let a = scan("a", 1_000_000, &[(1, "a_key", 1_000_000)]);
        let b = scan("b", 100, &[(2, "b_a_key", 100), (3, "b_c_key", 100)]);
        let c = scan("c", 10, &[(4, "c_key", 10)]);
        let ab = join(a, b, equality(1, "a_key", 2, "b_a_key"), JoinType::Inner);
        join(
            ab,
            c,
            equality(4, "c_key", 3, "b_c_key"),
            JoinType::Inner,
        )
    }

    fn conjunction(left: LogicalExpr, right: LogicalExpr) -> LogicalExpr {
        LogicalExpr::BinaryOp {
            left: Box::new(left),
            op: BinaryOp::And,
            right: Box::new(right),
            return_type: VeinDataType::Boolean,
        }
    }

    fn join_equality_count(plan: &LogicalPlan) -> usize {
        let mut pending = vec![plan];
        let mut count = 0;
        while let Some(node) = pending.pop() {
            if let LogicalPlan::Join { on: Some(on), .. } = node {
                count += equi_atoms(on).map_or(0, |atoms| atoms.len());
            }
            pending.extend(node.children());
        }
        count
    }

    fn optimizer(config: JoinOrderConfig) -> OptimizerPipeline {
        let optimizer_config = OptimizerConfig {
            join_order: config,
            ..OptimizerConfig::default()
        };
        OptimizerPipeline::new(vec![Arc::new(JoinOrderRule::default())])
            .with_config(optimizer_config)
    }

    #[test]
    fn binary_join_runs_cost_search_and_reports_its_candidate() {
        let plan = join(
            scan("orders", 100, &[(1, "order_key", 100)]),
            scan("lineitem", 400, &[(2, "order_key", 200)]),
            equality(1, "order_key", 2, "order_key"),
            JoinType::Inner,
        );
        let original = plan.clone();

        let (optimized, diagnostics) = optimizer(JoinOrderConfig::default())
            .optimize_with_diagnostics(plan)
            .expect("binary join should be retained");

        assert_eq!(optimized, original);
        let decision = diagnostics
            .join_order_decisions
            .first()
            .expect("binary join decision should be visible");
        assert_eq!(decision.outcome, JoinOrderOutcome::NoImprovement);
        assert_eq!(decision.relation_labels, vec!["orders", "lineitem"]);
        assert_eq!(decision.candidate_splits, 1);
        assert!(decision.original_cost.is_some());
        assert!(decision.selected_cost.is_some());
        assert_eq!(
            relation_order_labels(&decision.original_order, &decision.relation_labels),
            vec!["orders", "lineitem"]
        );
    }

    #[test]
    fn pruned_join_output_schema_preserves_original_relation_lineage_for_costing() {
        let a = scan("a", 100, &[(1, "a_key", 10)]);
        let b = scan("b", 100, &[(2, "b_key", 10), (3, "b_c", 10)]);
        let c = scan("c", 100, &[(4, "c_key", 10)]);
        let mut ab = join(
            a,
            b,
            equality(1, "a_key", 2, "b_key"),
            JoinType::Inner,
        );
        let LogicalPlan::Join { schema, .. } = &mut ab else {
            unreachable!("helper creates a join");
        };
        schema.fields.retain(|column| column.id == ColumnId(3));

        let plan = join(
            ab,
            c,
            equality(3, "b_c", 4, "c_key"),
            JoinType::Inner,
        );
        let (_, diagnostics) = optimizer(JoinOrderConfig::default())
            .optimize_with_diagnostics(plan)
            .expect("column pruning must not hide an original relation from costing");

        let decision = diagnostics
            .join_order_decisions
            .first()
            .expect("the three-relation inner join is eligible for costing");
        assert_eq!(decision.relation_count, 3);
        assert!(decision.original_cost.is_some());
        assert_ne!(decision.outcome, JoinOrderOutcome::InvalidCost);
    }

    #[test]
    fn selective_connected_inner_joins_choose_smaller_intermediate_and_preserve_schema() {
        let plan = selective_chain();
        let original_schema = plan.schema().clone();
        let (optimized, diagnostics) = optimizer(JoinOrderConfig::default())
            .optimize_with_diagnostics(plan)
            .expect("optimizer should select a safe alternative");

        let LogicalPlan::Join { left, right, schema, .. } = &optimized else {
            panic!("expected a join root");
        };
        assert!(matches!(left.as_ref(), LogicalPlan::Scan { table, .. } if table.name == "a"));
        assert!(matches!(
            right.as_ref(),
            LogicalPlan::Join { left, right, .. }
                if matches!(left.as_ref(), LogicalPlan::Scan { table, .. } if table.name == "b")
                    && matches!(right.as_ref(), LogicalPlan::Scan { table, .. } if table.name == "c")
        ));
        assert_eq!(schema.as_ref(), &original_schema);
        assert_eq!(join_equality_count(&optimized), 2, "each input predicate must survive once");
        let decision = diagnostics.join_order_decisions.first().expect("join decision");
        assert_eq!(decision.outcome, JoinOrderOutcome::Reordered);
        assert!(decision.selected_cost.unwrap() < decision.original_cost.unwrap());
        assert_eq!(decision.relation_count, 3);
        assert!(decision.candidate_splits > 0);
        assert_eq!(
            decision.cardinality_assumption,
            JoinCardinalityAssumption::UniformDistinctValueOverlap
        );
        assert!(decision.alternatives.iter().any(|alternative| {
            alternative.relation_mask == 0b111 && alternative.best_for_subset
        }));
        assert_eq!(
            decision
                .alternatives
                .iter()
                .filter(|alternative| alternative.original_tree_split)
                .count(),
            2
        );
        assert!(decision.alternatives.iter().any(|alternative| {
            alternative.cost.local_cost == Some(alternative.output_rows)
                && (alternative.cost.left_subtree_cost
                + alternative.cost.right_subtree_cost
                + alternative.cost.local_cost.unwrap()
                == alternative.cost.cumulative_cost)
        }));
    }

    #[test]
    fn inconsistent_decomposition_estimates_preserve_original_join_tree() {
        let a = scan("a", 1_000, &[(1, "a_ab", 100), (2, "a_ac", 1_000)]);
        let b = scan("b", 1_000, &[(3, "b_ab", 100), (4, "b_bc", 10)]);
        let c = scan("c", 1_000, &[(5, "c_bc", 10), (6, "c_ac", 1_000)]);
        let ab = join(a, b, equality(1, "a_ab", 3, "b_ab"), JoinType::Inner);
        let predicates = conjunction(
            equality(2, "a_ac", 6, "c_ac"),
            equality(4, "b_bc", 5, "c_bc"),
        );
        let plan = join(ab, c, predicates, JoinType::Inner);
        let original = plan.clone();

        let (optimized, diagnostics) = optimizer(JoinOrderConfig::default())
            .optimize_with_diagnostics(plan)
            .expect("ambiguous estimates should safely fall back");

        assert_eq!(optimized, original);
        assert_eq!(join_equality_count(&optimized), 3);
        assert_eq!(
            diagnostics.join_order_decisions[0].outcome,
            JoinOrderOutcome::AmbiguousEstimates
        );
    }

    #[test]
    fn unknown_statistics_preserve_original_join_tree() {
        let a = scan("a", 1_000, &[(1, "a_key", 100)]);
        let b = scan_without_statistics("b", &[(2, "b_key")]);
        let c = scan("c", 10, &[(3, "c_key", 10)]);
        let ab = join(a, b, equality(1, "a_key", 2, "b_key"), JoinType::Inner);
        let plan = join(ab, c, equality(2, "b_key", 3, "c_key"), JoinType::Inner);
        let original = plan.clone();

        let (optimized, diagnostics) = optimizer(JoinOrderConfig::default())
            .optimize_with_diagnostics(plan)
            .expect("unknown estimates should safely fall back");

        assert_eq!(optimized, original);
        assert_eq!(
            diagnostics.join_order_decisions[0].outcome,
            JoinOrderOutcome::InsufficientStatistics
        );
    }

    #[test]
    fn complete_row_group_population_supports_join_order_costing() {
        let mut a = scan("a", 100, &[(1, "a_key", 10)]);
        let LogicalPlan::Scan { table, .. } = &mut a else {
            unreachable!("scan fixture must produce a scan");
        };
        let statistics = Arc::make_mut(table)
            .statistics
            .as_mut()
            .expect("scan fixture has statistics");
        statistics.num_rows = None;
        statistics.row_group_count = Some(2);
        statistics.data_units = [40, 60]
            .into_iter()
            .enumerate()
            .map(|(ordinal, num_rows)| {
                let mut unit = DataUnitStatistics::new(StatisticsUnitKind::RowGroup, ordinal);
                unit.num_rows = Some(num_rows);
                unit
            })
            .collect();

        let b = scan("b", 200, &[(2, "b_key", 20)]);
        let plan = join(a, b, equality(1, "a_key", 2, "b_key"), JoinType::Inner);
        let original = plan.clone();

        let (optimized, diagnostics) = optimizer(JoinOrderConfig::default())
            .optimize_with_diagnostics(plan)
            .expect("complete row-group counts provide a population estimate");

        assert_eq!(optimized, original);
        let decision = diagnostics
            .join_order_decisions
            .first()
            .expect("the two-relation join must be costed");
        assert_eq!(decision.outcome, JoinOrderOutcome::NoImprovement);
        assert_eq!(decision.original_cost, decision.selected_cost);
        assert_eq!(decision.candidate_splits, 1);
    }

    #[test]
    fn candidate_budget_exhaustion_preserves_original_tree() {
        let plan = selective_chain();
        let original = plan.clone();
        let config = JoinOrderConfig {
            max_relations: 8,
            max_candidate_splits: 0,
        };

        let (optimized, diagnostics) = optimizer(config)
            .optimize_with_diagnostics(plan)
            .expect("budget exhaustion should fall back");

        assert_eq!(optimized, original);
        let decision = diagnostics.join_order_decisions.first().expect("fallback diagnostic");
        assert_eq!(decision.outcome, JoinOrderOutcome::SearchBudgetExceeded);
        assert_eq!(decision.selected_cost, None);
    }

    #[test]
    fn relation_limit_preserves_original_tree() {
        let plan = selective_chain();
        let original = plan.clone();
        let config = JoinOrderConfig {
            max_relations: 2,
            max_candidate_splits: 100,
        };

        let (optimized, diagnostics) = optimizer(config)
            .optimize_with_diagnostics(plan)
            .expect("relation limit should fall back");

        assert_eq!(optimized, original);
        assert_eq!(
            diagnostics.join_order_decisions[0].outcome,
            JoinOrderOutcome::RelationLimitExceeded
        );
    }

    #[test]
    fn outer_join_is_a_reordering_boundary() {
        let left = scan("left", 100, &[(1, "left_key", 10)]);
        let right = scan("right", 100, &[(2, "right_key", 10)]);
        let plan = join(
            left,
            right,
            equality(1, "left_key", 2, "right_key"),
            JoinType::Left,
        );
        let original = plan.clone();

        let (optimized, diagnostics) = optimizer(JoinOrderConfig::default())
            .optimize_with_diagnostics(plan)
            .expect("outer joins are retained");

        assert_eq!(optimized, original);
        assert!(diagnostics.join_order_decisions.is_empty());
    }

    #[test]
    fn residual_inner_join_predicate_is_a_reordering_boundary() {
        let a = scan("a", 100, &[(1, "a_key", 10)]);
        let b = scan("b", 100, &[(2, "b_key", 10)]);
        let c = scan("c", 100, &[(3, "c_key", 10)]);
        let key_equality = equality(1, "a_key", 2, "b_key");
        let residual = LogicalExpr::BinaryOp {
            left: Box::new(LogicalExpr::Column(Arc::new(ResolvedColumn::new(
                ColumnId(1),
                "a_key",
                None,
                VeinDataType::Int64,
            )))),
            op: BinaryOp::Lt,
            right: Box::new(LogicalExpr::Column(Arc::new(ResolvedColumn::new(
                ColumnId(2),
                "b_key",
                None,
                VeinDataType::Int64,
            )))),
            return_type: VeinDataType::Boolean,
        };
        let on = LogicalExpr::BinaryOp {
            left: Box::new(key_equality),
            op: BinaryOp::And,
            right: Box::new(residual),
            return_type: VeinDataType::Boolean,
        };
        let ab = join(a, b, on, JoinType::Inner);
        let plan = join(ab, c, equality(2, "b_key", 3, "c_key"), JoinType::Inner);
        let original = plan.clone();

        let (optimized, diagnostics) = optimizer(JoinOrderConfig::default())
            .optimize_with_diagnostics(plan)
            .expect("residual predicates must not be reassociated");

        assert_eq!(optimized, original);
        assert_eq!(diagnostics.join_order_decisions.len(), 1);
        let decision = &diagnostics.join_order_decisions[0];
        assert_eq!(decision.relation_count, 2);
        assert_eq!(decision.outcome, JoinOrderOutcome::NoImprovement);
        assert!(decision.original_cost.is_some());
        assert_eq!(decision.original_order, decision.selected_order);
    }

    #[test]
    fn catalog_resolved_external_join_has_a_costed_join_order_decision() {
        let LogicalPlan::Scan {
            table,
            schema,
            ..
        } = scan("external", 100, &[(1, "external_key", 10)])
        else {
            unreachable!();
        };
        let external = LogicalPlan::ExternalScan {
            url: "file:///external".into(),
            options: vec![],
            files: vec![],
            source_metadata: Some(table),
            schema,
        };
        let right = scan("right", 100, &[(2, "right_key", 10)]);
        let plan = join(
            external,
            right,
            equality(1, "external_key", 2, "right_key"),
            JoinType::Inner,
        );

        let (_, diagnostics) = optimizer(JoinOrderConfig::default())
            .optimize_with_diagnostics(plan)
            .expect("resolved external source statistics should support join costing");
        let decision = diagnostics
            .join_order_decisions
            .first()
            .expect("two relations produce a join-order decision");
        assert_ne!(decision.outcome, JoinOrderOutcome::InsufficientStatistics);
        assert!(decision.original_cost.is_some());
        assert_eq!(decision.candidate_splits, 1);
    }
}
