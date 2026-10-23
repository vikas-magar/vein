//! Statistics-driven selection of complete inputs to remaining joins.

use super::checkpoint_identity::{
    ScanPopulationSignature, scan_population_signature, signature_match_counts,
};
use super::*;
use optimizer::context::QueryStatisticsProvider;
use optimizer::estimation::{CardinalityEstimator, EstimateConfidence, StatisticsEstimator};
mod helpers;
mod cost;
pub use cost::{CheckpointCostModel, CheckpointWork, InputDominanceCost};
use helpers::{
    contains_blocking_relation, known_input_rows, missing_synopses, needs_feedback, safe_subtree,
};

/// Measures uncertain join inputs, including joins and aggregates, while
/// leaving already observed inputs and fully described populations streaming.
#[derive(Debug, Default)]
pub struct StatisticsCheckpointPolicy;

/// Statistics checkpoint selection with an injected materialization cost model.
pub struct CostedCheckpointPolicy<M> { model: M }

impl StatisticsCheckpointPolicy {
    /// Installs a cost strategy while retaining the same safety and lineage rules.
    pub fn with_cost_model<M: CheckpointCostModel>(model: M) -> CostedCheckpointPolicy<M> {
        CostedCheckpointPolicy { model }
    }
}

impl AdaptiveCheckpointPolicy for StatisticsCheckpointPolicy {
    fn choose_boundary(
        &self,
        plan: &LogicalPlan,
        statistics: Arc<QueryStatisticsContext>,
    ) -> Option<AdaptiveBoundary> {
        choose_boundary(plan, statistics, &InputDominanceCost)
    }
}

impl<M: CheckpointCostModel> AdaptiveCheckpointPolicy for CostedCheckpointPolicy<M> {
    fn choose_boundary(&self, plan: &LogicalPlan, statistics: Arc<QueryStatisticsContext>) -> Option<AdaptiveBoundary> {
        choose_boundary(plan, statistics, &self.model)
    }
}

fn choose_boundary(plan: &LogicalPlan, statistics: Arc<QueryStatisticsContext>, model: &impl CheckpointCostModel) -> Option<AdaptiveBoundary> {
        if !safe_subtree(plan) {
            return None;
        }
        let provider = QueryStatisticsProvider::new(Arc::clone(&statistics));
        let estimator = StatisticsEstimator::new(&provider);
        let observed = statistics.shared_query_state(0, "adaptive-checkpoint-populations-v1", || {
            std::sync::Mutex::<Vec<ScanPopulationSignature>>::new(Vec::new())
        });
        let mut observed = observed.lock().unwrap_or_else(|poison| poison.into_inner());
        let mut best: Option<((bool, u64), AdaptiveBoundary, Option<_>)> = None;
        let mut pending = vec![(plan, Vec::new(), None)];
        while let Some((node, path, join_input)) = pending.pop() {
            if let Some(sibling) = join_input
                && !matches!(node, LogicalPlan::Intermediate { .. })
                && needs_feedback(node)
                && known_input_rows(node).is_some()
                && super::execution::join_checkpoint_has_search_space(plan, node, &path)
            {
                let estimate = estimator.estimate_plan(node).pop()?.rows;
                // Fully described inputs need no new materialization barrier.
                // Missing source cardinality is not a reason to invent a cost.
                let work = known_input_rows(node)?;
                let candidate_cost = CheckpointWork { input_rows: Some(work), output_rows: estimate.value() };
                let sibling_cost = CheckpointWork {
                    input_rows: known_input_rows(sibling),
                    output_rows: estimator.estimate_plan(sibling).pop().and_then(|estimate| estimate.rows.value()),
                };
                let allowed = model.allows(candidate_cost, sibling_cost);
                if !allowed {
                    tracing::debug!(target: "vein.adaptive", event = "adaptive_checkpoint_candidate_skipped",
                        source = node.table_name().unwrap_or(node.operator_name()),
                        reason = "materialization_cost_dominated", candidate_input_rows = ?candidate_cost.input_rows,
                        candidate_output_rows = ?candidate_cost.output_rows,
                        sibling_input_rows = ?sibling_cost.input_rows, sibling_output_rows = ?sibling_cost.output_rows);
                }
                if allowed && (estimate.confidence < EstimateConfidence::Statistics || missing_synopses(node)) {
                    let signature = scan_population_signature(node);
                    let match_counts = signature
                        .as_ref()
                        .map(|signature| signature_match_counts(signature, &observed));
                    if match_counts.is_some_and(|counts| counts.exact > 0) {
                        tracing::info!(target: "vein.adaptive", event = "adaptive_checkpoint_candidate_skipped",
                            source = node.table_name().unwrap_or(node.operator_name()),
                            reason = "equivalent_population_already_scheduled",
                            "avoided repeating a checkpoint for the same source population");
                        continue;
                    }
                    if let Some(counts) = match_counts.filter(|counts| counts.same_source > 0) {
                        tracing::info!(target: "vein.adaptive.identity", event = "adaptive_checkpoint_population_mismatch",
                            source = node.table_name().unwrap_or(node.operator_name()),
                            prior_same_source = counts.same_source,
                            prior_same_filter = counts.same_filters,
                            prior_same_projection = counts.same_projection,
                            "same source had a scheduled checkpoint with a different population signature");
                    }
                    let rows = estimate
                        .value()
                        .filter(|v| *v <= u64::MAX as f64)
                        .map(|v| v as u64);
                    // Prefer the cheapest eligible input, preserving time for
                    // later join work instead of materializing a large subtree.
                    let score = (!contains_blocking_relation(node), work);
                    let better_candidate = best.as_ref().is_none_or(|(current, _, _)| {
                        (score.0 && !current.0)
                            || (score.0 == current.0 && score.1 < current.1)
                    });
                    if better_candidate {
                        let mut schema = node.schema().clone();
                        if let LogicalPlan::Scan {
                            projection: Some(columns),
                            ..
                        } = node
                        {
                            schema.fields.retain(|field| columns.contains(&field.id));
                        }
                        best = Some((
                            score,
                            AdaptiveBoundary {
                                producer: node.clone(),
                                path: path.clone(),
                                output_schema: Box::new(schema),
                                estimated_rows: rows,
                            },
                            signature,
                        ));
                    }
                }
            }
            for (index, child) in node.children().into_iter().enumerate().rev() {
                let mut child_path = path.clone();
                child_path.push(index);
                let sibling = match node {
                    LogicalPlan::Join { left, right, .. } => Some(if index == 0 { right.as_ref() } else { left.as_ref() }),
                    _ => None,
                };
                pending.push((child, child_path, sibling));
            }
        }
        best.map(|(_, boundary, signature)| {
            if let Some(signature) = signature {
                observed.push(signature);
            }
            boundary
        })
}
