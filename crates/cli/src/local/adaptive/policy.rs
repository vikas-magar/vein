//! Statistics-driven selection of complete inputs to remaining joins.

use super::*;
#[path = "checkpoint_identity.rs"]
mod checkpoint_identity;
use checkpoint_identity::{
    ScanPopulationSignature, scan_population_signature, signature_match_counts,
};
use optimizer::context::QueryStatisticsProvider;
use optimizer::estimation::{CardinalityEstimator, EstimateConfidence, StatisticsEstimator};

/// Measures uncertain join inputs, including joins and aggregates, while
/// leaving already observed inputs and fully described populations streaming.
#[derive(Debug, Default)]
pub struct StatisticsCheckpointPolicy;

impl AdaptiveCheckpointPolicy for StatisticsCheckpointPolicy {
    fn choose_boundary(
        &self,
        plan: &LogicalPlan,
        statistics: Arc<QueryStatisticsContext>,
    ) -> Option<AdaptiveBoundary> {
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
        let mut pending = vec![(plan, Vec::new(), false)];
        while let Some((node, path, join_input)) = pending.pop() {
            if join_input
                && !matches!(node, LogicalPlan::Intermediate { .. })
                && needs_feedback(node)
                && known_input_rows(node).is_some()
                && super::execution::join_checkpoint_has_search_space(plan, node, &path)
            {
                let estimate = estimator.estimate_plan(node).pop()?.rows;
                // Fully described inputs need no new materialization barrier.
                // Missing source cardinality is not a reason to invent a cost.
                let work = known_input_rows(node)?;
                if estimate.confidence < EstimateConfidence::Statistics || missing_synopses(node) {
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
                        score.0 > current.0 || (score.0 == current.0 && score.1 < current.1)
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
                pending.push((child, child_path, matches!(node, LogicalPlan::Join { .. })));
            }
        }
        best.map(|(_, boundary, signature)| {
            if let Some(signature) = signature {
                observed.push(signature);
            }
            boundary
        })
    }
}

fn contains_blocking_relation(plan: &LogicalPlan) -> bool {
    matches!(
        plan,
        LogicalPlan::Join { .. } | LogicalPlan::Aggregate { .. } | LogicalPlan::Distinct { .. }
    ) || plan.children().into_iter().any(contains_blocking_relation)
}

/// Only relational, side-effect-free subtrees can be replayed after a failure.
/// Subqueries and semantic/opaque calls require separate dependency handling.
fn safe_subtree(plan: &LogicalPlan) -> bool {
    if !matches!(
        plan,
        LogicalPlan::Scan { .. }
            | LogicalPlan::ExternalScan { .. }
            | LogicalPlan::Intermediate { .. }
            | LogicalPlan::Filter { .. }
            | LogicalPlan::Project { .. }
            | LogicalPlan::Join { .. }
            | LogicalPlan::Aggregate { .. }
            | LogicalPlan::Distinct { .. }
            | LogicalPlan::Sort { .. }
            | LogicalPlan::Limit { .. }
            | LogicalPlan::SubqueryAlias { .. }
    ) {
        return false;
    }
    let mut has_unsafe_expression = false;
    plan.for_each_expression(&mut |_, expression| {
        has_unsafe_expression |=
            expression.is_volatile() || !expression.subquery_plans().is_empty();
    });
    !has_unsafe_expression && plan.children().into_iter().all(safe_subtree)
}

fn needs_feedback(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Intermediate { .. } => false,
        // A known, unfiltered source population is already described by its
        // cardinality. Missing NDV alone does not justify retaining that whole
        // table and delaying useful downstream runtime filters.
        LogicalPlan::Scan { filters, table, .. } => {
            !filters.is_empty()
                || table
                .statistics
                .as_ref()
                .and_then(|stats| stats.num_rows)
                .is_none()
        }
        LogicalPlan::Join { .. } | LogicalPlan::Aggregate { .. } | LogicalPlan::Distinct { .. } => {
            true
        }
        LogicalPlan::Filter { input, .. } => contains_pending_operator(input),
        _ => plan.children().into_iter().any(needs_feedback),
    }
}

fn contains_pending_operator(plan: &LogicalPlan) -> bool {
    matches!(
        plan,
        LogicalPlan::Scan { .. }
            | LogicalPlan::ExternalScan { .. }
            | LogicalPlan::Join { .. }
            | LogicalPlan::Aggregate { .. }
            | LogicalPlan::Distinct { .. }
    ) || plan.children().into_iter().any(contains_pending_operator)
}

fn known_input_rows(plan: &LogicalPlan) -> Option<u64> {
    match plan {
        LogicalPlan::Scan { table, .. } => table.statistics.as_ref()?.num_rows,
        LogicalPlan::Intermediate { row_count, .. } => Some(*row_count),
        _ => {
            let mut rows = 0_u64;
            for child in plan.children() {
                rows = rows.checked_add(known_input_rows(child)?)?;
            }
            Some(rows)
        }
    }
}

fn missing_synopses(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Intermediate { .. } => false,
        LogicalPlan::Scan { table, schema, .. } => schema.fields.iter().any(|column| {
            table
                .statistics
                .as_ref()
                .and_then(|stats| stats.column_statistics.get(&column.name))
                .is_none_or(|stats| stats.distinct_count.is_none())
        }),
        _ => plan.children().into_iter().any(missing_synopses),
    }
}
