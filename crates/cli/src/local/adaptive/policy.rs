//! Statistics-driven selection of complete inputs to remaining joins.

use super::*;
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
        let provider = QueryStatisticsProvider::new(statistics);
        let estimator = StatisticsEstimator::new(&provider);
        let mut best: Option<((bool, f64), AdaptiveBoundary)> = None;
        let mut pending = vec![(plan, Vec::new(), false)];
        while let Some((node, path, join_input)) = pending.pop() {
            if join_input
                && !matches!(node, LogicalPlan::Intermediate { .. })
                && needs_feedback(node)
                && known_input_rows(node).is_some()
            {
                let estimate = estimator.estimate_plan(node).pop()?.rows;
                // Fully described inputs need no new materialization barrier.
                // Missing source cardinality is not a reason to invent a cost.
                let work = known_input_rows(node)?;
                if estimate.confidence < EstimateConfidence::Statistics || missing_synopses(node) {
                    let rows = estimate
                        .value()
                        .filter(|v| *v <= u64::MAX as f64)
                        .map(|v| v as u64);
                    // Measure selective inputs before freezing larger join
                    // subtrees, so the earliest feedback can still reorder them.
                    let score = (!contains_blocking_relation(node), work);
                    if best.as_ref().is_none_or(|(current, _)| score > *current) {
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
        best.map(|(_, boundary)| boundary)
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

fn known_input_rows(plan: &LogicalPlan) -> Option<f64> {
    match plan {
        LogicalPlan::Scan { table, .. } => table.statistics.as_ref()?.num_rows.map(|v| v as f64),
        LogicalPlan::Intermediate { row_count, .. } => Some(*row_count as f64),
        _ => plan
            .children()
            .into_iter()
            .map(known_input_rows)
            .collect::<Option<Vec<_>>>()
            .map(|rows| rows.into_iter().sum()),
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
