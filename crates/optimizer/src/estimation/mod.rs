//! Metadata-only cardinality estimation for cost-based planning.
//!
//! An iterative postorder walk borrows the authoritative logical tree. Only
//! scalar summaries and borrowed literal bounds are retained. There are no
//! row reads, Arrow buffers, plan clones, or execution-path changes.
//! Estimates describe cost inputs, never correctness or pruning proofs.

mod aggregate;
mod columns;
mod comparison;
mod derive;
mod estimate;
mod join;
mod selectivity;

pub use estimate::{Estimate, EstimateConfidence, EstimateSource};
pub use selectivity::{ColumnStatisticsLookup, PredicateEstimate, SelectivityEstimator, StatisticsSelectivity};

use plan::{LogicalPlan, PlanTraversal};
use crate::context::StatisticsProvider;
use columns::{ColumnEstimate, RelationEstimate};

impl ColumnStatisticsLookup for std::collections::HashMap<plan::ColumnId, ColumnEstimate<'_>> {
    fn distribution(&self, id: plan::ColumnId) -> Option<(Option<f64>, Option<f64>)> {
        self.get(&id).map(|c| (c.distinct, c.null_fraction))
    }

    fn bounds(&self, id: plan::ColumnId) -> Option<(Option<&plan::LogicalLiteral>, Option<&plan::LogicalLiteral>)> {
        self.get(&id).map(|c| (c.min, c.max))
    }
}

/// One node's estimates in postorder, with stable indices into the same result.
#[derive(Debug, Clone, PartialEq)]
pub struct NodeEstimate {
    /// Postorder position of this operator.
    pub node_id: usize,
    /// Operator name; no formatted plan or table payload is retained.
    pub operator: &'static str,
    /// Estimated output cardinality.
    pub rows: Estimate,
    /// Estimated TRUE fraction for filters and filtered scans.
    pub selectivity: Option<Estimate>,
}

/// Extension seam for alternative cardinality models.
pub trait CardinalityEstimator: Send + Sync {
    /// Estimates each node in postorder. The last element describes the root.
    fn estimate_plan(&self, plan: &LogicalPlan) -> Vec<NodeEstimate>;
}

/// Generic statistics-based estimator accepting static or dynamic providers.
///
/// Custom selectivity models share the same traversal and column-lineage code.
pub struct StatisticsEstimator<'a, P: ?Sized, S = StatisticsSelectivity> {
    provider: &'a P,
    selectivity: S,
}

impl<'a, P: StatisticsProvider + ?Sized> StatisticsEstimator<'a, P> {
    /// Borrows a statistics provider using the uniform selectivity model.
    pub fn new(provider: &'a P) -> Self {
        Self { provider, selectivity: StatisticsSelectivity }
    }
}

impl<'a, P: StatisticsProvider + ?Sized, S: SelectivityEstimator> StatisticsEstimator<'a, P, S> {
    /// Borrows a provider and installs a custom distribution strategy.
    pub fn with_selectivity(provider: &'a P, selectivity: S) -> Self {
        Self { provider, selectivity }
    }
}

impl<P: StatisticsProvider + ?Sized, S: SelectivityEstimator> CardinalityEstimator for StatisticsEstimator<'_, P, S> {
    fn estimate_plan(&self, plan: &LogicalPlan) -> Vec<NodeEstimate> {
        let mut pending = vec![(plan, None)];
        let mut relations: Vec<RelationEstimate<'_>> = Vec::new();
        let mut estimates = Vec::new();
        while let Some((node, child_count)) = pending.pop() {
            let Some(count) = child_count else {
                let children = node.children();
                pending.push((node, Some(children.len())));
                pending.extend(children.into_iter().rev().map(|child| (child, None)));
                continue;
            };
            let inputs = relations.drain(relations.len() - count..);
            let result = derive::derive(node, inputs, self.provider, &self.selectivity);
            let node_id = estimates.len();
            tracing::debug!(
                target: "vein.optimizer", event = "optimizer_cardinality_estimate",
                node_id, operator = node.operator_name(), rows = ?result.rows.value(),
                source = ?result.rows.source, confidence = ?result.rows.confidence,
                selectivity = ?result.selectivity,
                "optimizer_cardinality_estimate"
            );
            estimates.push(NodeEstimate {
                node_id,
                operator: node.operator_name(),
                rows: result.rows,
                selectivity: result.selectivity,
            });
            relations.push(result);
        }
        estimates
    }
}

#[cfg(test)]
mod tests;
