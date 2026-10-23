//! Physical choices consume the same statistics owner as logical optimization.

use optimizer::estimation::{CardinalityEstimator, EstimateConfidence, StatisticsEstimator};
use plan::LogicalPlan;
use super::{DefaultPipelineStageBuilder, PipelineCompiler};

impl DefaultPipelineStageBuilder {
    fn node_estimate(&self, plan: &LogicalPlan) -> Option<optimizer::estimation::NodeEstimate> {
        StatisticsEstimator::new(self.statistics.as_ref()?.as_ref()).estimate_plan(plan).pop()
    }

    pub(super) fn observed_rows(&self, plan: &LogicalPlan) -> Option<f64> {
        self.node_estimate(plan)?.rows.value()
    }

    pub(super) fn aggregate_plan(&self, plan: &LogicalPlan)
                                 -> common::Result<Option<common::protocol::AggregatePlan>> {
        let Some(mut aggregate) = PipelineCompiler::aggregate(plan)? else { return Ok(None); };
        if let Some(estimate) = self.node_estimate(plan) {
            // Grouping estimates are sizing hints, including approximate
            // NDVs. The query memory tracker remains allocation authority.
            if estimate.rows.confidence >= EstimateConfidence::Model {
                aggregate.estimated_groups = estimate.rows.value()
                    .filter(|rows| *rows <= u64::MAX as f64).map(|rows| rows.ceil() as u64);
                tracing::debug!(target: "vein.adaptive", event = "physical_aggregate_statistics",
                        estimated_groups = ?aggregate.estimated_groups, source = ?estimate.rows.source,
                        "physical aggregate sizing uses query statistics");
            }
        }
        Ok(Some(aggregate))
    }
}

#[cfg(test)]
#[path = "statistics/tests/mod.rs"]
mod tests;
