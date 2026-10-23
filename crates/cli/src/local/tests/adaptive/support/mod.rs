//! Explicit policies for testing checkpoint mechanics independently of cost selection.
use std::sync::Arc;
use common::statistics::QueryStatisticsContext;
use engine::local::{AdaptiveBoundary, AdaptiveCheckpointPolicy, CheckpointCostModel,
    CheckpointWork, LocalQueryEngine, StatisticsCheckpointPolicy};

pub(super) struct NoCheckpoints;
impl AdaptiveCheckpointPolicy for NoCheckpoints {
    fn choose_boundary(&self, _: &plan::LogicalPlan, _: Arc<QueryStatisticsContext>) -> Option<AdaptiveBoundary> { None }
}

struct AllowMeasurement;
impl CheckpointCostModel for AllowMeasurement {
    fn allows(&self, _: CheckpointWork, _: CheckpointWork) -> bool { true }
}

pub(super) fn adaptive_engine(root: &std::path::Path) -> LocalQueryEngine {
    crate::local::test_engine(root).with_adaptive_policy(Arc::new(
        StatisticsCheckpointPolicy::with_cost_model(AllowMeasurement)))
}
