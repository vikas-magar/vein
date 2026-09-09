//! Distributed aggregation builder abstraction: translates `LogicalPlan::Aggregate` into Map/Reduce stages.

use crate::job::StageEdge;
use crate::stage::Stage;
use common::identifiers::{QueryId, StageId};
use common::Result;
use error::VeinError;
use plan::LogicalPlan;

/// Trait for translating `LogicalPlan::Aggregate` into Map and Reduce stages separated by a shuffle barrier.
pub trait AggregateStageBuilder: Send + Sync {
    /// Builds Stage 0 (Map / Partial Aggregation) and Stage 1 (Reduce / Merge),
    /// connected by a [`StageEdge`].
    fn build(
        &self,
        query_id: QueryId,
        plan: &LogicalPlan,
        stages: &mut Vec<Stage>,
        edges: &mut Vec<StageEdge>,
    ) -> Result<StageId>;
}

/// Default distributed aggregation stage builder abstraction.
#[derive(Debug, Default, Clone)]
pub struct DefaultAggregateStageBuilder;

impl DefaultAggregateStageBuilder {
    /// Creates a new default aggregation stage builder.
    pub fn new() -> Self {
        Self
    }
}

impl AggregateStageBuilder for DefaultAggregateStageBuilder {
    fn build(
        &self,
        _query_id: QueryId,
        _plan: &LogicalPlan,
        _stages: &mut Vec<Stage>,
        _edges: &mut Vec<StageEdge>,
    ) -> Result<StageId> {
        Err(VeinError::internal("DefaultAggregateStageBuilder::build is not yet implemented"))
    }
}
