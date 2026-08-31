//! Physical plan builder abstraction: coordinates translation of logical plan trees into a [`Job`].

/// Two-phase Map/Reduce aggregation stage translation abstractions.
pub mod aggregate;
/// Expression translation abstractions from logical trees to index-based wire structures.
pub mod expr;
/// Linear pipeline stage translation abstractions (Scan -> Filter -> Project -> Limit).
pub mod pipeline;

use crate::builder::aggregate::{AggregateStageBuilder, DefaultAggregateStageBuilder};
use crate::builder::pipeline::{DefaultPipelineStageBuilder, PipelineStageBuilder};
use crate::job::Job;
use common::identifiers::QueryId;
use common::Result;
use error::VeinError;
use plan::LogicalPlan;
use std::sync::Arc;

/// Trait for coordinating the translation of an optimized [`LogicalPlan`] into a distributed [`Job`].
pub trait PlanBuilder: Send + Sync {
    /// Build a complete, validated [`Job`] from an optimized [`LogicalPlan`].
    fn build(&self, logical_plan: LogicalPlan) -> Result<Job>;
}

/// Default physical plan builder abstraction.
#[allow(dead_code)]
pub struct PhysicalPlanBuilder {
    query_id: QueryId,
    pipeline_builder: Arc<dyn PipelineStageBuilder>,
    aggregate_builder: Arc<dyn AggregateStageBuilder>,
}

impl PhysicalPlanBuilder {
    /// Create a new builder with default stage builder abstractions.
    pub fn new() -> Self {
        Self {
            query_id: QueryId::new(),
            pipeline_builder: Arc::new(DefaultPipelineStageBuilder::new()),
            aggregate_builder: Arc::new(DefaultAggregateStageBuilder::new()),
        }
    }

    /// Override the pipeline stage builder abstraction.
    pub fn with_pipeline_builder(mut self, builder: Arc<dyn PipelineStageBuilder>) -> Self {
        self.pipeline_builder = builder;
        self
    }

    /// Override the aggregate stage builder abstraction.
    pub fn with_aggregate_builder(mut self, builder: Arc<dyn AggregateStageBuilder>) -> Self {
        self.aggregate_builder = builder;
        self
    }
}

impl Default for PhysicalPlanBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl PlanBuilder for PhysicalPlanBuilder {
    fn build(&self, _logical_plan: LogicalPlan) -> Result<Job> {
        Err(VeinError::internal(
            "DagPhysicalPlanner::create_physical_plan is not yet implemented",
        ))
    }
}
