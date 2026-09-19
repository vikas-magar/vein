//! Default distributed physical planner implementation.

use std::sync::Arc;

use common::identifiers::QueryId;
use common::protocol::ExecutionMode;
use common::Result;
use error::VeinError;
use plan::LogicalPlan;

use super::PhysicalPlanner;
use crate::builder::aggregate::{AggregateStageBuilder, DefaultAggregateStageBuilder};
use crate::builder::pipeline::{DefaultPipelineStageBuilder, PipelineStageBuilder};
use crate::builder::join::{DefaultJoinStageBuilder, JoinStageBuilder};
use crate::job::Job;

/// The default distributed physical planner lowering [`LogicalPlan`] into a [`Job`].
#[derive(Clone)]
pub struct DefaultPhysicalPlanner {
    pipeline_builder: Arc<dyn PipelineStageBuilder>,
    aggregate_builder: Arc<dyn AggregateStageBuilder>,
    join_builder: Arc<dyn JoinStageBuilder>,
}

impl DefaultPhysicalPlanner {
    /// Creates a new [`DefaultPhysicalPlanner`] with default stage builders.
    pub fn new() -> Self {
        Self {
            pipeline_builder: Arc::new(DefaultPipelineStageBuilder::new()),
            aggregate_builder: Arc::new(DefaultAggregateStageBuilder::new()),
            join_builder: Arc::new(DefaultJoinStageBuilder::new()),
        }
    }

    /// Override the linear pipeline stage builder.
    pub fn with_pipeline_builder(mut self, builder: Arc<dyn PipelineStageBuilder>) -> Self {
        self.pipeline_builder = builder;
        self
    }

    /// Override the distributed aggregation stage builder.
    pub fn with_aggregate_builder(mut self, builder: Arc<dyn AggregateStageBuilder>) -> Self {
        self.aggregate_builder = builder;
        self
    }

    /// Override the distributed join stage builder.
    pub fn with_join_builder(mut self, builder: Arc<dyn JoinStageBuilder>) -> Self {
        self.join_builder = builder;
        self
    }

    /// Borrows the linear pipeline stage builder.
    pub fn pipeline_builder(&self) -> &Arc<dyn PipelineStageBuilder> {
        &self.pipeline_builder
    }

    /// Borrows the aggregation stage builder.
    pub fn aggregate_builder(&self) -> &Arc<dyn AggregateStageBuilder> {
        &self.aggregate_builder
    }

    /// Borrows the distributed join stage builder.
    pub fn join_builder(&self) -> &Arc<dyn JoinStageBuilder> {
        &self.join_builder
    }
}

impl Default for DefaultPhysicalPlanner {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for DefaultPhysicalPlanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DefaultPhysicalPlanner").finish_non_exhaustive()
    }
}

fn contains_linear_join(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Join { .. } => true,
        LogicalPlan::Project { input, .. } | LogicalPlan::Filter { input, .. } => {
            contains_linear_join(input)
        }
        _ => false,
    }
}

impl PhysicalPlanner for DefaultPhysicalPlanner {
    fn create_physical_plan(&self, logical_plan: LogicalPlan) -> Result<Job> {
        let query_id = QueryId::new();

        if contains_linear_join(&logical_plan) {
            let mut stages = Vec::new();
            let mut edges = Vec::new();
            let output_stage = self.join_builder.build(
                query_id,
                &logical_plan,
                &mut stages,
                &mut edges,
            )?;
            return Job::new(query_id, stages, edges, output_stage, ExecutionMode::Batch);
        }

        match &logical_plan {
            LogicalPlan::Scan { .. }
            | LogicalPlan::ExternalScan { .. }
            | LogicalPlan::Filter { .. }
            | LogicalPlan::Project { .. }
            | LogicalPlan::Write { .. } => {
                let mut stages = Vec::new();
                let output_stage = self
                    .pipeline_builder
                    .build(query_id, &logical_plan, &mut stages)?;
                Job::new(query_id, stages, vec![], output_stage, ExecutionMode::Batch)
            }
            LogicalPlan::Aggregate { .. } => {
                let mut stages = Vec::new();
                let mut edges = Vec::new();
                let output_stage = self.aggregate_builder.build(
                    query_id,
                    &logical_plan,
                    &mut stages,
                    &mut edges,
                )?;
                Job::new(query_id, stages, edges, output_stage, ExecutionMode::Batch)
            }
            LogicalPlan::Join { .. } => {
                let mut stages = Vec::new();
                let mut edges = Vec::new();
                let output_stage = self.join_builder.build(
                    query_id,
                    &logical_plan,
                    &mut stages,
                    &mut edges,
                )?;
                Job::new(query_id, stages, edges, output_stage, ExecutionMode::Batch)
            }
            other => Err(VeinError::planning(
                format!("Logical plan node not yet supported by physical planner: {other:?}"),
                None,
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use plan::schema::{ColumnId, ResolvedColumn, VeinDataType, VeinSchema};

    fn mock_scan_plan() -> LogicalPlan {
        let schema = Box::new(VeinSchema::new(vec![
            ResolvedColumn::new(ColumnId(0), "id", None, VeinDataType::Int64),
            ResolvedColumn::new(ColumnId(1), "name", None, VeinDataType::Utf8),
        ]));
        let table = std::sync::Arc::new(plan::TableMetadata::new(
            "users",
            std::sync::Arc::new(*schema.clone()),
        ));
        LogicalPlan::Scan {
            table,
            projection: None,
            filters: vec![],
            schema,
        }
    }

    #[test]
    fn test_create_physical_plan_for_linear_scan() {
        let planner = DefaultPhysicalPlanner::new();
        let plan = mock_scan_plan();
        let job = planner.create_physical_plan(plan).expect("plan should succeed");

        assert_eq!(job.topological_order().len(), 1);
        let stage_id = job.output_stage_id();
        let stage = job.get_stage(stage_id).expect("output stage exists");
        assert_eq!(stage.tasks().len(), 1);

        let task = &stage.tasks()[0];
        assert_eq!(task.program.register_count, 2);
    }
}
