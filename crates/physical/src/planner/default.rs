//! Default single-node physical planner implementation.

use std::sync::Arc;

use common::identifiers::{QueryId, RequestId};
use common::protocol::ExecutionMode;
use common::Result;
use error::VeinError;
use plan::LogicalPlan;

use super::PhysicalPlanner;
use crate::builder::pipeline::{DefaultPipelineStageBuilder, PipelineStageBuilder};
use crate::job::Job;

/// The default distributed physical planner lowering [`LogicalPlan`] into a [`Job`].
#[derive(Clone)]
pub struct DefaultPhysicalPlanner {
    pipeline_builder: Arc<dyn PipelineStageBuilder>,
}

impl DefaultPhysicalPlanner {
    /// Creates a new [`DefaultPhysicalPlanner`] with default stage builders.
    pub fn new() -> Self {
        Self {
            pipeline_builder: Arc::new(DefaultPipelineStageBuilder::new()),
        }
    }

    /// Override the linear pipeline stage builder.
    pub fn with_pipeline_builder(mut self, builder: Arc<dyn PipelineStageBuilder>) -> Self {
        self.pipeline_builder = builder;
        self
    }

    /// Borrows the linear pipeline stage builder.
    pub fn pipeline_builder(&self) -> &Arc<dyn PipelineStageBuilder> {
        &self.pipeline_builder
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

impl PhysicalPlanner for DefaultPhysicalPlanner {
    fn create_physical_plan(&self, request_id: RequestId, logical_plan: LogicalPlan) -> Result<Job> {
        let query_id = QueryId::new();
        tracing::info!(
            target: "vein.audit",
            event = "physical_planning_started",
            %query_id,
            "physical_planning_started"
        );

        match &logical_plan {
            LogicalPlan::Scan { .. }
            | LogicalPlan::ExternalScan { .. }
            | LogicalPlan::Filter { .. }
            | LogicalPlan::Project { .. }
            | LogicalPlan::Write { .. } => {
                let stage = self.pipeline_builder.build(&logical_plan)?;
                Ok(Job::new(request_id, query_id, stage, ExecutionMode::Batch))
            }
            LogicalPlan::Aggregate { .. } => Err(VeinError::planning(
                "aggregations are not supported by the single-node execution engine yet",
                None,
            )),
            LogicalPlan::Join { .. } => Err(VeinError::planning(
                "JOIN is not supported by the single-node execution engine yet",
                None,
            )),
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
        let table = std::sync::Arc::new(
            plan::TableMetadata::new("users", std::sync::Arc::new(*schema.clone()))
                .with_format(plan::TableFormat::Parquet),
        );
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
        let job = planner
            .create_physical_plan(RequestId::new(), plan)
            .expect("plan should succeed");

        let stage = job.stage();
        assert_eq!(stage.tasks().len(), 1);

        let task = &stage.tasks()[0];
        assert_eq!(task.program.register_count, 2);
    }

    #[test]
    fn rejects_aggregate_without_creating_a_distributed_plan() {
        let input = mock_scan_plan();
        let schema = match &input {
            LogicalPlan::Scan { schema, .. } => schema.clone(),
            _ => unreachable!(),
        };
        let aggregate = LogicalPlan::Aggregate {
            input: Box::new(input),
            grouping_sets: vec![],
            aggs: vec![],
            schema,
        };

        let error = DefaultPhysicalPlanner::new()
            .create_physical_plan(RequestId::new(), aggregate)
            .expect_err("aggregate is not implemented by the local engine");

        assert!(error.to_string().contains("aggregations are not supported"));
    }

    #[test]
    fn rejects_join_without_creating_shuffle_stages() {
        let left = mock_scan_plan();
        let right = mock_scan_plan();
        let schema = match &left {
            LogicalPlan::Scan { schema, .. } => schema.clone(),
            _ => unreachable!(),
        };
        let join = LogicalPlan::Join {
            join_type: parser::ast::pipe_op::JoinType::Inner,
            left: Box::new(left),
            right: Box::new(right),
            on: None,
            using: vec![],
            schema,
        };

        let error = DefaultPhysicalPlanner::new()
            .create_physical_plan(RequestId::new(), join)
            .expect_err("join is not implemented by the local engine");

        assert!(error.to_string().contains("JOIN is not supported"));
    }
}
