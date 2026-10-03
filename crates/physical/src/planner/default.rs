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
            | LogicalPlan::Join { .. }
            | LogicalPlan::Limit { .. }
            | LogicalPlan::SubqueryAlias { .. }
            | LogicalPlan::Aggregate { .. }
            | LogicalPlan::Distinct { .. }
            | LogicalPlan::Write { .. } => {
                let stage = self.pipeline_builder.build(&logical_plan)?;
                Ok(Job::new(request_id, query_id, stage, ExecutionMode::Batch))
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
    use plan::LogicalExpr;

    fn mock_scan_plan() -> LogicalPlan {
        let schema = Box::new(VeinSchema::new(vec![
            std::sync::Arc::new(ResolvedColumn::new(
                ColumnId(0),
                "id",
                None,
                VeinDataType::Int64,
            )),
            std::sync::Arc::new(ResolvedColumn::new(
                ColumnId(1),
                "name",
                None,
                VeinDataType::Utf8,
            )),
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
    fn plans_global_aggregate_from_a_scan_source() {
        let input = mock_scan_plan();
        let argument = match &input {
            LogicalPlan::Scan { schema, .. } => schema.fields[0].clone(),
            _ => unreachable!(),
        };
        let schema = Box::new(VeinSchema::new(vec![Arc::new(ResolvedColumn::new(
            ColumnId(2),
            "order_count",
            None,
            VeinDataType::Int64,
        ))]));
        let aggregate = LogicalPlan::Aggregate {
            input: Box::new(input),
            grouping_sets: vec![vec![]],
            aggs: vec![LogicalExpr::FunctionCall {
                name: "count".to_string(),
                args: vec![LogicalExpr::Column(argument)],
                is_aggregate: true,
                distinct: false,
                return_type: VeinDataType::Int64,
            }],
            schema,
        };

        let job = DefaultPhysicalPlanner::new()
            .create_physical_plan(RequestId::new(), aggregate)
            .expect("aggregate should compile back to its scan source");

        assert_eq!(job.stage().tasks().len(), 1);
        let task = &job.stage().tasks()[0];
        assert!(matches!(&task.source, common::protocol::DataSource::File(_)));
        let aggregate = task.aggregate.as_ref().expect("aggregate task metadata");
        assert_eq!(aggregate.groups, 0);
        assert_eq!(aggregate.aggs, vec![common::protocol::AggOpCode::Count]);
        assert_eq!(aggregate.types, vec![VeinDataType::Int64]);
    }

    #[test]
    fn plans_distinct_from_a_scan_source() {
        let input = mock_scan_plan();
        let schema = match &input {
            LogicalPlan::Scan { schema, .. } => schema.clone(),
            _ => unreachable!(),
        };
        let distinct = LogicalPlan::Distinct {
            input: Box::new(input),
            schema,
        };

        let job = DefaultPhysicalPlanner::new()
            .create_physical_plan(RequestId::new(), distinct)
            .expect("distinct should compile back to its scan source");

        assert_eq!(job.stage().tasks().len(), 1);
        let task = &job.stage().tasks()[0];
        assert!(matches!(&task.source, common::protocol::DataSource::File(_)));
        let aggregate = task.aggregate.as_ref().expect("distinct aggregate metadata");
        assert_eq!(aggregate.groups, 2);
        assert!(aggregate.aggs.is_empty());
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

        // A join is planned now, so the blanket refusal must be gone. These mock
        // inputs still cannot compile into a source, so this fails — but for a
        // reason, not because joins are unsupported.
        let error = DefaultPhysicalPlanner::new()
            .create_physical_plan(RequestId::new(), join)
            .expect_err("the mock join cannot execute");

        assert!(
            !error.to_string().contains("JOIN is not supported"),
            "joins must no longer be refused wholesale: {error}"
        );
    }
}
