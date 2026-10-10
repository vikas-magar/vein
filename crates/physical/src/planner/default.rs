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
            | LogicalPlan::Sort { .. }
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
        mock_scan_plan_with_column_id_offset(0)
    }

    fn mock_scan_plan_with_column_id_offset(column_id_offset: usize) -> LogicalPlan {
        let schema = Box::new(VeinSchema::new(vec![
            std::sync::Arc::new(ResolvedColumn::new(
                ColumnId(column_id_offset),
                "id",
                None,
                VeinDataType::Int64,
            )),
            std::sync::Arc::new(ResolvedColumn::new(
                ColumnId(column_id_offset + 1),
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
    fn plans_aggregate_join_input_as_recursive_pipeline_breaker() {
        let left = mock_scan_plan();
        let aggregate_input = mock_scan_plan();
        let (input_key, input_value) = match &aggregate_input {
            LogicalPlan::Scan { schema, .. } => {
                (Arc::clone(&schema.fields[0]), Arc::clone(&schema.fields[1]))
            }
            _ => unreachable!(),
        };
        let aggregate_key = Arc::new(ResolvedColumn::new(
            ColumnId(2),
            "group_id",
            None,
            VeinDataType::Int64,
        ));
        let aggregate_count = Arc::new(ResolvedColumn::new(
            ColumnId(3),
            "row_count",
            None,
            VeinDataType::Int64,
        ));
        let aggregate_schema = Box::new(VeinSchema::new(vec![
            Arc::clone(&aggregate_key),
            Arc::clone(&aggregate_count),
        ]));
        let aggregate = LogicalPlan::Aggregate {
            input: Box::new(aggregate_input),
            grouping_sets: vec![vec![LogicalExpr::Column(input_key)]],
            aggs: vec![LogicalExpr::FunctionCall {
                name: "count".to_owned(),
                args: vec![LogicalExpr::Column(input_value)],
                is_aggregate: true,
                distinct: false,
                return_type: VeinDataType::Int64,
            }],
            schema: aggregate_schema,
        };
        let left_key = match &left {
            LogicalPlan::Scan { schema, .. } => Arc::clone(&schema.fields[0]),
            _ => unreachable!(),
        };
        let join_schema = match &left {
            LogicalPlan::Scan { schema, .. } => {
                let mut fields = schema.fields.clone();
                fields.push(Arc::clone(&aggregate_key));
                fields.push(Arc::clone(&aggregate_count));
                Box::new(VeinSchema::new(fields))
            }
            _ => unreachable!(),
        };
        let join = LogicalPlan::Join {
            join_type: parser::ast::pipe_op::JoinType::Inner,
            left: Box::new(left),
            right: Box::new(aggregate),
            on: Some(LogicalExpr::BinaryOp {
                left: Box::new(LogicalExpr::Column(left_key)),
                op: parser::ast::expr::BinaryOp::Eq,
                right: Box::new(LogicalExpr::Column(aggregate_key)),
                return_type: VeinDataType::Boolean,
            }),
            using: vec![],
            schema: join_schema,
        };

        let job = DefaultPhysicalPlanner::new()
            .create_physical_plan(RequestId::new(), join)
            .expect("join with aggregate input should plan");
        let input = job.stage().tasks()[0]
            .input_plan
            .as_ref()
            .expect("recursive join input");
        let common::protocol::PhysicalInputPlan::Join { build, .. } = input.as_ref() else {
            panic!("root should be a join input");
        };
        assert!(
            matches!(build.as_ref(), common::protocol::PhysicalInputPlan::Aggregate { .. }),
            "aggregate build side should be represented as a pipeline breaker"
        );
    }

    #[test]
    fn dispatches_aggregate_over_join_to_recursive_join_planning() {
        let left_key = Arc::new(ResolvedColumn::new(
            ColumnId(10),
            "left_key",
            Some("left".to_string()),
            VeinDataType::Int64,
        ));
        let left_value = Arc::new(ResolvedColumn::new(
            ColumnId(11),
            "left_value",
            Some("left".to_string()),
            VeinDataType::Int64,
        ));
        let right_key = Arc::new(ResolvedColumn::new(
            ColumnId(20),
            "right_key",
            Some("right".to_string()),
            VeinDataType::Int64,
        ));
        let right_value = Arc::new(ResolvedColumn::new(
            ColumnId(21),
            "right_value",
            Some("right".to_string()),
            VeinDataType::Int64,
        ));
        let scan = |name: &str, fields: Vec<Arc<ResolvedColumn>>| {
            let schema = Arc::new(VeinSchema::new(fields));
            let table = Arc::new(
                plan::TableMetadata::new(name, Arc::clone(&schema))
                    .with_format(plan::TableFormat::Parquet),
            );
            LogicalPlan::Scan {
                table,
                projection: None,
                filters: Vec::new(),
                schema: Box::new((*schema).clone()),
            }
        };
        let left = scan("left", vec![Arc::clone(&left_key), Arc::clone(&left_value)]);
        let right = scan(
            "right",
            vec![Arc::clone(&right_key), Arc::clone(&right_value)],
        );
        let join_schema = Box::new(VeinSchema::new(vec![
            Arc::clone(&left_key),
            Arc::clone(&left_value),
            Arc::clone(&right_key),
            Arc::clone(&right_value),
        ]));
        let join = LogicalPlan::Join {
            join_type: parser::ast::pipe_op::JoinType::Inner,
            left: Box::new(left),
            right: Box::new(right),
            on: Some(LogicalExpr::BinaryOp {
                left: Box::new(LogicalExpr::Column(Arc::clone(&left_key))),
                op: parser::ast::expr::BinaryOp::Eq,
                right: Box::new(LogicalExpr::Column(Arc::clone(&right_key))),
                return_type: VeinDataType::Boolean,
            }),
            using: Vec::new(),
            schema: join_schema,
        };
        let group_key = Arc::new(ResolvedColumn::new(
            ColumnId(30),
            "left_key",
            None,
            VeinDataType::Int64,
        ));
        let row_count = Arc::new(ResolvedColumn::new(
            ColumnId(31),
            "row_count",
            None,
            VeinDataType::Int64,
        ));
        let aggregate = LogicalPlan::Aggregate {
            input: Box::new(join),
            grouping_sets: vec![vec![LogicalExpr::Column(Arc::clone(&left_key))]],
            aggs: vec![LogicalExpr::FunctionCall {
                name: "count".to_string(),
                args: vec![LogicalExpr::Column(right_value)],
                is_aggregate: true,
                distinct: false,
                return_type: VeinDataType::Int64,
            }],
            schema: Box::new(VeinSchema::new(vec![group_key, row_count])),
        };

        let job = DefaultPhysicalPlanner::new()
            .create_physical_plan(RequestId::new(), aggregate.clone())
            .expect("aggregate over a join should use recursive join planning");
        let input = job.stage().tasks()[0]
            .input_plan
            .as_ref()
            .expect("recursive aggregate input plan");
        let common::protocol::PhysicalInputPlan::Aggregate { input, .. } = input.as_ref() else {
            panic!("the recursive input root should be an aggregate");
        };
        let common::protocol::PhysicalInputPlan::Transform { input, .. } = input.as_ref() else {
            panic!("aggregate arguments should be projected by a transform");
        };
        assert!(
            matches!(input.as_ref(), common::protocol::PhysicalInputPlan::Join { .. }),
            "the aggregate transform must consume the planned join"
        );

        let filtered = LogicalPlan::Filter {
            input: Box::new(aggregate),
            predicate: LogicalExpr::BinaryOp {
                left: Box::new(LogicalExpr::Column(Arc::new(ResolvedColumn::new(
                    ColumnId(31),
                    "row_count",
                    None,
                    VeinDataType::Int64,
                )))),
                op: parser::ast::expr::BinaryOp::Gt,
                right: Box::new(LogicalExpr::Literal(plan::LogicalLiteral::Int64(2))),
                return_type: VeinDataType::Boolean,
            },
        };
        let filtered_job = DefaultPhysicalPlanner::new()
            .create_physical_plan(RequestId::new(), filtered)
            .expect("HAVING over an aggregate join should plan");
        let filtered_input = filtered_job.stage().tasks()[0]
            .input_plan
            .as_ref()
            .expect("recursive filtered input");
        let common::protocol::PhysicalInputPlan::Transform { input, program } = filtered_input.as_ref() else {
            panic!("HAVING should be represented as a transform above the aggregate");
        };
        assert!(matches!(
            input.as_ref(),
            common::protocol::PhysicalInputPlan::Aggregate { .. }
        ));
        assert!(
            program.instructions.iter().any(|op| matches!(op, common::protocol::MicroOp::FilterMask { .. })),
            "HAVING transform must contain its filter instruction"
        );
    }

    #[test]
    fn plans_filter_against_aggregate_output_in_a_separate_program() {
        let input = mock_scan_plan();
        let argument = match &input {
            LogicalPlan::Scan { schema, .. } => schema.fields[0].clone(),
            _ => unreachable!(),
        };
        let aggregate_schema = Box::new(VeinSchema::new(vec![Arc::new(ResolvedColumn::new(
            ColumnId(2),
            "order_count",
            None,
            VeinDataType::Int64,
        ))]));
        let count_column = aggregate_schema.fields[0].clone();
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
            schema: aggregate_schema,
        };
        let filtered = LogicalPlan::Filter {
            input: Box::new(aggregate),
            predicate: LogicalExpr::BinaryOp {
                left: Box::new(LogicalExpr::Column(count_column)),
                op: parser::ast::expr::BinaryOp::Gt,
                right: Box::new(LogicalExpr::Literal(plan::LogicalLiteral::Int64(2))),
                return_type: VeinDataType::Boolean,
            },
        };

        let job = DefaultPhysicalPlanner::new()
            .create_physical_plan(RequestId::new(), filtered)
            .expect("post-aggregate filter should compile");
        let task = &job.stage().tasks()[0];
        assert!(task
            .program
            .instructions
            .iter()
            .all(|op| !matches!(op, common::protocol::MicroOp::FilterMask { .. })));
        let post = task
            .post_aggregate_program
            .as_ref()
            .expect("post-aggregate filter program");
        assert!(matches!(
            post.instructions.last(),
            Some(common::protocol::MicroOp::FilterMask { .. })
        ));
    }

    #[test]
    fn plans_multiple_grouping_sets_over_one_union_key_projection() {
        let input = mock_scan_plan();
        let (id, name) = match &input {
            LogicalPlan::Scan { schema, .. } => {
                (schema.fields[0].clone(), schema.fields[1].clone())
            }
            _ => unreachable!(),
        };
        let schema = Box::new(VeinSchema::new(vec![
            Arc::new(ResolvedColumn::new(
                ColumnId(2),
                "id",
                None,
                VeinDataType::Int64,
            )),
            Arc::new(ResolvedColumn::new(
                ColumnId(3),
                "name",
                None,
                VeinDataType::Utf8,
            )),
            Arc::new(ResolvedColumn::new(
                ColumnId(4),
                "row_count",
                None,
                VeinDataType::Int64,
            )),
        ]));
        let aggregate = LogicalPlan::Aggregate {
            input: Box::new(input),
            grouping_sets: vec![
                vec![
                    LogicalExpr::Column(id.clone()),
                    LogicalExpr::Column(name),
                ],
                vec![LogicalExpr::Column(id.clone())],
                vec![],
            ],
            aggs: vec![LogicalExpr::FunctionCall {
                name: "count".to_string(),
                args: vec![LogicalExpr::Column(id)],
                is_aggregate: true,
                distinct: false,
                return_type: VeinDataType::Int64,
            }],
            schema,
        };

        let job = DefaultPhysicalPlanner::new()
            .create_physical_plan(RequestId::new(), aggregate)
            .expect("multiple grouping sets should be carried into the task plan");
        let plan = job.stage().tasks()[0]
            .aggregate
            .as_ref()
            .expect("aggregate task metadata");

        assert_eq!(plan.groups, 2);
        assert_eq!(
            plan.grouping_sets,
            Some(vec![vec![0, 1], vec![0], vec![]])
        );
        assert_eq!(plan.aggs, vec![common::protocol::AggOpCode::Count]);
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
    fn plans_distinct_over_join_as_recursive_aggregate() {
        let left_key = Arc::new(ResolvedColumn::new(
            ColumnId(10),
            "left_key",
            Some("left".to_string()),
            VeinDataType::Int64,
        ));
        let left_value = Arc::new(ResolvedColumn::new(
            ColumnId(11),
            "left_value",
            Some("left".to_string()),
            VeinDataType::Int64,
        ));
        let right_key = Arc::new(ResolvedColumn::new(
            ColumnId(20),
            "right_key",
            Some("right".to_string()),
            VeinDataType::Int64,
        ));
        let right_value = Arc::new(ResolvedColumn::new(
            ColumnId(21),
            "right_value",
            Some("right".to_string()),
            VeinDataType::Int64,
        ));
        let scan = |name: &str, fields: Vec<Arc<ResolvedColumn>>| {
            let schema = Arc::new(VeinSchema::new(fields));
            let table = Arc::new(
                plan::TableMetadata::new(name, Arc::clone(&schema))
                    .with_format(plan::TableFormat::Parquet),
            );
            LogicalPlan::Scan {
                table,
                projection: None,
                filters: Vec::new(),
                schema: Box::new((*schema).clone()),
            }
        };
        let left = scan("left", vec![Arc::clone(&left_key), Arc::clone(&left_value)]);
        let right = scan("right", vec![Arc::clone(&right_key), Arc::clone(&right_value)]);
        let schema = Box::new(VeinSchema::new(vec![
            left_key.clone(),
            left_value,
            right_key.clone(),
            right_value,
        ]));
        let join = LogicalPlan::Join {
            join_type: parser::ast::pipe_op::JoinType::Inner,
            left: Box::new(left),
            right: Box::new(right),
            on: Some(LogicalExpr::BinaryOp {
                left: Box::new(LogicalExpr::Column(left_key)),
                op: parser::ast::expr::BinaryOp::Eq,
                right: Box::new(LogicalExpr::Column(right_key)),
                return_type: VeinDataType::Boolean,
            }),
            using: Vec::new(),
            schema: schema.clone(),
        };
        let distinct = LogicalPlan::Distinct {
            input: Box::new(join),
            schema,
        };

        let job = DefaultPhysicalPlanner::new()
            .create_physical_plan(RequestId::new(), distinct)
            .expect("distinct over a join should use recursive planning");
        let input = job.stage().tasks()[0]
            .input_plan
            .as_ref()
            .expect("recursive distinct input");
        let common::protocol::PhysicalInputPlan::Aggregate { input, plan } = input.as_ref() else {
            panic!("distinct should be represented as an aggregate pipeline breaker");
        };
        assert_eq!(plan.groups, 4);
        assert_eq!(plan.grouping_sets, Some(vec![vec![0, 1, 2, 3]]));
        assert!(plan.aggs.is_empty());
        assert!(matches!(
            input.as_ref(),
            common::protocol::PhysicalInputPlan::Transform { input, .. }
                if matches!(input.as_ref(), common::protocol::PhysicalInputPlan::Join { .. })
        ));
    }

    #[test]
    fn plans_projection_above_an_intermediate_join_as_a_streaming_transform() {
        let left = mock_scan_plan_with_column_id_offset(0);
        let right = mock_scan_plan_with_column_id_offset(2);
        let base_schema = match &left {
            LogicalPlan::Scan { schema, .. } => schema.clone(),
            _ => unreachable!(),
        };
        let left_key = base_schema.fields[0].clone();
        let right_key = match &right {
            LogicalPlan::Scan { schema, .. } => schema.fields[0].clone(),
            _ => unreachable!(),
        };
        let inner = LogicalPlan::Join {
            join_type: parser::ast::pipe_op::JoinType::Inner,
            left: Box::new(left),
            right: Box::new(right),
            on: Some(LogicalExpr::BinaryOp {
                left: Box::new(LogicalExpr::Column(left_key.clone())),
                op: parser::ast::expr::BinaryOp::Eq,
                right: Box::new(LogicalExpr::Column(right_key)),
                return_type: VeinDataType::Boolean,
            }),
            using: vec![],
            schema: base_schema.clone(),
        };
        let projected_key = Arc::new(ResolvedColumn::new(
            ColumnId(4),
            "projected_id",
            None,
            VeinDataType::Int64,
        ));
        let projected_schema = Box::new(VeinSchema::new(vec![Arc::clone(&projected_key)]));
        let projected = LogicalPlan::Project {
            input: Box::new(inner),
            exprs: vec![LogicalExpr::Column(left_key)],
            schema: projected_schema.clone(),
        };
        let outer_right = mock_scan_plan_with_column_id_offset(5);
        let outer_right_key = match &outer_right {
            LogicalPlan::Scan { schema, .. } => schema.fields[0].clone(),
            _ => unreachable!(),
        };
        let outer_schema = Box::new(VeinSchema::new(vec![
            Arc::clone(&projected_key),
            Arc::clone(&outer_right_key),
        ]));
        let chained = LogicalPlan::Join {
            join_type: parser::ast::pipe_op::JoinType::Inner,
            left: Box::new(projected),
            right: Box::new(outer_right),
            on: Some(LogicalExpr::BinaryOp {
                left: Box::new(LogicalExpr::Column(projected_key)),
                op: parser::ast::expr::BinaryOp::Eq,
                right: Box::new(LogicalExpr::Column(outer_right_key)),
                return_type: VeinDataType::Boolean,
            }),
            using: vec![],
            schema: outer_schema,
        };

        let job = DefaultPhysicalPlanner::new()
            .create_physical_plan(RequestId::new(), chained)
            .expect("projected intermediate join should plan");
        let input = job.stage().tasks()[0]
            .input_plan
            .as_ref()
            .expect("recursive join input");
        let common::protocol::PhysicalInputPlan::Join { probe, .. } = input.as_ref() else {
            panic!("root should be a join input");
        };
        assert!(matches!(
            probe.as_ref(),
            common::protocol::PhysicalInputPlan::Transform { input, .. }
                if matches!(input.as_ref(), common::protocol::PhysicalInputPlan::Join { .. })
        ));
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
