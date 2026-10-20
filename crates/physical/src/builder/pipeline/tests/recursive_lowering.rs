use super::support::{equality, test_column};
use super::super::{DefaultPipelineStageBuilder, PipelineStageBuilder};
use common::protocol::{ExpressionStep, PhysicalPlanOperator};
use plan::schema::VeinDataType;
use plan::{LogicalExpr, LogicalPlan, SortKey as LogicalSortKey};
use plan::schema::VeinSchema;
use std::sync::Arc;

#[test]
fn builds_recursive_sort_and_limit_nodes_without_duplicate_task_windows() {
    let probe_column = test_column(1, "probe_key");
    let build_column = test_column(2, "build_key");
    let probe_schema = VeinSchema::new(vec![Arc::clone(&probe_column)]);
    let build_schema = VeinSchema::new(vec![Arc::clone(&build_column)]);
    let output_schema =
        VeinSchema::new(vec![Arc::clone(&probe_column), Arc::clone(&build_column)]);
    let scan = |url: &str, schema: &VeinSchema| LogicalPlan::ExternalScan {
        url: url.to_owned(),
        options: Vec::new(),
        files: Vec::new(),
        source_metadata: None,
        schema: Box::new(schema.clone()),
    };
    let join = LogicalPlan::Join {
        join_type: parser::ast::pipe_op::JoinType::Inner,
        left: Box::new(scan("probe", &probe_schema)),
        right: Box::new(scan("build", &build_schema)),
        on: Some(equality(
            Arc::clone(&probe_column),
            Arc::clone(&build_column),
        )),
        using: Vec::new(),
        schema: Box::new(output_schema.clone()),
    };
    let sort = |input| LogicalPlan::Sort {
        input: Box::new(input),
        sort_keys: vec![LogicalSortKey {
            expr: LogicalExpr::Column(Arc::clone(&probe_column)),
            asc: true,
            nulls_first: false,
        }],
        schema: Box::new(output_schema.clone()),
    };
    let limit = |input| LogicalPlan::Limit {
        input: Box::new(input),
        offset: 1,
        count: 2,
        schema: Box::new(output_schema.clone()),
    };

    for plan in [limit(sort(join.clone())), sort(limit(join))] {
        let stage = DefaultPipelineStageBuilder::new()
            .build(&plan)
            .expect("recursive join with sort and limit should plan");
        let task = stage.tasks.first().expect("join stage task");

        let PhysicalPlanOperator::Sink { input, .. } = &task.plan.root.operator else {
            panic!("canonical plan must have a sink root");
        };
        match (&plan, &input.operator) {
            (LogicalPlan::Limit { .. }, PhysicalPlanOperator::Limit { input, .. }) => {
                assert!(matches!(input.operator, PhysicalPlanOperator::Sort { .. }));
            }
            (LogicalPlan::Sort { .. }, PhysicalPlanOperator::Sort { input, .. }) => {
                assert!(matches!(input.operator, PhysicalPlanOperator::Limit { .. }));
            }
            _ => panic!("root operator order must be preserved by recursive plan"),
        }
    }
}

#[test]
fn recursive_lowering_fuses_adjacent_transforms_above_join() {
    let probe = test_column(1, "probe_key");
    let build = test_column(2, "build_value");
    let probe_schema = VeinSchema::new(vec![Arc::clone(&probe)]);
    let build_schema = VeinSchema::new(vec![Arc::clone(&build)]);
    let joined_schema = VeinSchema::new(vec![Arc::clone(&probe), Arc::clone(&build)]);
    let scan = |name: &str, schema: &VeinSchema| LogicalPlan::ExternalScan {
        url: name.to_owned(),
        options: Vec::new(),
        files: Vec::new(),
        source_metadata: None,
        schema: Box::new(schema.clone()),
    };
    let join = LogicalPlan::Join {
        join_type: parser::ast::pipe_op::JoinType::Inner,
        left: Box::new(scan("probe", &probe_schema)),
        right: Box::new(scan("build", &build_schema)),
        on: Some(equality(Arc::clone(&probe), Arc::clone(&build))),
        using: Vec::new(),
        schema: Box::new(joined_schema.clone()),
    };
    let filter = LogicalPlan::Filter {
        input: Box::new(join),
        predicate: LogicalExpr::BinaryOp {
            left: Box::new(LogicalExpr::Column(Arc::clone(&probe))),
            op: parser::ast::expr::BinaryOp::Gt,
            right: Box::new(LogicalExpr::Literal(plan::LogicalLiteral::Int64(0))),
            return_type: VeinDataType::Boolean,
        },
    };
    let project = LogicalPlan::Project {
        input: Box::new(filter),
        exprs: vec![LogicalExpr::Column(Arc::clone(&build))],
        schema: Box::new(VeinSchema::new(vec![Arc::clone(&build)])),
    };

    let stage = DefaultPipelineStageBuilder::new()
        .build(&project)
        .expect("row-wise chain above join should lower recursively");
    let task = stage.tasks.first().expect("join task");
    let PhysicalPlanOperator::Sink { input, .. } = &task.plan.root.operator else {
        panic!("canonical plan must have a sink root");
    };
    let PhysicalPlanOperator::Transform {
        input: join,
        program,
    } = &input.operator
    else {
        panic!("adjacent filter and projection should fuse into one transform");
    };
    assert!(matches!(join.operator, PhysicalPlanOperator::Join { .. }));
    assert!(program
        .steps
        .iter()
        .any(|step| matches!(step, ExpressionStep::Filter(_))));
    assert!(program.steps.iter().any(|step| matches!(
        step,
        ExpressionStep::Projection(expressions) if expressions.len() == 1
    )));
}

#[test]
fn recursive_lowering_fuses_source_filter_and_projection_into_pipeline() {
    let value = test_column(1, "value");
    let input_schema = VeinSchema::new(vec![Arc::clone(&value)]);
    let scan = LogicalPlan::ExternalScan {
        url: "memory://source".to_owned(),
        options: Vec::new(),
        files: Vec::new(),
        source_metadata: None,
        schema: Box::new(input_schema.clone()),
    };
    let filter = LogicalPlan::Filter {
        input: Box::new(scan),
        predicate: LogicalExpr::BinaryOp {
            left: Box::new(LogicalExpr::Column(Arc::clone(&value))),
            op: parser::ast::expr::BinaryOp::Gt,
            right: Box::new(LogicalExpr::Literal(plan::LogicalLiteral::Int64(0))),
            return_type: VeinDataType::Boolean,
        },
    };
    let project = LogicalPlan::Project {
        input: Box::new(filter),
        exprs: vec![LogicalExpr::Column(Arc::clone(&value))],
        schema: Box::new(VeinSchema::new(vec![value])),
    };

    let stage = DefaultPipelineStageBuilder::new()
        .build(&project)
        .expect("row-wise operators over a scan should lower as one source pipeline");
    let task = stage.tasks.first().expect("pipeline task");
    let PhysicalPlanOperator::Sink { input, .. } = &task.plan.root.operator else {
        panic!("canonical plan must have a sink root");
    };
    let PhysicalPlanOperator::Pipeline { program, .. } = &input.operator else {
        panic!("source row-wise operators should remain fused with the scan");
    };
    assert!(program
        .steps
        .iter()
        .any(|step| matches!(step, ExpressionStep::Filter(_))));
    assert!(program.steps.iter().any(|step| matches!(
        step,
        ExpressionStep::Projection(expressions) if expressions.len() == 1
    )));
}

#[test]
fn recursive_lowering_names_unsupported_logical_operators() {
    let schema = VeinSchema::empty();
    let call = LogicalPlan::Call {
        input: Box::new(LogicalPlan::ExternalScan {
            url: "memory://source".to_owned(),
            options: Vec::new(),
            files: Vec::new(),
            source_metadata: None,
            schema: Box::new(schema.clone()),
        }),
        name: "custom_call".to_owned(),
        args: Vec::new(),
        schema: Box::new(schema),
    };

    let error = match DefaultPipelineStageBuilder::new().build(&call) {
        Ok(_) => panic!("CALL has no physical implementation yet"),
        Err(error) => error,
    };

    assert!(error.to_string().contains("operator 'call'"));
    assert!(error.to_string().contains("recursive physical lowering"));
}
