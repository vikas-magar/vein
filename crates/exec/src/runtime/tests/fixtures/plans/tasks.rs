use super::*;
use super::super::executor::InputPlanFixture;

pub(in crate::runtime::tests) fn plan_with_input(task: &ExecuteTask, input: Arc<PhysicalPlanNode>) -> PhysicalPlan {
    let PhysicalPlanOperator::Sink { sink, .. } = &task.plan.root.operator else {
        panic!("task fixture must have a sink root");
    };
    let sink = match sink.as_ref() {
        DataSink::Client { .. } => Arc::new(DataSink::Client {
            schema: Arc::clone(&input.output_schema),
        }),
        _ => Arc::clone(sink),
    };
    PhysicalPlan {
        root: plan_node(
            Arc::clone(&input.output_schema),
            PhysicalPlanOperator::Sink { input, sink },
        ),
    }
}

pub(in crate::runtime::tests) fn set_input_plan(task: &mut ExecuteTask, input: InputPlanFixture) {
    task.plan = plan_with_input(task, canonical_input(input));
}

pub(in crate::runtime::tests) fn task() -> ExecuteTask {
    let schema = int64_schema();
    let input = plan_node(
        Arc::clone(&schema),
        PhysicalPlanOperator::Pipeline {
            source: Arc::new(DataSource::Memory),
            input_schema: Arc::clone(&schema),
            program: Arc::new(MicroProgram {
                register_count: 3,
                instructions: vec![
                    MicroOp::LoadConstant {
                        dst: 1,
                        value: types::LogicalLiteral::Int64(2),
                    },
                    MicroOp::CmpGt {
                        dst: 2,
                        src1: 0,
                        src2: 1,
                    },
                    MicroOp::FilterMask { bool_reg: 2 },
                    MicroOp::ProjectRegisters { out_regs: vec![0] },
                ],
            }),
        },
    );
    ExecuteTask {
        request_id: RequestId::new(),
        mode: ExecutionMode::Batch,
        task_id: TaskId::new(),
        attempt: 0,
        query_id: QueryId::new(),
        stage_id: StageId::new(),
        statistics_context: None,

        plan: PhysicalPlan {
            root: plan_node(
                Arc::clone(&schema),
                PhysicalPlanOperator::Sink {
                    input,
                    sink: Arc::new(DataSink::Client {
                        schema: Arc::clone(&schema),
                    }),
                },
            ),
        },
    }
}

pub(in crate::runtime::tests) fn aggregate_task_with_post_filter(threshold: i64) -> ExecuteTask {
    let mut task = task();
    let schema = int64_schema();
    let input = plan_node(
        Arc::clone(&schema),
        PhysicalPlanOperator::Pipeline {
            source: Arc::new(DataSource::Memory),
            input_schema: Arc::clone(&schema),
            program: Arc::new(MicroProgram {
                register_count: 1,
                instructions: vec![MicroOp::ProjectRegisters { out_regs: vec![0] }],
            }),
        },
    );
    let aggregate = plan_node(
        Arc::clone(&schema),
        PhysicalPlanOperator::Aggregate {
            input,
            plan: AggregatePlan {
                groups: 0,
                estimated_groups: None,
                grouping_sets: Some(vec![vec![]]),
                emit_empty_group_on_empty_input: true,
                aggs: vec![AggOpCode::Count],
                aggregate_input_types: vec![VeinDataType::Int64],
                types: vec![VeinDataType::Int64],
            },
        },
    );
    let filtered = plan_node(
        Arc::clone(&schema),
        PhysicalPlanOperator::Transform {
            input: aggregate,
            program: Arc::new(MicroProgram {
                register_count: 3,
                instructions: vec![
                    MicroOp::LoadConstant {
                        dst: 1,
                        value: types::LogicalLiteral::Int64(threshold),
                    },
                    MicroOp::CmpGt {
                        dst: 2,
                        src1: 0,
                        src2: 1,
                    },
                    MicroOp::FilterMask { bool_reg: 2 },
                ],
            }),
        },
    );
    task.plan = plan_with_input(&task, filtered);
    task
}










