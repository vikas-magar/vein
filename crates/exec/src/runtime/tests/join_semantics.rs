use super::*;

#[tokio::test]
async fn recursive_left_join_applies_residual_before_preserving_unmatched_probe_rows() {
    let leaf = || InputPlanFixture::Pipeline {
        source: DataSource::Memory,
        program: Arc::new(MicroProgram::empty()),
    };
    let mut task = task();
    set_input_plan(
        &mut task,
        InputPlanFixture::Join {
            probe: Box::new(leaf()),
            build: Box::new(leaf()),
            plan: JoinPlan {
                join_type: JoinKind::Left,
                probe_keys: vec![0],
                build_keys: vec![0],
                residual: Some(common::protocol::JoinResidualPlan {
                    input_columns: vec![JoinOutput::Build(0)],
                    program: Arc::new(MicroProgram::new(
                        3,
                        vec![
                            common::protocol::MicroOp::LoadConstant {
                                dst: 1,
                                value: types::LogicalLiteral::Int64(3),
                            },
                            common::protocol::MicroOp::CmpLt {
                                dst: 2,
                                src1: 0,
                                src2: 1,
                            },
                            common::protocol::MicroOp::FilterMask { bool_reg: 2 },
                        ],
                    )),
                }),
                outputs: vec![JoinOutput::Probe(0), JoinOutput::Build(0)],
                probe_types: vec![types::VeinDataType::Int64],
                build_types: vec![types::VeinDataType::Int64],
            },
        },
    );

    let result = runner()
        .execute(&task, CancellationToken::new())
        .await
        .expect("recursive left join with residual predicate should execute");

    assert_eq!(result.row_count, 5);
    let mut probe_rows = Vec::new();
    let mut build_rows = Vec::new();
    for batch in &result.output {
        let probe = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("probe output");
        let build = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("build output");
        for row in 0..batch.num_rows() {
            probe_rows.push(probe.value(row));
            build_rows.push(build.is_valid(row).then(|| build.value(row)));
        }
    }
    assert_eq!(probe_rows, vec![1, 2, 3, 4, 5]);
    assert_eq!(build_rows, vec![Some(1), Some(2), None, None, None]);
}

#[tokio::test]
async fn recursive_right_join_does_not_mark_residual_rejections_as_matches() {
    let leaf = || InputPlanFixture::Pipeline {
        source: DataSource::Memory,
        program: Arc::new(MicroProgram::empty()),
    };
    let mut task = task();
    set_input_plan(
        &mut task,
        InputPlanFixture::Join {
            probe: Box::new(leaf()),
            build: Box::new(leaf()),
            plan: JoinPlan {
                join_type: JoinKind::Right,
                probe_keys: vec![0],
                build_keys: vec![0],
                residual: Some(common::protocol::JoinResidualPlan {
                    input_columns: vec![JoinOutput::Build(0)],
                    program: Arc::new(MicroProgram::new(
                        3,
                        vec![
                            common::protocol::MicroOp::LoadConstant {
                                dst: 1,
                                value: types::LogicalLiteral::Int64(3),
                            },
                            common::protocol::MicroOp::CmpLt {
                                dst: 2,
                                src1: 0,
                                src2: 1,
                            },
                            common::protocol::MicroOp::FilterMask { bool_reg: 2 },
                        ],
                    )),
                }),
                outputs: vec![JoinOutput::Probe(0), JoinOutput::Build(0)],
                probe_types: vec![types::VeinDataType::Int64],
                build_types: vec![types::VeinDataType::Int64],
            },
        },
    );

    let result = runner()
        .execute(&task, CancellationToken::new())
        .await
        .expect("recursive right join with residual predicate should execute");

    assert_eq!(result.row_count, 5);
    let probe_rows = result
        .output
        .iter()
        .flat_map(|batch| {
            let probe = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .expect("probe output");
            (0..probe.len())
                .map(|i| probe.is_valid(i).then(|| probe.value(i)))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let build_rows = result
        .output
        .iter()
        .flat_map(|batch| {
            let build = batch
                .column(1)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .expect("build output");
            (0..build.len()).map(|i| build.value(i)).collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(probe_rows, vec![Some(1), Some(2), None, None, None]);
    assert_eq!(build_rows, vec![1, 2, 3, 4, 5]);
}
