use super::*;

#[tokio::test]
async fn recursive_left_join_applies_residual_before_preserving_unmatched_probe_rows() {
    let leaf = || InputPlanFixture::Pipeline {
        source: DataSource::Memory,
        program: Arc::new(ExpressionProgram::empty()),
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
                    program: filter_program(binary_expr(
                        BinaryExprOp::Less,
                        column_expr(0, types::VeinDataType::Int64),
                        literal_expr(LogicalLiteral::Int64(3), types::VeinDataType::Int64),
                        types::VeinDataType::Boolean,
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
        probe_rows.extend(view_int64_values(batch, 0));
        build_rows.extend(view_nullable_int64_values(batch, 1));
    }
    assert_eq!(probe_rows, vec![1, 2, 3, 4, 5]);
    assert_eq!(build_rows, vec![Some(1), Some(2), None, None, None]);
}

#[tokio::test]
async fn recursive_right_join_does_not_mark_residual_rejections_as_matches() {
    let leaf = || InputPlanFixture::Pipeline {
        source: DataSource::Memory,
        program: Arc::new(ExpressionProgram::empty()),
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
                    program: filter_program(binary_expr(
                        BinaryExprOp::Less,
                        column_expr(0, types::VeinDataType::Int64),
                        literal_expr(LogicalLiteral::Int64(3), types::VeinDataType::Int64),
                        types::VeinDataType::Boolean,
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
        .flat_map(|view| view_nullable_int64_values(view, 0))
        .collect::<Vec<_>>();
    let build_rows = result
        .output
        .iter()
        .flat_map(|view| view_int64_values(view, 1))
        .collect::<Vec<_>>();
    assert_eq!(probe_rows, vec![Some(1), Some(2), None, None, None]);
    assert_eq!(build_rows, vec![1, 2, 3, 4, 5]);
}
