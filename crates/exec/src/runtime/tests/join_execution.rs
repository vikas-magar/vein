use super::*;

#[tokio::test]
async fn cancellation_aborts() {
    let mut task = task();
    let input = InputPlanFixture::Pipeline {
        source: DataSource::Memory,
            program: Arc::new(ExpressionProgram::empty()),
    };
    set_input_plan(&mut task, input);
    let cancel = CancellationToken::new();
    cancel.cancel();
    let err = runner().execute(&task, cancel).await.unwrap_err();
    assert!(matches!(err, VeinError::Cancelled));
}

#[tokio::test]
async fn runs_a_join_task() {
    let mut task = task();
    set_input_plan(
        &mut task,
        InputPlanFixture::Sort {
            input: Box::new(InputPlanFixture::Join {
                probe: Box::new(InputPlanFixture::Pipeline {
                    source: DataSource::Memory,
                    program: Arc::new(ExpressionProgram::empty()),
                }),
                build: Box::new(InputPlanFixture::Pipeline {
                    source: DataSource::Memory,
                    program: Arc::new(ExpressionProgram::empty()),
                }),
                plan: JoinPlan {
                    join_type: JoinKind::Inner,
                    probe_keys: vec![0],
                    build_keys: vec![0],
                    residual: None,
                    outputs: vec![JoinOutput::Probe(0)],
                    probe_types: vec![types::VeinDataType::Int64],
                    build_types: vec![types::VeinDataType::Int64],
                },
            }),
            plan: SortPlan {
                keys: vec![common::protocol::SortKey {
                    column: 0,
                    ascending: false,
                    nulls_first: false,
                }],
            },
            key_program: None,
            output_columns: 1,
        },
    );

    let result = runner()
        .execute(&task, CancellationToken::new())
        .await
        .expect("join task");

    assert_eq!(result.row_count, 5);
    let rows: Vec<i64> = result
        .output
        .iter()
        .flat_map(|batch| {
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .expect("int64 output")
                .clone();
            (0..column.len())
                .map(|row| column.value(row))
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(rows, vec![5, 4, 3, 2, 1]);
}

#[tokio::test]
async fn streams_chained_joins_between_hash_join_nodes() {
    let leaf = || InputPlanFixture::Pipeline {
        source: DataSource::Memory,
        program: Arc::new(ExpressionProgram::empty()),
    };
    let join_plan = || JoinPlan {
        join_type: JoinKind::Inner,
        probe_keys: vec![0],
        build_keys: vec![0],
        residual: None,
        outputs: vec![JoinOutput::Probe(0), JoinOutput::Build(0)],
        probe_types: vec![types::VeinDataType::Int64],
        build_types: vec![types::VeinDataType::Int64],
    };
    let projected_inner = InputPlanFixture::Transform {
        input: Box::new(InputPlanFixture::Join {
            probe: Box::new(leaf()),
            build: Box::new(leaf()),
            plan: join_plan(),
        }),
        program: projection_program(vec![column_expr(0, types::VeinDataType::Int64)]),
    };
    let input = InputPlanFixture::Join {
        probe: Box::new(projected_inner),
        build: Box::new(leaf()),
        plan: join_plan(),
    };
    let mut task = task();
    set_input_plan(&mut task, input);

    let result = runner()
        .execute(&task, CancellationToken::new())
        .await
        .expect("chained hash joins should execute");

    assert_eq!(result.row_count, 5);
}
