use super::*;

#[tokio::test]
async fn recursive_sort_and_limit_run_in_logical_order_over_join_output() {
    let leaf = || InputPlanFixture::Pipeline {
        source: DataSource::Memory,
            program: Arc::new(ExpressionProgram::empty()),
    };
    let join = || InputPlanFixture::Join {
        probe: Box::new(leaf()),
        build: Box::new(leaf()),
        plan: JoinPlan {
            join_type: JoinKind::Inner,
            probe_keys: vec![0],
            build_keys: vec![0],
            residual: None,
            outputs: vec![JoinOutput::Probe(0)],
            probe_types: vec![VeinDataType::Int64],
            build_types: vec![VeinDataType::Int64],
        },
    };
    let sort = |input| InputPlanFixture::Sort {
        input: Box::new(input),
        plan: SortPlan {
            keys: vec![SortKey {
                column: 0,
                ascending: true,
                nulls_first: false,
            }],
        },
        key_program: None,
        output_columns: 1,
    };
    let mut task = task();
    set_input_plan(
        &mut task,
        InputPlanFixture::Limit {
            input: Box::new(sort(join())),
            spec: LimitSpec {
                offset: 1,
                count: 2,
            },
        },
    );

    let result = runner_with_values(vec![4, 1, 5, 2, 3])
        .execute(&task, CancellationToken::new())
        .await
        .expect("recursive sort then limit should execute");

    assert_eq!(result.row_count, 2);
    assert_eq!(
        view_int64_values(&result.output[0], 0),
        vec![2, 3]
    );

    // A limit below the sort selects the first two input rows, then orders
    // only those rows. This must produce a different result from above.
    set_input_plan(
        &mut task,
        sort(InputPlanFixture::Limit {
            input: Box::new(join()),
            spec: LimitSpec {
                offset: 0,
                count: 2,
            },
        }),
    );
    let result = runner_with_values(vec![4, 1, 5, 2, 3])
        .execute(&task, CancellationToken::new())
        .await
        .expect("recursive limit then sort should execute");
    assert_eq!(
        view_int64_values(&result.output[0], 0),
        vec![1, 4]
    );
}

#[tokio::test]
async fn recursive_aggregate_filter_sort_limit_execute_once_in_order() {
    let leaf = || InputPlanFixture::Pipeline {
        source: DataSource::Memory,
                program: Arc::new(ExpressionProgram::empty()),
    };
    let join = InputPlanFixture::Join {
        probe: Box::new(leaf()),
        build: Box::new(leaf()),
        plan: JoinPlan {
            join_type: JoinKind::Inner,
            probe_keys: vec![0],
            build_keys: vec![0],
            residual: None,
            outputs: vec![JoinOutput::Probe(0), JoinOutput::Probe(0)],
            probe_types: vec![VeinDataType::Int64],
            build_types: vec![VeinDataType::Int64],
        },
    };
    let aggregate = InputPlanFixture::Aggregate {
        input: Box::new(join),
        plan: AggregatePlan {
            groups: 1,
            estimated_groups: None,
            grouping_sets: Some(vec![vec![0]]),
            emit_empty_group_on_empty_input: false,
            aggs: vec![AggOpCode::Count],
            aggregate_input_types: vec![VeinDataType::Int64],
            types: vec![VeinDataType::Int64, VeinDataType::Int64],
        },
    };
    let filtered = InputPlanFixture::Transform {
        input: Box::new(aggregate),
        program: filter_program(binary_expr(
            BinaryExprOp::Greater,
            column_expr(1, VeinDataType::Int64),
            literal_expr(LogicalLiteral::Int64(1), VeinDataType::Int64),
            VeinDataType::Boolean,
        )),
    };
    let sorted = InputPlanFixture::Sort {
        input: Box::new(filtered),
        plan: SortPlan {
            keys: vec![SortKey {
                column: 1,
                ascending: false,
                nulls_first: false,
            }],
        },
        key_program: None,
        output_columns: 2,
    };
    let mut task = task();
    set_input_plan(
        &mut task,
        InputPlanFixture::Limit {
            input: Box::new(sorted),
            spec: LimitSpec {
                offset: 0,
                count: 1,
            },
        },
    );

    let result = runner_with_values(vec![3, 1, 1, 2])
        .execute(&task, CancellationToken::new())
        .await
        .expect("recursive aggregate, filter, sort, and limit should execute");

    assert_eq!(result.row_count, 1);
    assert_eq!(view_int64_values(&result.output[0], 0), vec![1]);
    assert_eq!(view_int64_values(&result.output[0], 1), vec![4]);
}
