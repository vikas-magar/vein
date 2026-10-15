use super::*;

#[tokio::test]
async fn aggregates_recursive_input_before_forwarding_batches() {
    let mut task = task();
    set_input_plan(
        &mut task,
        InputPlanFixture::Aggregate {
            input: Box::new(InputPlanFixture::Pipeline {
                source: DataSource::Memory,
                program: Arc::new(MicroProgram::empty()),
            }),
            plan: AggregatePlan {
                groups: 0,
                estimated_groups: None,
                grouping_sets: Some(vec![vec![]]),
                emit_empty_group_on_empty_input: true,
                aggs: vec![AggOpCode::Count],
                aggregate_input_types: vec![types::VeinDataType::Int64],
                types: vec![types::VeinDataType::Int64],
            },
        },
    );

    let result = runner()
        .execute(&task, CancellationToken::new())
        .await
        .expect("recursive aggregate should execute");

    assert_eq!(result.row_count, 1);
    let count = result.output[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("count result should be int64");
    assert_eq!(count.value(0), 5);
}

#[tokio::test]
async fn distinct_operator_removes_duplicate_rows_from_recursive_join() {
    let leaf = || InputPlanFixture::Pipeline {
        source: DataSource::Memory,
        program: Arc::new(MicroProgram::empty()),
    };
    let mut task = task();
    set_input_plan(
        &mut task,
        InputPlanFixture::Distinct {
            input: Box::new(InputPlanFixture::Join {
                probe: Box::new(leaf()),
                build: Box::new(leaf()),
                plan: JoinPlan {
                    join_type: JoinKind::Inner,
                    probe_keys: vec![0],
                    build_keys: vec![0],
                    residual: None,
                    outputs: vec![JoinOutput::Probe(0), JoinOutput::Build(0)],
                    probe_types: vec![types::VeinDataType::Int64],
                    build_types: vec![types::VeinDataType::Int64],
                },
            }),
            plan: AggregatePlan {
                groups: 2,
                estimated_groups: None,
                grouping_sets: Some(vec![vec![0, 1]]),
                emit_empty_group_on_empty_input: false,
                aggs: Vec::new(),
                aggregate_input_types: Vec::new(),
                types: vec![types::VeinDataType::Int64, types::VeinDataType::Int64],
            },
        },
    );

    let result = runner_with_values(vec![1, 1, 2])
        .execute(&task, CancellationToken::new())
        .await
        .expect("distinct operator should consume duplicate recursive join rows");

    assert_eq!(result.row_count, 2);
    let left = result.output[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("left distinct key");
    let right = result.output[0]
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("right distinct key");
    let rows: Vec<_> = (0..result.output[0].num_rows())
        .map(|row| (left.value(row), right.value(row)))
        .collect();
    assert_eq!(rows, vec![(1, 1), (2, 2)]);
}

#[tokio::test]
async fn recursive_chained_joins_wait_for_each_build_pipeline() {
    let leaf = || InputPlanFixture::Pipeline {
        source: DataSource::Memory,
        program: Arc::new(MicroProgram::empty()),
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
    let mut task = task();
    set_input_plan(
        &mut task,
        InputPlanFixture::Join {
            probe: Box::new(join()),
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
        },
    );

    let result = runner_with_values(vec![3, 1, 2])
        .execute(&task, CancellationToken::new())
        .await
        .expect("chained joins should complete their build dependencies before probing");

    assert_eq!(result.row_count, 3);
    let values = result.output[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("chained join output");
    assert_eq!(
        (0..values.len())
            .map(|row| values.value(row))
            .collect::<Vec<_>>(),
        vec![3, 1, 2]
    );
}
