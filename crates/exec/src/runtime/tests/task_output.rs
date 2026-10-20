use super::*;

#[tokio::test]
async fn filters_aggregate_output_in_the_post_aggregate_expression_pipeline() {
    let task = aggregate_task_with_post_filter(4);
    let result = runner()
        .execute(&task, CancellationToken::new())
        .await
        .expect("post-aggregate filter should execute");

    assert_eq!(result.row_count, 1);
    assert_eq!(view_int64_values(&result.output[0], 0), vec![5]);
}

#[tokio::test]
async fn post_aggregate_filter_can_remove_every_group() {
    let task = aggregate_task_with_post_filter(5);
    let result = runner()
        .execute(&task, CancellationToken::new())
        .await
        .expect("post-aggregate filter should execute");

    assert_eq!(result.row_count, 0);
    assert!(result.output.is_empty());
}

#[tokio::test]
async fn sorts_task_output_before_applying_limit() {
    let mut task = task();
    let schema = int64_schema();
    let input = plan_node(
        Arc::clone(&schema),
        PhysicalPlanOperator::Pipeline {
            source: Arc::new(DataSource::Memory),
            input_schema: Arc::clone(&schema),
            program: projection_program(vec![column_expr(0, types::VeinDataType::Int64)]),
        },
    );
    let sorted = plan_node(
        Arc::clone(&schema),
        PhysicalPlanOperator::Sort {
            input,
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
    let limited = plan_node(
        Arc::clone(&schema),
        PhysicalPlanOperator::Limit {
            input: sorted,
            spec: LimitSpec {
                offset: 1,
                count: 2,
            },
        },
    );
    task.plan = plan_with_input(&task, limited);

    let result = runner_with_values(vec![4, 1, 3, 2])
        .execute(&task, CancellationToken::new())
        .await
        .expect("sort before limit");

    assert_eq!(result.row_count, 2);
    assert_eq!(view_int64_values(&result.output[0], 0), vec![3, 2]);
}

#[tokio::test]
async fn runs_memory_filter_to_client() {
    let result = runner()
        .execute(&task(), CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(result.row_count, 3);
    assert_eq!(result.output.len(), 1);
    assert_eq!(view_int64_values(&result.output[0], 0), vec![3, 4, 5]);
    assert!(result.metrics.duration_ns > 0);
}

#[tokio::test]
async fn runs_empty_global_count_as_one_zero_row() {
    let mut task = task();
    let schema = int64_schema();
    let input = plan_node(
        Arc::clone(&schema),
        PhysicalPlanOperator::Pipeline {
            source: Arc::new(DataSource::Memory),
            input_schema: Arc::clone(&schema),
            program: projection_program(vec![column_expr(0, types::VeinDataType::Int64)]),
        },
    );
    let aggregate = plan_node(
        Arc::clone(&schema),
        PhysicalPlanOperator::Aggregate {
            input,
            plan: AggregatePlan {
                groups: 0,
                estimated_groups: None,
                grouping_sets: None,
                emit_empty_group_on_empty_input: true,
                aggs: vec![common::protocol::AggOpCode::Count],
                aggregate_input_types: vec![types::VeinDataType::Int64],
                types: vec![types::VeinDataType::Int64],
            },
        },
    );
    task.plan = plan_with_input(&task, aggregate);

    let result = runner_with_values(Vec::new())
        .execute(&task, CancellationToken::new())
        .await
        .expect("empty global aggregate should execute");

    assert_eq!(result.row_count, 1);
    assert_eq!(result.output.len(), 1);
    assert_eq!(view_int64_values(&result.output[0], 0), vec![0]);
}

#[tokio::test]
async fn streams_client_batches_without_retaining_output() {
    let (sender, receiver) = flume::bounded(1);
    let result = runner()
        .execute_streaming(&task(), CancellationToken::new(), sender)
        .await
        .unwrap();
    let batch = receiver.recv_async().await.unwrap();
    assert_eq!(batch.num_rows(), 3);
    assert!(receiver.is_empty());
    assert_eq!(result.row_count, 3);
    assert!(result.output.is_empty());
}
