use super::*;

#[tokio::test]
async fn client_result_stays_reference_backed_through_join_sort_limit() {
    use arrow::array::Int64Array;

    let builder = Arc::new(MultiFileFeedBuilder::new());
    let runner = DefaultTaskExecutor::new(builder.clone());
    let schema = int64_schema();
    let leaf = || {
        plan_node(
            Arc::clone(&schema),
            PhysicalPlanOperator::Pipeline {
                source: Arc::new(DataSource::File(FileScanTask {
                    files: vec!["3.parquet".into(), "1.parquet".into(), "2.parquet".into()],
                    format: FileFormat::Parquet,
                    byte_range: None,
                    scan_unit_range: None,
                    projected: Arc::clone(&schema),
                    dictionary_columns: Vec::new(),
                    pushdown: None,
                    task_id: TaskId::new(),
            catalog_snapshot: None,
            runtime_statistics: None,
                })),
                input_schema: Arc::clone(&schema),
            program: Arc::new(ExpressionProgram::empty()),
            },
        )
    };
    let join = plan_node(
        Arc::clone(&schema),
        PhysicalPlanOperator::Join {
            probe: leaf(),
            build: leaf(),
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
    let sort = plan_node(
        Arc::clone(&schema),
        PhysicalPlanOperator::Sort {
            input: join,
            plan: SortPlan {
                keys: vec![SortKey {
                    column: 0,
                    ascending: true,
                    nulls_first: false,
                }],
            },
            key_program: None,
            output_columns: 1,
        },
    );
    let input = plan_node(
        schema,
        PhysicalPlanOperator::Limit {
            input: sort,
            spec: LimitSpec {
                offset: 1,
                count: 2,
            },
        },
    );
    let mut task = task();
    task.plan = plan_with_input(&task, input);
    let result = runner
        .execute(&task, CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(result.row_count, 2);
    let mut actual = Vec::new();
    {
        let source_arrays = builder.source_arrays.lock().unwrap();
        for view in &result.output {
            for row in 0..view.num_rows() {
                let (array, source_row) = view.resolve(0, row).unwrap().unwrap();
                let values = array.as_any().downcast_ref::<Int64Array>().unwrap();
                assert!(
                    source_arrays
                        .iter()
                        .any(|source| std::ptr::eq(source.as_ref(), values))
                );
                actual.push(values.value(source_row));
            }
        }
    }
    assert_eq!(actual, vec![2, 3]);
}

#[tokio::test]
async fn view_results_stream_without_retention_and_propagate_cancellation_and_channel_errors() {
    let (sender, receiver) = flume::bounded(1);
    let result = runner()
        .execute_streaming(&task(), CancellationToken::new(), sender)
        .await
        .unwrap();
    let batch = receiver.recv().unwrap();
    assert_eq!(batch.num_rows(), 3);
    assert_eq!(result.row_count, 3);
    assert!(result.output.is_empty());
    assert!(receiver.is_empty());
    let (sender, receiver) = flume::bounded(1);
    drop(receiver);
    let error = runner()
        .execute_streaming(&task(), CancellationToken::new(), sender)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("client result channel closed"));
    let cancel = CancellationToken::new();
    cancel.cancel();
    assert!(matches!(
            runner().execute(&task(), cancel).await,
            Err(VeinError::Cancelled)
        ));
}
