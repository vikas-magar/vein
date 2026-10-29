use super::*;

#[tokio::test]
async fn multi_file_scan_uses_bounded_zero_copy_fan_in_on_current_thread_runtime() {
    let builder = Arc::new(MultiFileFeedBuilder::new());
    let runner = DefaultTaskExecutor::new(builder.clone());
    let mut task = task();
    let schema = int64_schema();
    let input = plan_node(
        Arc::clone(&schema),
        PhysicalPlanOperator::Pipeline {
            source: Arc::new(DataSource::File(FileScanTask {
                files: vec!["3.parquet".into(), "1.parquet".into(), "2.parquet".into()],
                storage_metadata: Vec::new(),
                data_file_format: DataFileFormatId::new("parquet"),
                format_options: Vec::new(),
                column_encoding_hints: Vec::new(),
                byte_range: None,
                scan_unit_range: None,
                projected: Arc::clone(&schema),
                pushdown: None,
                task_id: TaskId::new(),
                catalog_snapshot: None,
                runtime_statistics: None,
            })),
            input_schema: Arc::clone(&schema),
            program: Arc::new(ExpressionProgram::empty()),
        },
    );
    task.plan = plan_with_input(&task, input);

    let result = runner
        .execute(&task, CancellationToken::new())
        .await
        .expect("bounded file scan should complete on a current-thread runtime");

    assert_eq!(builder.max_parallelism(), 2);
    assert_eq!(builder.peak_sources.load(Ordering::Relaxed), 2);
    assert_eq!(result.row_count, 3);
    let mut actual = result
        .output
        .iter()
        .flat_map(|view| view_int64_values(view, 0))
        .collect::<Vec<_>>();
    actual.sort_unstable();
    assert_eq!(actual, vec![1, 2, 3]);

    let source_arrays = builder
        .source_arrays
        .lock()
        .expect("test source array mutex")
        .clone();
    for batch in &result.output {
        let (output, _) = batch
            .resolve(0, 0)
            .expect("resolve scan output")
            .expect("scan output is non-null");
        let output = output
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("int64 output");
        assert!(
            source_arrays
                .iter()
                .any(|source| { source.values().as_ptr() == output.values().as_ptr() })
        );
    }
}

#[tokio::test]
async fn intermediate_physical_source_replays_registered_checkpoint_views() {
    let runner = DefaultTaskExecutor::new(Arc::new(MultiFileFeedBuilder::new()));
    let mut task = task();
    let schema = int64_schema();
    let arrow_schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("column_0", arrow::datatypes::DataType::Int64, false),
    ]));
    let batch = arrow::record_batch::RecordBatch::try_new(
        Arc::clone(&arrow_schema),
        vec![Arc::new(arrow::array::Int64Array::from(vec![7, 11]))],
    )
    .expect("valid checkpoint batch");
    let memory = QueryMemoryTracker::new(1 << 20, CancellationToken::new());
    let relation = crate::runtime::checkpoint::IntermediateRelation::new(arrow_schema, memory);
    let mut sink = relation.sink();
    sink.push(crate::pipeline::arrow_adapter::from_record_batch(batch.clone()).unwrap())
        .await.expect("retain checkpoint batch");
    sink.flush().await.expect("seal checkpoint before execution");

    let source_id = common::identifiers::IntermediateSourceId::new();
    let _registration = runner
        .register_intermediate_source(task.query_id, source_id, relation)
        .expect("register source for this query");
    let base = plan_node(
        Arc::clone(&schema),
        PhysicalPlanOperator::Pipeline {
            source: Arc::new(DataSource::Memory),
            input_schema: Arc::clone(&schema),
            program: Arc::new(ExpressionProgram::empty()),
        },
    );
    let boundary = plan_node(
        Arc::clone(&schema),
        PhysicalPlanOperator::Transform {
            input: base,
            program: Arc::new(ExpressionProgram::empty()),
        },
    );
    let downstream = plan_node(
        Arc::clone(&schema),
        PhysicalPlanOperator::Transform {
            input: Arc::clone(&boundary),
            program: Arc::new(ExpressionProgram::empty()),
        },
    );
    let plan = PhysicalPlan {
        root: plan_node(
            Arc::clone(&schema),
            PhysicalPlanOperator::Sink {
                input: downstream,
                sink: Arc::new(DataSink::Client {
                    schema: Arc::clone(&schema),
                }),
            },
        ),
    };
    let fragments = plan
        .split_at_checkpoint(&boundary, source_id)
        .expect("split plan at the selected adaptive checkpoint");
    task.plan = fragments.continuation;

    let result = runner
        .execute(&task, CancellationToken::new())
        .await
        .expect("intermediate source should run as a normal pipeline input");

    assert_eq!(result.row_count, 2);
    let output_values = result
        .output
        .iter()
        .flat_map(|output| {
            (0..output.num_rows())
                .map(|row| {
                    let (array, physical_row) = output
                        .resolve(0, row)
                        .expect("resolve reference-backed output row")
                        .expect("non-null output row");
                    array
                        .as_any()
                        .downcast_ref::<arrow::array::Int64Array>()
                        .expect("int64 result")
                        .value(physical_row)
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(output_values, vec![7, 11]);
    assert!(
        result.output.iter().any(|output| {
            let (array, _) = output.resolve(0, 0).unwrap().unwrap();
            Arc::ptr_eq(array, batch.column(0))
        }),
        "checkpoint replay and view operators should retain the original Arrow array"
    );
}

#[tokio::test]
async fn multi_file_worker_failure_is_returned_without_waiting_on_blocked_senders() {
    let builder = Arc::new(MultiFileFeedBuilder::new());
    let runner = DefaultTaskExecutor::new(builder);
    let mut task = task();
    let schema = int64_schema();
    let input = plan_node(
        Arc::clone(&schema),
        PhysicalPlanOperator::Pipeline {
            source: Arc::new(DataSource::File(FileScanTask {
                files: vec![
                    "1.parquet".into(),
                    "invalid.parquet".into(),
                    "2.parquet".into(),
                ],
                storage_metadata: Vec::new(),
                data_file_format: DataFileFormatId::new("parquet"),
                format_options: Vec::new(),
                column_encoding_hints: Vec::new(),
                byte_range: None,
                scan_unit_range: None,
                projected: Arc::clone(&schema),
                pushdown: None,
                task_id: TaskId::new(),
                catalog_snapshot: None,
                runtime_statistics: None,
            })),
            input_schema: Arc::clone(&schema),
            program: Arc::new(ExpressionProgram::empty()),
        },
    );
    task.plan = plan_with_input(&task, input);

    let result = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        runner.execute(&task, CancellationToken::new()),
    )
    .await
    .expect("failed scan workers must not leave peers blocked")
    .expect_err("an invalid source work item should fail the task");
    assert!(result.to_string().contains("invalid value"));
}
