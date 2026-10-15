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
                format: FileFormat::Parquet,
                byte_range: None,
                scan_unit_range: None,
                projected: Arc::clone(&schema),
                dictionary_columns: Vec::new(),
                pushdown: None,
                task_id: TaskId::new(),
                catalog_snapshot: None,
            })),
            input_schema: Arc::clone(&schema),
            program: Arc::new(MicroProgram::empty()),
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
        .map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .expect("int64 output")
                .value(0)
        })
        .collect::<Vec<_>>();
    actual.sort_unstable();
    assert_eq!(actual, vec![1, 2, 3]);

    let source_arrays = builder
        .source_arrays
        .lock()
        .expect("test source array mutex")
        .clone();
    for batch in &result.output {
        let output = batch
            .column(0)
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
                format: FileFormat::Parquet,
                byte_range: None,
                scan_unit_range: None,
                projected: Arc::clone(&schema),
                dictionary_columns: Vec::new(),
                pushdown: None,
                task_id: TaskId::new(),
                catalog_snapshot: None,
            })),
            input_schema: Arc::clone(&schema),
            program: Arc::new(MicroProgram::empty()),
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
