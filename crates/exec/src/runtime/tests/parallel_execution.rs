use super::*;

#[tokio::test]
async fn parallel_join_probe_combines_worker_local_aggregate_state() {
    let builder = Arc::new(MultiFileFeedBuilder::new());
    let runner = DefaultTaskExecutor::new(builder.clone());
    let input_schema = int64_schema();
    let leaf = |files: &[&str]| {
        plan_node(
            Arc::clone(&input_schema),
            PhysicalPlanOperator::Pipeline {
                source: Arc::new(DataSource::File(FileScanTask {
                    files: files.iter().map(|file| (*file).to_owned()).collect(),
                    format: FileFormat::Parquet,
                    byte_range: None,
                    scan_unit_range: None,
                    projected: Arc::clone(&input_schema),
                    dictionary_columns: Vec::new(),
                    pushdown: None,
                    task_id: TaskId::new(),
            catalog_snapshot: None,
            runtime_statistics: None,
                })),
                input_schema: Arc::clone(&input_schema),
            program: Arc::new(ExpressionProgram::empty()),
            },
        )
    };
    let join_output_schema = schema_for_types(&[
        VeinDataType::Int64,
        VeinDataType::Int64,
    ]);
    let join = plan_node(
        Arc::clone(&join_output_schema),
        PhysicalPlanOperator::Join {
            probe: leaf(&["3.parquet", "1.parquet", "2.parquet", "2.parquet"]),
            build: leaf(&[
                "1.parquet",
                "2.parquet",
                "2.parquet",
                "3.parquet",
            ]),
            plan: JoinPlan {
                join_type: JoinKind::Inner,
                probe_keys: vec![0],
                build_keys: vec![0],
                residual: None,
                outputs: vec![JoinOutput::Probe(0), JoinOutput::Probe(0)],
                probe_types: vec![VeinDataType::Int64],
                build_types: vec![VeinDataType::Int64],
            },
        },
    );
    let aggregate = plan_node(
        schema_for_types(&[VeinDataType::Int64, VeinDataType::Int64]),
        PhysicalPlanOperator::Aggregate {
            input: join,
            plan: AggregatePlan {
                groups: 1,
                estimated_groups: None,
                grouping_sets: None,
                emit_empty_group_on_empty_input: false,
                aggs: vec![AggOpCode::Count],
                aggregate_input_types: vec![VeinDataType::Int64],
                types: vec![VeinDataType::Int64, VeinDataType::Int64],
            },
        },
    );
    let mut task = task();
    task.plan = plan_with_input(&task, aggregate);

    let result = runner
        .execute(&task, CancellationToken::new())
        .await
        .expect("parallel join probe and aggregate should execute");

    assert_eq!(result.row_count, 3);
    assert_eq!(builder.max_parallelism(), 2);
    assert_eq!(builder.peak_sources.load(Ordering::Relaxed), 2);
    let mut counts = std::collections::BTreeMap::new();
    for batch in &result.output {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("aggregate key");
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("aggregate count");
        for row in 0..batch.num_rows() {
            counts.insert(keys.value(row), values.value(row));
        }
    }
    assert_eq!(counts, std::collections::BTreeMap::from([(1, 1), (2, 4), (3, 1)]));
}

#[tokio::test]
async fn multi_file_scan_cancellation_releases_workers_and_channel_senders() {
    let builder = Arc::new(MultiFileFeedBuilder::new());
    let runner = DefaultTaskExecutor::new(builder.clone());
    let mut task = task();
    let schema = int64_schema();
    let input = plan_node(
        Arc::clone(&schema),
        PhysicalPlanOperator::Pipeline {
            source: Arc::new(DataSource::File(FileScanTask {
                files: vec!["1.parquet".into(), "2.parquet".into(), "3.parquet".into()],
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
    );
    task.plan = plan_with_input(&task, input);
    let cancel = CancellationToken::new();
    let cancel_worker = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        cancel_worker.cancel();
    });

    let result = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        runner.execute(&task, cancel),
    )
        .await
        .expect("cancelled scans must release bounded-channel senders");
    assert!(result.is_err());
    assert_eq!(builder.active_sources.load(Ordering::Relaxed), 0);
}
