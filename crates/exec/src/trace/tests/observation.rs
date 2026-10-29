use super::*;
#[tokio::test]
async fn output_observer_counts_batches_and_rows_without_materializing_the_view() {
    let source = source();
    let trace = ExecutionTrace::new(&source, "q".into(), "t".into());
    let node_id = trace.node_id(&source);
    let values: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30]));
    let view = crate::pipeline::test_support::dense_view(vec![values]);
    let (array, _) = view.resolve(0, 0).expect("resolve source array").unwrap();
    let original = Arc::clone(array);
    let mut observed = trace.observer(
        node_id,
        Box::new(Discard { expected: original }),
        "q".into(),
        "t".into(),
    );

    observed.push(view).await.expect("observe output batch");

    let metrics = &trace.nodes[node_id as usize];
    assert_eq!(metrics.statistics.output_batches(), 1);
    assert_eq!(metrics.statistics.output_rows(), 3);
    trace.record_source_bytes(node_id, 128);
    assert_eq!(metrics.statistics.source_bytes_read(), 128);
    assert_eq!(
        trace.statistics_context.operator_statistics()[0].output_rows,
        3
    );
}

#[tokio::test]
async fn trace_retains_the_query_resource_tracker_for_summary_emission() {
    let source = source();
    let mut trace = ExecutionTrace::new(&source, "q".into(), "t".into());
    let tracker = QueryMemoryTracker::unlimited(common::cancellation::CancellationToken::new());
    trace.attach_memory_tracker(tracker.clone());

    let attached = trace.memory.as_ref().expect("query tracker attached");
    assert_eq!(attached.identity(), tracker.identity());
    assert_eq!(attached.limit(), usize::MAX);
    assert_eq!(attached.used(), 0);
    assert_eq!(attached.peak(), 0);
    assert_eq!(attached.spill_bytes_used(), 0);
}

#[tokio::test]
async fn node_trace_attributes_time_memory_and_spill() {
    let root = source();
    let trace = ExecutionTrace::new(&root, "query".into(), "task".into());
    let node_id = trace.node_id(&root);
    let metrics = &trace.nodes[node_id as usize];
    let query_memory =
        QueryMemoryTracker::unlimited(common::cancellation::CancellationToken::new());
    let operator_memory = trace.operator_memory(node_id, &query_memory);

    let reservation = operator_memory
        .reserve(64, "trace test state")
        .expect("reserve operator state");
    assert_eq!(
        query_memory.used(),
        64,
        "query-wide limit still accounts scoped reservations"
    );
    assert_eq!(metrics.resources.memory_used(), 64);
    assert_eq!(metrics.resources.memory_peak(), 64);

    let manager = operator_memory.spill_manager();
    let mut pending = manager.create_run().expect("create scoped spill run");
    let mut writer = pending.writer().expect("open scoped spill writer");
    writer
        .write_all(b"spill")
        .expect("write scoped spill bytes");
    let run = pending.commit(writer).expect("commit scoped spill run");
    assert_eq!(metrics.resources.spill_used(), 5);
    assert_eq!(metrics.resources.spill_peak(), 5);
    assert_eq!(metrics.resources.spill_written(), 5);

    let mut guard = trace.execution_guard(node_id);
    // An immediately dropped timer may share the same clock tick. Give the
    // guard a known start interval without sleeping or changing runtime time.
    guard.started = Instant::now()
        .checked_sub(std::time::Duration::from_millis(1))
        .expect("clock supports the test interval");
    guard.finish(&Ok(1));
    drop(guard);
    assert!(metrics.statistics.elapsed_ns() > 0);
    assert_eq!(metrics.execution_completed.value(), 1);
    let snapshot = trace.statistics_context.operator_statistics();
    assert_eq!(snapshot[0].metrics["execution_completed"], 1);

    drop(run);
    drop(reservation);
    assert_eq!(metrics.resources.spill_used(), 0);
    assert_eq!(
        metrics.resources.spill_written(),
        5,
        "cumulative writes survive run cleanup"
    );
    assert_eq!(metrics.resources.memory_used(), 0);
    assert_eq!(query_memory.used(), 0);
}
