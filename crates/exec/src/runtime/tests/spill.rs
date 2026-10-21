use super::*;

#[tokio::test]
async fn full_join_spill_matches_in_memory_result_under_configured_budgets() {
    let task = full_join_spill_task();
    let spill_factory_calls = Arc::new(AtomicUsize::new(0));
    let in_memory = DefaultTaskExecutor::new(full_join_feed())
        .execute(&task, CancellationToken::new())
        .await
        .expect("in-memory full join");
    let spilling = DefaultTaskExecutor::with_resource_limits(
        full_join_feed(),
        ExecutionResourceLimits::new(48_000, 1_000_000),
    )
        .with_spill_storage_factory(Arc::new(CountingSpillStorageFactory(Arc::clone(
            &spill_factory_calls,
        ))))
        .execute(&task, CancellationToken::new())
        .await
        .expect("full join spills within configured disk quota");

    assert_eq!(spill_factory_calls.load(Ordering::Relaxed), 1);
    assert_eq!(in_memory.row_count, 1_029);
    assert_eq!(spilling.row_count, in_memory.row_count);
    assert_eq!(
        join_result_pairs(&spilling.output),
        join_result_pairs(&in_memory.output),
        "spilled output preserves duplicate matches and null extensions"
    );
}

#[tokio::test]
async fn full_join_spill_respects_query_disk_quota() {
    let task = full_join_spill_task();
    let factory_calls = Arc::new(AtomicUsize::new(0));
    let executor = DefaultTaskExecutor::with_resource_limits(
        full_join_feed(),
        ExecutionResourceLimits::new(40_000, 0),
    )
        .with_spill_storage_factory(Arc::new(CountingSpillStorageFactory(Arc::clone(
            &factory_calls,
        ))));

    let error = executor
        .execute(&task, CancellationToken::new())
        .await
        .expect_err("zero spill quota rejects a build-side run");

    assert_eq!(error.category(), error::ErrorCategory::Resource);
    assert!(
        matches!(error, VeinError::ResourceExhausted { .. }),
        "expected spill-quota exhaustion, received {error:?}"
    );
    assert_eq!(factory_calls.load(Ordering::Relaxed), 1);
}
