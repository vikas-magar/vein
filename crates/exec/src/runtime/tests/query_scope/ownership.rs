use super::*;

#[test]
fn scope_retains_identity_statistics_and_one_memory_spill_cancellation_root() {
    let (_, task, resources) = scoped_task();
    assert_eq!(resources.query_id(), task.query_id);
    assert_eq!(resources.request_id(), task.request_id);
    assert!(Arc::ptr_eq(resources.statistics_context(), task.statistics_context.as_ref().unwrap()));
    let worker = Arc::clone(&resources);
    let guard = resources.guard();
    let memory = resources.memory_tracker();
    let reservation = memory.reserve(32, "scope ownership").unwrap();
    assert_eq!(resources.memory_used_bytes(), 32);
    drop(guard);
    assert!(worker.cancellation().is_cancelled());
    assert!(matches!(memory.reserve(1, "cancelled scope"), Err(VeinError::Cancelled)));
    assert!(matches!(memory.spill_manager().create_run(), Err(VeinError::Cancelled)));
    drop(reservation);
    assert_eq!(resources.memory_used_bytes(), 0);
}

#[tokio::test]
async fn cross_query_identity_and_foreign_statistics_are_rejected_before_execution() {
    let (executor, task, resources) = scoped_task();
    for mismatch in 0..3 {
        let mut other = task.clone();
        match mismatch {
            0 => other.query_id = QueryId::new(),
            1 => other.request_id = RequestId::new(),
            _ => other.statistics_context = Some(Arc::new(QueryStatisticsContext::new([]))),
        }
        assert!(executor.execute_with_resources(&other, Arc::clone(&resources)).await.is_err());
    }
    assert!(!resources.cancellation().is_cancelled());
    assert_eq!(executor.execute_with_resources(&task, resources).await.unwrap().row_count, 3);
}

#[test]
fn closing_execution_releases_spill_runs_after_the_last_owner() {
    use std::io::Write;
    let (_, _, resources) = scoped_task();
    let guard = resources.guard();
    let manager = resources.memory_tracker().spill_manager();
    let mut pending = manager.create_run().unwrap();
    let mut output = pending.writer().unwrap();
    output.write_all(b"scope-owned spill").unwrap();
    let run = pending.commit(output).unwrap();
    let retained = resources.retain_until_cancel(run).unwrap();
    assert!(resources.spill_used_bytes() > 0);
    drop(guard);
    assert!(resources.cancellation().is_cancelled());
    assert_eq!(resources.spill_used_bytes(), 0);
    drop(retained);
    let directory = manager.local_directory().unwrap();
    drop(manager);
    drop(resources);
    assert!(!directory.exists());
}

#[tokio::test]
async fn cancellation_unregisters_checkpoints_before_the_stream_owner_is_dropped() {
    use common::identifiers::IntermediateSourceId;
    let (executor, task, resources) = scoped_task();
    let checkpoint = executor.execute_to_checkpoint_with_resources(&task, Arc::clone(&resources)).await.unwrap();
    let source = IntermediateSourceId::new();
    let registration = executor.register_intermediate_source(task.query_id, source, checkpoint.relation).unwrap();
    let cleanup = resources.retain_until_cancel(registration).unwrap();
    assert!(resources.memory_used_bytes() > 0);
    assert!(executor.intermediate_sources.source(task.query_id, source).is_ok());
    resources.cancel();
    assert!(executor.intermediate_sources.source(task.query_id, source).is_err());
    assert_eq!(resources.memory_used_bytes(), 0);
    drop(cleanup);
}
