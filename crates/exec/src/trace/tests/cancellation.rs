use super::*;
#[tokio::test]
async fn dropped_node_execution_guard_records_cancel_or_drop_outcome_and_elapsed_time() {
    let root = source();
    let trace = ExecutionTrace::new(&root, "query".into(), "task".into());
    let node_id = trace.node_id(&root);
    let guard = trace.execution_guard(node_id);
    std::thread::yield_now();
    drop(guard);

    let metrics = &trace.nodes[node_id as usize];
    assert!(metrics.statistics.elapsed_ns() > 0);
    assert_eq!(metrics.execution_dropped.value(), 1);
}

#[tokio::test]
async fn canceled_node_execution_is_distinguished_from_failed_execution() {
    let root = source();
    let trace = ExecutionTrace::new(&root, "query".into(), "task".into());
    let node_id = trace.node_id(&root);
    let mut guard = trace.execution_guard(node_id);
    guard.finish(&Err(error::VeinError::cancelled()));
    drop(guard);

    let metrics = &trace.nodes[node_id as usize];
    assert_eq!(metrics.execution_cancelled.value(), 1);
}
