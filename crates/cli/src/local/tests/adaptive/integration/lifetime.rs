use super::*;
use futures::StreamExt;

#[tokio::test]
async fn dropping_a_prepared_query_cancels_its_scope_before_any_execution() {
    let root = join_fixture();
    let engine = test_engine(root.path()).with_adaptive_policy(Arc::new(NoCheckpoints));
    let query = engine.prepare(JOIN_QUERY).await.unwrap();
    let resources = Arc::clone(query.execution_resources());
    assert_eq!(resources.request_id(), query.request_id());
    assert_eq!(resources.query_id(), query.query_id());
    assert!(Arc::ptr_eq(resources.statistics_context(), query.statistics_context()));
    assert!(!resources.cancellation().is_cancelled());
    drop(query);
    assert!(resources.cancellation().is_cancelled());
    assert_eq!(resources.memory_used_bytes(), 0);
}

#[tokio::test]
async fn dropping_a_nonadaptive_stream_cancels_the_same_scope_and_preserves_fetched_rows() {
    let root = join_fixture();
    let engine = test_engine(root.path()).with_adaptive_policy(Arc::new(NoCheckpoints));
    let query = engine.prepare(JOIN_QUERY).await.unwrap();
    assert!(!query.has_adaptive_checkpoint());
    let resources = Arc::clone(query.execution_resources());
    let mut stream = query.into_stream();
    let batch = stream.next().await.unwrap().unwrap();
    let rows = batch.num_rows();
    assert!(rows > 0);
    assert!(!resources.cancellation().is_cancelled());
    drop(stream);
    assert!(resources.cancellation().is_cancelled());
    assert_eq!(batch.num_rows(), rows);
    assert!(batch.materialize_column(0).is_ok());
}

#[tokio::test]
async fn cancellation_before_first_poll_is_not_replaced_by_a_fresh_task_token() {
    let root = join_fixture();
    let engine = test_engine(root.path()).with_adaptive_policy(Arc::new(NoCheckpoints));
    let query = engine.prepare(JOIN_QUERY).await.unwrap();
    let resources = Arc::clone(query.execution_resources());
    resources.cancel();
    let mut stream = query.into_stream();
    assert!(matches!(stream.next().await.unwrap(), Err(common::VeinError::Cancelled)));
    assert!(stream.next().await.is_none());
    assert_eq!(resources.memory_used_bytes(), 0);
}
