use super::*;
use engine::query::{QueryHandle, QueryStatus};

async fn drain(handle: &mut QueryHandle) -> usize {
    let mut rows = 0;
    while let Some(batch) = handle.fetch().await.unwrap() { rows += batch.view().num_rows(); }
    rows
}

#[tokio::test]
async fn a_query_handle_outlives_its_engine_and_reports_completion_instead_of_disposal_cancel() {
    let root = join_fixture();
    let engine = test_engine(root.path()).with_adaptive_policy(Arc::new(NoCheckpoints));
    let prepared = engine.prepare(JOIN_QUERY).await.unwrap();
    let statistics = Arc::clone(prepared.statistics_context());
    let mut handle = prepared.start().unwrap();
    let control = handle.control();
    assert_eq!(handle.schema().fields.len(), 3);
    assert_eq!(control.status(), QueryStatus::Prepared);
    drop(engine);
    assert_eq!(drain(&mut handle).await, 6);
    assert_eq!(control.status(), QueryStatus::Finished);
    assert_eq!(control.progress().rows_delivered, 6);
    assert!(handle.fetch().await.unwrap().is_none());
    handle.close();
    assert_eq!(control.status(), QueryStatus::Finished);
    assert!(!statistics.sources().is_empty());
}

#[tokio::test]
async fn cancelling_one_cursor_does_not_cancel_another_query_from_the_same_engine() {
    let root = join_fixture();
    let engine = test_engine(root.path()).with_adaptive_policy(Arc::new(NoCheckpoints));
    let mut first = engine.prepare(JOIN_QUERY).await.unwrap().start().unwrap();
    let mut second = engine.prepare(JOIN_QUERY).await.unwrap().start().unwrap();
    let first_control = first.control();
    let second_control = second.control();
    assert_ne!(first_control.query_id(), second_control.query_id());
    first_control.cancel();
    assert!(first.fetch().await.is_err());
    assert_eq!(drain(&mut second).await, 6);
    assert_eq!(first_control.status(), QueryStatus::Cancelled);
    assert_eq!(second_control.status(), QueryStatus::Finished);
}

#[tokio::test]
async fn write_handles_publish_only_confirmed_commits() {
    let root = join_fixture();
    let engine = test_engine(root.path());
    let mut handle = engine.prepare("FROM a |> SELECT a_key |> WRITE archived").await.unwrap().start().unwrap();
    let control = handle.control();
    assert!(control.write_receipt().is_none());
    assert!(handle.schema().fields.is_empty());
    assert!(handle.fetch().await.unwrap().is_none());
    assert_eq!(control.status(), QueryStatus::Finished);
    assert_eq!(control.write_receipt().unwrap().row_count, 5);
    handle.close();
    assert_eq!(control.status(), QueryStatus::Finished);
}
