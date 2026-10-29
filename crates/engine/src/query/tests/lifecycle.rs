use super::*;

#[test]
fn empty_batches_and_repeated_completion_have_distinct_semantics() {
    let mut query = handle(vec![Action::Batch(view(vec![])), Action::Complete(None)]);
    let control = query.control();
    assert_eq!(control.status(), QueryStatus::Prepared);
    futures::executor::block_on(async {
        assert_eq!(query.fetch().await.unwrap().unwrap().view().num_rows(), 0);
        assert_eq!(control.status(), QueryStatus::Running);
        assert!(query.fetch().await.unwrap().is_none());
        assert!(query.fetch().await.unwrap().is_none());
    });
    let progress = control.progress();
    assert_eq!(progress.status, QueryStatus::Finished);
    assert_eq!((progress.rows_delivered, progress.batches_delivered), (0, 1));
    query.close();
    assert_eq!(control.status(), QueryStatus::Finished);
    assert_eq!(control.progress().elapsed, progress.elapsed);
}

#[test]
fn repeated_failure_keeps_the_original_error_object_and_category() {
    let error = VeinError::io("read failed", std::io::Error::other("original I/O cause"));
    let mut query = handle(vec![Action::Fail(error)]);
    let control = query.control();
    let (first, second) = futures::executor::block_on(async {
        (query.fetch().await.err().unwrap(), query.fetch().await.err().unwrap())
    });
    assert!(Arc::ptr_eq(&first, &second));
    assert!(Arc::ptr_eq(&first, &control.failure().unwrap()));
    assert_eq!(first.category(), error::ErrorCategory::Storage);
    assert!(first.causes().any(|cause| cause.to_string().contains("original I/O cause")));
    query.close();
    assert_eq!(control.status(), QueryStatus::Failed);
}

#[test]
fn close_before_fetch_is_idempotent_and_retains_metadata() {
    let mut query = handle(vec![Action::Batch(view(vec![Some(1)]))]);
    let control = query.control();
    query.close();
    query.close();
    assert_eq!(control.status(), QueryStatus::Closed);
    assert_eq!(query.schema().fields[0].name, "v");
    assert!(query.schema().fields[0].nullable);
    assert!(futures::executor::block_on(query.fetch()).unwrap().is_none());
}

#[test]
fn a_second_cursor_is_rejected_without_cancelling_the_owner() {
    let scope = resources(ExecutionResourceLimits::default());
    let mut first = typed(Arc::clone(&scope), schema(), QueryKind::Rows, vec![Action::Complete(None)]);
    let script = Script {
        actions: VecDeque::new(),
        resources: Arc::clone(&scope),
        calls: Arc::new(AtomicUsize::new(0)),
        drops: Arc::new(AtomicUsize::new(0)),
    };
    assert!(QueryHandle::from_execution(scope, schema(), QueryKind::Rows, script).is_err());
    assert!(futures::executor::block_on(first.fetch()).unwrap().is_none());
    assert_eq!(first.control().status(), QueryStatus::Finished);
}

#[test]
fn the_stream_adapter_reports_failure_once_then_ends() {
    use futures::StreamExt;
    let query = handle(vec![Action::Fail(VeinError::invalid_input("bad input"))]);
    let control = query.control();
    futures::executor::block_on(async {
        let mut stream = query.into_stream();
        assert!(stream.next().await.unwrap().is_err());
        assert!(stream.next().await.is_none());
        assert!(stream.next().await.is_none());
    });
    assert_eq!(control.status(), QueryStatus::Failed);
}
