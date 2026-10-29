use super::*;
use arrow::array::Int64Array;

#[test]
fn native_row_browser_batch_and_custom_consumers_use_the_same_contract() {
    for client in 0..3 {
        let mut query = handle(vec![Action::Batch(view(vec![Some(7), None, Some(7)])), Action::Complete(None)]);
        let mut observed = Vec::new();
        futures::executor::block_on(async {
            while let Some(batch) = query.fetch().await.unwrap() {
                match client {
                    0 => { // A lending row-cursor pattern suitable for ODBC/JDBC adapters.
                        for row in 0..batch.view().num_rows() {
                            let (array, index) = batch.view().resolve(0, row).unwrap().unwrap();
                            let values = array.as_any().downcast_ref::<Int64Array>().unwrap();
                            observed.push(arrow::array::Array::is_valid(values, index).then(|| values.value(index)));
                        }
                    }
                    _ => { // Batch/transport adapters borrow the same canonical view.
                        let array = batch.view().materialize_column(0).unwrap();
                        observed.extend(array.as_any().downcast_ref::<Int64Array>().unwrap().iter());
                    }
                }
            }
        });
        assert_eq!(observed, vec![Some(7), None, Some(7)]);
        assert_eq!(query.control().status(), QueryStatus::Finished);
    }
}

#[test]
fn independent_queries_and_type_erased_extensions_do_not_share_state() {
    let mut first = handle(vec![Action::Complete(None)]);
    let scope = resources(ExecutionResourceLimits::default());
    let script = Script {
        actions: vec![Action::Batch(view(vec![Some(1)])), Action::Complete(None)].into(),
        resources: Arc::clone(&scope),
        calls: Arc::new(AtomicUsize::new(0)),
        drops: Arc::new(AtomicUsize::new(0)),
    };
    let execution: Pin<Box<dyn QueryExecution>> = Box::pin(script);
    let mut second = QueryHandle::from_pinned(scope, schema(), QueryKind::Rows, execution).unwrap();
    assert_ne!(first.control().query_id(), second.control().query_id());
    first.control().cancel();
    futures::executor::block_on(async {
        assert!(first.fetch().await.is_err());
        assert_eq!(second.fetch().await.unwrap().unwrap().view().num_rows(), 1);
        assert!(second.fetch().await.unwrap().is_none());
    });
}

#[test]
fn local_backends_need_not_be_send_but_control_handles_remain_shareable() {
    struct Local(std::rc::Rc<std::cell::Cell<usize>>);
    impl QueryExecution for Local {
        fn poll_event(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<QueryEvent>> {
            self.0.set(self.0.get() + 1);
            Poll::Ready(Ok(QueryEvent::Complete { write_receipt: None }))
        }
    }
    let calls = std::rc::Rc::new(std::cell::Cell::new(0));
    let mut query = QueryHandle::from_execution(resources(ExecutionResourceLimits::default()),
                                                schema(), QueryKind::Rows, Local(std::rc::Rc::clone(&calls))).unwrap();
    let control = query.control();
    std::thread::spawn(move || assert_eq!(control.status(), QueryStatus::Prepared)).join().unwrap();
    assert!(futures::executor::block_on(query.fetch()).unwrap().is_none());
    assert_eq!(calls.get(), 1);
}
