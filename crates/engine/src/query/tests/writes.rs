use super::*;
use exec::operators::sink::WriteReceipt;

#[test]
fn write_success_requires_a_confirmed_commit_receipt() {
    for receipt in [None, Some(WriteReceipt { destination: "target".into(), row_count: 7 })] {
        let confirmed = receipt.is_some();
        let mut query = typed(resources(ExecutionResourceLimits::default()),
                              Arc::new(VeinSchema::empty()), QueryKind::Write, vec![Action::Complete(receipt)]);
        let control = query.control();
        assert!(control.write_receipt().is_none());
        let result = futures::executor::block_on(query.fetch());
        if confirmed {
            assert!(result.unwrap().is_none());
            assert_eq!(control.status(), QueryStatus::Finished);
            assert_eq!(control.write_receipt().unwrap().row_count, 7);
        } else {
            assert!(result.is_err());
            assert_eq!(control.status(), QueryStatus::Failed);
            assert!(control.write_receipt().is_none());
        }
    }
}

#[test]
fn incompatible_result_events_cannot_report_success() {
    let mut write = typed(resources(ExecutionResourceLimits::default()), Arc::new(VeinSchema::empty()),
                          QueryKind::Write, vec![Action::Batch(view(vec![Some(1)]))]);
    assert!(futures::executor::block_on(write.fetch()).is_err());
    let mut rows = handle(vec![Action::Complete(Some(WriteReceipt { destination: "x".into(), row_count: 1 }))]);
    assert!(futures::executor::block_on(rows.fetch()).is_err());
    assert_eq!(rows.control().status(), QueryStatus::Failed);
}
