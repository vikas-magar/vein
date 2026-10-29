use super::*;
use arrow::array::{Array, ArrayRef, Int64Array};

#[test]
fn fetched_payloads_and_leases_survive_close_and_release_independently() {
    let array: ArrayRef = Arc::new(Int64Array::from(vec![Some(5), None]));
    let input = exec::pipeline::DataChunkView::from_arrays(vec![Arc::clone(&array)], 2,
                                                           common::arrow::SelectionVector::new_dense(2)).unwrap();
    let scope = resources(ExecutionResourceLimits::default());
    let mut query = typed(Arc::clone(&scope), schema(), QueryKind::Rows,
                          vec![Action::Batch(input), Action::Complete(None)]);
    let control = query.control();
    let fetched = futures::executor::block_on(query.fetch()).unwrap().unwrap();
    assert_eq!(control.progress().rows_delivered, 2);
    query.close();
    drop(query);
    assert_eq!(control.status(), QueryStatus::Closed);
    assert!(scope.memory_used_bytes() > 0);
    let output = fetched.view().materialize_column(0).unwrap();
    assert_eq!(output.to_data().buffers()[0].as_ptr(), array.to_data().buffers()[0].as_ptr());
    assert!(output.is_null(1));
    drop(output);
    drop(fetched);
    assert_eq!(scope.memory_used_bytes(), 0);
}

#[test]
fn schema_drift_fails_before_delivering_a_batch() {
    let input = exec::pipeline::DataChunkView::from_arrays(
        vec![Arc::new(arrow::array::StringArray::from(vec!["wrong"]))], 1,
        common::arrow::SelectionVector::new_dense(1)).unwrap();
    let mut query = handle(vec![Action::Batch(input)]);
    let error = futures::executor::block_on(query.fetch()).err().unwrap();
    assert!(!error.to_string().is_empty());
    assert_eq!(query.control().status(), QueryStatus::Failed);
    assert_eq!(query.control().progress().rows_delivered, 0);
}

#[test]
fn output_accounting_failure_is_terminal_and_preserves_its_category() {
    let scope = resources(ExecutionResourceLimits::new(1, u64::MAX));
    let mut query = typed(scope, schema(), QueryKind::Rows, vec![Action::Batch(view(vec![Some(9)]))]);
    let error = futures::executor::block_on(query.fetch()).err().unwrap();
    assert_eq!(error.category(), error::ErrorCategory::Resource);
    assert_eq!(query.control().status(), QueryStatus::Failed);
}

#[test]
fn decimal_metadata_is_stable_before_fetch_and_after_close() {
    let mut column = ResolvedColumn::new(ColumnId(1), "amount", None, VeinDataType::Decimal128(18, 4));
    column.nullable = true;
    let schema = Arc::new(VeinSchema::new(vec![Arc::new(column)]));
    let mut query = typed(resources(ExecutionResourceLimits::default()), Arc::clone(&schema),
                          QueryKind::Rows, vec![Action::Complete(None)]);
    assert!(Arc::ptr_eq(query.schema(), &schema));
    query.close();
    assert_eq!(query.schema().fields[0].data_type, VeinDataType::Decimal128(18, 4));
    assert!(query.schema().fields[0].nullable);
}
