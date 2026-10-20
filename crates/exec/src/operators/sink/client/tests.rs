use super::*;
use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use common::arrow::SelectionVector;
use std::sync::Arc;

fn schema(nullable: bool) -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, nullable)]))
}

fn selected(source: &ArrayRef, rows: Vec<u32>) -> DataChunkView {
    crate::pipeline::test_support::selected_view(
        vec![Arc::clone(source)],
        SelectionVector::new_sparse(rows),
    )
}

#[test]
fn collector_attaches_schema_and_keeps_source_arrays() {
    let source: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30]));
    let schema = schema(false);
    let mut sink = CollectSink::new(Arc::clone(&schema));
    sink.push(selected(&source, vec![2, 0])).unwrap();

    let output = sink.collect();
    assert_eq!(output.len(), 1);
    assert!(Arc::ptr_eq(output[0].schema().unwrap(), &schema));
    assert!(Arc::ptr_eq(
        output[0].resolve(0, 0).unwrap().unwrap().0,
        &source
    ));
    assert_eq!(output[0].num_rows(), 2);
}

#[test]
fn collector_validates_only_selected_rows_against_nullability() {
    let source: ArrayRef = Arc::new(Int64Array::from(vec![None, Some(20)]));
    let mut sink = CollectSink::new(schema(false));
    sink.push(selected(&source, vec![1])).unwrap();
    assert!(sink.push(selected(&source, vec![0])).is_err());
    assert_eq!(sink.row_count(), 1);
    assert_eq!(sink.collect().len(), 1);
}

#[test]
fn collector_rejects_schema_drift_and_preserves_zero_column_row_count() {
    let source: ArrayRef = Arc::new(Int64Array::from(vec![1]));
    let wrong_schema = Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, true)]));
    let mut invalid = CollectSink::new(wrong_schema);
    assert!(invalid.push(selected(&source, vec![0])).is_err());
    assert_eq!(invalid.row_count(), 0);

    let mut empty = CollectSink::new(Arc::new(Schema::empty()));
    let zero_columns = DataChunkView::from_arrays(
        Vec::new(),
        3,
        SelectionVector::new_dense(3),
    )
    .unwrap();
    empty.push(zero_columns).unwrap();
    assert_eq!(empty.collect()[0].num_rows(), 3);
}

#[test]
fn streaming_sink_emits_views_and_reports_closed_channels() {
    let source: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
    let (sender, receiver) = flume::bounded(1);
    let mut sink = CollectSink::with_sender(schema(false), sender);
    sink.push(selected(&source, vec![2, 0])).unwrap();
    let streamed = receiver.recv().unwrap();
    assert_eq!(streamed.num_rows(), 2);
    assert!(Arc::ptr_eq(
        streamed.resolve(0, 1).unwrap().unwrap().0,
        &source
    ));
    assert!(sink.collect().is_empty());
    drop(receiver);
    assert!(sink.push(selected(&source, vec![1])).is_err());
    assert_eq!(sink.row_count(), 2);
}
