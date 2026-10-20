use super::*;
use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use common::arrow::SelectionVector;
use std::sync::Arc;

#[test]
fn attaching_schema_retains_source_buffers_and_selection() {
    let source: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30]));
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let view = DataChunkView::from_arrays(
        vec![Arc::clone(&source)],
        3,
        SelectionVector::new_sparse(vec![2, 0]),
    )
    .unwrap()
    .with_schema(Arc::clone(&schema))
    .unwrap();

    assert!(Arc::ptr_eq(view.schema().unwrap(), &schema));
    assert_eq!(view.num_rows(), 2);
    assert!(Arc::ptr_eq(
        view.resolve(0, 0).unwrap().unwrap().0,
        &source
    ));
}

#[test]
fn attaching_schema_rejects_column_type_mismatch() {
    let view = DataChunkView::from_dense_arrays(vec![Arc::new(Int64Array::from(vec![1]))])
        .unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, false)]));

    assert!(view.with_schema(schema).is_err());
}
