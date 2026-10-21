use super::*;
use arrow::array::{ArrayRef, Int64Array, StringArray};
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

#[test]
fn projection_preserves_schema_order_metadata_and_source_arrays() {
    let id: ArrayRef = Arc::new(Int64Array::from(vec![10, 20]));
    let label: ArrayRef = Arc::new(StringArray::from(vec!["a", "b"]));
    let metadata = std::collections::HashMap::from([("owner".into(), "test".into())]);
    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, false),
        ],
        metadata,
    ));
    let view = DataChunkView::from_dense_arrays(vec![Arc::clone(&id), Arc::clone(&label)])
        .unwrap()
        .with_schema(Arc::clone(&schema))
        .unwrap();

    let projected = view.project(&[1, 0, 1]).expect("project view and schema");
    let projected_schema = projected.schema().expect("projected schema is retained");

    assert_eq!(
        projected_schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        ["label", "id", "label"]
    );
    assert_eq!(projected_schema.metadata(), schema.metadata());
    assert!(Arc::ptr_eq(&projected_schema.fields()[0], &schema.fields()[1]));
    assert!(Arc::ptr_eq(projected.resolve(0, 0).unwrap().unwrap().0, &label));
    assert!(Arc::ptr_eq(projected.resolve(1, 0).unwrap().unwrap().0, &id));
}

#[test]
fn projection_keeps_schema_absent_for_schema_less_views() {
    let view = DataChunkView::from_dense_arrays(vec![Arc::new(Int64Array::from(vec![1]))])
        .unwrap();

    assert!(view.project(&[0]).unwrap().schema().is_none());
}
