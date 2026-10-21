use super::*;
use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use common::arrow::SelectionVector;
use exec::pipeline::{DataChunkView, RowMapping, SourceRow, ViewColumn};
use std::sync::Arc;

#[test]
fn pretty_output_reads_noncontiguous_view_rows_in_logical_order() {
    let values: ArrayRef = Arc::new(StringArray::from(vec!["alpha", "beta", "gamma"]));
    let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
    let view = DataChunkView::from_selected_arrays(
        vec![values],
        SelectionVector::new_sparse(vec![2, 0]),
    )
        .unwrap();
    let batch = view.with_schema(schema).unwrap();

    assert_eq!(
        pretty_format_views(&[batch]).unwrap(),
        "+-------+\n| name  |\n+-------+\n| gamma |\n| alpha |\n+-------+\n"
    );
}

#[test]
fn pretty_output_renders_null_extended_view_rows_as_null() {
    let source: ArrayRef = Arc::new(StringArray::from(vec!["present"]));
    let mapping = Arc::new(
        RowMapping::new(Arc::from(vec![1]), [None::<SourceRow>]).expect("valid null mapping"),
    );
    let column = ViewColumn::new(vec![source], DataType::Utf8, mapping).unwrap();
    let view = DataChunkView::new(1, vec![column], SelectionVector::new_dense(1)).unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)]));
    let batch = view.with_schema(schema).unwrap();

    assert_eq!(
        pretty_format_views(&[batch]).unwrap(),
        "+------+\n| name |\n+------+\n| NULL |\n+------+\n"
    );
}
