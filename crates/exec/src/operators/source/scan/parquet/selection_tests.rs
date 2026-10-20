use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

#[test]
fn scan_handoff_reorders_output_without_changing_arrow_array_identity() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("predicate", DataType::Int64, false),
        Field::new("output", DataType::Int64, false),
    ]));
    let predicate: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30, 40]));
    let output: ArrayRef = Arc::new(Int64Array::from(vec![100, 200, 300, 400]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::clone(&predicate), Arc::clone(&output)],
    )
        .expect("test batch");
    let input = crate::pipeline::arrow_adapter::from_record_batch(batch).expect("scan view");
    let view = input.project(&[1, 0]).expect("projected view");

    assert_eq!(view.num_columns(), 2);
    assert_eq!(view.num_rows(), 4);
    let (source, physical_row) = view.resolve(0, 0).expect("resolve output row").unwrap();
    assert!(Arc::ptr_eq(source, &output));
    assert_eq!(physical_row, 0);
}

#[test]
fn predicate_projection_selects_array_handles_without_an_intermediate_batch() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("unused", DataType::Int64, false),
        Field::new("predicate", DataType::Int64, false),
    ]));
    let predicate: ArrayRef = Arc::new(Int64Array::from(vec![7, 8, 9]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![1, 2, 3])), Arc::clone(&predicate)],
    )
        .expect("predicate input batch");

    let input = crate::pipeline::arrow_adapter::from_record_batch_ref(&batch).expect("scan view");
    let view = input.project(&[1]).expect("predicate view");

    assert_eq!(view.num_columns(), 1);
    assert_eq!(view.num_rows(), 3);
    let (source, row) = view.resolve(0, 1).expect("resolve predicate row").unwrap();
    assert!(Arc::ptr_eq(source, &predicate));
    assert_eq!(row, 1);
}
