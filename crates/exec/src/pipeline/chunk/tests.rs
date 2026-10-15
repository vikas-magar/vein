use super::DataChunk;
use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::Schema;
use common::arrow::SelectionVector;
use std::sync::Arc;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
        "value",
        arrow::datatypes::DataType::Int64,
        false,
    )]))
}

#[test]
fn taking_record_batch_columns_into_a_chunk_shares_the_array() {
    let input: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
    let batch = arrow::record_batch::RecordBatch::try_new(
        schema(),
        vec![Arc::clone(&input)],
    )
        .expect("valid input batch");

    let chunk = DataChunk::from_record_batch(batch);

    assert!(Arc::ptr_eq(&input, chunk.column(0)));
}

#[test]
fn materializing_a_contiguous_selection_slices_the_existing_values() {
    let input: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
    let input_values = input
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 input")
        .values()
        .as_ptr();
    let chunk = DataChunk::new(
        vec![input],
        SelectionVector::new_sparse(vec![0, 1]),
    );

    let batch = chunk.materialize(schema()).expect("materialize selection");
    let output = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 output");

    assert_eq!(output.values().as_ptr(), input_values);
    assert_eq!(output.values().as_ref(), &[1, 2]);
}

#[test]
fn materializing_a_sparse_selection_gathers_values_into_a_new_buffer() {
    let input: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
    let input_values = input
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 input")
        .values()
        .as_ptr();
    let chunk = DataChunk::new(
        vec![input],
        SelectionVector::new_sparse(vec![0, 2]),
    );

    let batch = chunk.materialize(schema()).expect("materialize selection");
    let output = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 output");

    assert_ne!(output.values().as_ptr(), input_values);
    assert_eq!(output.values().as_ref(), &[1, 3]);
}

#[test]
fn materializes_zero_column_chunks_with_their_row_count() {
    let chunk = DataChunk::new(Vec::new(), SelectionVector::new_dense(3));
    let batch = chunk
        .materialize(Arc::new(Schema::empty()))
        .expect("zero-column batch should materialize");

    assert_eq!(batch.num_columns(), 0);
    assert_eq!(batch.num_rows(), 3);
}

#[test]
fn materializes_empty_zero_column_chunks_as_zero_rows() {
    let chunk = DataChunk::new(Vec::new(), SelectionVector::new_dense(0));
    let batch = chunk
        .materialize(Arc::new(Schema::empty()))
        .expect("empty zero-column batch should materialize");

    assert_eq!(batch.num_columns(), 0);
    assert_eq!(batch.num_rows(), 0);
}
