use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float32Array, Float64Array, ListArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field};

use super::VectorDistanceFunction;
use crate::ScalarFunction;

fn vectors(rows: &[&[f32]]) -> ArrayRef {
    let mut values = Vec::new();
    let lengths = rows
        .iter()
        .map(|row| {
            values.extend_from_slice(row);
            row.len()
        })
        .collect::<Vec<_>>();
    Arc::new(ListArray::new(
        Arc::new(Field::new("item", DataType::Float32, false)),
        OffsetBuffer::from_lengths(lengths),
        Arc::new(Float32Array::from(values)),
        None,
    ))
}

#[test]
fn computes_cosine_distance_from_borrowed_list_inputs() {
    let output = crate::test_support::invoke_dense(
        &VectorDistanceFunction,
        vec![vectors(&[&[1.0, 0.0], &[1.0, 0.0]]), vectors(&[&[0.0, 1.0], &[1.0, 0.0]])],
    )
        .expect("vector distance should execute");
    let values = output.as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!(values.value(0), 1.0);
    assert_eq!(values.value(1), 0.0);
}

#[test]
fn rejects_invalid_vector_arguments_and_mismatched_dimensions() {
    assert!(VectorDistanceFunction.return_type(&[DataType::Float32]).is_err());
    let error = crate::test_support::invoke_dense(
        &VectorDistanceFunction,
        vec![vectors(&[&[1.0, 0.0]]), vectors(&[&[1.0]])],
    )
        .expect_err("mismatched vector dimensions must fail");
    assert!(error.to_string().contains("dimension mismatch"));
}
