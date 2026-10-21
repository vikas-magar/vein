use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array, StringArray};
use arrow::datatypes::DataType;

use super::SubstringFunction;
use crate::ScalarFunction;
use crate::test_support::{dense_output, invoke_dense, invoke_inputs, DenseInput};

fn strings(values: &[Option<&str>]) -> ArrayRef {
    Arc::new(StringArray::from(values.to_vec()))
}

fn integers(values: &[Option<i64>]) -> ArrayRef {
    Arc::new(Int64Array::from(values.to_vec()))
}

fn output_values(array: &ArrayRef) -> Vec<Option<&str>> {
    let strings = array
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("UTF-8 substring output");
    strings.iter().collect()
}

#[test]
fn substring_supports_duckdb_indexing_and_utf8_character_boundaries() {
    let output = invoke_dense(
        &SubstringFunction,
        vec![
            strings(&[
                Some("abcdef"),
                Some("abcdef"),
                Some("abcdef"),
                Some("abcdef"),
                Some("aé水z"),
            ]),
            integers(&[Some(1), Some(0), Some(-3), Some(4), Some(2)]),
            integers(&[Some(2), Some(5), Some(3), Some(-2), Some(2)]),
        ],
    )
        .expect("substring should run");

    assert_eq!(
        output_values(&output),
        vec![
            Some("ab"),
            Some("abcd"),
            Some("def"),
            Some("bc"),
            Some("é水")
        ]
    );
}

#[test]
fn substring_without_length_runs_to_end_and_propagates_nulls() {
    let output = invoke_dense(
        &SubstringFunction,
        vec![
            strings(&[Some("abcdef"), Some("abcdef"), None, Some("abcdef")]),
            integers(&[Some(2), Some(-2), Some(1), None]),
        ],
    )
        .expect("two-argument substring should run");

    assert_eq!(
        output_values(&output),
        vec![Some("bcdef"), Some("ef"), None, None]
    );
}

#[test]
fn substring_clips_out_of_range_and_zero_length_slices() {
    let output = invoke_dense(
        &SubstringFunction,
        vec![
            strings(&[Some("abc"), Some("abc"), Some("abc")]),
            integers(&[Some(5), Some(1), Some(1)]),
            integers(&[Some(2), Some(0), Some(-5)]),
        ],
    )
        .expect("bounded substring should run");

    assert_eq!(output_values(&output), vec![Some(""), Some(""), Some("")]);
}

#[test]
fn substring_validates_types_and_argument_count() {
    assert_eq!(
        SubstringFunction
            .return_type(&[DataType::Utf8, DataType::Int64])
            .expect("two-argument signature"),
        DataType::Utf8
    );
    assert_eq!(
        SubstringFunction
            .return_type(&[DataType::Utf8, DataType::Int64, DataType::Int64])
            .expect("three-argument signature"),
        DataType::Utf8
    );
    assert!(SubstringFunction.return_type(&[DataType::Utf8]).is_err());
    assert!(
        SubstringFunction
            .return_type(&[DataType::Utf8, DataType::Int32])
            .is_err()
    );
    assert!(invoke_dense(&SubstringFunction, vec![]).is_err());
}

#[test]
fn substring_broadcasts_scalar_start_without_expanding_its_input_buffer() {
    let inputs = [
        DenseInput::column(strings(&[Some("abc"), Some("é水"), None])),
        DenseInput::scalar(integers(&[Some(2)])),
    ];
    let output = invoke_inputs(&SubstringFunction, &inputs, 3)
        .expect("scalar start should broadcast over the input column");
    let array = dense_output(output, &inputs).expect("one computed array");

    assert_eq!(output_values(&array), vec![Some("bc"), Some("水"), None]);
}
