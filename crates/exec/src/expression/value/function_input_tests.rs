use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Date32Array, Int32Array};
use arrow::datatypes::DataType;
use functions::{FunctionInput, FunctionOutput, FunctionRegistry};

use super::{ExpressionFunctionInputSource, ExpressionValue, into_expression_value};
use crate::pipeline::test_support;

#[path = "function_input/tests/repeated.rs"]
mod repeated;

#[test]
fn temporal_function_reads_reference_backed_view_and_preserves_nulls() {
    let source = Arc::new(Date32Array::from(vec![Some(0), Some(19_723), None])) as ArrayRef;
    let view = Arc::new(test_support::dense_view(vec![Arc::clone(&source)]));
    let value = ExpressionValue::view_column(Arc::clone(&view), 0)
        .expect("view-backed date input");
    let adapter = ExpressionFunctionInputSource::new(&value);
    let arguments = [FunctionInput::new(&adapter, view.num_rows())];
    let functions = FunctionRegistry::with_builtins();

    let output = functions
        .invoke("year", &arguments, view.num_rows(), &DataType::Int32)
        .expect("YEAR evaluates from Arrow-backed segments");
    let output = into_expression_value(output, std::slice::from_ref(&value))
        .expect("convert function output");
    let years = output
        .array()
        .expect("computed scalar result")
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("Int32 year output");

    assert_eq!(years.value(0), 1970);
    assert_eq!(years.value(1), 2024);
    assert!(years.is_null(2));
}

#[test]
fn function_input_reuse_keeps_the_original_view_mapping() {
    let source = Arc::new(Date32Array::from(vec![Some(0), Some(1)])) as ArrayRef;
    let view = Arc::new(test_support::dense_view(vec![source]));
    let value = ExpressionValue::view_column(Arc::clone(&view), 0)
        .expect("view-backed date input");

    let output = into_expression_value(FunctionOutput::reuse_input(0), &[value])
        .expect("reuse original expression input");
    let ExpressionValue::ViewColumn(column) = output else {
        panic!("input reuse must preserve the reference-backed view value");
    };

    assert_eq!(column.column_in_view(&view), Some(0));
}
