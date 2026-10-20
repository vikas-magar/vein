use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::DataType;

use super::{FunctionRegistry, ScalarFunction, FunctionInput, UdfOutput};
use crate::expression::value::ExpressionValue;
use crate::pipeline::DataChunkView;

#[derive(Debug)]
struct Identity;

impl ScalarFunction for Identity {
    fn name(&self) -> &str {
        "identity"
    }

    fn return_type(&self, arguments: &[DataType]) -> error::Result<DataType> {
        let [data_type] = arguments else {
            return Err(error::VeinError::planning(
                "identity expects one argument",
                None,
            ));
        };
        Ok(data_type.clone())
    }

    fn invoke(
        &self,
        arguments: &[FunctionInput<'_>],
        _row_count: usize,
        _return_type: &DataType,
    ) -> error::Result<UdfOutput> {
        let [input] = arguments else {
            return Err(error::VeinError::internal("identity expects one argument"));
        };
        Ok(UdfOutput::reuse(input))
    }
}

#[derive(Debug)]
struct InvalidLength;

impl ScalarFunction for InvalidLength {
    fn name(&self) -> &str {
        "invalid_length"
    }

    fn return_type(&self, _arguments: &[DataType]) -> error::Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke(
        &self,
        _arguments: &[FunctionInput<'_>],
        _row_count: usize,
        _return_type: &DataType,
    ) -> error::Result<UdfOutput> {
        Ok(UdfOutput::from_array(
            Arc::new(Int64Array::from(vec![1])) as ArrayRef
        ))
    }
}

#[test]
fn custom_registration_returns_the_original_arrow_buffer() {
    let mut registry = FunctionRegistry::new();
    registry.register(Identity);
    let source = Arc::new(Int64Array::from(vec![4, 8, 15])) as ArrayRef;
    let value = ExpressionValue::Array(Arc::clone(&source));

    let output = registry
        .invoke("IDENTITY", &[FunctionInput::new(&value, 3)], 3)
        .expect("custom UDF should execute case-insensitively")
        .into_expression_value();

    assert!(Arc::ptr_eq(
        &source,
        output.array().expect("dense identity output")
    ));
}

#[test]
fn custom_registration_reuses_a_reference_backed_input() {
    let mut registry = FunctionRegistry::new();
    registry.register(Identity);
    let source = Arc::new(Int64Array::from(vec![4, 8, 15])) as ArrayRef;
    let view =
        Arc::new(DataChunkView::from_dense_arrays(vec![source]).expect("single-column view"));
    let value = ExpressionValue::view_column(Arc::clone(&view), 0).expect("view column");

    let output = registry
        .invoke(
            "identity",
            &[FunctionInput::new(&value, view.num_rows())],
            view.num_rows(),
        )
        .expect("identity should retain the input mapping")
        .into_expression_value();

    assert!(output.is_mapped());
}

#[test]
fn registry_rejects_a_custom_function_with_the_wrong_output_length() {
    let mut registry = FunctionRegistry::new();
    registry.register(InvalidLength);
    let value = ExpressionValue::Array(Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef);

    let error = registry
        .invoke("invalid_length", &[FunctionInput::new(&value, 2)], 2)
        .expect_err("the registry validates plugin output shape");

    assert!(
        error
            .to_string()
            .contains("returned 1 rows for a batch of 2")
    );
}
