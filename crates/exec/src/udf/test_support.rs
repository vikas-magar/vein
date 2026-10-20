use arrow::array::ArrayRef;
use common::Result;

use crate::expression::value::ExpressionValue;

use super::{ScalarFunction, FunctionInput};

pub(crate) fn invoke_values(
    function: &dyn ScalarFunction,
    values: &[ExpressionValue],
    row_count: usize,
) -> Result<ExpressionValue> {
    let inputs = values
        .iter()
        .map(|value| FunctionInput::new(value, row_count))
        .collect::<Vec<_>>();
    let argument_types = inputs
        .iter()
        .map(|input| input.data_type().clone())
        .collect::<Vec<_>>();
    let return_type = function.return_type(&argument_types)?;
    let output = function.invoke(&inputs, row_count, &return_type)?;
    output.validate(row_count, &return_type)?;
    Ok(output.into_expression_value())
}

pub(crate) fn invoke_dense(
    function: &dyn ScalarFunction,
    arrays: Vec<ArrayRef>,
) -> Result<ArrayRef> {
    let row_count = arrays.first().map_or(0, |array| array.len());
    if arrays.iter().any(|array| array.len() != row_count) {
        return Err(error::VeinError::internal(
            "test UDF arguments have different lengths",
        ));
    }
    let values = arrays.into_iter().map(ExpressionValue::Array).collect::<Vec<_>>();
    invoke_values(function, &values, row_count)?.materialized_array()
}
