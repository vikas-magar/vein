use std::sync::Arc;

use arrow::array::{Array, ArrayRef};
use arrow::datatypes::DataType;
use error::{Result, VeinError};

use crate::{FunctionInput, FunctionInputRow, FunctionInputSegment, FunctionInputSource, FunctionOutput, ScalarFunction};

pub(crate) struct DenseInput {
    array: ArrayRef,
    scalar: bool,
}

impl DenseInput {
    pub(crate) fn column(array: ArrayRef) -> Self { Self { array, scalar: false } }
    pub(crate) fn scalar(array: ArrayRef) -> Self { Self { array, scalar: true } }
}

impl FunctionInputSource for DenseInput {
    fn data_type(&self) -> &DataType { self.array.data_type() }
    fn is_scalar(&self) -> bool { self.scalar }

    fn segment(&self, start: usize, max_len: usize, _rows: usize) -> Result<FunctionInputSegment> {
        let array = if self.scalar { Arc::clone(&self.array) } else { self.array.slice(start, max_len) };
        FunctionInputSegment::new(Some(array), self.scalar, max_len, self.data_type().clone())
    }

    fn source_row(&self, row: usize, _rows: usize) -> Result<Option<FunctionInputRow>> {
        Ok(Some(FunctionInputRow::new(
            Arc::clone(&self.array),
            if self.scalar { 0 } else { row },
        )))
    }
}

pub(crate) fn invoke_dense(function: &dyn ScalarFunction, arrays: Vec<ArrayRef>) -> Result<ArrayRef> {
    let rows = arrays.first().map_or(0, |array| array.len());
    if arrays.iter().any(|array| array.len() != rows) {
        return Err(VeinError::internal("test function inputs have inconsistent lengths"));
    }
    let inputs = arrays.iter().cloned().map(DenseInput::column).collect::<Vec<_>>();
    let arguments = inputs.iter().map(|input| FunctionInput::new(input, rows)).collect::<Vec<_>>();
    let types = arguments.iter().map(|argument| argument.data_type().clone()).collect::<Vec<_>>();
    let return_type = function.return_type(&types)?;
    let output = function.invoke(&arguments, rows, &return_type)?;
    output.validate(rows, &return_type, &arguments)?;
    dense_output(output, &inputs)
}

pub(crate) fn invoke_inputs(
    function: &dyn ScalarFunction,
    inputs: &[DenseInput],
    rows: usize,
) -> Result<FunctionOutput> {
    let arguments = inputs.iter().map(|input| FunctionInput::new(input, rows)).collect::<Vec<_>>();
    let types = arguments.iter().map(|argument| argument.data_type().clone()).collect::<Vec<_>>();
    let return_type = function.return_type(&types)?;
    let output = function.invoke(&arguments, rows, &return_type)?;
    output.validate(rows, &return_type, &arguments)?;
    Ok(output)
}

pub(crate) fn dense_output(output: FunctionOutput, inputs: &[DenseInput]) -> Result<ArrayRef> {
    match output {
        FunctionOutput::Array(array) | FunctionOutput::Scalar(array) => Ok(array),
        FunctionOutput::ReuseInput(index) => inputs.get(index).map(|input| Arc::clone(&input.array))
            .ok_or_else(|| VeinError::internal("test function reused a missing input")),
        FunctionOutput::Segments(_) => Err(VeinError::internal("test helper cannot materialize segmented output")),
    }
}
