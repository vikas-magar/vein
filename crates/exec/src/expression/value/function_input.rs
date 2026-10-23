use std::sync::Arc;

use arrow::datatypes::DataType;
use common::Result;
use error::VeinError;
use functions::{FunctionInputRow, FunctionInputSegment, FunctionInputSource};

use super::ExpressionValue;

/// Presents executor expression mappings through the function crate's neutral input API.
pub struct ExpressionFunctionInputSource<'a> {
    value: &'a ExpressionValue,
}

impl<'a> ExpressionFunctionInputSource<'a> {
    /// Adapts an expression value while its view and memory leases remain alive.
    pub const fn new(value: &'a ExpressionValue) -> Self {
        Self { value }
    }
}

impl FunctionInputSource for ExpressionFunctionInputSource<'_> {
    fn data_type(&self) -> &DataType {
        self.value.data_type()
    }

    fn is_scalar(&self) -> bool {
        self.value.is_scalar()
    }

    fn segment(
        &self,
        start: usize,
        max_len: usize,
        rows: usize,
    ) -> Result<FunctionInputSegment> {
        let segment = self.value.segment(start, max_len, rows)?;
        let scalar = segment.is_scalar();
        let len = segment.len();
        let array = segment.into_array();
        FunctionInputSegment::new(
            array,
            scalar,
            len,
            self.data_type().clone(),
        )
    }

    fn source_row(&self, row: usize, rows: usize) -> Result<Option<FunctionInputRow>> {
        let Some((array, source_row, _lease)) = self.value.source_row(row, rows)? else {
            return Ok(None);
        };
        Ok(Some(FunctionInputRow::new(Arc::clone(array), source_row)))
    }
}

/// Converts a function result to the executor's reference-backed value model.
pub(crate) fn into_expression_value(
    output: functions::FunctionOutput,
    inputs: &[ExpressionValue],
) -> Result<ExpressionValue> {
    match output {
        functions::FunctionOutput::Array(array) => Ok(ExpressionValue::Array(array)),
        functions::FunctionOutput::Scalar(array) => Ok(ExpressionValue::Scalar(array)),
        functions::FunctionOutput::Segments(segments) => Ok(ExpressionValue::Segments(
            super::SegmentedArray::from_runs(segments)?,
        )),
        functions::FunctionOutput::ReuseInput(index) => inputs.get(index).cloned().ok_or_else(|| {
            VeinError::internal(format!("function reused missing input argument {index}"))
        }),
    }
}
