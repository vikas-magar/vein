//! Validated scalar-function outputs that retain Arrow buffers and segments.

use arrow::array::{ArrayRef, new_empty_array};
use arrow::datatypes::DataType;
use common::Result;
use error::VeinError;

use crate::expression::value::{ExpressionValue, SegmentedArray};

use super::FunctionInput;

/// A scalar-function result represented as a scalar, array, view, or segments.
#[derive(Debug, Clone)]
pub struct UdfOutput {
    value: ExpressionValue,
}

impl UdfOutput {
    /// Wraps a computed Arrow array without copying its buffers.
    pub fn from_array(array: ArrayRef) -> Self {
        Self {
            value: ExpressionValue::Array(array),
        }
    }

    /// Wraps a one-value Arrow array as a broadcast scalar.
    pub fn from_scalar(array: ArrayRef) -> Result<Self> {
        if array.len() != 1 {
            return Err(VeinError::internal(format!(
                "scalar UDF output has {} values, expected one",
                array.len()
            )));
        }
        Ok(Self {
            value: ExpressionValue::Scalar(array),
        })
    }

    /// Retains ordered result segments without concatenating their buffers.
    pub fn from_segments(arrays: Vec<ArrayRef>) -> Result<Self> {
        if arrays.len() == 1 {
            return Ok(Self::from_array(arrays.into_iter().next().ok_or_else(|| {
                VeinError::internal("single UDF output segment disappeared")
            })?));
        }
        Ok(Self {
            value: ExpressionValue::Segments(SegmentedArray::new(arrays)?),
        })
    }

    /// Creates an output that shares the input expression's existing storage.
    pub fn reuse(input: &FunctionInput<'_>) -> Self {
        Self::from_expression_value(input.expression_value().clone())
    }

    pub(crate) fn empty(data_type: &DataType) -> Self {
        Self::from_array(new_empty_array(data_type))
    }

    pub(crate) fn from_expression_value(value: ExpressionValue) -> Self {
        Self { value }
    }

    /// Arrow type of this output.
    pub fn data_type(&self) -> &DataType {
        self.value.data_type()
    }

    /// Physical number of values represented by this output.
    pub fn len(&self) -> usize {
        self.value.physical_len()
    }

    /// Whether this output is a broadcast scalar.
    pub fn is_scalar(&self) -> bool {
        self.value.is_scalar()
    }

    pub(crate) fn validate(&self, rows: usize, expected_type: &DataType) -> Result<()> {
        if self.data_type() != expected_type {
            return Err(VeinError::internal(format!(
                "scalar UDF returned {:?}, expected {expected_type:?}",
                self.data_type()
            )));
        }
        if !self.is_scalar() && self.len() != rows {
            return Err(VeinError::internal(format!(
                "scalar UDF returned {} rows for a batch of {rows}",
                self.len()
            )));
        }
        Ok(())
    }

    pub(crate) fn into_expression_value(self) -> ExpressionValue {
        self.value
    }
}
