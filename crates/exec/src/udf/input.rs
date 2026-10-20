//! Borrowed inputs and zero-copy segment access for scalar functions.

use arrow::array::{Array, ArrayRef};
use arrow::datatypes::DataType;
use common::Result;
use error::VeinError;

use crate::expression::value::ExpressionValue;

/// One function argument mapped onto the current logical batch.
#[derive(Debug, Clone, Copy)]
pub struct FunctionInput<'a> {
    value: &'a ExpressionValue,
    row_count: usize,
}

impl<'a> FunctionInput<'a> {
    pub(crate) const fn new(value: &'a ExpressionValue, row_count: usize) -> Self {
        Self { value, row_count }
    }

    /// Logical rows visible to the function for this invocation.
    pub const fn len(&self) -> usize {
        self.row_count
    }

    /// Whether the logical batch contains no rows.
    pub const fn is_empty(&self) -> bool {
        self.row_count == 0
    }

    /// Arrow type of this argument.
    pub fn data_type(&self) -> &DataType {
        self.value.data_type()
    }

    /// Whether this argument is a single value broadcast over the batch.
    pub fn is_scalar(&self) -> bool {
        self.value.is_scalar()
    }

    /// Returns one contiguous logical run backed by a shared Arrow slice.
    pub fn segment(&self, start: usize, max_len: usize) -> Result<FunctionInputSegment> {
        let segment = self.value.segment(start, max_len, self.row_count)?;
        let len = segment.len();
        let scalar = segment.is_scalar();
        Ok(FunctionInputSegment {
            array: segment.into_array(),
            scalar,
            len,
            data_type: self.data_type().clone(),
        })
    }

    /// Resolves a logical row without gathering its value.
    pub fn source_row(&self, logical_row: usize) -> Result<Option<UdfInputRow<'_>>> {
        let Some((array, row_index, _lease)) =
            self.value.source_row(logical_row, self.row_count)?
        else {
            return Ok(None);
        };
        Ok(Some(UdfInputRow {
            array: array.as_ref(),
            row_index,
        }))
    }

    /// Returns an output that reuses this argument's existing storage.
    pub fn reuse(&self) -> super::UdfOutput {
        super::UdfOutput::from_expression_value(self.value.clone())
    }

    pub(crate) fn expression_value(&self) -> &ExpressionValue {
        self.value
    }
}

/// A contiguous function-input segment; `None` represents outer-join NULLs.
#[derive(Debug, Clone)]
pub struct FunctionInputSegment {
    array: Option<ArrayRef>,
    scalar: bool,
    len: usize,
    data_type: DataType,
}

impl FunctionInputSegment {
    /// Number of logical rows covered by this segment.
    pub const fn len(&self) -> usize {
        self.len
    }

    /// Whether this segment contains no logical rows.
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Whether the segment is a broadcast scalar.
    pub const fn is_scalar(&self) -> bool {
        self.scalar
    }

    /// Arrow type of the segment, including null-extended runs.
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    /// Borrowed Arrow array slice, or `None` for a NULL-extended run.
    pub fn array(&self) -> Option<&dyn Array> {
        self.array.as_deref()
    }

    /// Shared Arrow array handle for UDFs that use Arrow APIs directly.
    pub fn array_ref(&self) -> Option<&ArrayRef> {
        self.array.as_ref()
    }

    /// Resolves one logical row while preserving scalar broadcasting as an index.
    pub fn row(&self, logical_row: usize) -> Result<Option<UdfInputRow<'_>>> {
        if logical_row >= self.len {
            return Err(VeinError::internal(format!(
                "UDF segment row {logical_row} is outside {} rows",
                self.len
            )));
        }
        Ok(self.array.as_deref().map(|array| UdfInputRow {
            array,
            row_index: if self.scalar { 0 } else { logical_row },
        }))
    }
}

/// A borrowed source-array row resolved from a [`FunctionInput`].
#[derive(Clone, Copy)]
pub struct UdfInputRow<'a> {
    array: &'a dyn Array,
    row_index: usize,
}

impl<'a> UdfInputRow<'a> {
    /// Underlying source array; no row payload is copied.
    pub const fn array(&self) -> &'a dyn Array {
        self.array
    }

    /// Physical row index in [`Self::array`].
    pub const fn row_index(&self) -> usize {
        self.row_index
    }
}
