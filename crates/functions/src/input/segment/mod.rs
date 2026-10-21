use std::sync::Arc;

use arrow::array::{Array, ArrayRef};
use arrow::datatypes::DataType;
use error::{Result, VeinError};

use super::FunctionInputRow;

/// One contiguous Arrow slice or a null-extended run from an outer join.
#[derive(Debug, Clone)]
pub struct FunctionInputSegment {
    array: Option<ArrayRef>,
    scalar: bool,
    len: usize,
    data_type: DataType,
}

impl FunctionInputSegment {
    /// Creates a segment from shared Arrow storage and logical metadata.
    pub fn new(array: Option<ArrayRef>, scalar: bool, len: usize, data_type: DataType) -> Result<Self> {
        if len == 0 {
            return Err(VeinError::internal("function input segment cannot be empty"));
        }
        if let Some(array) = &array {
            let expected_len = if scalar { 1 } else { len };
            if array.data_type() != &data_type || array.len() != expected_len {
                return Err(VeinError::internal("function input segment has an invalid type or length"));
            }
        } else if scalar {
            return Err(VeinError::internal("a null-extended function segment cannot be scalar"));
        }
        Ok(Self { array, scalar, len, data_type })
    }

    /// Logical rows covered by this segment.
    pub const fn len(&self) -> usize { self.len }
    /// Whether the segment is empty.
    pub const fn is_empty(&self) -> bool { self.len == 0 }
    /// Whether one physical value is broadcast over the segment.
    pub const fn is_scalar(&self) -> bool { self.scalar }
    /// Arrow type, including for null-extended segments.
    pub fn data_type(&self) -> &DataType { &self.data_type }
    /// Borrowed Arrow array, or `None` for a null-extended run.
    pub fn array(&self) -> Option<&dyn Array> { self.array.as_deref() }
    /// Shared Arrow handle for kernels that use Arrow APIs directly.
    pub fn array_ref(&self) -> Option<&ArrayRef> { self.array.as_ref() }

    /// Resolves a segment-local row, preserving scalar broadcasting.
    pub fn row(&self, row: usize) -> Result<Option<FunctionInputRow>> {
        if row >= self.len {
            return Err(VeinError::internal("function segment row is outside its range"));
        }
        Ok(self.array.as_ref().map(|array| {
            FunctionInputRow::new(Arc::clone(array), if self.scalar { 0 } else { row })
        }))
    }
}
