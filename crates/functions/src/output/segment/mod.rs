//! Validated physical storage and logical length for a computed output run.
use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use error::{Result, VeinError};

/// A dense array or one retained value repeated over logical rows.
#[derive(Clone, Debug)]
pub struct OutputSegment {
    array: ArrayRef,
    rows: usize,
    scalar: bool,
}

impl OutputSegment {
    /// Retains an array without changing its row interpretation.
    pub fn dense(array: ArrayRef) -> Self {
        Self { rows: array.len(), array, scalar: false }
    }

    /// Validates output shape against the kernel's explicit scalar semantics.
    pub fn new(array: ArrayRef, rows: usize, scalar: bool) -> Result<Self> {
        let physical_rows = if scalar { 1 } else { rows };
        if array.len() != physical_rows || (scalar && rows == 0) {
            return Err(VeinError::internal(format!(
                "output segment has {} physical values for {rows} logical rows (scalar={scalar})",
                array.len()
            )));
        }
        Ok(Self { array, rows, scalar })
    }

    /// Shared Arrow storage, without materializing repeated values.
    pub fn array(&self) -> &ArrayRef { &self.array }
    /// Logical row count.
    pub const fn len(&self) -> usize { self.rows }
    /// Whether no logical rows are represented.
    pub const fn is_empty(&self) -> bool { self.rows == 0 }
    /// Whether the physical value repeats across the logical run.
    pub const fn is_scalar(&self) -> bool { self.scalar }
    /// Arrow type of the retained values.
    pub fn data_type(&self) -> &DataType { self.array.data_type() }
    /// Transfers the shared handle without copying its payload.
    pub fn into_array(self) -> ArrayRef { self.array }
}

#[cfg(test)]
mod tests;
