//! Borrowed, row-mapped inputs for scalar UDFs that can consume source arrays.

use arrow::array::Array;
use arrow::datatypes::DataType;
use common::Result;

use crate::vm::register::RegValue;

/// One UDF argument as a logical batch mapped onto existing Arrow storage.
///
/// The wrapper borrows the VM register, so mapped source arrays and their memory
/// leases stay alive for the duration of the UDF call. Resolving a row never
/// gathers or copies its value.
#[derive(Debug, Clone, Copy)]
pub struct MappedUdfInput<'a> {
    value: &'a RegValue,
    row_count: usize,
}

impl<'a> MappedUdfInput<'a> {
    pub(crate) const fn new(value: &'a RegValue, row_count: usize) -> Self {
        Self { value, row_count }
    }

    /// Logical rows visible to this UDF for the current batch.
    pub const fn len(&self) -> usize {
        self.row_count
    }

    /// Whether this batch contains no logical rows.
    pub const fn is_empty(&self) -> bool {
        self.row_count == 0
    }

    /// Arrow type of this logical input column.
    pub fn data_type(&self) -> &DataType {
        self.value.data_type()
    }

    /// Whether the input is a broadcast scalar rather than a column.
    pub fn is_scalar(&self) -> bool {
        self.value.is_scalar()
    }

    /// Resolves a logical row without materializing its mapped input.
    ///
    /// `None` represents an outer-join null extension. A returned row borrows
    /// the input register and therefore cannot outlive its source storage.
    pub fn source_row(&self, logical_row: usize) -> Result<Option<MappedUdfRow<'_>>> {
        let Some((array, row_index, _lease)) =
            self.value.source_row(logical_row, self.row_count)?
        else {
            return Ok(None);
        };
        Ok(Some(MappedUdfRow {
            array: array.as_ref(),
            row_index,
        }))
    }
}

/// A borrowed Arrow array row resolved from a [`MappedUdfInput`].
#[derive(Clone, Copy)]
pub struct MappedUdfRow<'a> {
    array: &'a dyn Array,
    row_index: usize,
}

impl<'a> MappedUdfRow<'a> {
    /// Underlying source array; it is not gathered for the logical batch.
    pub const fn array(&self) -> &'a dyn Array {
        self.array
    }

    /// Physical row index in [`Self::array`].
    pub const fn row_index(&self) -> usize {
        self.row_index
    }
}
