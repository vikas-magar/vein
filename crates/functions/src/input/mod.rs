//! Zero-copy function arguments over Arrow-backed input sources.

mod row;
mod segment;

pub use row::FunctionInputRow;
pub use segment::FunctionInputSegment;

use arrow::datatypes::DataType;
use error::{Result, VeinError};

/// Adapts an engine value to zero-copy logical segments and borrowed rows.
pub trait FunctionInputSource {
    /// Arrow type of the logical input.
    fn data_type(&self) -> &DataType;
    /// Whether one physical value is broadcast across the logical rows.
    fn is_scalar(&self) -> bool;
    /// Returns an Arrow-backed contiguous run for a logical range.
    fn segment(
        &self,
        start: usize,
        max_len: usize,
        row_count: usize,
    ) -> Result<FunctionInputSegment>;
    /// Resolves a row by reference, returning `None` for a null-extended join row.
    fn source_row(&self, row: usize, row_count: usize) -> Result<Option<FunctionInputRow>>;
}

/// One function argument borrowed from an engine-owned expression value.
#[derive(Clone, Copy)]
pub struct FunctionInput<'a> {
    source: &'a dyn FunctionInputSource,
    row_count: usize,
}

impl<'a> FunctionInput<'a> {
    /// Borrows a source adapter for one logical batch.
    pub const fn new(source: &'a dyn FunctionInputSource, row_count: usize) -> Self {
        Self { source, row_count }
    }

    /// Logical row count visible to the function.
    pub const fn len(&self) -> usize {
        self.row_count
    }

    /// Whether the logical input has no rows.
    pub const fn is_empty(&self) -> bool {
        self.row_count == 0
    }

    /// Arrow type of this argument.
    pub fn data_type(&self) -> &DataType {
        self.source.data_type()
    }

    /// Whether this argument is broadcast from a one-value scalar.
    pub fn is_scalar(&self) -> bool {
        self.source.is_scalar()
    }

    /// Returns a zero-copy segment beginning at `start`.
    pub fn segment(&self, start: usize, max_len: usize) -> Result<FunctionInputSegment> {
        if max_len == 0 || start.checked_add(max_len).is_none_or(|end| end > self.row_count) {
            return Err(VeinError::internal(
                "function input segment is outside its row range",
            ));
        }
        self.source.segment(start, max_len, self.row_count)
    }

    /// Resolves one logical row without copying its value.
    pub fn source_row(&self, row: usize) -> Result<Option<FunctionInputRow>> {
        if row >= self.row_count {
            return Err(VeinError::internal(
                "function input row is outside its row range",
            ));
        }
        let source = self.source.source_row(row, self.row_count)?;
        if source
            .as_ref()
            .is_some_and(|value| value.row_index() >= value.array().len())
        {
            return Err(VeinError::internal(
                "function source row exceeds its backing array",
            ));
        }
        Ok(source)
    }
}
