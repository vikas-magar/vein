use arrow::array::{Array, ArrayRef};

/// One physical row backed by a shared Arrow array handle.
#[derive(Clone)]
pub struct FunctionInputRow {
    array: ArrayRef,
    row_index: usize,
}

impl FunctionInputRow {
    /// Creates a row handle for a custom input adapter without copying values.
    pub fn new(array: ArrayRef, row_index: usize) -> Self {
        Self { array, row_index }
    }

    /// Underlying source array; its value buffers remain borrowed.
    pub fn array(&self) -> &dyn Array { self.array.as_ref() }
    /// Shared Arrow handle, used when an aggregate retains one scalar row.
    pub fn array_ref(&self) -> &ArrayRef { &self.array }
    /// Physical index in the source array.
    pub const fn row_index(&self) -> usize { self.row_index }
}
