use arrow::array::{Array, ArrayRef, Datum};

/// Borrowed dense Arrow input with an explicit scalar-broadcast flag.
#[derive(Clone, Copy)]
pub struct ExpressionDatum<'a> {
    pub(super) array: &'a ArrayRef,
    pub(super) scalar: bool,
}

impl<'a> ExpressionDatum<'a> {
    pub(crate) fn from_array(array: &'a ArrayRef, scalar: bool) -> Self {
        Self { array, scalar }
    }
}

impl Datum for ExpressionDatum<'_> {
    fn get(&self) -> (&dyn Array, bool) {
        (self.array.as_ref(), self.scalar)
    }
}
