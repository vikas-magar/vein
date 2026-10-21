//! Test-only constructors for views used in operator regression tests.
use arrow::array::{Array, ArrayRef};
use common::arrow::SelectionVector;

use super::DataChunkView;

/// Builds a dense view from valid Arrow test arrays.
pub(crate) fn dense_view(arrays: Vec<ArrayRef>) -> DataChunkView {
    DataChunkView::from_dense_arrays(arrays).expect("test arrays form a valid dense view")
}

/// Builds a selected view from valid Arrow test arrays.
pub(crate) fn selected_view(
    arrays: Vec<ArrayRef>,
    selection: SelectionVector,
) -> DataChunkView {
    DataChunkView::from_selected_arrays(arrays, selection)
        .expect("test arrays and selection form a valid view")
}

/// Reads one logical output column as a typed Arrow array in assertions.
///
/// The view stays the pipeline representation; this helper materializes only
/// the column a test inspects, then clones its Arrow handles into the typed
/// array wrapper.
pub(crate) fn materialized_column<T>(view: &DataChunkView, index: usize) -> T
where
    T: Array + Clone + 'static,
{
    let column = view
        .materialize_column(index)
        .expect("test output column materializes");
    column
        .as_any()
        .downcast_ref::<T>()
        .expect("test output column has the requested Arrow type")
        .clone()
}
