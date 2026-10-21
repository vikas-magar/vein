//! Constructors for canonical view-backed pipeline batches.
use arrow::array::ArrayRef;
use common::Result;
use common::arrow::SelectionVector;

use super::DataChunkView;

impl DataChunkView {
    /// Wraps owned Arrow array handles with a dense selection.
    ///
    /// This is the standard constructor for newly read or computed columns.
    /// It moves array references into the view without copying Arrow buffers.
    pub fn from_dense_arrays(arrays: Vec<ArrayRef>) -> Result<Self> {
        let row_count = arrays.first().map_or(0, |array| array.len());
        Self::from_arrays(
            arrays,
            row_count,
            SelectionVector::new_dense(row_count),
        )
    }

    /// Wraps arrays while retaining an existing active-row selection.
    ///
    /// For zero-column views, the physical row extent is inferred from the
    /// selection. Use from_arrays when an explicit physical row count is known.
    pub fn from_selected_arrays(
        arrays: Vec<ArrayRef>,
        selection: SelectionVector,
    ) -> Result<Self> {
        let row_count = arrays
            .first()
            .map_or_else(|| selected_extent(&selection), |array| array.len());
        Self::from_arrays(arrays, row_count, selection)
    }
}

fn selected_extent(selection: &SelectionVector) -> usize {
    selection.contiguous_range().map_or_else(
        || {
            (0..selection.size())
                .map(|row| selection.get_index(row).saturating_add(1))
                .max()
                .unwrap_or(0)
        },
        |range| range.end,
    )
}
