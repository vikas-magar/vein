//! In-place selection vector filter masking for the Vector VM.
//!
//! Evaluates [`MicroOp::FilterMask`] by intersecting a boolean condition register
//! with the in-flight [`SelectionVector`], discarding rejected rows with zero memory allocations.

use arrow::array::{Array, ArrayRef, BooleanArray};
use common::arrow::SelectionVector;
use common::Result;
use error::VeinError;

/// Applies a boolean condition array as a filter mask onto a [`SelectionVector`].
///
/// Only currently active rows in `selection` where `bool_mask` is valid and `true`
/// survive into the resulting selection vector.
pub fn apply_filter_mask(
    selection: &SelectionVector,
    bool_mask: &ArrayRef,
) -> Result<SelectionVector> {
    let bool_array = bool_mask
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| VeinError::internal("FilterMask target register is not a BooleanArray"))?;

    let updated = selection.filter(|row_idx| {
        row_idx < bool_array.len() && bool_array.is_valid(row_idx) && bool_array.value(row_idx)
    });

    Ok(updated)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_apply_filter_mask() {
        let dense_sel = SelectionVector::new_dense(5);
        let mask: ArrayRef = Arc::new(BooleanArray::from(vec![
            true, false, true, false, true,
        ]));

        let filtered = apply_filter_mask(&dense_sel, &mask).unwrap();
        assert_eq!(filtered.size(), 3);
        assert_eq!(filtered.get_index(0), 0);
        assert_eq!(filtered.get_index(1), 2);
        assert_eq!(filtered.get_index(2), 4);

        // Apply a second mask over the already filtered sparse selection
        let mask2: ArrayRef = Arc::new(BooleanArray::from(vec![
            false, false, true, false, false,
        ]));
        let filtered2 = apply_filter_mask(&filtered, &mask2).unwrap();
        assert_eq!(filtered2.size(), 1);
        assert_eq!(filtered2.get_index(0), 2);
    }
}
