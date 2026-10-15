//! In-place selection vector filter masking for the Vector VM.
//!
//! Evaluates [`MicroOp::FilterMask`] by intersecting a boolean condition register
//! with the in-flight [`SelectionVector`], discarding rejected rows with zero memory allocations.

use arrow::array::{Array, BooleanArray};
use common::arrow::SelectionVector;
use common::Result;
use error::VeinError;
use crate::vm::register::RegValue;

/// Applies a boolean condition array as a filter mask onto a [`SelectionVector`].
///
/// Only currently active rows in `selection` where `bool_mask` is valid and `true`
/// survive into the resulting selection vector.
pub fn apply_filter_mask(
    selection: &SelectionVector,
    bool_mask: &RegValue,
) -> Result<SelectionVector> {
    let bool_array = bool_mask
        .array()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| VeinError::internal("FilterMask target register is not a BooleanArray"))?;

    if bool_mask.is_scalar() {
        if bool_array.len() != 1 {
            return Err(VeinError::internal(
                "FilterMask scalar register must contain exactly one value",
            ));
        }
        return Ok(if bool_array.is_valid(0) && bool_array.value(0) {
            selection.clone()
        } else {
            SelectionVector::new_dense(0)
        });
    }

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
        let mask = RegValue::Array(Arc::new(BooleanArray::from(vec![
            true, false, true, false, true,
        ])));

        let filtered = apply_filter_mask(&dense_sel, &mask).unwrap();
        assert_eq!(filtered.size(), 3);
        assert_eq!(filtered.get_index(0), 0);
        assert_eq!(filtered.get_index(1), 2);
        assert_eq!(filtered.get_index(2), 4);

        // Apply a second mask over the already filtered sparse selection
        let mask2 = RegValue::Array(Arc::new(BooleanArray::from(vec![
            false, false, true, false, false,
        ])));
        let filtered2 = apply_filter_mask(&filtered, &mask2).unwrap();
        assert_eq!(filtered2.size(), 1);
        assert_eq!(filtered2.get_index(0), 2);
    }

    #[test]
    fn scalar_filter_mask_preserves_or_clears_selection_without_expansion() {
        let selection = SelectionVector::new_sparse(vec![1, 4]);

        let keep = RegValue::Scalar(Arc::new(BooleanArray::from(vec![true])));
        let kept = apply_filter_mask(&selection, &keep).unwrap();
        assert_eq!(kept.get_index(0), 1);
        assert_eq!(kept.get_index(1), 4);

        for reject in [
            RegValue::Scalar(Arc::new(BooleanArray::from(vec![false]))),
            RegValue::Scalar(Arc::new(BooleanArray::from(vec![None]))),
        ] {
            assert!(apply_filter_mask(&selection, &reject).unwrap().is_empty());
        }
    }
}
