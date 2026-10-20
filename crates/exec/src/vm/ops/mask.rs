//! In-place selection vector filter masking for the Vector VM.
//!
//! Evaluates [`MicroOp::FilterMask`] by intersecting a boolean condition register
//! with the in-flight [`SelectionVector`], filtering row indices without copying column payloads.

use arrow::array::{Array, ArrayRef, BooleanArray};
use arrow::datatypes::DataType;
use common::arrow::SelectionVector;
use common::{Result, VeinError};
use crate::vm::register::RegValue;

/// Applies a boolean condition array as a filter mask onto a [`SelectionVector`].
///
/// Only currently active rows in `selection` where `bool_mask` is valid and `true`
/// survive into the resulting selection vector.
pub fn apply_filter_mask(
    selection: &SelectionVector,
    bool_mask: &RegValue,
    row_count: usize,
) -> Result<SelectionVector> {
    if bool_mask.data_type() != &DataType::Boolean {
        return Err(VeinError::internal("FilterMask target register is not boolean"));
    }

    if let RegValue::Scalar(array) = bool_mask {
        let bool_array = boolean_array(array)?;
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

    if let RegValue::Array(array) = bool_mask {
        let bool_array = boolean_array(array)?;
        if bool_array.len() != row_count {
            return Err(VeinError::internal(format!(
                "FilterMask array has {} rows, expected {row_count}",
                bool_array.len()
            )));
        }
        let mut invalid_row = None;
        let updated = selection.filter(|row| {
            if row >= row_count {
                invalid_row = Some(row);
                return false;
            }
            bool_array.is_valid(row) && bool_array.value(row)
        });
        if let Some(row) = invalid_row {
            return Err(VeinError::internal(format!(
                "FilterMask selection row {row} is outside {row_count} rows"
            )));
        }
        return Ok(updated);
    }

    let mut failure = None;
    let updated = selection.filter(|row| {
        if failure.is_some() {
            return false;
        }
        match mapped_mask_matches(bool_mask, row, row_count) {
            Ok(matches) => matches,
            Err(error) => {
                failure = Some(error);
                false
            }
        }
    });
    if let Some(error) = failure {
        return Err(error);
    }

    Ok(updated)
}

fn boolean_array(array: &ArrayRef) -> Result<&BooleanArray> {
    array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| VeinError::internal("FilterMask target register is not a BooleanArray"))
}

fn mapped_mask_matches(mask: &RegValue, row: usize, row_count: usize) -> Result<bool> {
    let Some((array, source_row, _lease)) = mask.source_row(row, row_count)? else {
        return Ok(false);
    };
    let values = boolean_array(array)?;
    Ok(values.is_valid(source_row) && values.value(source_row))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::{DataChunk, DataChunkView, ViewColumnRef, ViewInputRows};
    use std::sync::Arc;
    use crate::vm::register::SegmentedArray;

    #[test]
    fn test_apply_filter_mask() {
        let dense_sel = SelectionVector::new_dense(5);
        let mask = RegValue::Array(Arc::new(BooleanArray::from(vec![
            true, false, true, false, true,
        ])));

        let filtered = apply_filter_mask(&dense_sel, &mask, 5).unwrap();
        assert_eq!(filtered.size(), 3);
        assert_eq!(filtered.get_index(0), 0);
        assert_eq!(filtered.get_index(1), 2);
        assert_eq!(filtered.get_index(2), 4);

        // Apply a second mask over the already filtered sparse selection
        let mask2 = RegValue::Array(Arc::new(BooleanArray::from(vec![
            false, false, true, false, false,
        ])));
        let filtered2 = apply_filter_mask(&filtered, &mask2, 5).unwrap();
        assert_eq!(filtered2.size(), 1);
        assert_eq!(filtered2.get_index(0), 2);
    }

    #[test]
    fn scalar_filter_mask_preserves_or_clears_selection_without_expansion() {
        let selection = SelectionVector::new_sparse(vec![1, 4]);

        let keep = RegValue::Scalar(Arc::new(BooleanArray::from(vec![true])));
        let kept = apply_filter_mask(&selection, &keep, 5).unwrap();
        assert_eq!(kept.get_index(0), 1);
        assert_eq!(kept.get_index(1), 4);

        for reject in [
            RegValue::Scalar(Arc::new(BooleanArray::from(vec![false]))),
            RegValue::Scalar(Arc::new(BooleanArray::from(vec![None]))),
        ] {
            assert!(apply_filter_mask(&selection, &reject, 5).unwrap().is_empty());
        }
    }

    #[test]
    fn segmented_filter_mask_reads_arrow_segments_without_concatenating_them() {
        let segments = SegmentedArray::new(vec![
            Arc::new(BooleanArray::from(vec![true, false])) as arrow::array::ArrayRef,
            Arc::new(BooleanArray::from(vec![false, true])) as arrow::array::ArrayRef,
        ]).unwrap();
        let mask = RegValue::Segments(segments.clone());

        let selected = apply_filter_mask(&SelectionVector::new_dense(4), &mask, 4).unwrap();

        assert_eq!(selected.size(), 2);
        assert_eq!(selected.get_index(0), 0);
        assert_eq!(selected.get_index(1), 3);
        assert!(!segments.is_materialized());
    }

    #[test]
    fn view_filter_mask_drops_null_extended_rows_without_materializing_values() {
        let source: arrow::array::ArrayRef = Arc::new(BooleanArray::from(vec![true, false]));
        let input = DataChunkView::from_chunk(DataChunk::from_vectors(vec![Arc::clone(&source)])).unwrap();
        let rows = [Some(0), None, Some(1)];
        let view = Arc::new(
            DataChunkView::combine(
                &[ViewInputRows { view: &input, rows: &rows }],
                &[ViewColumnRef { input: 0, column: 0 }],
            )
                .unwrap(),
        );
        let mask = RegValue::view_column(view, 0).unwrap();

        let selected = apply_filter_mask(&SelectionVector::new_dense(3), &mask, 3).unwrap();

        assert_eq!(selected.size(), 1);
        assert_eq!(selected.get_index(0), 0);
        assert_eq!(source.len(), 2);
    }
}
