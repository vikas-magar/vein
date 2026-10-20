use std::ops::Range;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, FixedSizeListArray, Float32Array, Float64Array, ListArray, ListLikeArray,
};
use common::Result;
use error::VeinError;

use crate::expression::value::ExpressionValue;

pub(super) fn eval_mapped_cosine_distance(
    lhs: &ExpressionValue,
    rhs: &ExpressionValue,
    rows: usize,
) -> Result<ArrayRef> {
    let mut f64_values = Vec::with_capacity(rows);
    for row in 0..rows {
        let left = vector_row(lhs, row, rows)?;
        let right = vector_row(rhs, row, rows)?;
        let value = match (left, right) {
            (Some(left), Some(right)) => compute_cosine_row(left, right, row)?,
            _ => None,
        };
        f64_values.push(value);
    }
    Ok(Arc::new(Float64Array::from(f64_values)))
}

struct VectorRow<'a> {
    values: &'a Float32Array,
    range: Range<usize>,
}

fn vector_row(value: &ExpressionValue, row: usize, rows: usize) -> Result<Option<VectorRow<'_>>> {
    let Some((array, source_row, _lease)) = value.source_row(row, rows)? else {
        return Ok(None);
    };
    if array.is_null(source_row) {
        return Ok(None);
    }
    let (values, range) = vector_range(array.as_ref(), source_row)?;
    Ok(Some(VectorRow { values, range }))
}

fn vector_range(array: &dyn Array, row: usize) -> Result<(&Float32Array, Range<usize>)> {
    let (values, range) = if let Some(fixed) = array.as_any().downcast_ref::<FixedSizeListArray>() {
        (fixed.values(), fixed.element_range(row))
    } else if let Some(list) = array.as_any().downcast_ref::<ListArray>() {
        (list.values(), list.element_range(row))
    } else {
        return Err(VeinError::internal(format!(
            "Expected FixedSizeList or List of Float32, found {:?}",
            array.data_type()
        )));
    };
    let values = values
        .as_any()
        .downcast_ref::<Float32Array>()
        .ok_or_else(|| VeinError::internal("vector elements must be Float32"))?;
    Ok((values, range))
}

fn compute_cosine_row(
    lhs: VectorRow<'_>,
    rhs: VectorRow<'_>,
    row: usize,
) -> Result<Option<f64>> {
    if lhs.range.len() != rhs.range.len() {
        return Err(VeinError::internal(format!(
            "vector embedding dimension mismatch at row {row}: {} vs {}",
            lhs.range.len(),
            rhs.range.len()
        )));
    }
    let mut dot = 0.0f64;
    let mut norm_lhs = 0.0f64;
    let mut norm_rhs = 0.0f64;
    for (left_index, right_index) in lhs.range.zip(rhs.range) {
        if lhs.values.is_null(left_index) || rhs.values.is_null(right_index) {
            return Ok(None);
        }
        let left = f64::from(lhs.values.value(left_index));
        let right = f64::from(rhs.values.value(right_index));
        dot += left * right;
        norm_lhs += left * left;
        norm_rhs += right * right;
    }
    let denominator = norm_lhs.sqrt() * norm_rhs.sqrt();
    Ok(Some(if denominator == 0.0 {
        0.0
    } else {
        1.0 - dot / denominator
    }))
}

#[cfg(test)]
#[path = "mapped_tests.rs"]
mod tests;
