use std::ops::Range;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, FixedSizeListArray, Float32Array, Float64Array, ListArray, ListLikeArray,
};
use common::Result;
use error::VeinError;

use crate::vm::register::RegValue;

#[derive(Clone, Copy)]
pub(crate) enum VectorDistance {
    Cosine,
    L2,
    Dot,
}

pub(crate) fn eval_mapped_vector_distance(
    lhs: &RegValue,
    rhs: &RegValue,
    rows: usize,
    distance: VectorDistance,
) -> Result<ArrayRef> {
    let mut f32_values = Vec::with_capacity(rows);
    let mut f64_values = Vec::with_capacity(rows);
    for row in 0..rows {
        let left = vector_row(lhs, row, rows)?;
        let right = vector_row(rhs, row, rows)?;
        let value = match (left, right) {
            (Some(left), Some(right)) => compute_row(left, right, row, distance)?,
            _ => None,
        };
        match distance {
            VectorDistance::Cosine => f64_values.push(value),
            VectorDistance::L2 | VectorDistance::Dot => {
                f32_values.push(value.map(|value| value as f32));
            }
        }
    }
    Ok(match distance {
        VectorDistance::Cosine => Arc::new(Float64Array::from(f64_values)),
        VectorDistance::L2 | VectorDistance::Dot => Arc::new(Float32Array::from(f32_values)),
    })
}

struct VectorRow<'a> {
    values: &'a Float32Array,
    range: Range<usize>,
}

fn vector_row(value: &RegValue, row: usize, rows: usize) -> Result<Option<VectorRow<'_>>> {
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

fn compute_row(
    lhs: VectorRow<'_>,
    rhs: VectorRow<'_>,
    row: usize,
    distance: VectorDistance,
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
    let mut squared_distance = 0.0f64;
    for (left_index, right_index) in lhs.range.zip(rhs.range) {
        if lhs.values.is_null(left_index) || rhs.values.is_null(right_index) {
            return Ok(None);
        }
        let left = f64::from(lhs.values.value(left_index));
        let right = f64::from(rhs.values.value(right_index));
        dot += left * right;
        norm_lhs += left * left;
        norm_rhs += right * right;
        let delta = left - right;
        squared_distance += delta * delta;
    }
    Ok(Some(match distance {
        VectorDistance::Cosine => {
            let denominator = norm_lhs.sqrt() * norm_rhs.sqrt();
            if denominator == 0.0 {
                0.0
            } else {
                1.0 - dot / denominator
            }
        }
        VectorDistance::L2 => squared_distance.sqrt(),
        VectorDistance::Dot => dot,
    }))
}

#[cfg(test)]
#[path = "mapped_tests.rs"]
mod tests;
