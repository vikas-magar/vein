//! AI Vector distance execution kernels for the Vector VM.
//!
//! Evaluates [`MicroOp::VectorCosineDistance`] and [`MicroOp::VectorL2Distance`]
//! on embedded vector representations ([`arrow::array::FixedSizeListArray`] and [`arrow::array::ListArray`]).

use std::sync::Arc;
use arrow::array::{Array, ArrayRef, FixedSizeListArray, Float32Array, ListArray, ListLikeArray};
use common::Result;
use error::VeinError;

/// Helper function to extract a float slice for row `idx` from a list or fixed-size list array.
fn get_vector_slice<'a>(array: &'a ArrayRef, idx: usize) -> Result<&'a [f32]> {
    if let Some(fixed) = array.as_any().downcast_ref::<FixedSizeListArray>() {
        let values = fixed
            .values()
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| VeinError::internal("FixedSizeList elements must be Float32"))?;
        let size = fixed.value_length() as usize;
        let start = idx * size;
        let end = start + size;
        if end > values.len() {
            return Err(VeinError::internal("Vector index out of bounds in FixedSizeList"));
        }
        Ok(&values.values()[start..end])
    } else if let Some(list) = array.as_any().downcast_ref::<ListArray>() {
        let values = list
            .values()
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| VeinError::internal("List elements must be Float32"))?;
        let offsets = list.value_offsets();
        let start = offsets[idx] as usize;
        let end = offsets[idx + 1] as usize;
        Ok(&values.values()[start..end])
    } else {
        Err(VeinError::internal(format!(
            "Expected FixedSizeList or List of Float32, found {:?}",
            array.data_type()
        )))
    }
}

/// Evaluates row-wise cosine distance: `1.0 - (A · B) / (||A|| * ||B||)`.
pub fn eval_cosine_distance(lhs: &ArrayRef, rhs: &ArrayRef) -> Result<ArrayRef> {
    let len = lhs.len();
    if len != rhs.len() {
        return Err(VeinError::internal(format!(
            "Cosine distance array lengths mismatch: {} vs {}",
            len,
            rhs.len()
        )));
    }

    let mut distances = Vec::with_capacity(len);
    for i in 0..len {
        if lhs.is_null(i) || rhs.is_null(i) {
            distances.push(None);
            continue;
        }
        let (a, a_range) = get_vector_range(lhs, i)?;
        let (b, b_range) = get_vector_range(rhs, i)?;
        if a_range.len() != b_range.len() {
            return Err(VeinError::internal(format!(
                "Vector embedding dimension mismatch at row {i}: {} vs {}",
                a_range.len(),
                b_range.len()
            )));
        }

        let mut dot = 0.0f64;
        let mut norm_a = 0.0f64;
        let mut norm_b = 0.0f64;
        let mut has_null_element = false;

        for (a_index, b_index) in a_range.zip(b_range) {
            if a.is_null(a_index) || b.is_null(b_index) {
                has_null_element = true;
                break;
            }
            let v1 = f64::from(a.value(a_index));
            let v2 = f64::from(b.value(b_index));
            dot += v1 * v2;
            norm_a += v1 * v1;
            norm_b += v2 * v2;
        }

        if has_null_element {
            distances.push(None);
            continue;
        }
        let denom = norm_a.sqrt() * norm_b.sqrt();
        let dist = if denom == 0.0 {
            0.0
        } else {
            1.0 - (dot / denom)
        };
        distances.push(Some(dist));
    }

    Ok(Arc::new(arrow::array::Float64Array::from(distances)))
}

/// Returns the child values and the range for one list row without copying its
/// vector elements. Arrow's list-like range accounts for sliced parent arrays.
fn get_vector_range(array: &ArrayRef, idx: usize) -> Result<(&Float32Array, std::ops::Range<usize>)> {
    let (values, range) = if let Some(fixed) = array.as_any().downcast_ref::<FixedSizeListArray>() {
        (fixed.values(), fixed.element_range(idx))
    } else if let Some(list) = array.as_any().downcast_ref::<ListArray>() {
        (list.values(), list.element_range(idx))
    } else {
        return Err(VeinError::internal(format!(
            "Expected FixedSizeList or List of Float32, found {:?}",
            array.data_type()
        )));
    };
    let values = values
        .as_any()
        .downcast_ref::<Float32Array>()
        .ok_or_else(|| VeinError::internal("Vector elements must be Float32"))?;
    Ok((values, range))
}

/// Evaluates row-wise L2 (Euclidean) distance: `sqrt(sum((A_i - B_i)^2))`.
pub fn eval_l2_distance(lhs: &ArrayRef, rhs: &ArrayRef) -> Result<ArrayRef> {
    let len = lhs.len();
    if len != rhs.len() {
        return Err(VeinError::internal(format!(
            "L2 distance array lengths mismatch: {} vs {}",
            len,
            rhs.len()
        )));
    }

    let mut distances = Vec::with_capacity(len);
    for i in 0..len {
        let a = get_vector_slice(lhs, i)?;
        let b = get_vector_slice(rhs, i)?;
        if a.len() != b.len() {
            return Err(VeinError::internal(format!(
                "Vector embedding dimension mismatch at row {i}: {} vs {}",
                a.len(),
                b.len()
            )));
        }

        let mut sum_sq = 0.0f32;
        for (v1, v2) in a.iter().zip(b.iter()) {
            let diff = v1 - v2;
            sum_sq += diff * diff;
        }
        distances.push(sum_sq.sqrt());
    }

    Ok(Arc::new(Float32Array::from(distances)))
}

/// Evaluates row-wise vector dot product: `sum(A_i * B_i)`.
pub fn eval_dot_product(lhs: &ArrayRef, rhs: &ArrayRef) -> Result<ArrayRef> {
    let len = lhs.len();
    if len != rhs.len() {
        return Err(VeinError::internal(format!(
            "Dot product array lengths mismatch: {} vs {}",
            len,
            rhs.len()
        )));
    }

    let mut products = Vec::with_capacity(len);
    for i in 0..len {
        let a = get_vector_slice(lhs, i)?;
        let b = get_vector_slice(rhs, i)?;
        if a.len() != b.len() {
            return Err(VeinError::internal(format!(
                "Vector embedding dimension mismatch at row {i}: {} vs {}",
                a.len(),
                b.len()
            )));
        }

        let mut dot = 0.0f32;
        for (v1, v2) in a.iter().zip(b.iter()) {
            dot += v1 * v2;
        }
        products.push(dot);
    }

    Ok(Arc::new(Float32Array::from(products)))
}

#[cfg(test)]
mod tests;
