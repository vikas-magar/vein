//! AI Vector distance execution kernels for the Vector VM.
//!
//! Evaluates [`MicroOp::VectorCosineDistance`] and [`MicroOp::VectorL2Distance`]
//! on embedded vector representations ([`arrow::array::FixedSizeListArray`] and [`arrow::array::ListArray`]).

use std::sync::Arc;
use arrow::array::ArrayRef;
use common::Result;
use crate::vm::register::RegValue;

mod mapped;
pub(crate) use mapped::{VectorDistance, eval_mapped_vector_distance};

/// Evaluates row-wise cosine distance: `1.0 - (A · B) / (||A|| * ||B||)`.
pub fn eval_cosine_distance(lhs: &ArrayRef, rhs: &ArrayRef) -> Result<ArrayRef> {
    eval_dense(lhs, rhs, VectorDistance::Cosine)
}

/// Evaluates row-wise L2 (Euclidean) distance: `sqrt(sum((A_i - B_i)^2))`.
pub fn eval_l2_distance(lhs: &ArrayRef, rhs: &ArrayRef) -> Result<ArrayRef> {
    eval_dense(lhs, rhs, VectorDistance::L2)
}

/// Evaluates row-wise vector dot product: `sum(A_i * B_i)`.
pub fn eval_dot_product(lhs: &ArrayRef, rhs: &ArrayRef) -> Result<ArrayRef> {
    eval_dense(lhs, rhs, VectorDistance::Dot)
}

fn eval_dense(lhs: &ArrayRef, rhs: &ArrayRef, distance: VectorDistance) -> Result<ArrayRef> {
    eval_mapped_vector_distance(
        &RegValue::Array(Arc::clone(lhs)),
        &RegValue::Array(Arc::clone(rhs)),
        lhs.len(),
        distance,
    )
}

#[cfg(test)]
mod tests;
