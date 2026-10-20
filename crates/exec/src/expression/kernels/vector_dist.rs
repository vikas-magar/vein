//! Vector distance kernels for typed scalar function calls.

use crate::expression::value::ExpressionValue;
use arrow::array::ArrayRef;
use common::Result;
use std::sync::Arc;

mod mapped;
use mapped::eval_mapped_cosine_distance;

/// Evaluates row-wise cosine distance: `1.0 - (A · B) / (||A|| * ||B||)`.
pub fn eval_cosine_distance(lhs: &ArrayRef, rhs: &ArrayRef) -> Result<ArrayRef> {
    eval_mapped_cosine_distance(
        &ExpressionValue::Array(Arc::clone(lhs)),
        &ExpressionValue::Array(Arc::clone(rhs)),
        lhs.len(),
    )
}

#[cfg(test)]
mod tests;
