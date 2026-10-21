//! Predicate kernels over source-backed expression values.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray};
use common::Result;

use crate::expression::value::ExpressionValue;

/// Evaluates an Arrow unary predicate over mapped rows without gathering values.
/// `null_extension` is the Boolean result for an outer-join row; `None` means SQL NULL.
pub(crate) fn eval_mapped_unary_predicate(
    value: &ExpressionValue,
    row_count: usize,
    null_extension: Option<bool>,
    kernel: impl FnMut(&dyn Array) -> Result<ArrayRef>,
) -> Result<ExpressionValue> {
    super::eval_mapped_unary_value(
        value,
        row_count,
        |len| Ok(Arc::new(BooleanArray::from(vec![null_extension; len]))),
        kernel,
    )
}

/// Evaluates a Boolean Arrow kernel over mapped inputs without gathering source values.
///
/// A run is a maximal contiguous range from the same source array. Fragmented
/// mappings invoke the vector kernel per run. Missing input runs become typed
/// Arrow NULL arrays, leaving SQL three-valued logic to the kernel.
pub(crate) fn eval_mapped_boolean_binary(
    lhs: &ExpressionValue,
    rhs: &ExpressionValue,
    row_count: usize,
    kernel: impl FnMut(&dyn arrow::array::Datum, &dyn arrow::array::Datum) -> Result<ArrayRef>,
) -> Result<ExpressionValue> {
    super::eval_mapped_binary_value(lhs, rhs, row_count, kernel)
}
