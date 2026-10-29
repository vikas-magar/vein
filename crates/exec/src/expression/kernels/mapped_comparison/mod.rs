//! Comparisons over fragmented references with one output bitmap per batch.
//! Contiguous runs and unsupported layouts retain Arrow's vector kernels.
use crate::expression::value::ExpressionValue;
use arrow::array::*;
use arrow::datatypes::{ArrowPrimitiveType, DataType};
use common::protocol::BinaryExprOp;
use error::{Result, VeinError};
use std::cmp::Ordering;
use std::sync::Arc;
mod ordered;

pub(crate) fn evaluate(
    op: BinaryExprOp, left: &ExpressionValue, right: &ExpressionValue, rows: usize,
) -> Result<ExpressionValue> {
    let kernel = match op {
        BinaryExprOp::Equal => super::eval_cmp_eq,
        BinaryExprOp::NotEqual => super::eval_cmp_neq,
        BinaryExprOp::Less => super::eval_cmp_lt,
        BinaryExprOp::LessEqual => super::eval_cmp_lte,
        BinaryExprOp::Greater => super::eval_cmp_gt,
        BinaryExprOp::GreaterEqual => super::eval_cmp_gte,
        _ => return Err(VeinError::internal("ordered comparison received a non-comparison operator")),
    };
    let fragmented = rows > 1 && [left, right].into_iter().any(|value| {
        value.is_mapped() && value.segment(0, rows, rows).is_ok_and(|run| run.len() < rows)
    });
    if fragmented && left.data_type() == right.data_type() {
        if let Some(result) = ordered::dispatch(op, left, right, rows) {
            tracing::debug!(target: "vein.audit", event = "reference_comparison_batch",
                rows, data_type = ?left.data_type(), output_arrays = 1,
                input_payload_bytes_copied = 0, "compared fragmented inputs as one batch");
            return result;
        }
    }
    super::eval_mapped_binary_value(left, right, rows, kernel)
}

fn matches(op: BinaryExprOp, ordering: Ordering) -> bool {
    match op {
        BinaryExprOp::Equal => ordering.is_eq(),
        BinaryExprOp::NotEqual => !ordering.is_eq(),
        BinaryExprOp::Less => ordering.is_lt(),
        BinaryExprOp::LessEqual => !ordering.is_gt(),
        BinaryExprOp::Greater => ordering.is_gt(),
        BinaryExprOp::GreaterEqual => !ordering.is_lt(),
        _ => unreachable!("comparison dispatch validates its operator"),
    }
}

#[cfg(test)]
mod tests;
