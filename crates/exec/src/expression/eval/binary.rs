use arrow::array::{Array, ArrayRef, Datum, UInt32Array, new_empty_array, new_null_array};
use arrow::compute::take;
use arrow::compute::kernels::concat_elements::concat_elements_dyn;
use common::Result;
use common::protocol::{BinaryExprOp, ExprKind, PhysicalExpr};
use error::VeinError;

use crate::expression::kernels as ops;
use crate::expression::value::ExpressionValue;

use super::ExpressionEvaluationContext;

pub(super) fn evaluate_binary(
    expression: &PhysicalExpr,
    context: &dyn ExpressionEvaluationContext,
) -> Result<ExpressionValue> {
    let ExprKind::Binary { op, left, right } = &expression.kind else {
        return Err(VeinError::internal("binary kernel received another expression kind"));
    };
    let lhs = context.evaluate(left)?;
    let rhs = context.evaluate(right)?;
    let rows = context.input().num_rows();
    let result = match op {
        BinaryExprOp::Add => ops::eval_mapped_binary_value(&lhs, &rhs, rows, ops::eval_add)?,
        BinaryExprOp::Subtract => ops::eval_mapped_binary_value(&lhs, &rhs, rows, ops::eval_sub)?,
        BinaryExprOp::Multiply => ops::eval_mapped_binary_value(&lhs, &rhs, rows, ops::eval_mul)?,
        BinaryExprOp::Divide => ops::eval_mapped_binary_value(&lhs, &rhs, rows, ops::eval_div)?,
        BinaryExprOp::Modulo => ops::eval_mapped_binary_value(&lhs, &rhs, rows, ops::eval_mod)?,
        BinaryExprOp::Concat => ops::eval_mapped_binary_value(&lhs, &rhs, rows, concat)?,
        BinaryExprOp::Equal | BinaryExprOp::NotEqual | BinaryExprOp::Less
        | BinaryExprOp::LessEqual | BinaryExprOp::Greater | BinaryExprOp::GreaterEqual =>
            ops::mapped_comparison::evaluate(*op, &lhs, &rhs, rows)?,
        BinaryExprOp::And => ops::eval_mapped_boolean_binary(&lhs, &rhs, rows, ops::eval_and_datum)?,
        BinaryExprOp::Or => ops::eval_mapped_boolean_binary(&lhs, &rhs, rows, ops::eval_or_datum)?,
    };
    let result = if expression.return_type.is_int128() {
        ops::eval_mapped_unary_value(&result, rows,
            |len| Ok(new_null_array(&types::wide_integer::INT128_STORAGE, len)),
            ops::wide_integer::normalize)?
    } else { result };
    Ok(super::normalize_scalar(result, lhs.is_scalar() && rhs.is_scalar()))
}

fn concat(left: &dyn Datum, right: &dyn Datum) -> Result<ArrayRef> {
    let (left_array, left_scalar) = left.get();
    let (right_array, right_scalar) = right.get();
    let left_expanded = expand_scalar(left_array, left_scalar, right_array.len(), right_scalar)?;
    let right_expanded = expand_scalar(right_array, right_scalar, left_array.len(), left_scalar)?;
    let left = left_expanded.as_deref().unwrap_or(left_array);
    let right = right_expanded.as_deref().unwrap_or(right_array);
    concat_elements_dyn(left, right)
        .map_err(|error| VeinError::internal(format!("string concatenation failed: {error}")))
}

fn expand_scalar(
    array: &dyn Array,
    scalar: bool,
    other_len: usize,
    other_scalar: bool,
) -> Result<Option<ArrayRef>> {
    if !scalar || other_scalar || other_len == 1 {
        return Ok(None);
    }
    if array.len() != 1 {
        return Err(VeinError::internal("scalar concatenation operand must have one value"));
    }
    if other_len == 0 {
        return Ok(Some(new_empty_array(array.data_type())));
    }
    if array.is_null(0) {
        return Ok(Some(new_null_array(array.data_type(), other_len)));
    }
    let indices = UInt32Array::from(vec![0; other_len]);
    take(array, &indices, None)
        .map(Some)
        .map_err(|error| VeinError::internal(format!("broadcast concat scalar failed: {error}")))
}
