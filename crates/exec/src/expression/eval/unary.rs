use common::Result;
use common::protocol::{ExprKind, PhysicalExpr, UnaryExprOp};

use crate::expression::kernels as ops;
use crate::expression::value::ExpressionValue;

use super::ExpressionEvaluationContext;

pub(super) fn evaluate_unary(
    physical: &PhysicalExpr,
    context: &dyn ExpressionEvaluationContext,
) -> Result<ExpressionValue> {
    let ExprKind::Unary { op, expr } = &physical.kind else {
        return Err(error::VeinError::internal("unary kernel received another expression kind"));
    };
    let value = context.evaluate(expr)?;
    let rows = context.input().num_rows();
    let result = match op {
        UnaryExprOp::Negate => {
            let data_type = value.data_type().clone();
            ops::eval_mapped_unary_value(
                &value,
                rows,
                |length| Ok(arrow::array::new_null_array(&data_type, length)),
                ops::eval_neg,
            )?
        }
        UnaryExprOp::Not => ops::eval_mapped_unary_predicate(&value, rows, None, ops::eval_not)?,
        UnaryExprOp::IsNull => {
            ops::eval_mapped_unary_predicate(&value, rows, Some(true), ops::eval_is_null)?
        }
        UnaryExprOp::IsNotNull => {
            ops::eval_mapped_unary_predicate(&value, rows, Some(false), ops::eval_is_not_null)?
        }
    };
    Ok(super::normalize_scalar(result, value.is_scalar()))
}
