use std::sync::Arc;

use common::Result;
use common::protocol::{ExprKind, PhysicalExpr};
use error::VeinError;

use crate::expression::kernels;
use crate::expression::value::ExpressionValue;

use super::complex;
use super::kernel::{
    ExpressionEvaluationContext, ExpressionKernel, ExpressionKernelFn, ExpressionNodeKind,
};
use super::{binary, unary};

pub(super) fn builtin_kernels() -> Vec<Arc<dyn ExpressionKernel>> {
    use ExpressionNodeKind::*;
    vec![
        Arc::new(ExpressionKernelFn::new(Column, evaluate_column)),
        Arc::new(ExpressionKernelFn::new(Literal, evaluate_literal)),
        Arc::new(ExpressionKernelFn::new(Binary, binary::evaluate_binary)),
        Arc::new(ExpressionKernelFn::new(Cast, evaluate_cast)),
        Arc::new(ExpressionKernelFn::new(Unary, unary::evaluate_unary)),
        Arc::new(ExpressionKernelFn::new(InList, complex::evaluate_in_list)),
        Arc::new(ExpressionKernelFn::new(Like, complex::evaluate_like)),
        Arc::new(ExpressionKernelFn::new(Case, complex::evaluate_case)),
        Arc::new(ExpressionKernelFn::new(Call, complex::evaluate_call)),
    ]
}

fn evaluate_column(
    expression: &PhysicalExpr,
    context: &dyn ExpressionEvaluationContext,
) -> Result<ExpressionValue> {
    let ExprKind::Column { index } = &expression.kind else {
        return Err(VeinError::internal("column kernel received another expression kind"));
    };
    let index = usize::try_from(*index)
        .map_err(|_| VeinError::planning("expression column ordinal is too large", None))?;
    let input = context.input();
    if index >= input.num_columns() {
        return Err(VeinError::internal(format!(
            "expression column {index} is outside the {}-column input view",
            input.num_columns()
        )));
    }
    ExpressionValue::view_column(Arc::clone(input), index)
}

fn evaluate_literal(
    expression: &PhysicalExpr,
    _context: &dyn ExpressionEvaluationContext,
) -> Result<ExpressionValue> {
    let ExprKind::Literal(value) = &expression.kind else {
        return Err(VeinError::internal("literal kernel received another expression kind"));
    };
    Ok(ExpressionValue::Scalar(kernels::literal_to_scalar(value)?))
}

fn evaluate_cast(
    expression: &PhysicalExpr,
    context: &dyn ExpressionEvaluationContext,
) -> Result<ExpressionValue> {
    let ExprKind::Cast { expr, target_type } = &expression.kind else {
        return Err(VeinError::internal("cast kernel received another expression kind"));
    };
    let value = context.evaluate(expr)?;
    let result = kernels::mapped_cast::evaluate(&value, target_type, context.input().num_rows())?;
    Ok(super::normalize_scalar(result, value.is_scalar()))
}
