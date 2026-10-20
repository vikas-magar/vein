use common::Result;
use common::protocol::{ExprKind, PhysicalExpr};
use error::VeinError;

use crate::expression::kernels as ops;
use crate::expression::value::ExpressionValue;
use crate::udf::FunctionInput;

use super::ExpressionEvaluationContext;

pub(super) fn evaluate_in_list(
    expression: &PhysicalExpr,
    context: &dyn ExpressionEvaluationContext,
) -> Result<ExpressionValue> {
    let ExprKind::InList { expr, candidates, negated } = &expression.kind else {
        return Err(VeinError::internal("IN kernel received another expression kind"));
    };
    let value = context.evaluate(expr)?;
    let candidates = candidates.iter().map(|item| context.evaluate(item))
        .collect::<Result<Vec<_>>>()?;
    ops::eval_in_list_value(&value, &candidates, context.input().num_rows(), *negated)
}

pub(super) fn evaluate_like(
    expression: &PhysicalExpr,
    context: &dyn ExpressionEvaluationContext,
) -> Result<ExpressionValue> {
    let ExprKind::Like { expr, pattern, negated } = &expression.kind else {
        return Err(VeinError::internal("LIKE kernel received another expression kind"));
    };
    let value = context.evaluate(expr)?;
    let pattern = context.evaluate(pattern)?;
    let result = ops::eval_mapped_boolean_binary(
        &value,
        &pattern,
        context.input().num_rows(),
        |left, right| ops::eval_like(left, right, *negated),
    )?;
    Ok(super::normalize_scalar(result, value.is_scalar() && pattern.is_scalar()))
}

pub(super) fn evaluate_case(
    expression: &PhysicalExpr,
    context: &dyn ExpressionEvaluationContext,
) -> Result<ExpressionValue> {
    let ExprKind::Case { when_then, else_expr } = &expression.kind else {
        return Err(VeinError::internal("CASE kernel received another expression kind"));
    };
    let values = when_then.iter().map(|(condition, value)| {
        Ok((context.evaluate(condition)?, context.evaluate(value)?))
    }).collect::<Result<Vec<_>>>()?;
    let (conditions, results): (Vec<_>, Vec<_>) = values.into_iter().unzip();
    let otherwise = else_expr.as_deref().map(|value| context.evaluate(value)).transpose()?;
    ops::eval_case_value(
        &conditions,
        &results,
        otherwise.as_ref(),
        &expression.return_type.to_arrow(),
        context.input().num_rows(),
    )
}

pub(super) fn evaluate_call(
    expression: &PhysicalExpr,
    context: &dyn ExpressionEvaluationContext,
) -> Result<ExpressionValue> {
    let ExprKind::Call { name, args } = &expression.kind else {
        return Err(VeinError::internal("function kernel received another expression kind"));
    };
    let values = args.iter().map(|argument| context.evaluate(argument))
        .collect::<Result<Vec<_>>>()?;
    let rows = context.input().num_rows();
    let arguments = values.iter().map(|value| FunctionInput::new(value, rows))
        .collect::<Vec<_>>();
    Ok(context
        .functions()
        .invoke(name, &arguments, rows)?
        .into_expression_value())
}
