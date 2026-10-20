use std::sync::Arc;

use arrow::array::{Array, ArrayRef, UInt32Array, new_empty_array, new_null_array};
use arrow::compute::take;
use common::Result;
use common::protocol::{ExprKind, PhysicalExpr};
use error::VeinError;

use crate::expression::kernels as ops;
use crate::expression::value::ExpressionValue;
use crate::udf::MappedUdfInput;

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
    let mapped = values.iter().map(|value| MappedUdfInput::new(value, rows))
        .collect::<Vec<_>>();
    if let Some(result) = context.functions().invoke_mapped(name, &mapped, rows)? {
        return Ok(output_value(result, rows));
    }
    if values.iter().any(ExpressionValue::is_mapped) {
        return Err(VeinError::unsupported(
            format!("function {name} must support reference-backed expression inputs"), None,
        ));
    }
    let arguments = values.iter().map(|value| dense_argument(value, rows))
        .collect::<Result<Vec<_>>>()?;
    Ok(output_value(context.functions().invoke(name, &arguments)?, rows))
}

fn output_value(array: ArrayRef, rows: usize) -> ExpressionValue {
    if array.len() == 1 && rows != 1 {
        ExpressionValue::Scalar(array)
    } else {
        ExpressionValue::Array(array)
    }
}

fn dense_argument(value: &ExpressionValue, rows: usize) -> Result<ArrayRef> {
    match value {
        ExpressionValue::Array(array) if array.len() == rows => Ok(Arc::clone(array)),
        ExpressionValue::Scalar(array) if array.len() == 1 => broadcast(array, rows),
        ExpressionValue::Segments(segments) if segments.len() == rows => segments.materialized_array(),
        _ => Err(VeinError::internal("function argument has an invalid dense shape")),
    }
}

pub(crate) fn broadcast(array: &ArrayRef, rows: usize) -> Result<ArrayRef> {
    if rows == 0 { return Ok(new_empty_array(array.data_type())); }
    if array.is_null(0) { return Ok(new_null_array(array.data_type(), rows)); }
    if rows == 1 { return Ok(Arc::clone(array)); }
    let indices = UInt32Array::from(vec![0; rows]);
    take(array.as_ref(), &indices, None)
        .map_err(|error| VeinError::internal(format!("broadcast scalar failed: {error}")))
}
