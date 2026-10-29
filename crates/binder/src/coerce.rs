//! Implicit type coercion shared by every binary/comparison and set-op
//! binding path.
//!
//! A single reference implementation (`coerce_types`) prevents the
//! "left-type-fallback" divergence that plagued ad-hoc binder code: every
//! unifiable pair is handled here, every non-unifiable pair surfaces as a
//! `Semantic` error instead of a silent type lie.

use error::{Result, VeinError};
use parser::ast::expr::BinaryOp;
use plan::LogicalExpr;
use types::{VeinDataType, unify_types};

/// Resolves the common supertype of two expressions, injecting `Cast` nodes
/// where implicit coercion is required.
///
/// The subset mirrors GoogleSQL conversion rules:
/// - identical types pass through unchanged;
/// - `Null` unifies with anything (no cast is injected);
/// - numeric widening follows the integer family `Int8 → Int16 → Int32 →
///   Int64` and then `→ Float32 → Float64`;
/// - `Utf8`/`StringView` unify with `Null` (returning the string type);
/// - string-family members unify symmetrically to the widest member;
/// - `Date32` unifies with `Timestamp` (returning `Timestamp`, preserving
///   the other side's timezone).
///
/// Anything else is a `Semantic` error: there is no silent fallback.
pub fn coerce_types(
    left: LogicalExpr,
    right: LogicalExpr,
) -> Result<(LogicalExpr, LogicalExpr, VeinDataType)> {
    let left_type = left.data_type();
    let right_type = right.data_type();
    let common = unify_types(&left_type, &right_type)?;

    let (left, right) = coerce_to_common_type(left, right, &left_type, &right_type, &common);
    Ok((left, right, common))
}

fn coerce_to_common_type(
    left: LogicalExpr,
    right: LogicalExpr,
    left_type: &VeinDataType,
    right_type: &VeinDataType,
    common: &VeinDataType,
) -> (LogicalExpr, LogicalExpr) {
    // A `Null` operand is never wrapped in a cast; it simply adopts the other
    // side's type. Any other differing operand is coerced to the common type.
    let left = if left_type == &VeinDataType::Null {
        left
    } else {
        cast_to(left, (*common).clone())
    };
    let right = if right_type == &VeinDataType::Null {
        right
    } else {
        cast_to(right, (*common).clone())
    };

    (left, right)
}

/// Applies operator-specific temporal arithmetic rules before falling back to
/// ordinary common-type coercion.
pub fn coerce_arithmetic(
    left: LogicalExpr,
    right: LogicalExpr,
    op: BinaryOp,
) -> Result<(LogicalExpr, LogicalExpr, VeinDataType)> {
    let left_type = left.data_type();
    let right_type = right.data_type();
    let is_temporal = |data_type: &VeinDataType| {
        matches!(
            data_type,
            VeinDataType::Date32 | VeinDataType::Date64 | VeinDataType::Timestamp(_, _)
        )
    };

    if matches!(op, BinaryOp::Plus | BinaryOp::Minus)
        && matches!(&right_type, VeinDataType::Interval(_))
        && is_temporal(&left_type)
    {
        return Ok((left, right, left_type));
    }

    // Arrow's vectorized temporal kernels require the temporal array on the
    // left, so normalize the commutative `INTERVAL + DATE` form here.
    if op == BinaryOp::Plus
        && matches!(&left_type, VeinDataType::Interval(_))
        && is_temporal(&right_type)
    {
        return Ok((right, left, right_type));
    }

    if op == BinaryOp::Multiply
        && let Some((left_type, right_type)) =
            types::coercion::decimal_multiplication_operands(&left_type, &right_type)
    {
        let result_type = types::coercion::decimal_arithmetic_result_type(
            types::coercion::DecimalArithmeticOp::Multiply,
            &left_type,
            &right_type,
        )?
        .ok_or_else(|| VeinError::internal("decimal operands have no arithmetic result type"))?;
        return Ok((
            cast_to(left, left_type),
            cast_to(right, right_type),
            result_type,
        ));
    }

    let common_type = unify_types(&left_type, &right_type)?;
    if op == BinaryOp::Divide
        && (common_type.is_int128() || matches!(
            &common_type,
            VeinDataType::Decimal128(_, _) | VeinDataType::Decimal256(_, _)
        ))
    {
        let result_type = VeinDataType::Float64;
        return Ok((
            cast_to(left, result_type.clone()),
            cast_to(right, result_type.clone()),
            result_type,
        ));
    }

    let (left, right) = coerce_to_common_type(left, right, &left_type, &right_type, &common_type);
    let decimal_op = match op {
        BinaryOp::Plus => Some(types::coercion::DecimalArithmeticOp::Add),
        BinaryOp::Minus => Some(types::coercion::DecimalArithmeticOp::Subtract),
        BinaryOp::Multiply => Some(types::coercion::DecimalArithmeticOp::Multiply),
        BinaryOp::Divide => Some(types::coercion::DecimalArithmeticOp::Divide),
        BinaryOp::Modulo => Some(types::coercion::DecimalArithmeticOp::Remainder),
        _ => None,
    };
    let result_type = match decimal_op {
        Some(decimal_op) => types::coercion::decimal_arithmetic_result_type(
            decimal_op,
            &left.data_type(),
            &right.data_type(),
        )?
        .unwrap_or(common_type),
        None => common_type,
    };

    Ok((left, right, result_type))
}

/// Wraps `expr` in a `Cast` if its type differs from `target` (no-op otherwise).
fn cast_to(expr: LogicalExpr, target: VeinDataType) -> LogicalExpr {
    if expr.data_type() == target {
        expr
    } else {
        LogicalExpr::Cast {
            expr: Box::new(expr),
            target_type: target,
        }
    }
}

#[cfg(test)]
#[allow(missing_docs)]
mod tests;
