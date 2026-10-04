//! Implicit type coercion shared by every binary/comparison and set-op
//! binding path.
//!
//! A single reference implementation (`coerce_types`) prevents the
//! "left-type-fallback" divergence that plagued ad-hoc binder code: every
//! unifiable pair is handled here, every non-unifiable pair surfaces as a
//! `Semantic` error instead of a silent type lie.

use plan::LogicalExpr;
use types::{unify_types, VeinDataType};
use error::Result;
use parser::ast::expr::BinaryOp;

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

    // A `Null` operand is never wrapped in a cast; it simply adopts the other
    // side's type. Any other differing operand is coerced to the common type.
    let left = if left_type == VeinDataType::Null {
        left
    } else {
        cast_to(left, common.clone())
    };
    let right = if right_type == VeinDataType::Null {
        right
    } else {
        cast_to(right, common.clone())
    };

    Ok((left, right, common))
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

    coerce_types(left, right)
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
mod tests {
    use super::*;
    use error::VeinError;
    use plan::LogicalLiteral;

    fn lit(t: VeinDataType) -> LogicalExpr {
        LogicalExpr::Literal(LogicalLiteral::Null(t))
    }

    #[test]
    fn identical_types_pass_through() {
        let a = lit(VeinDataType::Int64);
        let b = lit(VeinDataType::Int64);
        let (l, r, t) = coerce_types(a.clone(), b.clone()).unwrap();
        assert_eq!(l, a);
        assert_eq!(r, b);
        assert_eq!(t, VeinDataType::Int64);
    }

    #[test]
    fn widens_int32_to_int64() {
        let (_, _, t) = coerce_types(lit(VeinDataType::Int32), lit(VeinDataType::Int64)).unwrap();
        assert_eq!(t, VeinDataType::Int64);
        let (l, _, _) = coerce_types(lit(VeinDataType::Int32), lit(VeinDataType::Int64)).unwrap();
        assert!(matches!(
            l,
            LogicalExpr::Cast {
                target_type: VeinDataType::Int64,
                ..
            }
        ));
    }

    #[test]
    fn null_unifies_without_cast() {
        let (l, r, t) = coerce_types(lit(VeinDataType::Null), lit(VeinDataType::Utf8)).unwrap();
        assert!(matches!(l, LogicalExpr::Literal(_)));
        assert!(matches!(r, LogicalExpr::Literal(_)));
        assert_eq!(t, VeinDataType::Utf8);
    }

    #[test]
    fn date_promotes_to_timestamp() {
        let (_, _, t) = coerce_types(
            lit(VeinDataType::Date32),
            lit(VeinDataType::Timestamp(types::TimeUnit::Microsecond, None)),
        )
            .unwrap();
        assert_eq!(t, VeinDataType::Timestamp(types::TimeUnit::Microsecond, None));
    }

    #[test]
    fn date_interval_arithmetic_preserves_temporal_type_and_normalizes_addition() {
        let date = lit(VeinDataType::Date32);
        let interval = lit(VeinDataType::Interval(types::IntervalUnit::MonthDayNano));

        let (left, right, result) =
            coerce_arithmetic(date.clone(), interval.clone(), BinaryOp::Minus).unwrap();
        assert_eq!((left.data_type(), right.data_type(), result), (
            VeinDataType::Date32,
            VeinDataType::Interval(types::IntervalUnit::MonthDayNano),
            VeinDataType::Date32,
        ));

        let (left, right, result) =
            coerce_arithmetic(interval, date, BinaryOp::Plus).unwrap();
        assert_eq!((left.data_type(), right.data_type(), result), (
            VeinDataType::Date32,
            VeinDataType::Interval(types::IntervalUnit::MonthDayNano),
            VeinDataType::Date32,
        ));
    }

    #[test]
    fn interval_minus_date_is_not_supported() {
        assert!(coerce_arithmetic(
            lit(VeinDataType::Interval(types::IntervalUnit::MonthDayNano)),
            lit(VeinDataType::Date32),
            BinaryOp::Minus,
        )
            .is_err());
    }

    #[test]
    fn date_promotes_preserving_timezone() {
        let (l, _, t) = coerce_types(
            lit(VeinDataType::Date32),
            lit(VeinDataType::Timestamp(types::TimeUnit::Nanosecond, Some("UTC".to_string()))),
        )
            .unwrap();
        assert_eq!(t, VeinDataType::Timestamp(types::TimeUnit::Nanosecond, Some("UTC".to_string())));
        assert!(matches!(
            l,
            LogicalExpr::Cast {
                target_type: VeinDataType::Timestamp(types::TimeUnit::Nanosecond, Some(ref tz)),
                ..
            } if tz == "UTC"
        ));
    }

    #[test]
    fn timestamp_promotes_date_to_its_timezone() {
        let (_, r, t) = coerce_types(
            lit(VeinDataType::Timestamp(
                types::TimeUnit::Nanosecond,
                Some("America/New_York".to_string()),
            )),
            lit(VeinDataType::Date32),
        )
            .unwrap();
        assert_eq!(
            t,
            VeinDataType::Timestamp(
                types::TimeUnit::Nanosecond,
                Some("America/New_York".to_string()),
            )
        );
        assert!(matches!(
            r,
            LogicalExpr::Cast {
                target_type: VeinDataType::Timestamp(
                    types::TimeUnit::Nanosecond,
                    Some(ref tz),
                ),
                ..
            } if tz == "America/New_York"
        ));
    }

    #[test]
    fn string_family_is_order_independent() {
        let (l, r, t) =
            coerce_types(lit(VeinDataType::Utf8), lit(VeinDataType::StringView)).unwrap();
        assert_eq!(t, VeinDataType::StringView);
        assert!(matches!(
            l,
            LogicalExpr::Cast {
                target_type: VeinDataType::StringView,
                ..
            }
        ));
        assert!(matches!(r, LogicalExpr::Literal(_)));

        // Same pair, reversed arguments, must yield the same common type.
        let (_, _, t) =
            coerce_types(lit(VeinDataType::StringView), lit(VeinDataType::Utf8)).unwrap();
        assert_eq!(t, VeinDataType::StringView);
    }

    #[test]
    fn large_utf8_is_widest_string() {
        let (l, _, t) =
            coerce_types(lit(VeinDataType::Utf8), lit(VeinDataType::LargeUtf8)).unwrap();
        assert_eq!(t, VeinDataType::LargeUtf8);
        assert!(matches!(
            l,
            LogicalExpr::Cast {
                target_type: VeinDataType::LargeUtf8,
                ..
            }
        ));
    }

    #[test]
    fn conflicting_types_are_semantic_errors() {
        let err = coerce_types(lit(VeinDataType::Boolean), lit(VeinDataType::Int64)).unwrap_err();
        assert!(matches!(err, VeinError::Semantic { .. }), "got {err}");
    }
}
