//! Implicit type coercion shared by every binary/comparison and set-op
//! binding path.
//!
//! A single reference implementation (`coerce_types`) prevents the
//! "left-type-fallback" divergence that plagued ad-hoc binder code: every
//! unifiable pair is handled here, every non-unifiable pair surfaces as a
//! `Semantic` error instead of a silent type lie.

use plan::LogicalExpr;
use plan::VeinDataType;
use error::{Result, VeinError};

/// Resolves the common supertype of two expressions, injecting `Cast` nodes
/// where implicit coercion is required.
///
/// The subset mirrors GoogleSQL conversion rules:
/// - identical types pass through unchanged;
/// - `Null` unifies with anything (no cast is injected);
/// - numeric widening follows the integer family `Int8 → Int16 → Int32 →
///   Int64` and then `→ Float32 → Float64`;
/// - `Utf8`/`StringView` unify with `Null` (returning the string type);
/// - `Date32` unifies with `Timestamp` (returning `Timestamp`).
///
/// Anything else is a `Semantic` error: there is no silent fallback.
pub fn coerce_types(
    left: LogicalExpr,
    right: LogicalExpr,
) -> Result<(LogicalExpr, LogicalExpr, VeinDataType)> {
    let left_type = left.data_type();
    let right_type = right.data_type();

    if left_type == right_type {
        return Ok((left, right, left_type));
    }

    // Null unifies with any other type without injection.
    if left_type == VeinDataType::Null {
        return Ok((left, right, right_type));
    }
    if right_type == VeinDataType::Null {
        return Ok((left, right, left_type));
    }

    if let Some(common) = common_numeric(&left_type, &right_type) {
        return Ok((
            cast_to(left, common.clone()),
            cast_to(right, common.clone()),
            common,
        ));
    }

    // String family unifies with itself (only Null flows in above).
    if is_string_type(&left_type) && is_string_type(&right_type) {
        let common = if left_type == VeinDataType::StringView {
            VeinDataType::StringView
        } else {
            VeinDataType::Utf8
        };
        return Ok((
            cast_to(left, common.clone()),
            cast_to(right, common.clone()),
            common,
        ));
    }

    if matches!(left_type, VeinDataType::Date32) && matches!(right_type, VeinDataType::Timestamp(_))
    {
        return Ok((
            cast_to(left, VeinDataType::Timestamp(None)),
            right,
            VeinDataType::Timestamp(None),
        ));
    }
    if matches!(left_type, VeinDataType::Timestamp(_)) && matches!(right_type, VeinDataType::Date32)
    {
        return Ok((
            left,
            cast_to(right, VeinDataType::Timestamp(None)),
            VeinDataType::Timestamp(None),
        ));
    }

    Err(VeinError::semantic(
        format!("no implicit coercion between types {left_type:?} and {right_type:?}"),
        None,
    ))
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

/// True for the UTF-8 string types.
fn is_string_type(t: &VeinDataType) -> bool {
    matches!(
        t,
        VeinDataType::Utf8 | VeinDataType::LargeUtf8 | VeinDataType::StringView
    )
}

/// The common supertype of two numeric types, if one exists.
fn common_numeric(left: &VeinDataType, right: &VeinDataType) -> Option<VeinDataType> {
    let rank = |t: &VeinDataType| -> Option<u8> {
        Some(match t {
            VeinDataType::Int8 => 0,
            VeinDataType::Int16 => 1,
            VeinDataType::Int32 => 2,
            VeinDataType::Int64 => 3,
            VeinDataType::Float32 => 4,
            VeinDataType::Float64 => 5,
            _ => return None,
        })
    };

    let (lr, rr) = (rank(left)?, rank(right)?);
    Some(if lr >= rr {
        left.clone()
    } else {
        right.clone()
    })
}

#[cfg(test)]
#[allow(missing_docs)]
mod tests {
    use super::*;
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
            lit(VeinDataType::Timestamp(None)),
        )
        .unwrap();
        assert_eq!(t, VeinDataType::Timestamp(None));
    }

    #[test]
    fn conflicting_types_are_semantic_errors() {
        let err = coerce_types(lit(VeinDataType::Boolean), lit(VeinDataType::Int64)).unwrap_err();
        assert!(matches!(err, VeinError::Semantic { .. }), "got {err}");
    }
}
