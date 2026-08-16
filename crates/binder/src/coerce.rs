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

/// Resolves the common supertype of two types, or errors if none exists.
///
/// This is the type-level core shared by [`coerce_types`] and by column-level
/// checks (e.g. `SetOp` branch schemas) where no expression values are at hand.
/// The rules mirror [`coerce_types`]: identical types pass through, `Null`
/// unifies with anything, numeric/string widening is symmetric, and `Date32`
/// promotes to `Timestamp` (preserving the other side's timezone).
pub fn unify_types(
    left: &VeinDataType,
    right: &VeinDataType,
) -> Result<VeinDataType> {
    if left == right {
        return Ok(left.clone());
    }

    // Null unifies with any other type.
    if *left == VeinDataType::Null {
        return Ok(right.clone());
    }
    if *right == VeinDataType::Null {
        return Ok(left.clone());
    }

    if let Some(common) = common_numeric(left, right) {
        return Ok(common);
    }

    // String family unifies to the widest member, symmetrically.
    if is_string_type(left) && is_string_type(right) {
        return Ok(common_string(left, right));
    }

    // Date32 promotes to a Timestamp, preserving the other side's timezone.
    if let VeinDataType::Timestamp(tz) = right
        && matches!(left, VeinDataType::Date32)
    {
        return Ok(VeinDataType::Timestamp(tz.clone()));
    }
    if let VeinDataType::Timestamp(tz) = left
        && matches!(right, VeinDataType::Date32)
    {
        return Ok(VeinDataType::Timestamp(tz.clone()));
    }

    Err(VeinError::semantic(
        format!("no implicit coercion between types {left:?} and {right:?}"),
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

/// The widest string type able to hold both operands without loss. Ranks the
/// family `Utf8 → StringView → LargeUtf8`, so the result is symmetric.
fn common_string(left: &VeinDataType, right: &VeinDataType) -> VeinDataType {
    let rank = |t: &VeinDataType| -> u8 {
        match t {
            VeinDataType::Utf8 => 0,
            VeinDataType::StringView => 1,
            VeinDataType::LargeUtf8 => 2,
            _ => unreachable!("common_string called with non-string type {t:?}"),
        }
    };
    if rank(left) >= rank(right) {
        left.clone()
    } else {
        right.clone()
    }
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
    fn date_promotes_preserving_timezone() {
        let (l, _, t) = coerce_types(
            lit(VeinDataType::Date32),
            lit(VeinDataType::Timestamp(Some("UTC".to_string()))),
        )
        .unwrap();
        assert_eq!(t, VeinDataType::Timestamp(Some("UTC".to_string())));
        assert!(matches!(
            l,
            LogicalExpr::Cast {
                target_type: VeinDataType::Timestamp(Some(ref tz)),
                ..
            } if tz == "UTC"
        ));
    }

    #[test]
    fn timestamp_promotes_date_to_its_timezone() {
        let (_, r, t) = coerce_types(
            lit(VeinDataType::Timestamp(Some("America/New_York".to_string()))),
            lit(VeinDataType::Date32),
        )
        .unwrap();
        assert_eq!(
            t,
            VeinDataType::Timestamp(Some("America/New_York".to_string()))
        );
        assert!(matches!(
            r,
            LogicalExpr::Cast {
                target_type: VeinDataType::Timestamp(Some(ref tz)),
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

    #[test]
    fn unify_types_is_type_level_and_symmetric() {
        assert_eq!(
            unify_types(&VeinDataType::Int32, &VeinDataType::Int64).unwrap(),
            VeinDataType::Int64
        );
        // Reversed arguments yield the same common type.
        assert_eq!(
            unify_types(&VeinDataType::Int64, &VeinDataType::Int32).unwrap(),
            VeinDataType::Int64
        );
        assert_eq!(
            unify_types(&VeinDataType::Utf8, &VeinDataType::Null).unwrap(),
            VeinDataType::Utf8
        );
        assert_eq!(
            unify_types(&VeinDataType::Date32, &VeinDataType::Timestamp(Some("UTC".into())))
                .unwrap(),
            VeinDataType::Timestamp(Some("UTC".into()))
        );
        let err = unify_types(&VeinDataType::Boolean, &VeinDataType::Int64).unwrap_err();
        assert!(matches!(err, VeinError::Semantic { .. }), "got {err}");
    }
}
