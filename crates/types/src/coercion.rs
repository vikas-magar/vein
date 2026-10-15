//! Type-level coercion rules shared by binding and planning.

use error::{Result, VeinError};

use crate::{TimeUnit, VeinDataType};

/// Resolves the common supertype of two data types, or errors if none exists.
///
/// Identical types pass through, `Null` unifies with any type, numeric and
/// string widening is symmetric, and `Date32` promotes to `Timestamp` while
/// preserving timestamp timezone and selecting the finest common time unit.
pub fn unify_types(left: &VeinDataType, right: &VeinDataType) -> Result<VeinDataType> {
    if left == right {
        return Ok(left.clone());
    }

    if *left == VeinDataType::Null {
        return Ok(right.clone());
    }
    if *right == VeinDataType::Null {
        return Ok(left.clone());
    }

    if let Some(common) = common_numeric(left, right) {
        return Ok(common);
    }

    if is_string_type(left) && is_string_type(right) {
        return Ok(common_string(left, right));
    }

    if let (
        VeinDataType::Timestamp(left_unit, left_tz),
        VeinDataType::Timestamp(right_unit, right_tz),
    ) = (left, right)
        && left_tz == right_tz
    {
        return Ok(VeinDataType::Timestamp(
            common_timestamp_unit(*left_unit, *right_unit),
            left_tz.clone(),
        ));
    }

    if let VeinDataType::Timestamp(unit, tz) = right
        && matches!(left, VeinDataType::Date32)
    {
        return Ok(VeinDataType::Timestamp(*unit, tz.clone()));
    }
    if let VeinDataType::Timestamp(unit, tz) = left
        && matches!(right, VeinDataType::Date32)
    {
        return Ok(VeinDataType::Timestamp(*unit, tz.clone()));
    }

    Err(VeinError::semantic(
        format!("no implicit coercion between types {left:?} and {right:?}"),
        None,
    ))
}

/// Returns Arrow's result type for multiplying two already-coerced decimal
/// operands. Arrow grows precision by one digit beyond the sum of input
/// precisions and adds the operand scales; result precision is capped at the
/// selected decimal width's maximum, as in Arrow's decimal kernels.
///
/// Returns `Ok(None)` for non-decimal operands. An unrepresentable result scale
/// is rejected during binding instead of failing later in vector execution.
pub fn decimal_multiply_result_type(
    left: &VeinDataType,
    right: &VeinDataType,
) -> Result<Option<VeinDataType>> {
    if !matches!(left, VeinDataType::Decimal128(_, _) | VeinDataType::Decimal256(_, _))
        || !matches!(right, VeinDataType::Decimal128(_, _) | VeinDataType::Decimal256(_, _))
    {
        return Ok(None);
    }

    let Some((left_precision, left_scale, left_is_decimal256)) = decimal_shape(left) else {
        return Ok(None);
    };
    let Some((right_precision, right_scale, right_is_decimal256)) = decimal_shape(right) else {
        return Ok(None);
    };

    let is_decimal256 = left_is_decimal256 || right_is_decimal256;
    let max_precision = if is_decimal256 { 76 } else { 38 };
    let precision = (left_precision + right_precision + 1).min(max_precision);
    let scale = left_scale + right_scale;
    if scale < 0 || scale > max_precision || precision < scale {
        return Err(VeinError::semantic(
            format!(
                "decimal multiplication result cannot be represented: precision {precision}, scale {scale}"
            ),
            None,
        ));
    }

    let result = if is_decimal256 {
        VeinDataType::Decimal256(precision as u8, scale as i8)
    } else {
        VeinDataType::Decimal128(precision as u8, scale as i8)
    };
    Ok(Some(result))
}

fn common_timestamp_unit(left: TimeUnit, right: TimeUnit) -> TimeUnit {
    use TimeUnit::{Microsecond, Millisecond, Nanosecond, Second};
    let precision = |unit| match unit {
        Second => 0,
        Millisecond => 1,
        Microsecond => 2,
        Nanosecond => 3,
    };
    if precision(left) >= precision(right) { left } else { right }
}

fn is_string_type(data_type: &VeinDataType) -> bool {
    matches!(
        data_type,
        VeinDataType::Utf8 | VeinDataType::LargeUtf8 | VeinDataType::StringView
    )
}

fn common_string(left: &VeinDataType, right: &VeinDataType) -> VeinDataType {
    let rank = |data_type: &VeinDataType| -> u8 {
        match data_type {
            VeinDataType::Utf8 => 0,
            VeinDataType::StringView => 1,
            VeinDataType::LargeUtf8 => 2,
            _ => unreachable!("common_string called with non-string type {data_type:?}"),
        }
    };
    if rank(left) >= rank(right) {
        left.clone()
    } else {
        right.clone()
    }
}

fn common_numeric(left: &VeinDataType, right: &VeinDataType) -> Option<VeinDataType> {
    if let Some(common) = common_decimal(left, right) {
        return Some(common);
    }

    let rank = |data_type: &VeinDataType| -> Option<u8> {
        Some(match data_type {
            VeinDataType::Int8 => 0,
            VeinDataType::Int16 => 1,
            VeinDataType::Int32 => 2,
            VeinDataType::Int64 => 3,
            VeinDataType::Float32 => 4,
            VeinDataType::Float64 => 5,
            _ => return None,
        })
    };

    let (left_rank, right_rank) = (rank(left)?, rank(right)?);
    Some(if left_rank >= right_rank {
        left.clone()
    } else {
        right.clone()
    })
}

/// Finds a decimal supertype for decimal/integer operands without losing
/// fractional scale or the integer range.
fn common_decimal(left: &VeinDataType, right: &VeinDataType) -> Option<VeinDataType> {
    if !matches!(left, VeinDataType::Decimal128(_, _) | VeinDataType::Decimal256(_, _))
        && !matches!(right, VeinDataType::Decimal128(_, _) | VeinDataType::Decimal256(_, _))
    {
        return None;
    }

    let (left_precision, left_scale, left_is_decimal256) = decimal_shape(left)?;
    let (right_precision, right_scale, right_is_decimal256) = decimal_shape(right)?;

    let scale = left_scale.max(right_scale).max(0);
    let integer_digits = (left_precision - left_scale)
        .max(right_precision - right_scale)
        .max(0);
    let precision = integer_digits + scale;
    let use_decimal256 = left_is_decimal256 || right_is_decimal256 || precision > 38;

    let max_precision = if use_decimal256 { 76 } else { 38 };
    if precision <= 0 || precision > max_precision {
        return None;
    }

    if use_decimal256 {
        Some(VeinDataType::Decimal256(precision as u8, scale as i8))
    } else {
        Some(VeinDataType::Decimal128(precision as u8, scale as i8))
    }
}

/// Returns precision, scale, and decimal width for decimal types and integral
/// layouts. Integral precision covers every value of that layout.
fn decimal_shape(data_type: &VeinDataType) -> Option<(i16, i16, bool)> {
    Some(match data_type {
        VeinDataType::Decimal128(precision, scale) =>
            (i16::from(*precision), i16::from(*scale), false),
        VeinDataType::Decimal256(precision, scale) =>
            (i16::from(*precision), i16::from(*scale), true),
        VeinDataType::Int8 | VeinDataType::UInt8 => (3, 0, false),
        VeinDataType::Int16 | VeinDataType::UInt16 => (5, 0, false),
        VeinDataType::Int32 | VeinDataType::UInt32 => (10, 0, false),
        VeinDataType::Int64 => (19, 0, false),
        VeinDataType::UInt64 => (20, 0, false),
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::TimeUnit;

    #[test]
    fn unifies_numeric_types_symmetrically() {
        assert_eq!(
            unify_types(&VeinDataType::Int32, &VeinDataType::Int64).unwrap(),
            VeinDataType::Int64
        );
        assert_eq!(
            unify_types(&VeinDataType::Int64, &VeinDataType::Int32).unwrap(),
            VeinDataType::Int64
        );
    }

    #[test]
    fn unifies_integer_and_decimal_without_losing_range_or_scale() {
        let decimal = VeinDataType::Decimal128(15, 2);
        let expected = VeinDataType::Decimal128(21, 2);

        assert_eq!(
            unify_types(&VeinDataType::Int64, &decimal).unwrap(),
            expected
        );
        assert_eq!(
            unify_types(&decimal, &VeinDataType::Int64).unwrap(),
            expected
        );
        assert_eq!(
            unify_types(&VeinDataType::UInt64, &decimal).unwrap(),
            VeinDataType::Decimal128(22, 2)
        );
    }

    #[test]
    fn decimal_unification_preserves_widest_scale_and_uses_decimal256_when_needed() {
        assert_eq!(
            unify_types(
                &VeinDataType::Decimal128(12, 3),
                &VeinDataType::Decimal128(15, 5)
            )
                .unwrap(),
            VeinDataType::Decimal128(15, 5)
        );
        assert_eq!(
            unify_types(&VeinDataType::Int64, &VeinDataType::Decimal128(38, 20)).unwrap(),
            VeinDataType::Decimal256(39, 20)
        );
    }

    #[test]
    fn decimal_multiplication_matches_arrow_precision_and_scale() {
        assert_eq!(
            decimal_multiply_result_type(
                &VeinDataType::Decimal128(17, 4),
                &VeinDataType::Decimal128(17, 4),
            )
                .unwrap(),
            Some(VeinDataType::Decimal128(35, 8))
        );
        assert_eq!(
            decimal_multiply_result_type(
                &VeinDataType::Decimal256(40, 4),
                &VeinDataType::Decimal256(40, 4),
            )
                .unwrap(),
            Some(VeinDataType::Decimal256(76, 8))
        );
        assert!(decimal_multiply_result_type(
            &VeinDataType::Decimal128(38, 20),
            &VeinDataType::Decimal128(38, 20),
        )
            .is_err());
    }

    #[test]
    fn null_unifies_with_other_types() {
        assert_eq!(
            unify_types(&VeinDataType::Utf8, &VeinDataType::Null).unwrap(),
            VeinDataType::Utf8
        );
    }

    #[test]
    fn date_promotes_to_timestamp_and_preserves_timezone() {
        assert_eq!(
            unify_types(
                &VeinDataType::Date32,
                &VeinDataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
            )
                .unwrap(),
            VeinDataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
    }

    #[test]
    fn timestamp_unification_selects_finest_unit_and_requires_matching_timezones() {
        let seconds = VeinDataType::Timestamp(TimeUnit::Second, None);
        let nanos = VeinDataType::Timestamp(TimeUnit::Nanosecond, None);
        let expected = VeinDataType::Timestamp(TimeUnit::Nanosecond, None);

        assert_eq!(unify_types(&seconds, &nanos).unwrap(), expected);
        assert_eq!(unify_types(&nanos, &seconds).unwrap(), expected);
        assert!(unify_types(
            &seconds,
            &VeinDataType::Timestamp(TimeUnit::Second, Some("UTC".into()))
        )
            .is_err());
    }

    #[test]
    fn incompatible_types_return_semantic_error() {
        let error = unify_types(&VeinDataType::Boolean, &VeinDataType::Int64).unwrap_err();
        assert!(matches!(error, VeinError::Semantic { .. }));
    }
}
