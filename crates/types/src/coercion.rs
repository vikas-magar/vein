//! Type-level coercion rules shared by binding and planning.

use error::{Result, VeinError};

use crate::{TimeUnit, VeinDataType};

mod decimal_arithmetic;
pub use decimal_arithmetic::{DecimalArithmeticOp, decimal_arithmetic_result_type,
    decimal_multiplication_operands};

/// Resolves the common supertype of two data types, or errors if none exists.
///
/// Identical types pass through, `Null` unifies with any type, numeric and
/// string widening is symmetric, and `Date32` promotes to `Timestamp` while
/// preserving timestamp timezone and selecting the finest common time unit.
/// Mixed decimal/floating operations use `Float64`, because floating-point
/// aggregate results must interoperate with exact decimal literals.
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
    if let Some(common) = crate::wide_integer::common_type(left, right) { return Some(common); }
    let is_decimal = |data_type: &VeinDataType| {
        matches!(data_type, VeinDataType::Decimal128(_, _) | VeinDataType::Decimal256(_, _))
    };
    let is_float = |data_type: &VeinDataType| {
        matches!(
            data_type,
            VeinDataType::Float16 | VeinDataType::Float32 | VeinDataType::Float64
        )
    };
    if (is_decimal(left) && is_float(right)) || (is_float(left) && is_decimal(right)) {
        return Some(VeinDataType::Float64);
    }

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
    if data_type.is_int128() { return Some((39, 0, true)); }
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
mod tests;
