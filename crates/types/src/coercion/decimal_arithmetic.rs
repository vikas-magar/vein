//! Arrow-compatible result precision and scale for decimal arithmetic.

use error::{Result, VeinError};
use arrow::array::types::{Decimal128Type, Decimal256Type, DecimalType};

use super::decimal_shape;
use crate::VeinDataType;

/// Decimal operators supported by Vein's vector arithmetic kernels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecimalArithmeticOp {
    /// Addition.
    Add,
    /// Subtraction.
    Subtract,
    /// Multiplication.
    Multiply,
    /// Division.
    Divide,
    /// Remainder.
    Remainder,
}

/// Infers the result type used by Arrow for two decimal operands.
///
/// The caller supplies operands after common-type coercion, matching the types
/// passed to the Arrow kernel. Non-decimal pairs return `None`.
pub fn decimal_arithmetic_result_type(
    op: DecimalArithmeticOp,
    left: &VeinDataType,
    right: &VeinDataType,
) -> Result<Option<VeinDataType>> {
    if !is_decimal(left) || !is_decimal(right) {
        return Ok(None);
    }
    let Some((left_precision, left_scale, left_256)) = decimal_shape(left) else {
        return Ok(None);
    };
    let Some((right_precision, right_scale, right_256)) = decimal_shape(right) else {
        return Ok(None);
    };

    let decimal256 = left_256 || right_256;
    let (max_precision, max_scale) = if decimal256 {
        (
            i16::from(<Decimal256Type as DecimalType>::MAX_PRECISION),
            i16::from(<Decimal256Type as DecimalType>::MAX_SCALE),
        )
    } else {
        (
            i16::from(<Decimal128Type as DecimalType>::MAX_PRECISION),
            i16::from(<Decimal128Type as DecimalType>::MAX_SCALE),
        )
    };
    let (precision, scale) = result_shape(
        op,
        left_precision,
        left_scale,
        right_precision,
        right_scale,
        max_precision,
        max_scale,
    );
    if !(1..=max_precision).contains(&precision)
        || scale > max_scale
        || (scale > 0 && scale > precision)
    {
        return Err(VeinError::semantic(
            format!("decimal {op:?} result cannot be represented: precision {precision}, scale {scale}"),
            None,
        ));
    }

    Ok(Some(if decimal256 {
        VeinDataType::Decimal256(precision as u8, scale as i8)
    } else {
        VeinDataType::Decimal128(precision as u8, scale as i8)
    }))
}

fn is_decimal(data_type: &VeinDataType) -> bool {
    matches!(data_type, VeinDataType::Decimal128(_, _) | VeinDataType::Decimal256(_, _))
}

fn result_shape(
    op: DecimalArithmeticOp,
    p1: i16,
    s1: i16,
    p2: i16,
    s2: i16,
    max_precision: i16,
    max_scale: i16,
) -> (i16, i16) {
    match op {
        DecimalArithmeticOp::Add | DecimalArithmeticOp::Subtract => {
            let scale = s1.max(s2);
            (
                (scale + (p1 - s1).max(p2 - s2) + 1).min(max_precision),
                scale,
            )
        }
        DecimalArithmeticOp::Multiply => (
            (p1 + p2 + 1).min(max_precision),
            i16::from((s1 as i8).saturating_add(s2 as i8)),
        ),
        DecimalArithmeticOp::Divide => {
            let scale = i16::from((s1 as i8).saturating_add(4).min(max_scale as i8));
            (
                (p1 + scale - s1 + s2).min(max_precision),
                scale,
            )
        }
        DecimalArithmeticOp::Remainder => {
            let scale = s1.max(s2);
            (
                (scale + (p1 - s1).min(p2 - s2)).min(max_precision),
                scale,
            )
        }
    }
}
