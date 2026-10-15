//! Decimal multiplication result precision and scale rules.

use error::{Result, VeinError};
use super::decimal_shape;
use crate::VeinDataType;

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
