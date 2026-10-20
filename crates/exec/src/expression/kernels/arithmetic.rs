//! Arrow-backed vector arithmetic kernels used by typed expressions.

use arrow::array::{Array, ArrayRef, Datum};
use arrow::compute::kernels::numeric;
use common::Result;
use error::VeinError;


/// Performs element-wise vector addition between `lhs` and `rhs`.
///
/// Operands are [`Datum`]s, so a broadcast scalar operand is never expanded to a full column.
pub fn eval_add(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<ArrayRef> {
    numeric::add(lhs, rhs).map_err(|e| VeinError::internal(format!("expression addition failed: {e}")))
}

/// Performs element-wise vector subtraction (`lhs - rhs`).
pub fn eval_sub(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<ArrayRef> {
    numeric::sub(lhs, rhs).map_err(|e| VeinError::internal(format!("expression subtraction failed: {e}")))
}

/// Performs element-wise vector multiplication (`lhs * rhs`).
pub fn eval_mul(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<ArrayRef> {
    numeric::mul(lhs, rhs).map_err(|e| VeinError::internal(format!("expression multiplication failed: {e}")))
}

/// Performs element-wise vector division (`lhs / rhs`).
pub fn eval_div(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<ArrayRef> {
    numeric::div(lhs, rhs).map_err(|e| VeinError::internal(format!("expression division failed: {e}")))
}

/// Performs element-wise vector modulo (`lhs % rhs`).
pub fn eval_mod(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<ArrayRef> {
    numeric::rem(lhs, rhs).map_err(|e| VeinError::internal(format!("expression modulo failed: {e}")))
}

/// Performs element-wise unary negation (`-src`).
///
/// Arrow's negation kernel accepts an `Array` rather than a [`Datum`].
pub fn eval_neg(src: &dyn Array) -> Result<ArrayRef> {
    numeric::neg(src).map_err(|e| VeinError::internal(format!("expression negation failed: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Decimal128Array, Float64Array, Int64Array};
    use types::VeinDataType;
    use std::sync::Arc;

    #[test]
    fn test_arithmetic_kernels() {
        let a: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30]));
        let b: ArrayRef = Arc::new(Int64Array::from(vec![2, 5, 6]));

        let add_res = eval_add(&a, &b).unwrap();
        let add_down = add_res.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(add_down.value(0), 12);
        assert_eq!(add_down.value(1), 25);
        assert_eq!(add_down.value(2), 36);

        let sub_res = eval_sub(&a, &b).unwrap();
        let sub_down = sub_res.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(sub_down.value(0), 8);
        assert_eq!(sub_down.value(1), 15);
        assert_eq!(sub_down.value(2), 24);

        let mul_res = eval_mul(&a, &b).unwrap();
        let mul_down = mul_res.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(mul_down.value(0), 20);
        assert_eq!(mul_down.value(1), 100);
        assert_eq!(mul_down.value(2), 180);

        let div_res = eval_div(&a, &b).unwrap();
        let div_down = div_res.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(div_down.value(0), 5);
        assert_eq!(div_down.value(1), 4);
        assert_eq!(div_down.value(2), 5);
    }

    #[test]
    fn float64_decimal_division_preserves_precision_and_nulls() {
        let left = Decimal128Array::from(vec![Some(38_125_i128), None])
            .with_precision_and_scale(10, 2)
            .unwrap();
        let right = Decimal128Array::from(vec![Some(700_i128), Some(700_i128)])
            .with_precision_and_scale(10, 2)
            .unwrap();
        let left = crate::expression::kernels::eval_cast(&left, &VeinDataType::Float64).unwrap();
        let right = crate::expression::kernels::eval_cast(&right, &VeinDataType::Float64).unwrap();

        let result = eval_div(&left, &right).unwrap();
        let values = result.as_any().downcast_ref::<Float64Array>().unwrap();

        assert!((values.value(0) - 54.464_285_714_285_715).abs() < 1e-12);
        assert!(values.is_null(1));
    }
}
