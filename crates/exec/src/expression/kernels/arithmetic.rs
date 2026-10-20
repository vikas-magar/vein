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
    use arrow::array::Int64Array;
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
}
