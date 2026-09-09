//! Vector comparison execution kernels for the Vector VM.
//!
//! Evaluates comparison instructions ([`MicroOp::CmpEq`], [`MicroOp::CmpNeq`],
//! [`MicroOp::CmpGt`], [`MicroOp::CmpGtEq`], [`MicroOp::CmpLt`], [`MicroOp::CmpLtEq`])
//! producing [`arrow::array::BooleanArray`] results wrapped as [`ArrayRef`].

use std::sync::Arc;
use arrow::array::ArrayRef;
use arrow::compute::kernels::cmp;
use common::Result;
use error::VeinError;

/// Performs element-wise equality comparison (`lhs == rhs`).
pub fn eval_cmp_eq(lhs: &ArrayRef, rhs: &ArrayRef) -> Result<ArrayRef> {
    let res = cmp::eq(lhs, rhs)
        .map_err(|e| VeinError::internal(format!("Vector VM CmpEq error: {e}")))?;
    Ok(Arc::new(res))
}

/// Performs element-wise inequality comparison (`lhs != rhs`).
pub fn eval_cmp_neq(lhs: &ArrayRef, rhs: &ArrayRef) -> Result<ArrayRef> {
    let res = cmp::neq(lhs, rhs)
        .map_err(|e| VeinError::internal(format!("Vector VM CmpNeq error: {e}")))?;
    Ok(Arc::new(res))
}

/// Performs element-wise greater-than comparison (`lhs > rhs`).
pub fn eval_cmp_gt(lhs: &ArrayRef, rhs: &ArrayRef) -> Result<ArrayRef> {
    let res = cmp::gt(lhs, rhs)
        .map_err(|e| VeinError::internal(format!("Vector VM CmpGt error: {e}")))?;
    Ok(Arc::new(res))
}

/// Performs element-wise greater-than-or-equal comparison (`lhs >= rhs`).
pub fn eval_cmp_gte(lhs: &ArrayRef, rhs: &ArrayRef) -> Result<ArrayRef> {
    let res = cmp::gt_eq(lhs, rhs)
        .map_err(|e| VeinError::internal(format!("Vector VM CmpGte error: {e}")))?;
    Ok(Arc::new(res))
}

/// Performs element-wise less-than comparison (`lhs < rhs`).
pub fn eval_cmp_lt(lhs: &ArrayRef, rhs: &ArrayRef) -> Result<ArrayRef> {
    let res = cmp::lt(lhs, rhs)
        .map_err(|e| VeinError::internal(format!("Vector VM CmpLt error: {e}")))?;
    Ok(Arc::new(res))
}

/// Performs element-wise less-than-or-equal comparison (`lhs <= rhs`).
pub fn eval_cmp_lte(lhs: &ArrayRef, rhs: &ArrayRef) -> Result<ArrayRef> {
    let res = cmp::lt_eq(lhs, rhs)
        .map_err(|e| VeinError::internal(format!("Vector VM CmpLte error: {e}")))?;
    Ok(Arc::new(res))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BooleanArray, Int32Array};

    #[test]
    fn test_comparison_kernels() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 5, 10]));
        let b: ArrayRef = Arc::new(Int32Array::from(vec![2, 5, 8]));

        let eq_res = eval_cmp_eq(&a, &b).unwrap();
        let eq_bool = eq_res.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(eq_bool.value(0), false);
        assert_eq!(eq_bool.value(1), true);
        assert_eq!(eq_bool.value(2), false);

        let gt_res = eval_cmp_gt(&a, &b).unwrap();
        let gt_bool = gt_res.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(gt_bool.value(0), false);
        assert_eq!(gt_bool.value(1), false);
        assert_eq!(gt_bool.value(2), true);

        let lte_res = eval_cmp_lte(&a, &b).unwrap();
        let lte_bool = lte_res.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(lte_bool.value(0), true);
        assert_eq!(lte_bool.value(1), true);
        assert_eq!(lte_bool.value(2), false);
    }
}
