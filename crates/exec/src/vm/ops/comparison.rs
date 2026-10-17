//! Vector comparison execution kernels for the Vector VM.
//!
//! Evaluates comparison instructions ([`MicroOp::CmpEq`], [`MicroOp::CmpNeq`],
//! [`MicroOp::CmpGt`], [`MicroOp::CmpGtEq`], [`MicroOp::CmpLt`], [`MicroOp::CmpLtEq`])
//! producing [`arrow::array::BooleanArray`] results wrapped as [`ArrayRef`].

use std::sync::Arc;
use arrow::array::{ArrayRef, Datum};
use arrow::compute::kernels::cmp;
use common::Result;
use error::VeinError;

/// Performs element-wise equality comparison (`lhs == rhs`).
///
/// Operands are [`Datum`]s, so a broadcast scalar operand is never expanded to a full column.
pub fn eval_cmp_eq(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<ArrayRef> {
    let res = cmp::eq(lhs, rhs)
        .map_err(|e| VeinError::internal(format!("Vector VM CmpEq error: {e}")))?;
    Ok(Arc::new(res))
}

/// Performs element-wise inequality comparison (`lhs != rhs`).
pub fn eval_cmp_neq(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<ArrayRef> {
    let res = cmp::neq(lhs, rhs)
        .map_err(|e| VeinError::internal(format!("Vector VM CmpNeq error: {e}")))?;
    Ok(Arc::new(res))
}

/// Performs element-wise greater-than comparison (`lhs > rhs`).
pub fn eval_cmp_gt(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<ArrayRef> {
    let res = cmp::gt(lhs, rhs)
        .map_err(|e| VeinError::internal(format!("Vector VM CmpGt error: {e}")))?;
    Ok(Arc::new(res))
}

/// Performs element-wise greater-than-or-equal comparison (`lhs >= rhs`).
pub fn eval_cmp_gte(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<ArrayRef> {
    let res = cmp::gt_eq(lhs, rhs)
        .map_err(|e| VeinError::internal(format!("Vector VM CmpGte error: {e}")))?;
    Ok(Arc::new(res))
}

/// Performs element-wise less-than comparison (`lhs < rhs`).
pub fn eval_cmp_lt(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<ArrayRef> {
    let res = cmp::lt(lhs, rhs)
        .map_err(|e| VeinError::internal(format!("Vector VM CmpLt error: {e}")))?;
    Ok(Arc::new(res))
}

/// Performs element-wise less-than-or-equal comparison (`lhs <= rhs`).
pub fn eval_cmp_lte(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<ArrayRef> {
    let res = cmp::lt_eq(lhs, rhs)
        .map_err(|e| VeinError::internal(format!("Vector VM CmpLte error: {e}")))?;
    Ok(Arc::new(res))
}

/// Performs SQL LIKE or NOT LIKE with Arrow's vectorized string kernel.
pub fn eval_like(value: &dyn Datum, pattern: &dyn Datum, negated: bool) -> Result<ArrayRef> {
    let result = if negated {
        arrow::compute::kernels::comparison::nlike(value, pattern)
    } else {
        arrow::compute::kernels::comparison::like(value, pattern)
    }
        .map_err(|error| VeinError::internal(format!("Vector VM Like error: {error}")))?;
    Ok(Arc::new(result))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, BooleanArray, Int32Array};

    #[test]
    fn test_comparison_kernels() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 5, 10]));
        let b: ArrayRef = Arc::new(Int32Array::from(vec![2, 5, 8]));

        let eq_res = eval_cmp_eq(&a, &b).unwrap();
        let eq_bool = eq_res.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(!eq_bool.value(0));
        assert!(eq_bool.value(1));
        assert!(!eq_bool.value(2));

        let gt_res = eval_cmp_gt(&a, &b).unwrap();
        let gt_bool = gt_res.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(!gt_bool.value(0));
        assert!(!gt_bool.value(1));
        assert!(gt_bool.value(2));

        let lte_res = eval_cmp_lte(&a, &b).unwrap();
        let lte_bool = lte_res.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(lte_bool.value(0));
        assert!(lte_bool.value(1));
        assert!(!lte_bool.value(2));
    }

    #[test]
    fn sql_like_kernel_supports_wildcards_negation_and_nulls() {
        use arrow::array::StringArray;

        let values = StringArray::from(vec![Some("POLISHED BRASS"), Some("BRASS"), None]);
        let pattern = StringArray::from(vec![Some("%BRASS"), Some("B_ASS"), Some("%")]);
        let matched = eval_like(&values, &pattern, false).expect("LIKE kernel should run");
        let matched = matched
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("LIKE returns booleans");
        assert!(matched.value(0));
        assert!(matched.value(1));
        assert!(matched.is_null(2));

        let values = StringArray::from(vec![Some("POLISHED BRASS"), Some("BRASS"), None]);
        let pattern = StringArray::from(vec![Some("%BRASS"), Some("B_ASS"), Some("%")]);
        let unmatched = eval_like(&values, &pattern, true).expect("NOT LIKE kernel should run");
        let unmatched = unmatched
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("NOT LIKE returns booleans");
        assert!(!unmatched.value(0));
        assert!(!unmatched.value(1));
        assert!(unmatched.is_null(2));
    }
}
