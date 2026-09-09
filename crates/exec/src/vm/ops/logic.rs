//! Boolean logic execution kernels for the Vector VM.
//!
//! Evaluates [`MicroOp::And`], [`MicroOp::Or`], and [`MicroOp::Not`]
//! against [`arrow::array::BooleanArray`] registers.

use std::sync::Arc;
use arrow::array::{ArrayRef, BooleanArray};
use arrow::compute::kernels::boolean;
use common::Result;
use error::VeinError;

/// Performs element-wise logical AND between two boolean registers.
pub fn eval_and(lhs: &ArrayRef, rhs: &ArrayRef) -> Result<ArrayRef> {
    let b_lhs = lhs
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| VeinError::internal("Logical AND requires BooleanArray for LHS"))?;
    let b_rhs = rhs
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| VeinError::internal("Logical AND requires BooleanArray for RHS"))?;

    let res = boolean::and(b_lhs, b_rhs)
        .map_err(|e| VeinError::internal(format!("Vector VM And error: {e}")))?;
    Ok(Arc::new(res))
}

/// Performs element-wise logical OR between two boolean registers.
pub fn eval_or(lhs: &ArrayRef, rhs: &ArrayRef) -> Result<ArrayRef> {
    let b_lhs = lhs
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| VeinError::internal("Logical OR requires BooleanArray for LHS"))?;
    let b_rhs = rhs
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| VeinError::internal("Logical OR requires BooleanArray for RHS"))?;

    let res = boolean::or(b_lhs, b_rhs)
        .map_err(|e| VeinError::internal(format!("Vector VM Or error: {e}")))?;
    Ok(Arc::new(res))
}

/// Performs element-wise logical NOT on a boolean register.
pub fn eval_not(src: &ArrayRef) -> Result<ArrayRef> {
    let b_src = src
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| VeinError::internal("Logical NOT requires BooleanArray for operand"))?;

    let res = boolean::not(b_src)
        .map_err(|e| VeinError::internal(format!("Vector VM Not error: {e}")))?;
    Ok(Arc::new(res))
}

/// Evaluates row-wise `IS NULL` predicate on a register, producing a [`BooleanArray`].
pub fn eval_is_null(src: &ArrayRef) -> Result<ArrayRef> {
    let res = boolean::is_null(src.as_ref())
        .map_err(|e| VeinError::internal(format!("Vector VM IsNull error: {e}")))?;
    Ok(Arc::new(res))
}

/// Evaluates row-wise `IS NOT NULL` predicate on a register, producing a [`BooleanArray`].
pub fn eval_is_not_null(src: &ArrayRef) -> Result<ArrayRef> {
    let res = boolean::is_not_null(src.as_ref())
        .map_err(|e| VeinError::internal(format!("Vector VM IsNotNull error: {e}")))?;
    Ok(Arc::new(res))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_logic_kernels() {
        let b1: ArrayRef = Arc::new(BooleanArray::from(vec![true, true, false, false]));
        let b2: ArrayRef = Arc::new(BooleanArray::from(vec![true, false, true, false]));

        let and_res = eval_and(&b1, &b2).unwrap();
        let and_bool = and_res.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(and_bool.value(0), true);
        assert_eq!(and_bool.value(1), false);
        assert_eq!(and_bool.value(2), false);
        assert_eq!(and_bool.value(3), false);

        let or_res = eval_or(&b1, &b2).unwrap();
        let or_bool = or_res.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(or_bool.value(0), true);
        assert_eq!(or_bool.value(1), true);
        assert_eq!(or_bool.value(2), true);
        assert_eq!(or_bool.value(3), false);

        let not_res = eval_not(&b1).unwrap();
        let not_bool = not_res.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(not_bool.value(0), false);
        assert_eq!(not_bool.value(1), false);
        assert_eq!(not_bool.value(2), true);
        assert_eq!(not_bool.value(3), true);
    }
}
