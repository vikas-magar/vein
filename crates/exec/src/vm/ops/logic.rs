//! Boolean logic execution kernels for the Vector VM.
//!
//! Evaluates [`MicroOp::And`], [`MicroOp::Or`], and [`MicroOp::Not`]
//! against [`arrow::array::BooleanArray`] registers.

use std::sync::Arc;
use arrow::array::{Array, ArrayRef, BooleanArray, Datum};
use arrow::compute::kernels::boolean;
use common::Result;
use error::VeinError;
use crate::vm::register::RegisterDatum;

/// Performs element-wise logical AND between two boolean registers.
pub fn eval_and(lhs: &ArrayRef, rhs: &ArrayRef) -> Result<ArrayRef> {
    let lhs = RegisterDatum::from_array(lhs, false);
    let rhs = RegisterDatum::from_array(rhs, false);
    eval_and_datum(&lhs, &rhs)
}

/// Performs element-wise logical OR between two boolean registers.
pub fn eval_or(lhs: &ArrayRef, rhs: &ArrayRef) -> Result<ArrayRef> {
    let lhs = RegisterDatum::from_array(lhs, false);
    let rhs = RegisterDatum::from_array(rhs, false);
    eval_or_datum(&lhs, &rhs)
}

pub(crate) fn eval_and_datum(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<ArrayRef> {
    eval_binary_boolean(lhs, rhs, "AND", "And", boolean::and_kleene)
}

pub(crate) fn eval_or_datum(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<ArrayRef> {
    eval_binary_boolean(lhs, rhs, "OR", "Or", boolean::or_kleene)
}

fn eval_binary_boolean(
    lhs: &dyn Datum,
    rhs: &dyn Datum,
    operator: &str,
    kernel_name: &str,
    kernel: impl FnOnce(
        &BooleanArray,
        &BooleanArray,
    ) -> std::result::Result<BooleanArray, arrow::error::ArrowError>,
) -> Result<ArrayRef> {
    let (lhs_array, lhs_scalar) = lhs.get();
    let (rhs_array, rhs_scalar) = rhs.get();
    let lhs = lhs_array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| {
            VeinError::internal(format!("Logical {operator} requires BooleanArray for LHS"))
        })?;
    let rhs = rhs_array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| {
            VeinError::internal(format!("Logical {operator} requires BooleanArray for RHS"))
        })?;

    let lhs_expanded = (lhs_scalar && !rhs_scalar)
        .then(|| expand_boolean_scalar(lhs, rhs.len()))
        .transpose()?;
    let rhs_expanded = (rhs_scalar && !lhs_scalar)
        .then(|| expand_boolean_scalar(rhs, lhs.len()))
        .transpose()?;
    let lhs = lhs_expanded.as_ref().unwrap_or(lhs);
    let rhs = rhs_expanded.as_ref().unwrap_or(rhs);

    let res = kernel(lhs, rhs)
        .map_err(|e| VeinError::internal(format!("Vector VM {kernel_name} error: {e}")))?;
    Ok(Arc::new(res))
}

fn expand_boolean_scalar(value: &BooleanArray, len: usize) -> Result<BooleanArray> {
    if value.len() != 1 {
        return Err(VeinError::internal(format!(
            "Boolean scalar has {} values, expected one",
            value.len()
        )));
    }
    let scalar = (!value.is_null(0)).then(|| value.value(0));
    Ok(BooleanArray::from(vec![scalar; len]))
}

/// Performs element-wise logical NOT on a boolean register.
pub fn eval_not(src: &dyn Array) -> Result<ArrayRef> {
    let b_src = src
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| VeinError::internal("Logical NOT requires BooleanArray for operand"))?;

    let res = boolean::not(b_src)
        .map_err(|e| VeinError::internal(format!("Vector VM Not error: {e}")))?;
    Ok(Arc::new(res))
}

/// Evaluates row-wise `IS NULL` predicate on a register, producing a [`BooleanArray`].
pub fn eval_is_null(src: &dyn Array) -> Result<ArrayRef> {
    let res = boolean::is_null(src)
        .map_err(|e| VeinError::internal(format!("Vector VM IsNull error: {e}")))?;
    Ok(Arc::new(res))
}

/// Evaluates row-wise `IS NOT NULL` predicate on a register, producing a [`BooleanArray`].
pub fn eval_is_not_null(src: &dyn Array) -> Result<ArrayRef> {
    let res = boolean::is_not_null(src)
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
        assert!(and_bool.value(0));
        assert!(!and_bool.value(1));
        assert!(!and_bool.value(2));
        assert!(!and_bool.value(3));

        let or_res = eval_or(&b1, &b2).unwrap();
        let or_bool = or_res.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(or_bool.value(0));
        assert!(or_bool.value(1));
        assert!(or_bool.value(2));
        assert!(!or_bool.value(3));

        let not_res = eval_not(&b1).unwrap();
        let not_bool = not_res.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(!not_bool.value(0));
        assert!(!not_bool.value(1));
        assert!(not_bool.value(2));
        assert!(not_bool.value(3));
    }

    #[test]
    fn binary_boolean_kernels_preserve_operator_error_context() {
        let lhs: ArrayRef = Arc::new(BooleanArray::from(vec![true, false]));
        let rhs: ArrayRef = Arc::new(BooleanArray::from(vec![true]));

        assert!(eval_and(&lhs, &rhs)
            .unwrap_err()
            .to_string()
            .contains("Vector VM And error"));
        assert!(eval_or(&lhs, &rhs)
            .unwrap_err()
            .to_string()
            .contains("Vector VM Or error"));
    }

    #[test]
    fn nullable_boolean_scalars_broadcast_over_dense_inputs() {
        let values: ArrayRef = Arc::new(BooleanArray::from(vec![Some(false), Some(true)]));
        let values = RegisterDatum::from_array(&values, false);

        for (scalar, and_expected, or_expected) in [
            (Some(true), vec![Some(false), Some(true)], vec![Some(true), Some(true)]),
            (None, vec![Some(false), None], vec![None, Some(true)]),
        ] {
            let scalar_array: ArrayRef = Arc::new(BooleanArray::from(vec![scalar]));
            let scalar = RegisterDatum::from_array(&scalar_array, true);
            let and = eval_and_datum(&scalar, &values).unwrap();
            let or = eval_or_datum(&scalar, &values).unwrap();
            assert_eq!(
                and.as_any().downcast_ref::<BooleanArray>().unwrap(),
                &BooleanArray::from(and_expected)
            );
            assert_eq!(
                or.as_any().downcast_ref::<BooleanArray>().unwrap(),
                &BooleanArray::from(or_expected)
            );
        }
    }
}
