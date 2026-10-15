//! Vector kernels for SQL CASE and IN/NOT IN expressions.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, new_null_array};
use arrow::compute::kernels::{boolean, zip};
use arrow::datatypes::DataType;
use common::Result;
use error::VeinError;

use crate::vm::register::RegValue;

/// Evaluates SQL IN/NOT IN while preserving nullable comparison semantics.
pub fn eval_in_list(
    value: &RegValue,
    candidates: &[RegValue],
    rows: usize,
    negated: bool,
) -> Result<ArrayRef> {
    let mut result = BooleanArray::from(vec![Some(false); rows]);
    for candidate in candidates {
        // Arrow represents an untyped NULL literal with NullArray. Comparison
        // kernels cannot compare that physical type to a typed value, while SQL
        // defines every equality involving NULL as unknown.
        let equality = if value.data_type() == &DataType::Null
            || candidate.data_type() == &DataType::Null
        {
            BooleanArray::from(vec![None; rows])
        } else {
            arrow::compute::kernels::cmp::eq(value, candidate)
                .map_err(|e| VeinError::internal(format!("Vector VM IN equality error: {e}")))?
        };
        result = boolean::or_kleene(&result, &equality)
            .map_err(|e| VeinError::internal(format!("Vector VM IN combine error: {e}")))?;
    }
    if negated {
        result = boolean::not(&result)
            .map_err(|e| VeinError::internal(format!("Vector VM NOT IN error: {e}")))?;
    }
    Ok(Arc::new(result))
}

/// Evaluates a searched CASE expression. Inputs remain Arrow arrays/scalars; only
/// the selected output values are assembled into the result array.
pub fn eval_case(
    conditions: &[RegValue],
    results: &[RegValue],
    otherwise: Option<&RegValue>,
    return_type: &DataType,
    rows: usize,
) -> Result<ArrayRef> {
    if conditions.len() != results.len() {
        return Err(VeinError::internal(
            "CASE condition/result arm count mismatch",
        ));
    }

    let mut output = otherwise
        .cloned()
        .unwrap_or_else(|| RegValue::Array(new_null_array(return_type, rows)));

    for (condition, result) in conditions.iter().zip(results).rev() {
        let condition_array = if condition.data_type() == &DataType::Null {
            // A bare NULL has Arrow's Null type, but SQL permits it as a WHEN
            // condition and considers it non-true, just like NULL::BOOLEAN.
            BooleanArray::from(vec![None; rows])
        } else if condition.is_scalar() {
            let array = condition.array();
            let boolean = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| VeinError::internal("CASE WHEN condition must be boolean"))?;
            let scalar = boolean.value(0);
            BooleanArray::from(vec![
                if boolean.is_null(0) {
                    None
                } else {
                    Some(scalar)
                };
                rows
            ])
        } else {
            condition
                .array()
                .as_any()
                .downcast_ref::<BooleanArray>()
                .cloned()
                .ok_or_else(|| VeinError::internal("CASE WHEN condition must be boolean"))?
        };
        output = RegValue::Array(
            zip::zip(&condition_array, result, &output)
                .map_err(|e| VeinError::internal(format!("Vector VM CASE zip error: {e}")))?,
        );
    }

    if output.data_type() != return_type {
        return Err(VeinError::internal(format!(
            "CASE result type mismatch: expected {return_type}, got {}",
            output.data_type()
        )));
    }
    Ok(output.array().clone())
}

#[cfg(test)]
mod tests;
