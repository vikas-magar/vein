//! Vector kernels for SQL CASE and IN/NOT IN expressions.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, new_null_array};
use arrow::compute::kernels::zip;
use arrow::datatypes::DataType;
use common::Result;
use error::VeinError;

use crate::vm::register::RegValue;

#[path = "conditional/mapped_case.rs"]
mod mapped_case;
#[path = "conditional/in_list.rs"]
mod in_list;
#[cfg(test)]
#[path = "conditional/mapped_tests.rs"]
mod mapped_tests;

pub use in_list::eval_in_list;
pub(crate) use in_list::eval_in_list_value;

/// Evaluates a searched CASE expression. Inputs remain Arrow arrays/scalars; only
/// the selected output values are assembled into the result array.
pub fn eval_case(
    conditions: &[RegValue],
    results: &[RegValue],
    otherwise: Option<&RegValue>,
    return_type: &DataType,
    rows: usize,
) -> Result<ArrayRef> {
    let value = eval_case_value(conditions, results, otherwise, return_type, rows)?;
    match value {
        RegValue::ViewColumn(column) => Ok(column.materialize_with_report()?.0),
        value => value.materialized_array(),
    }
}

pub(crate) fn eval_case_value(
    conditions: &[RegValue],
    results: &[RegValue],
    otherwise: Option<&RegValue>,
    return_type: &DataType,
    rows: usize,
) -> Result<RegValue> {
    if conditions.len() != results.len() {
        return Err(VeinError::internal(
            "CASE condition/result arm count mismatch",
        ));
    }
    let has_mapped_input = conditions
        .iter()
        .chain(results)
        .chain(otherwise)
        .any(RegValue::is_mapped);
    if has_mapped_input {
        return mapped_case::eval_case_mapped(conditions, results, otherwise, return_type, rows);
    }
    eval_case_dense(conditions, results, otherwise, return_type, rows).map(RegValue::Array)
}

fn eval_case_dense(
    conditions: &[RegValue],
    results: &[RegValue],
    otherwise: Option<&RegValue>,
    return_type: &DataType,
    rows: usize,
) -> Result<ArrayRef> {
    let mut output = otherwise
        .cloned()
        .unwrap_or_else(|| RegValue::Array(new_null_array(return_type, rows)));

    for (condition, result) in conditions.iter().zip(results).rev() {
        let condition_array = if condition.data_type() == &DataType::Null {
            // A bare NULL has Arrow's Null type, but SQL permits it as a WHEN
            // condition and considers it non-true, just like NULL::BOOLEAN.
            BooleanArray::from(vec![None; rows])
        } else if condition.is_scalar() {
            let array = condition.array()?;
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
                .array()?
                .as_any()
                .downcast_ref::<BooleanArray>()
                .cloned()
                .ok_or_else(|| VeinError::internal("CASE WHEN condition must be boolean"))?
        };
        let result = result.dense_datum()?;
        let output_datum = output.dense_datum()?;
        output = RegValue::Array(
            zip::zip(&condition_array, &result, &output_datum)
                .map_err(|e| VeinError::internal(format!("Vector VM CASE zip error: {e}")))?,
        );
    }

    if output.data_type() != return_type {
        return Err(VeinError::internal(format!(
            "CASE result type mismatch: expected {return_type}, got {}",
            output.data_type()
        )));
    }
    Ok(output.array()?.clone())
}

#[cfg(test)]
mod tests;
