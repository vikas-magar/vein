use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray};
use arrow::datatypes::DataType;
use common::Result;
use error::VeinError;

use crate::vm::register::RegValue;

/// Dense compatibility API for callers that explicitly require one Arrow array.
pub fn eval_in_list(
    value: &RegValue,
    candidates: &[RegValue],
    rows: usize,
    negated: bool,
) -> Result<ArrayRef> {
    eval_in_list_value(value, candidates, rows, negated)?.materialized_array()
}

/// Evaluates IN/NOT IN while retaining computed results as mapped segments.
pub(crate) fn eval_in_list_value(
    value: &RegValue,
    candidates: &[RegValue],
    rows: usize,
    negated: bool,
) -> Result<RegValue> {
    let Some((first, rest)) = candidates.split_first() else {
        let result = BooleanArray::from(vec![negated; rows]);
        return Ok(RegValue::Array(Arc::new(result)));
    };

    let mut result = eval_in_candidate(value, first, rows)?;
    for candidate in rest {
        let equality = eval_in_candidate(value, candidate, rows)?;
        result = super::super::eval_mapped_boolean_binary(
            &result,
            &equality,
            rows,
            super::super::eval_or_datum,
        )?;
    }

    if negated {
        result = super::super::eval_mapped_unary_predicate(
            &result,
            rows,
            None,
            |array| super::super::eval_not(array),
        )?;
    }
    Ok(result)
}

fn eval_in_candidate(value: &RegValue, candidate: &RegValue, rows: usize) -> Result<RegValue> {
    // A bare SQL NULL compares as UNKNOWN without asking Arrow to compare NullArray.
    if value.data_type() == &DataType::Null || candidate.data_type() == &DataType::Null {
        return Ok(RegValue::Array(Arc::new(BooleanArray::from(vec![None; rows]))));
    }
    if value.is_mapped() || candidate.is_mapped() {
        return super::super::eval_mapped_boolean_binary(
            value,
            candidate,
            rows,
            super::super::eval_cmp_eq,
        );
    }

    let value = value.dense_datum()?;
    let candidate = candidate.dense_datum()?;
    let equality = arrow::compute::kernels::cmp::eq(&value, &candidate)
        .map_err(|error| VeinError::internal(format!("Vector VM IN equality error: {error}")))?;
    Ok(RegValue::Array(Arc::new(equality)))
}
