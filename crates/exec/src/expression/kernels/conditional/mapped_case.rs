use std::sync::Arc;

use arrow::array::{Array, BooleanArray, new_empty_array};
use arrow::datatypes::DataType;
use common::Result;
use error::VeinError;

use crate::pipeline::view::{DerivedColumnBuilder, ViewSourceRow};
use crate::expression::value::ExpressionValue;

pub(super) fn eval_case_mapped(
    conditions: &[ExpressionValue],
    results: &[ExpressionValue],
    otherwise: Option<&ExpressionValue>,
    return_type: &DataType,
    rows: usize,
) -> Result<ExpressionValue> {
    if rows == 0 {
        return Ok(ExpressionValue::Array(new_empty_array(return_type)));
    }
    for value in results.iter().chain(otherwise) {
        if value.data_type() != return_type && value.data_type() != &DataType::Null {
            return Err(VeinError::internal(format!(
                "CASE result type mismatch: expected {return_type}, got {}",
                value.data_type()
            )));
        }
    }

    let mut output = DerivedColumnBuilder::new(return_type.clone(), rows);
    for row in 0..rows {
        let mut selected = None;
        for (arm, condition) in conditions.iter().enumerate() {
            if condition_is_true(condition, row, rows)? {
                selected = Some(arm);
                break;
            }
        }
        let source = match selected {
            Some(arm) => result_source(&results[arm], row, rows)?,
            None => otherwise
                .map(|value| result_source(value, row, rows))
                .transpose()?
                .flatten(),
        };
        output.push(source)?;
    }
    Ok(ExpressionValue::view_column(Arc::new(output.finish()?), 0)?)
}

fn condition_is_true(condition: &ExpressionValue, row: usize, rows: usize) -> Result<bool> {
    if condition.data_type() == &DataType::Null {
        return Ok(false);
    }
    let Some((array, source_row, _lease)) = condition.source_row(row, rows)? else {
        return Ok(false);
    };
    let condition = array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| VeinError::internal("CASE WHEN condition must be boolean"))?;
    Ok(!condition.is_null(source_row) && condition.value(source_row))
}

fn result_source(
    value: &ExpressionValue,
    row: usize,
    rows: usize,
) -> Result<Option<ViewSourceRow<'_>>> {
    if value.data_type() == &DataType::Null {
        return Ok(None);
    }
    value.source_row(row, rows)
}
