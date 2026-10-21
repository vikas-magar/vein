use std::sync::Arc;

use arrow::array::{Array, BooleanArray};
use common::Result;
use error::VeinError;

use crate::expression::value::ExpressionValue;
use crate::pipeline::DataChunkView;

pub(super) fn apply(
    input: &Arc<DataChunkView>,
    predicate: &ExpressionValue,
) -> Result<DataChunkView> {
    let row_count = input.num_rows();
    let mut expression_row = 0usize;
    let mut failure = None;
    let selection = input.selection().filter(|_| {
        let row = expression_row;
        expression_row += 1;
        match predicate_value(predicate, row, row_count) {
            Ok(value) => value,
            Err(error) => {
                failure = Some(error);
                false
            }
        }
    });
    if let Some(error) = failure {
        return Err(error);
    }
    input.as_ref().clone().with_selection(selection)
}

fn predicate_value(value: &ExpressionValue, row: usize, rows: usize) -> Result<bool> {
    let Some((array, index, _lease)) = value.source_row(row, rows)? else {
        return Ok(false);
    };
    let boolean = array.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| {
        VeinError::internal("filter expression did not produce boolean values")
    })?;
    if index >= boolean.len() {
        return Err(VeinError::internal("filter expression row is out of bounds"));
    }
    Ok(boolean.is_valid(index) && boolean.value(index))
}

pub(super) fn validate_rows(value: &ExpressionValue, rows: usize) -> Result<()> {
    let valid_length = if value.is_scalar() {
        value.physical_len() == 1
    } else {
        value.physical_len() == rows
    };
    if !valid_length {
        return Err(VeinError::internal(format!(
            "filter expression returned {} rows, expected {rows}",
            value.physical_len()
        )));
    }
    if value.data_type() != &arrow::datatypes::DataType::Boolean {
        return Err(VeinError::internal("filter expression did not return boolean values"));
    }
    Ok(())
}
