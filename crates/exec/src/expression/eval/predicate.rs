use arrow::array::{Array, BooleanArray, BooleanBuilder};
use common::Result;
use error::VeinError;

use crate::expression::value::ExpressionValue;

pub(super) fn evaluate_boolean(value: ExpressionValue, rows: usize) -> Result<BooleanArray> {
    if let ExpressionValue::Array(array) = &value {
        if array.len() != rows {
            return Err(VeinError::internal(format!(
                "filter expression returned {} rows, expected {rows}",
                array.len()
            )));
        }
        return array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .cloned()
            .ok_or_else(|| VeinError::internal("filter expression did not return boolean values"));
    }

    if value.data_type() != &arrow::datatypes::DataType::Boolean {
        return Err(VeinError::internal("filter expression did not return boolean values"));
    }
    let mut output = BooleanBuilder::with_capacity(rows);
    for row in 0..rows {
        let source = value.source_row(row, rows)?;
        let Some((array, index, _lease)) = source else {
            output.append_null();
            continue;
        };
        let boolean = array.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| {
            VeinError::internal("filter expression source row is not boolean")
        })?;
        if index >= boolean.len() {
            return Err(VeinError::internal("filter expression row is out of bounds"));
        }
        output.append_option(boolean.is_valid(index).then(|| boolean.value(index)));
    }
    Ok(output.finish())
}
