//! Batch casts over fragmented references without gathering source columns.
use crate::expression::value::ExpressionValue;
use arrow::array::*;
use arrow::datatypes::{ArrowPrimitiveType, DataType};
use error::{Result, VeinError};
use std::sync::Arc;
use ::types::VeinDataType;

pub(crate) fn evaluate(
    value: &ExpressionValue, target: &VeinDataType, rows: usize,
) -> Result<ExpressionValue> {
    if target == &VeinDataType::Float64 && value.is_mapped() && rows > 1
        && value.segment(0, rows, rows)?.len() < rows
        && let Some(result) = numeric(value, rows)
    {
        tracing::debug!(target: "vein.audit", event = "reference_cast_batch",
            rows, source_type = ?value.data_type(), output_arrays = 1,
            input_payload_bytes_copied = 0, "cast fragmented references as one batch");
        return result;
    }
    super::eval_mapped_unary_value(value, rows,
                                   |len| Ok(new_null_array(&target.to_arrow(), len)),
                                   |array| super::eval_cast(array, target))
}

fn numeric(value: &ExpressionValue, rows: usize) -> Option<Result<ExpressionValue>> {
    macro_rules! native {
        ($array:ty) => { Some(convert::<$array, _>(value, rows, |v| v as f64)) };
    }
    match value.data_type() {
        DataType::Int8 => native!(Int8Type),
        DataType::Int16 => native!(Int16Type),
        DataType::Int32 => native!(Int32Type),
        DataType::Int64 => native!(Int64Type),
        DataType::UInt8 => native!(UInt8Type),
        DataType::UInt16 => native!(UInt16Type),
        DataType::UInt32 => native!(UInt32Type),
        DataType::UInt64 => native!(UInt64Type),
        DataType::Float32 => native!(Float32Type),
        DataType::Float64 => native!(Float64Type),
        DataType::Decimal128(_, scale) => {
            // Same rounding and scale operation as Arrow's decimal-to-float cast.
            let divisor = 10_f64.powi(i32::from(*scale));
            Some(convert::<Decimal128Type, _>(value, rows, |v| v as f64 / divisor))
        }
        _ => None,
    }
}

use arrow::datatypes::{
    Decimal128Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
    Int8Type, UInt16Type,
    UInt32Type, UInt64Type, UInt8Type,
};

fn convert<T: ArrowPrimitiveType, F: Fn(T::Native) -> f64>(
    value: &ExpressionValue, rows: usize, cast: F,
) -> Result<ExpressionValue> {
    let mut result = Float64Builder::with_capacity(rows);
    for row in 0..rows {
        let Some((array, index, _)) = value.source_row(row, rows)? else {
            result.append_null();
            continue;
        };
        if array.is_null(index) {
            result.append_null();
            continue;
        }
        let array = array.as_any().downcast_ref::<PrimitiveArray<T>>()
            .ok_or_else(|| VeinError::internal("mapped numeric cast input layout differs"))?;
        result.append_value(cast(array.value(index)));
    }
    Ok(ExpressionValue::Array(Arc::new(result.finish())))
}

#[cfg(test)]
mod tests;
