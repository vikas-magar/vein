use std::ops::Range;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, FixedSizeListArray, Float32Array, Float64Array, ListArray, ListLikeArray,
};
use arrow::datatypes::DataType;
use error::{Result, VeinError};

use crate::{FunctionInput, FunctionInputRow, FunctionOutput, ScalarFunction};

#[derive(Debug)]
pub(super) struct VectorDistanceFunction;

impl ScalarFunction for VectorDistanceFunction {
    fn return_type(&self, argument_types: &[DataType]) -> Result<DataType> {
        if argument_types.len() != 2 || !argument_types.iter().all(is_float32_vector) {
            return Err(VeinError::planning(
                "VECTOR_DISTANCE expects two Float32 vector arguments",
                None,
            ));
        }
        Ok(DataType::Float64)
    }

    fn invoke(
        &self,
        arguments: &[FunctionInput<'_>],
        rows: usize,
        return_type: &DataType,
    ) -> Result<FunctionOutput> {
        let [left, right] = arguments else {
            return Err(VeinError::internal("VECTOR_DISTANCE expects two arguments"));
        };
        if rows == 0 {
            return Ok(FunctionOutput::empty(return_type));
        }

        let scalar = left.is_scalar() && right.is_scalar();
        let mut values = Vec::with_capacity(if scalar { 1 } else { rows });
        for row in 0..if scalar { 1 } else { rows } {
            let left_row = left.source_row(row)?;
            let right_row = right.source_row(row)?;
            values.push(cosine_distance(left_row, right_row, row)?);
        }
        let output: ArrayRef = Arc::new(Float64Array::from(values));
        if scalar {
            FunctionOutput::from_scalar(output)
        } else {
            Ok(FunctionOutput::from_array(output))
        }
    }
}

fn is_float32_vector(data_type: &DataType) -> bool {
    match data_type {
        DataType::List(field) | DataType::FixedSizeList(field, _) => {
            field.data_type() == &DataType::Float32
        }
        _ => false,
    }
}

fn cosine_distance(
    left: Option<FunctionInputRow>,
    right: Option<FunctionInputRow>,
    row: usize,
) -> Result<Option<f64>> {
    let (Some(left), Some(right)) = (left, right) else {
        return Ok(None);
    };
    if left.array().is_null(left.row_index()) || right.array().is_null(right.row_index()) {
        return Ok(None);
    }

    let (left_values, left_range) = vector_range(left.array(), left.row_index())?;
    let (right_values, right_range) = vector_range(right.array(), right.row_index())?;
    if left_range.len() != right_range.len() {
        return Err(VeinError::internal(format!(
            "vector embedding dimension mismatch at row {row}: {} vs {}",
            left_range.len(),
            right_range.len()
        )));
    }

    let mut dot = 0.0;
    let mut left_norm = 0.0;
    let mut right_norm = 0.0;
    for (left_index, right_index) in left_range.zip(right_range) {
        if left_values.is_null(left_index) || right_values.is_null(right_index) {
            return Ok(None);
        }
        let left_value = f64::from(left_values.value(left_index));
        let right_value = f64::from(right_values.value(right_index));
        dot += left_value * right_value;
        left_norm += left_value * left_value;
        right_norm += right_value * right_value;
    }
    let denominator = left_norm.sqrt() * right_norm.sqrt();
    Ok(Some(if denominator == 0.0 {
        0.0
    } else {
        1.0 - dot / denominator
    }))
}

fn vector_range(array: &dyn Array, row: usize) -> Result<(&Float32Array, Range<usize>)> {
    let (values, range) = if let Some(fixed) = array.as_any().downcast_ref::<FixedSizeListArray>() {
        (fixed.values(), fixed.element_range(row))
    } else if let Some(list) = array.as_any().downcast_ref::<ListArray>() {
        (list.values(), list.element_range(row))
    } else {
        return Err(VeinError::internal(format!(
            "expected FixedSizeList or List of Float32, found {:?}",
            array.data_type()
        )));
    };
    let values = values
        .as_any()
        .downcast_ref::<Float32Array>()
        .ok_or_else(|| VeinError::internal("vector elements must be Float32"))?;
    Ok((values, range))
}

#[cfg(test)]
mod tests;
