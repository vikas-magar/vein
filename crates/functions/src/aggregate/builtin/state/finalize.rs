use std::ops::Range;
use std::sync::Arc;

use arrow::array::{ArrayRef, Decimal128Array, Decimal256Array, Float32Array, Float64Array,
                   Int8Array, Int16Array, Int32Array, Int64Array, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
                   new_null_array};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use error::{Result, VeinError};

use crate::AggregateOperation;

use super::{BuiltinState, number::AggregateNumber, numeric::NumericAccumulator};

impl BuiltinState {
    pub(super) fn finalize(
        &self,
        operation: &AggregateOperation,
        result_type: &DataType,
        range: Range<usize>,
    ) -> Result<ArrayRef> {
        let len = self.len();
        if range.start > range.end || range.end > len {
            return Err(VeinError::internal("aggregate output range is outside state bounds"));
        }
        if let Self::SingleValue(states) = self {
            return finalize_single(states, result_type, range);
        }
        let output = match (self, operation, result_type) {
            (Self::Count(values), AggregateOperation::Count | AggregateOperation::CountDistinct, DataType::Int64) => {
                Arc::new(Int64Array::from(values[range].to_vec())) as ArrayRef
            }
            (Self::Int64(values), AggregateOperation::Avg, DataType::Float64) => {
                Arc::new(Float64Array::from(averages(&values[range])))
            }
            (Self::Int128(values), AggregateOperation::Avg, DataType::Float64) => {
                Arc::new(Float64Array::from(averages(&values[range])))
            }
            (Self::UInt64(values), AggregateOperation::Avg, DataType::Float64) => {
                Arc::new(Float64Array::from(averages(&values[range])))
            }
            (Self::UInt128(values), AggregateOperation::Avg, DataType::Float64) => {
                Arc::new(Float64Array::from(averages(&values[range])))
            }
            (Self::Float32(values), AggregateOperation::Sum | AggregateOperation::Min | AggregateOperation::Max, DataType::Float32) => {
                Arc::new(Float32Array::from(values_of(&values[range])))
            }
            (Self::Float32(values), AggregateOperation::Min | AggregateOperation::Max, DataType::Float16) => {
                let source: ArrayRef = Arc::new(Float32Array::from(values_of(&values[range])));
                cast(&source, result_type).map_err(|error| VeinError::internal(
                    format!("cannot emit Float16 aggregate result: {error}"),
                ))?
            }
            (Self::Float64(values), AggregateOperation::Avg, DataType::Float64) => {
                Arc::new(Float64Array::from(averages(&values[range])))
            }
            (Self::Float64(values), AggregateOperation::Sum | AggregateOperation::Min | AggregateOperation::Max, DataType::Float64) => {
                Arc::new(Float64Array::from(values_of(&values[range])))
            }
            (Self::Int64(values), AggregateOperation::Sum | AggregateOperation::Min | AggregateOperation::Max, DataType::Int64) => {
                Arc::new(Int64Array::from(values_of(&values[range])))
            }
            (Self::Int64(values), AggregateOperation::Min | AggregateOperation::Max, DataType::Int8) => {
                Arc::new(Int8Array::from(convert_values(&values[range], |value| value as i8)))
            }
            (Self::Int64(values), AggregateOperation::Min | AggregateOperation::Max, DataType::Int16) => {
                Arc::new(Int16Array::from(convert_values(&values[range], |value| value as i16)))
            }
            (Self::Int64(values), AggregateOperation::Min | AggregateOperation::Max, DataType::Int32) => {
                Arc::new(Int32Array::from(convert_values(&values[range], |value| value as i32)))
            }
            (Self::UInt64(values), AggregateOperation::Sum, DataType::UInt64) => {
                Arc::new(UInt64Array::from(values_of(&values[range])))
            }
            (Self::UInt64(values), AggregateOperation::Min | AggregateOperation::Max, DataType::UInt8) => {
                Arc::new(UInt8Array::from(convert_values(&values[range], |value| value as u8)))
            }
            (Self::UInt64(values), AggregateOperation::Min | AggregateOperation::Max, DataType::UInt16) => {
                Arc::new(UInt16Array::from(convert_values(&values[range], |value| value as u16)))
            }
            (Self::UInt64(values), AggregateOperation::Min | AggregateOperation::Max, DataType::UInt32) => {
                Arc::new(UInt32Array::from(convert_values(&values[range], |value| value as u32)))
            }
            (Self::UInt64(values), AggregateOperation::Min | AggregateOperation::Max, DataType::UInt64) => {
                Arc::new(UInt64Array::from(values_of(&values[range])))
            }
            (Self::Decimal128(values), AggregateOperation::Avg, DataType::Float64) => {
                Arc::new(Float64Array::from(averages(&values[range])))
            }
            (Self::Decimal128(values), AggregateOperation::Sum | AggregateOperation::Min | AggregateOperation::Max, DataType::Decimal128(precision, scale)) => {
                Arc::new(Decimal128Array::from(values_of(&values[range]))
                    .with_precision_and_scale(*precision, *scale)
                    .map_err(|error| VeinError::internal(format!("invalid Decimal128 aggregate type: {error}")))?)
            }
            (Self::Decimal256(values), AggregateOperation::Avg, DataType::Float64) => {
                Arc::new(Float64Array::from(averages(&values[range])))
            }
            (Self::Decimal256(values), AggregateOperation::Sum | AggregateOperation::Min | AggregateOperation::Max, DataType::Decimal256(precision, scale)) => {
                Arc::new(Decimal256Array::from(values_of(&values[range]))
                    .with_precision_and_scale(*precision, *scale)
                    .map_err(|error| VeinError::internal(format!("invalid Decimal256 aggregate type: {error}")))?)
            }
            _ => return Err(VeinError::internal(format!(
                "aggregate {operation:?} cannot emit output type {result_type:?}"
            ))),
        };
        Ok(output)
    }

    fn len(&self) -> usize {
        match self {
            Self::Count(values) => values.len(),
            Self::SingleValue(values) => values.len(),
            Self::Int64(values) => values.len(),
            Self::Int128(values) => values.len(),
            Self::UInt64(values) => values.len(),
            Self::UInt128(values) => values.len(),
            Self::Float32(values) => values.len(),
            Self::Float64(values) => values.len(),
            Self::Decimal128(values) => values.len(),
            Self::Decimal256(values) => values.len(),
        }
    }
}

fn finalize_single(
    states: &[Option<Option<crate::FunctionInputRow>>],
    data_type: &DataType,
    range: Range<usize>,
) -> Result<ArrayRef> {
    if range != (0..1) {
        return Err(VeinError::internal("SINGLE_VALUE requires one global group"));
    }
    match states.first() {
        Some(Some(Some(value))) => Ok(value.array_ref().slice(value.row_index(), 1)),
        Some(Some(None)) | Some(None) => Ok(new_null_array(data_type, 1)),
        _ => Err(VeinError::internal("SINGLE_VALUE state has no global group")),
    }
}

fn values_of<T: Copy>(states: &[NumericAccumulator<T>]) -> Vec<Option<T>> {
    states.iter().map(|state| state.value).collect()
}

fn convert_values<T: Copy, U>(states: &[NumericAccumulator<T>], convert: impl Fn(T) -> U) -> Vec<Option<U>> {
    states.iter().map(|state| state.value.map(&convert)).collect()
}

fn averages<T: AggregateNumber>(states: &[NumericAccumulator<T>]) -> Vec<Option<f64>> {
    states.iter().map(|state| {
        (state.count > 0).then(|| state.value.expect("non-empty AVG has a sum").to_f64(state.scale) / state.count as f64)
    }).collect()
}
