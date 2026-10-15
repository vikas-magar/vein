//! Validate and materialize requested aggregate output ranges.

use super::*;

impl AggregateStateColumn {
    pub(in crate::operators::aggregate) fn len(&self) -> usize {
        match self {
            Self::SingleValue(states) => states.len(),
            Self::Count(states) => states.len(),
            Self::Int64 { states, .. } => states.len(),
            Self::Int128Avg { states } => states.len(),
            Self::UInt64 { states, .. } => states.len(),
            Self::UInt128Avg { states } => states.len(),
            Self::Float32 { states, .. } => states.len(),
            Self::Float64 { states, .. } => states.len(),
            Self::Decimal128 { states, .. } => states.len(),
            Self::Decimal256 { states, .. } => states.len(),
        }
    }

    pub(in crate::operators::aggregate) fn to_array_range(
        &self,
        op: AggOpCode,
        data_type: &VeinDataType,
        range: Range<usize>,
    ) -> Result<ArrayRef> {
        if range.start > range.end || range.end > self.len() {
            return Err(VeinError::internal(
                "aggregate output range is outside its state column",
            ));
        }
        if let (Self::SingleValue(states), AggOpCode::SingleValue) = (self, op) {
            if range != (0..1) {
                return Err(VeinError::internal(
                    "SINGLE_VALUE requires exactly one global aggregate group",
                ));
            }
            return match states.as_slice() {
                [Some((array, row))] => Ok(array.slice(*row, 1)),
                [None] => Ok(new_null_array(&data_type.to_arrow(), 1)),
                _ => Err(VeinError::internal(
                    "SINGLE_VALUE requires exactly one global aggregate group",
                )),
            };
        }
        Ok(match (self, op, data_type) {
            (Self::Count(states), AggOpCode::Count, VeinDataType::Int64) => Arc::new(
                Int64Array::from_iter_values(states[range.clone()].iter().copied()),
            ),
            (
                Self::Int64 { states, .. },
                AggOpCode::Sum | AggOpCode::Min | AggOpCode::Max,
                VeinDataType::Int64,
            ) => Arc::new(Int64Array::from(numeric_values(&states[range.clone()]))),
            (Self::Int64 { states, .. }, AggOpCode::Min | AggOpCode::Max, VeinDataType::Int8) => {
                Arc::new(Int8Array::from(
                    numeric_values(&states[range.clone()])
                        .into_iter()
                        .map(|value| value.map(|value| value as i8))
                        .collect::<Vec<_>>(),
                ))
            }
            (Self::Int64 { states, .. }, AggOpCode::Min | AggOpCode::Max, VeinDataType::Int16) => {
                Arc::new(Int16Array::from(
                    numeric_values(&states[range.clone()])
                        .into_iter()
                        .map(|value| value.map(|value| value as i16))
                        .collect::<Vec<_>>(),
                ))
            }
            (Self::Int64 { states, .. }, AggOpCode::Min | AggOpCode::Max, VeinDataType::Int32) => {
                Arc::new(Int32Array::from(
                    numeric_values(&states[range.clone()])
                        .into_iter()
                        .map(|value| value.map(|value| value as i32))
                        .collect::<Vec<_>>(),
                ))
            }
            (Self::UInt64 { states, .. }, AggOpCode::Sum, VeinDataType::UInt64) => {
                Arc::new(UInt64Array::from(numeric_values(&states[range.clone()])))
            }
            (Self::UInt64 { states, .. }, AggOpCode::Min | AggOpCode::Max, VeinDataType::UInt8) => {
                Arc::new(UInt8Array::from(
                    numeric_values(&states[range.clone()])
                        .into_iter()
                        .map(|value| value.map(|value| value as u8))
                        .collect::<Vec<_>>(),
                ))
            }
            (
                Self::UInt64 { states, .. },
                AggOpCode::Min | AggOpCode::Max,
                VeinDataType::UInt16,
            ) => Arc::new(UInt16Array::from(
                numeric_values(&states[range.clone()])
                    .into_iter()
                    .map(|value| value.map(|value| value as u16))
                    .collect::<Vec<_>>(),
            )),
            (
                Self::UInt64 { states, .. },
                AggOpCode::Min | AggOpCode::Max,
                VeinDataType::UInt32,
            ) => Arc::new(UInt32Array::from(
                numeric_values(&states[range.clone()])
                    .into_iter()
                    .map(|value| value.map(|value| value as u32))
                    .collect::<Vec<_>>(),
            )),
            (
                Self::UInt64 { states, .. },
                AggOpCode::Min | AggOpCode::Max,
                VeinDataType::UInt64,
            ) => Arc::new(UInt64Array::from(numeric_values(&states[range.clone()]))),
            (
                Self::Float32 { states, .. },
                AggOpCode::Sum | AggOpCode::Min | AggOpCode::Max,
                VeinDataType::Float32,
            ) => Arc::new(Float32Array::from(numeric_values(&states[range.clone()]))),
            (
                Self::Float32 { states, .. },
                AggOpCode::Min | AggOpCode::Max,
                VeinDataType::Float16,
            ) => {
                let values: ArrayRef = Arc::new(Float32Array::from(numeric_values(
                    &states[range.clone()],
                )));
                arrow::compute::cast(&values, &data_type.to_arrow()).map_err(|error| {
                    VeinError::internal(format!("cannot emit Float16 aggregate result: {error}"))
                })?
            }
            (
                Self::Float64 { states, .. },
                AggOpCode::Sum | AggOpCode::Min | AggOpCode::Max,
                VeinDataType::Float64,
            ) => Arc::new(Float64Array::from(numeric_values(&states[range.clone()]))),
            (
                Self::Decimal128 { states, .. },
                AggOpCode::Sum | AggOpCode::Min | AggOpCode::Max,
                VeinDataType::Decimal128(precision, scale),
            ) => Arc::new(
                Decimal128Array::from(numeric_values(&states[range.clone()]))
                    .with_precision_and_scale(*precision, *scale)
                    .map_err(|error| {
                        VeinError::internal(format!("invalid decimal aggregate type: {error}"))
                    })?,
            ),
            (
                Self::Decimal256 { states, .. },
                AggOpCode::Sum | AggOpCode::Min | AggOpCode::Max,
                VeinDataType::Decimal256(precision, scale),
            ) => Arc::new(
                Decimal256Array::from(numeric_values(&states[range.clone()]))
                    .with_precision_and_scale(*precision, *scale)
                    .map_err(|error| {
                        VeinError::internal(format!("invalid decimal aggregate type: {error}"))
                    })?,
            ),
            (Self::Int128Avg { states }, AggOpCode::Avg, VeinDataType::Float64) => {
                Arc::new(Float64Array::from(average_values(&states[range.clone()])))
            }
            (Self::UInt128Avg { states }, AggOpCode::Avg, VeinDataType::Float64) => {
                Arc::new(Float64Array::from(average_values(&states[range.clone()])))
            }
            (Self::Float64 { states, .. }, AggOpCode::Avg, VeinDataType::Float64) => {
                Arc::new(Float64Array::from(average_values(&states[range.clone()])))
            }
            (Self::Decimal128 { states, .. }, AggOpCode::Avg, VeinDataType::Float64) => {
                Arc::new(Float64Array::from(average_values(&states[range.clone()])))
            }
            (Self::Decimal256 { states, .. }, AggOpCode::Avg, VeinDataType::Float64) => {
                Arc::new(Float64Array::from(average_values(&states[range.clone()])))
            }
            _ => {
                return Err(VeinError::internal(format!(
                    "aggregate {op:?} cannot emit output type {data_type:?}"
                )));
            }
        })
    }
}
