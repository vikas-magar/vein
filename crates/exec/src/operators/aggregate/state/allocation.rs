//! Aggregate state allocation and per-group initialization.

use super::*;

impl AggregateStateColumn {
    pub(in crate::operators::aggregate) fn bytes_per_group(&self) -> usize {
        match self {
            Self::SingleValue(_) => size_of::<Option<(ArrayRef, usize)>>(),
            Self::Count(_) => size_of::<i64>(),
            Self::Int64 { .. } => size_of::<NumericAccumulator<i64>>(),
            Self::Int128Avg { .. } | Self::Decimal128 { .. } => {
                size_of::<NumericAccumulator<i128>>()
            }
            Self::UInt64 { .. } => size_of::<NumericAccumulator<u64>>(),
            Self::UInt128Avg { .. } => size_of::<NumericAccumulator<u128>>(),
            Self::Float32 { .. } => size_of::<NumericAccumulator<f32>>(),
            Self::Float64 { .. } => size_of::<NumericAccumulator<f64>>(),
            Self::Decimal256 { .. } => size_of::<NumericAccumulator<i256>>(),
        }
    }

    pub(in crate::operators::aggregate) fn new(op: AggOpCode, input_type: &VeinDataType, result_type: &VeinDataType) -> Result<Self> {
        if op == AggOpCode::SingleValue {
            return Ok(Self::SingleValue(Vec::new()));
        }
        if matches!(op, AggOpCode::Count | AggOpCode::CountDistinct) {
            return Ok(Self::Count(Vec::new()));
        }
        // Integer SUM/MIN/MAX use an Int64 state; integer AVG needs a wide
        // exact sum so cancellation and large values are preserved until finish.
        if matches!(
            input_type,
            VeinDataType::Int8 | VeinDataType::Int16 | VeinDataType::Int32 | VeinDataType::Int64
        ) {
            if op == AggOpCode::Avg {
                return Ok(Self::Int128Avg { states: Vec::new() });
            }
            return Ok(Self::Int64 {
                op,
                states: Vec::new(),
            });
        }
        if matches!(
            input_type,
            VeinDataType::UInt8
                | VeinDataType::UInt16
                | VeinDataType::UInt32
                | VeinDataType::UInt64
        ) {
            if op == AggOpCode::Avg {
                return Ok(Self::UInt128Avg { states: Vec::new() });
            }
            return Ok(Self::UInt64 {
                op,
                states: Vec::new(),
            });
        }
        if input_type == &VeinDataType::Float16 && matches!(op, AggOpCode::Min | AggOpCode::Max) {
            return Ok(Self::Float32 {
                op,
                states: Vec::new(),
            });
        }
        let state_type = if op == AggOpCode::Avg {
            match input_type {
                VeinDataType::Float16 | VeinDataType::Float32 => {
                    return Ok(Self::Float64 {
                        op,
                        states: Vec::new(),
                    });
                }
                input_type => input_type,
            }
        } else {
            result_type
        };
        Ok(match state_type {
            VeinDataType::Int64 => Self::Int64 {
                op,
                states: Vec::new(),
            },
            VeinDataType::Float32 => Self::Float32 {
                op,
                states: Vec::new(),
            },
            VeinDataType::Float64 => Self::Float64 {
                op,
                states: Vec::new(),
            },
            VeinDataType::Decimal128(_, scale) => Self::Decimal128 {
                op,
                scale: *scale,
                states: Vec::new(),
            },
            VeinDataType::Decimal256(_, scale) => Self::Decimal256 {
                op,
                scale: *scale,
                states: Vec::new(),
            },
            other => {
                return Err(VeinError::internal(format!(
                    "aggregate state type {other:?} is not supported yet"
                )));
            }
        })
    }

    pub(in crate::operators::aggregate) fn push_group(&mut self) {
        match self {
            Self::SingleValue(states) => states.push(None),
            Self::Count(states) => states.push(0),
            Self::Int64 { op, states } => states.push(NumericAccumulator::new(*op, 0, false)),
            Self::Int128Avg { states } => {
                states.push(NumericAccumulator::new(AggOpCode::Avg, 0, false))
            }
            Self::UInt64 { op, states } => states.push(NumericAccumulator::new(*op, 0, false)),
            Self::UInt128Avg { states } => {
                states.push(NumericAccumulator::new(AggOpCode::Avg, 0, false))
            }
            Self::Float32 { op, states } => states.push(NumericAccumulator::new(*op, 0, false)),
            Self::Float64 { op, states } => states.push(NumericAccumulator::new(*op, 0, false)),
            Self::Decimal128 { op, scale, states } => {
                states.push(NumericAccumulator::new(*op, *scale, *op == AggOpCode::Sum))
            }
            Self::Decimal256 { op, scale, states } => {
                states.push(NumericAccumulator::new(*op, *scale, *op == AggOpCode::Sum))
            }
        }
    }
}
