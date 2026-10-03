//! Hash aggregation: the pipeline breaker that groups rows and folds them.
//!
//! Like a join, an aggregate needs the whole input before it can emit anything,
//! so it is not a VM instruction: the runtime drains the program into
//! [`AggregateSink`] and then takes the groups. The columns it receives are the
//! program's output registers — the group keys first, then one argument column
//! per aggregate — so a key or an argument like `sum(price * quantity)` is an
//! ordinary register and this module never interprets an expression.
//!
//! A `DISTINCT` arrives here as the same shape with every column a key and no
//! aggregates, which is why it has no operator of its own.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arrow::array::{
    Array, ArrayRef, Decimal128Array, Float16Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array, StringArray, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array, Decimal256Array,
};
use arrow::datatypes::i256;

use crate::pipeline::{DataChunk, PipelineNode};
use common::Result;
use common::protocol::{AggOpCode, AggregatePlan};
use error::VeinError;
use types::VeinDataType;

/// One group key value, kept in the shape its output column needs.
#[derive(Debug, Clone, PartialEq)]
enum KeyValue {
    Null,
    Int64(i64),
    UInt64(u64),
    Utf8(String),
}

/// Operations shared by every numeric accumulator type.
trait AggregateValue: Copy + PartialOrd {
    fn checked_add(self, other: Self, decimal_precision_limit: bool) -> Option<Self>;
    fn minimum(self, other: Self) -> Self;
    fn maximum(self, other: Self) -> Self;
    fn to_f64(self, scale: i8) -> f64;
}

impl AggregateValue for i64 {
    fn checked_add(self, other: Self, _decimal_precision_limit: bool) -> Option<Self> {
        i64::checked_add(self, other)
    }

    fn minimum(self, other: Self) -> Self {
        self.min(other)
    }

    fn maximum(self, other: Self) -> Self {
        self.max(other)
    }

    fn to_f64(self, _scale: i8) -> f64 {
        self as f64
    }
}

impl AggregateValue for i128 {
    fn checked_add(self, other: Self, decimal_precision_limit: bool) -> Option<Self> {
        i128::checked_add(self, other).filter(|sum| {
            !decimal_precision_limit || {
                let max_decimal_38 = 10_i128.pow(38) - 1;
                (-max_decimal_38..=max_decimal_38).contains(sum)
            }
        })
    }

    fn minimum(self, other: Self) -> Self {
        self.min(other)
    }

    fn maximum(self, other: Self) -> Self {
        self.max(other)
    }

    fn to_f64(self, scale: i8) -> f64 {
        self as f64 / 10_f64.powi(scale as i32)
    }
}

impl AggregateValue for i256 {
    fn checked_add(self, other: Self, decimal_precision_limit: bool) -> Option<Self> {
        i256::checked_add(self, other).filter(|sum| {
            !decimal_precision_limit || {
                let max_decimal_76 = i256::from(10_i8)
                    .checked_pow(76)
                    .and_then(|limit| limit.checked_sub(i256::ONE))
                    .expect("decimal precision limit fits in i256");
                (-max_decimal_76..=max_decimal_76).contains(sum)
            }
        })
    }

    fn minimum(self, other: Self) -> Self {
        self.min(other)
    }

    fn maximum(self, other: Self) -> Self {
        self.max(other)
    }

    fn to_f64(self, scale: i8) -> f64 {
        self.to_string()
            .parse::<f64>()
            .expect("i256 decimal representation parses as f64")
            / 10_f64.powi(scale as i32)
    }
}

impl AggregateValue for u64 {
    fn checked_add(self, other: Self, _decimal_precision_limit: bool) -> Option<Self> {
        u64::checked_add(self, other)
    }

    fn minimum(self, other: Self) -> Self {
        self.min(other)
    }

    fn maximum(self, other: Self) -> Self {
        self.max(other)
    }

    fn to_f64(self, _scale: i8) -> f64 {
        self as f64
    }
}

impl AggregateValue for u128 {
    fn checked_add(self, other: Self, _decimal_precision_limit: bool) -> Option<Self> {
        u128::checked_add(self, other)
    }

    fn minimum(self, other: Self) -> Self {
        self.min(other)
    }

    fn maximum(self, other: Self) -> Self {
        self.max(other)
    }

    fn to_f64(self, _scale: i8) -> f64 {
        self as f64
    }
}

impl AggregateValue for f64 {
    fn checked_add(self, other: Self, _decimal_precision_limit: bool) -> Option<Self> {
        Some(self + other)
    }

    fn minimum(self, other: Self) -> Self {
        f64::min(self, other)
    }

    fn maximum(self, other: Self) -> Self {
        f64::max(self, other)
    }

    fn to_f64(self, _scale: i8) -> f64 {
        self
    }
}

impl AggregateValue for f32 {
    fn checked_add(self, other: Self, _decimal_precision_limit: bool) -> Option<Self> {
        Some(self + other)
    }

    fn minimum(self, other: Self) -> Self {
        f32::min(self, other)
    }

    fn maximum(self, other: Self) -> Self {
        f32::max(self, other)
    }

    fn to_f64(self, _scale: i8) -> f64 {
        self as f64
    }
}

/// The shared fold logic, monomorphized for the input's physical value type.
#[derive(Debug, Clone, Copy)]
struct NumericAccumulator<T> {
    op: AggOpCode,
    value: Option<T>,
    count: i64,
    scale: i8,
    decimal_precision_limit: bool,
}

impl<T: AggregateValue> NumericAccumulator<T> {
    fn new(op: AggOpCode, scale: i8, decimal_precision_limit: bool) -> Self {
        Self {
            op,
            value: None,
            count: 0,
            scale,
            decimal_precision_limit,
        }
    }

    fn update(&mut self, value: Option<T>) -> Result<()> {
        let Some(value) = value else {
            return Ok(());
        };
        match self.op {
            AggOpCode::Count => self.count += 1,
            AggOpCode::Sum | AggOpCode::Avg => {
                let total = match self.value {
                    Some(current) => current
                        .checked_add(value, self.decimal_precision_limit)
                        .ok_or_else(|| {
                            let operation = match self.op {
                                AggOpCode::Sum => "SUM",
                                AggOpCode::Avg => "AVG",
                                _ => unreachable!("only additive aggregates reach this arm"),
                            };
                            VeinError::internal(format!(
                                "numeric overflow while computing {operation}"
                            ))
                        })?,
                    None => value,
                };
                self.value = Some(total);
                if self.op == AggOpCode::Avg {
                    self.count += 1;
                }
            }
            AggOpCode::Min => {
                self.value = Some(self.value.map_or(value, |current| current.minimum(value)));
            }
            AggOpCode::Max => {
                self.value = Some(self.value.map_or(value, |current| current.maximum(value)));
            }
        }
        Ok(())
    }
}

/// A borrowed aggregate input, downcast once for the current input batch.
///
/// The array remains owned by the input chunk; this view only caches its
/// physical layout while rows are folded into their groups.
#[derive(Clone, Copy)]
enum AggregateInput<'a> {
    Count(&'a ArrayRef),
    Int8(&'a Int8Array),
    Int16(&'a Int16Array),
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
    UInt8(&'a UInt8Array),
    UInt16(&'a UInt16Array),
    UInt32(&'a UInt32Array),
    UInt64(&'a UInt64Array),
    Float16(&'a Float16Array),
    Float32(&'a Float32Array),
    Float64(&'a Float64Array),
    Decimal128(&'a Decimal128Array),
    Decimal256(&'a Decimal256Array),
}

impl<'a> AggregateInput<'a> {
    fn new(
        op: AggOpCode,
        input_type: &VeinDataType,
        array: &'a ArrayRef,
    ) -> Result<Self> {
        if op == AggOpCode::Count {
            return Ok(Self::Count(array));
        }
        // The borrowed array always follows the argument layout. The
        // accumulator may use a wider result layout, such as Float32 SUM.
        let data_type = input_type;
        let mismatch = || {
            VeinError::internal(format!(
                "aggregate input array does not match {data_type:?} kernel"
            ))
        };
        match data_type {
            VeinDataType::Int8 => array
                .as_any()
                .downcast_ref::<Int8Array>()
                .map(Self::Int8)
                .ok_or_else(mismatch),
            VeinDataType::Int16 => array
                .as_any()
                .downcast_ref::<Int16Array>()
                .map(Self::Int16)
                .ok_or_else(mismatch),
            VeinDataType::Int32 => array
                .as_any()
                .downcast_ref::<Int32Array>()
                .map(Self::Int32)
                .ok_or_else(mismatch),
            VeinDataType::Int64 => array
                .as_any()
                .downcast_ref::<Int64Array>()
                .map(Self::Int64)
                .ok_or_else(mismatch),
            VeinDataType::UInt8 => array
                .as_any()
                .downcast_ref::<UInt8Array>()
                .map(Self::UInt8)
                .ok_or_else(mismatch),
            VeinDataType::UInt16 => array
                .as_any()
                .downcast_ref::<UInt16Array>()
                .map(Self::UInt16)
                .ok_or_else(mismatch),
            VeinDataType::UInt32 => array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .map(Self::UInt32)
                .ok_or_else(mismatch),
            VeinDataType::UInt64 => array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .map(Self::UInt64)
                .ok_or_else(mismatch),
            VeinDataType::Float16 => array
                .as_any()
                .downcast_ref::<Float16Array>()
                .map(Self::Float16)
                .ok_or_else(mismatch),
            VeinDataType::Float32 => array
                .as_any()
                .downcast_ref::<Float32Array>()
                .map(Self::Float32)
                .ok_or_else(mismatch),
            VeinDataType::Float64 => array
                .as_any()
                .downcast_ref::<Float64Array>()
                .map(Self::Float64)
                .ok_or_else(mismatch),
            VeinDataType::Decimal128(_, _) => array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .map(Self::Decimal128)
                .ok_or_else(mismatch),
            VeinDataType::Decimal256(_, _) => array
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .map(Self::Decimal256)
                .ok_or_else(mismatch),
            other => Err(VeinError::internal(format!(
                "aggregate input type {other:?} is not supported yet"
            ))),
        }
    }
}

/// Shared selected-row fold, monomorphized for a kernel's accumulator type.
fn update_numeric_states<T: AggregateValue>(
    states: &mut [NumericAccumulator<T>],
    group_rows: &[(usize, usize)],
    value_at: impl Fn(usize) -> Option<T>,
) -> Result<()> {
    for &(slot, row) in group_rows {
        states[slot].update(value_at(row))?;
    }
    Ok(())
}

fn numeric_values<T: Copy>(states: &[NumericAccumulator<T>]) -> Vec<Option<T>> {
    states.iter().map(|state| state.value).collect()
}

fn average_values<T: AggregateValue>(states: &[NumericAccumulator<T>]) -> Vec<Option<f64>> {
    states
        .iter()
        .map(|state| {
            (state.count > 0).then(|| {
                state.value.expect("a non-empty average has a sum").to_f64(state.scale)
                    / state.count as f64
            })
        })
        .collect()
}

/// A typed state vector selected once when the aggregate sink is created.
enum AggregateStateColumn {
    Count(Vec<i64>),
    Int64 {
        op: AggOpCode,
        states: Vec<NumericAccumulator<i64>>,
    },
    Int128Avg {
        states: Vec<NumericAccumulator<i128>>,
    },
    UInt64 {
        op: AggOpCode,
        states: Vec<NumericAccumulator<u64>>,
    },
    UInt128Avg {
        states: Vec<NumericAccumulator<u128>>,
    },
    Float32 {
        op: AggOpCode,
        states: Vec<NumericAccumulator<f32>>,
    },
    Float64 {
        op: AggOpCode,
        states: Vec<NumericAccumulator<f64>>,
    },
    Decimal128 {
        op: AggOpCode,
        scale: i8,
        states: Vec<NumericAccumulator<i128>>,
    },
    Decimal256 {
        op: AggOpCode,
        scale: i8,
        states: Vec<NumericAccumulator<i256>>,
    },
}

impl AggregateStateColumn {
    fn new(
        op: AggOpCode,
        input_type: &VeinDataType,
        result_type: &VeinDataType,
    ) -> Result<Self> {
        if op == AggOpCode::Count {
            return Ok(Self::Count(Vec::new()));
        }
        // Integer SUM/MIN/MAX use an Int64 state; integer AVG needs a wide
        // exact sum so cancellation and large values are preserved until finish.
        if matches!(
            input_type,
            VeinDataType::Int8
                | VeinDataType::Int16
                | VeinDataType::Int32
                | VeinDataType::Int64
        ) {
            if op == AggOpCode::Avg {
                return Ok(Self::Int128Avg {
                    states: Vec::new(),
                });
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
                return Ok(Self::UInt128Avg {
                    states: Vec::new(),
                });
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

    fn push_group(&mut self) {
        match self {
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
                states.push(NumericAccumulator::new(
                    *op,
                    *scale,
                    *op == AggOpCode::Sum,
                ))
            }
            Self::Decimal256 { op, scale, states } => states.push(NumericAccumulator::new(
                *op,
                *scale,
                *op == AggOpCode::Sum,
            )),
        }
    }

    fn update_batch(
        &mut self,
        input: AggregateInput<'_>,
        group_rows: &[(usize, usize)],
    ) -> Result<()> {
        match (self, input) {
            (Self::Count(states), AggregateInput::Count(array)) => {
                for &(slot, row) in group_rows {
                    if !array.is_null(row) {
                        states[slot] += 1;
                    }
                }
                Ok(())
            }
            (Self::Int64 { states, .. }, AggregateInput::Int8(array)) => {
                update_numeric_states(states, group_rows, |row| {
                    (!array.is_null(row)).then(|| array.value(row) as i64)
                })
            }
            (Self::Int64 { states, .. }, AggregateInput::Int16(array)) => {
                update_numeric_states(states, group_rows, |row| {
                    (!array.is_null(row)).then(|| array.value(row) as i64)
                })
            }
            (Self::Int64 { states, .. }, AggregateInput::Int32(array)) => {
                update_numeric_states(states, group_rows, |row| {
                    (!array.is_null(row)).then(|| array.value(row) as i64)
                })
            }
            (Self::Int64 { states, .. }, AggregateInput::Int64(array)) => {
                update_numeric_states(states, group_rows, |row| {
                    (!array.is_null(row)).then(|| array.value(row))
                })
            }
            (Self::Int128Avg { states }, AggregateInput::Int8(array)) => {
                update_numeric_states(states, group_rows, |row| {
                    (!array.is_null(row)).then(|| array.value(row) as i128)
                })
            }
            (Self::Int128Avg { states }, AggregateInput::Int16(array)) => {
                update_numeric_states(states, group_rows, |row| {
                    (!array.is_null(row)).then(|| array.value(row) as i128)
                })
            }
            (Self::Int128Avg { states }, AggregateInput::Int32(array)) => {
                update_numeric_states(states, group_rows, |row| {
                    (!array.is_null(row)).then(|| array.value(row) as i128)
                })
            }
            (Self::Int128Avg { states }, AggregateInput::Int64(array)) => {
                update_numeric_states(states, group_rows, |row| {
                    (!array.is_null(row)).then(|| array.value(row) as i128)
                })
            }
            (Self::UInt64 { states, .. }, AggregateInput::UInt8(array)) => {
                update_numeric_states(states, group_rows, |row| {
                    (!array.is_null(row)).then(|| array.value(row) as u64)
                })
            }
            (Self::UInt64 { states, .. }, AggregateInput::UInt16(array)) => {
                update_numeric_states(states, group_rows, |row| {
                    (!array.is_null(row)).then(|| array.value(row) as u64)
                })
            }
            (Self::UInt64 { states, .. }, AggregateInput::UInt32(array)) => {
                update_numeric_states(states, group_rows, |row| {
                    (!array.is_null(row)).then(|| array.value(row) as u64)
                })
            }
            (Self::UInt64 { states, .. }, AggregateInput::UInt64(array)) => {
                update_numeric_states(states, group_rows, |row| {
                    (!array.is_null(row)).then(|| array.value(row))
                })
            }
            (Self::UInt128Avg { states }, AggregateInput::UInt8(array)) => {
                update_numeric_states(states, group_rows, |row| {
                    (!array.is_null(row)).then(|| array.value(row) as u128)
                })
            }
            (Self::UInt128Avg { states }, AggregateInput::UInt16(array)) => {
                update_numeric_states(states, group_rows, |row| {
                    (!array.is_null(row)).then(|| array.value(row) as u128)
                })
            }
            (Self::UInt128Avg { states }, AggregateInput::UInt32(array)) => {
                update_numeric_states(states, group_rows, |row| {
                    (!array.is_null(row)).then(|| array.value(row) as u128)
                })
            }
            (Self::UInt128Avg { states }, AggregateInput::UInt64(array)) => {
                update_numeric_states(states, group_rows, |row| {
                    (!array.is_null(row)).then(|| array.value(row) as u128)
                })
            }
            (Self::Float32 { states, .. }, AggregateInput::Float16(array)) => {
                update_numeric_states(states, group_rows, |row| {
                    (!array.is_null(row)).then(|| array.value(row).to_f32())
                })
            }
            (Self::Float64 { states, .. }, AggregateInput::Float16(array)) => {
                update_numeric_states(states, group_rows, |row| {
                    (!array.is_null(row)).then(|| array.value(row).to_f64())
                })
            }
            (Self::Float32 { states, .. }, AggregateInput::Float32(array)) => {
                update_numeric_states(states, group_rows, |row| {
                    (!array.is_null(row)).then(|| array.value(row))
                })
            }
            (Self::Float64 { states, .. }, AggregateInput::Float32(array)) => {
                update_numeric_states(states, group_rows, |row| {
                    (!array.is_null(row)).then(|| array.value(row) as f64)
                })
            }
            (Self::Float64 { states, .. }, AggregateInput::Float64(array)) => {
                update_numeric_states(states, group_rows, |row| {
                    (!array.is_null(row)).then(|| array.value(row))
                })
            }
            (Self::Decimal128 { states, .. }, AggregateInput::Decimal128(array)) => {
                update_numeric_states(states, group_rows, |row| {
                    (!array.is_null(row)).then(|| array.value(row))
                })
            }
            (Self::Decimal256 { states, .. }, AggregateInput::Decimal256(array)) => {
                update_numeric_states(states, group_rows, |row| {
                    (!array.is_null(row)).then(|| array.value(row))
                })
            }
            _ => Err(VeinError::internal(
                "aggregate input layout does not match its typed state column",
            )),
        }
    }

    fn to_array(&self, op: AggOpCode, data_type: &VeinDataType) -> Result<ArrayRef> {
        Ok(match (self, op, data_type) {
            (Self::Count(states), AggOpCode::Count, VeinDataType::Int64) => {
                Arc::new(Int64Array::from(states.clone()))
            }
            (Self::Int64 { states, .. }, AggOpCode::Sum | AggOpCode::Min | AggOpCode::Max, VeinDataType::Int64) => {
                Arc::new(Int64Array::from(numeric_values(states)))
            }
            (Self::Int64 { states, .. }, AggOpCode::Min | AggOpCode::Max, VeinDataType::Int8) => {
                Arc::new(Int8Array::from(
                    numeric_values(states)
                        .into_iter()
                        .map(|value| value.map(|value| value as i8))
                        .collect::<Vec<_>>(),
                ))
            }
            (Self::Int64 { states, .. }, AggOpCode::Min | AggOpCode::Max, VeinDataType::Int16) => {
                Arc::new(Int16Array::from(
                    numeric_values(states)
                        .into_iter()
                        .map(|value| value.map(|value| value as i16))
                        .collect::<Vec<_>>(),
                ))
            }
            (Self::Int64 { states, .. }, AggOpCode::Min | AggOpCode::Max, VeinDataType::Int32) => {
                Arc::new(Int32Array::from(
                    numeric_values(states)
                        .into_iter()
                        .map(|value| value.map(|value| value as i32))
                        .collect::<Vec<_>>(),
                ))
            }
            (Self::UInt64 { states, .. }, AggOpCode::Sum, VeinDataType::UInt64) => {
                Arc::new(UInt64Array::from(numeric_values(states)))
            }
            (Self::UInt64 { states, .. }, AggOpCode::Min | AggOpCode::Max, VeinDataType::UInt8) => {
                Arc::new(UInt8Array::from(numeric_values(states).into_iter().map(|value| value.map(|value| value as u8)).collect::<Vec<_>>()))
            }
            (Self::UInt64 { states, .. }, AggOpCode::Min | AggOpCode::Max, VeinDataType::UInt16) => {
                Arc::new(UInt16Array::from(numeric_values(states).into_iter().map(|value| value.map(|value| value as u16)).collect::<Vec<_>>()))
            }
            (Self::UInt64 { states, .. }, AggOpCode::Min | AggOpCode::Max, VeinDataType::UInt32) => {
                Arc::new(UInt32Array::from(numeric_values(states).into_iter().map(|value| value.map(|value| value as u32)).collect::<Vec<_>>()))
            }
            (Self::UInt64 { states, .. }, AggOpCode::Min | AggOpCode::Max, VeinDataType::UInt64) => {
                Arc::new(UInt64Array::from(numeric_values(states)))
            }
            (Self::Float32 { states, .. }, AggOpCode::Sum | AggOpCode::Min | AggOpCode::Max, VeinDataType::Float32) => {
                Arc::new(Float32Array::from(numeric_values(states)))
            }
            (Self::Float32 { states, .. }, AggOpCode::Min | AggOpCode::Max, VeinDataType::Float16) => {
                let values: ArrayRef = Arc::new(Float32Array::from(numeric_values(states)));
                arrow::compute::cast(&values, &data_type.to_arrow()).map_err(|error| {
                    VeinError::internal(format!("cannot emit Float16 aggregate result: {error}"))
                })?
            }
            (Self::Float64 { states, .. }, AggOpCode::Sum | AggOpCode::Min | AggOpCode::Max, VeinDataType::Float64) => {
                Arc::new(Float64Array::from(numeric_values(states)))
            }
            (Self::Decimal128 { states, .. }, AggOpCode::Sum | AggOpCode::Min | AggOpCode::Max, VeinDataType::Decimal128(precision, scale)) => {
                Arc::new(
                    Decimal128Array::from(numeric_values(states))
                        .with_precision_and_scale(*precision, *scale)
                        .map_err(|error| {
                            VeinError::internal(format!(
                                "invalid decimal aggregate type: {error}"
                            ))
                        })?,
                )
            }
            (Self::Decimal256 { states, .. }, AggOpCode::Sum | AggOpCode::Min | AggOpCode::Max, VeinDataType::Decimal256(precision, scale)) => {
                Arc::new(
                    Decimal256Array::from(numeric_values(states))
                        .with_precision_and_scale(*precision, *scale)
                        .map_err(|error| {
                            VeinError::internal(format!(
                                "invalid decimal aggregate type: {error}"
                            ))
                        })?,
                )
            }
            (Self::Int128Avg { states }, AggOpCode::Avg, VeinDataType::Float64) => {
                Arc::new(Float64Array::from(average_values(states)))
            }
            (Self::UInt128Avg { states }, AggOpCode::Avg, VeinDataType::Float64) => {
                Arc::new(Float64Array::from(average_values(states)))
            }
            (Self::Float64 { states, .. }, AggOpCode::Avg, VeinDataType::Float64) => {
                Arc::new(Float64Array::from(average_values(states)))
            }
            (Self::Decimal128 { states, .. }, AggOpCode::Avg, VeinDataType::Float64) => {
                Arc::new(Float64Array::from(average_values(states)))
            }
            (Self::Decimal256 { states, .. }, AggOpCode::Avg, VeinDataType::Float64) => {
                Arc::new(Float64Array::from(average_values(states)))
            }
            _ => {
                return Err(VeinError::internal(format!(
                    "aggregate {op:?} cannot emit output type {data_type:?}"
                )));
            }
        })
    }
}

/// One group: its key and slot in every aggregate state column.
struct Group {
    key: Vec<KeyValue>,
}

/// Absorbs rows into groups, then emits one row per group.
pub struct AggregateSink {
    plan: AggregatePlan,
    /// Each grouping set's key positions in the projected key columns.
    grouping_sets: Vec<Vec<u32>>,
    /// Encoded key to index in `groups`: encoding each key once makes a group
    /// lookup a hash of bytes rather than a comparison of typed columns.
    index: HashMap<Vec<u8>, usize>,
    groups: Vec<Group>,
    aggregate_states: Vec<AggregateStateColumn>,
}

impl AggregateSink {
    /// An accumulator for `plan`, refusing a shape it cannot emit.
    pub fn new(plan: AggregatePlan) -> Result<Self> {
        let expected = plan.groups as usize + plan.aggs.len();
        if plan.aggregate_input_types.len() != plan.aggs.len() {
            return Err(VeinError::internal(format!(
                "aggregate plan describes {} inputs but {} aggregate operations",
                plan.aggregate_input_types.len(),
                plan.aggs.len()
            )));
        }
        if plan.types.len() != expected {
            return Err(VeinError::internal(format!(
                "aggregate plan describes {} output columns but {} types",
                expected,
                plan.types.len()
            )));
        }
        let groups = plan.groups as usize;
        let grouping_sets = plan.grouping_sets.clone().unwrap_or_else(|| {
            vec![(0..plan.groups).collect()]
        });
        if grouping_sets.is_empty() {
            return Err(VeinError::internal(
                "aggregate plan must contain at least one grouping set",
            ));
        }
        for grouping_set in &grouping_sets {
            let mut seen = Vec::with_capacity(grouping_set.len());
            for &column in grouping_set {
                if column as usize >= groups {
                    return Err(VeinError::internal(format!(
                        "aggregate grouping set references key column {column}, but only {groups} key columns exist"
                    )));
                }
                if seen.contains(&column) {
                    return Err(VeinError::internal(format!(
                        "aggregate grouping set repeats key column {column}"
                    )));
                }
                seen.push(column);
            }
        }
        let aggregate_states = plan
            .aggs
            .iter()
            .zip(plan.aggregate_input_types.iter())
            .zip(plan.types.iter().skip(groups))
            .map(|((op, input_type), result_type)| {
                AggregateStateColumn::new(*op, input_type, result_type)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            plan,
            grouping_sets,
            index: HashMap::new(),
            groups: Vec::new(),
            aggregate_states,
        })
    }

    /// Adds a group slot and its corresponding state to every aggregate.
    fn push_group(
        groups: &mut Vec<Group>,
        aggregate_states: &mut [AggregateStateColumn],
        key: Vec<KeyValue>,
    ) -> Result<usize> {
        let slot = groups.len();
        groups.push(Group { key });
        for column in aggregate_states {
            column.push_group();
        }
        Ok(slot)
    }

    /// Folds one chunk into the groups.
    pub fn accumulate(&mut self, chunk: DataChunk) -> Result<()> {
        let groups = self.plan.groups as usize;
        if chunk.num_columns() != groups + self.plan.aggs.len() {
            return Err(VeinError::internal(format!(
                "aggregate expects {} columns, got {}",
                groups + self.plan.aggs.len(),
                chunk.num_columns()
            )));
        }
        let mut key = Vec::new();
        let mut group_rows = Vec::with_capacity(
            chunk.selection.size().saturating_mul(self.grouping_sets.len()),
        );
        for logical_row in 0..chunk.selection.size() {
            let row = chunk.selection.get_index(logical_row);
            for (set_id, grouping_set) in self.grouping_sets.iter().enumerate() {
                key.clear();
                key.extend_from_slice(&(set_id as u64).to_le_bytes());
                let mut values = vec![KeyValue::Null; groups];
                for &column in grouping_set {
                    let column = column as usize;
                    let value = key_value(chunk.column(column), row)?;
                    encode(&value, &mut key);
                    values[column] = value;
                }

                // Include the set identity: an omitted key and a real NULL key
                // can have equal output values but remain distinct groups.
                let slot = match self.index.get(&key).copied() {
                    Some(slot) => slot,
                    None => {
                        let slot = Self::push_group(
                            &mut self.groups,
                            &mut self.aggregate_states,
                            values,
                        )?;
                        self.index.insert(key.clone(), slot);
                        slot
                    }
                };
                group_rows.push((slot, row));
            }
        }

        // Resolve each physical input layout once per aggregate column and
        // batch, then fold the selected rows through the borrowed view.
        for (offset, op) in self.plan.aggs.iter().enumerate() {
            let input = AggregateInput::new(
                *op,
                &self.plan.aggregate_input_types[offset],
                chunk.column(groups + offset),
            )?;
            self.aggregate_states[offset].update_batch(input, &group_rows)?;
        }
        Ok(())
    }

    /// The finished groups, as one chunk in the plan's output types.
    pub fn finish(&mut self) -> Result<Vec<DataChunk>> {
        if self.groups.is_empty() {
            let empty_sets: Vec<usize> = self
                .grouping_sets
                .iter()
                .enumerate()
                .filter_map(|(set_id, grouping_set)| grouping_set.is_empty().then_some(set_id))
                .collect();
            for set_id in empty_sets {
                let key = (set_id as u64).to_le_bytes().to_vec();
                if !self.index.contains_key(&key) {
                    Self::push_group(
                        &mut self.groups,
                        &mut self.aggregate_states,
                        vec![KeyValue::Null; self.plan.groups as usize],
                    )?;
                    let slot = self.groups.len() - 1;
                    self.index.insert(key, slot);
                }
            }
            if self.groups.is_empty() {
                return Ok(Vec::new());
            }
        }
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.plan.types.len());
        for (column, data_type) in self.plan.types.iter().enumerate() {
            arrays.push(self.column(column, data_type)?);
        }
        Ok(vec![DataChunk::from_vectors(arrays)])
    }

    /// One output column, built from every group in group order.
    fn column(&self, column: usize, data_type: &VeinDataType) -> Result<ArrayRef> {
        let groups = self.plan.groups as usize;
        if column < groups {
            return key_column(&self.groups, column, data_type);
        }
        let offset = column - groups;
        self.aggregate_states[offset].to_array(self.plan.aggs[offset], data_type)
    }

    /// How many groups were found, for accounting.
    pub fn group_count(&self) -> usize {
        self.groups.len()
    }
}

/// Forwards pushes into the shared accumulator so the runtime can take the
/// groups once the source has been drained.
struct AggregateHandle(Arc<Mutex<AggregateSink>>);

impl PipelineNode<DataChunk> for AggregateHandle {
    fn push(&mut self, chunk: DataChunk) -> Result<()> {
        self.0
            .lock()
            .map_err(|_| VeinError::internal("aggregate mutex poisoned"))?
            .accumulate(chunk)
    }

    /// Nothing to do: the runtime asks for the groups explicitly, because it
    /// has to push them into the sink itself.
    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

/// Builds the accumulator and the node that feeds it.
pub fn shared(
    plan: AggregatePlan,
) -> Result<(Arc<Mutex<AggregateSink>>, Box<dyn PipelineNode<DataChunk>>)> {
    let sink = Arc::new(Mutex::new(AggregateSink::new(plan)?));
    let handle: Box<dyn PipelineNode<DataChunk>> = Box::new(AggregateHandle(Arc::clone(&sink)));
    Ok((sink, handle))
}

/// One key column's value at `row`.
fn key_value(array: &ArrayRef, row: usize) -> Result<KeyValue> {
    if array.is_null(row) {
        return Ok(KeyValue::Null);
    }
    if let Some(values) = array.as_any().downcast_ref::<Int64Array>() {
        return Ok(KeyValue::Int64(values.value(row)));
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt8Array>() {
        return Ok(KeyValue::UInt64(values.value(row) as u64));
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt16Array>() {
        return Ok(KeyValue::UInt64(values.value(row) as u64));
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt32Array>() {
        return Ok(KeyValue::UInt64(values.value(row) as u64));
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt64Array>() {
        return Ok(KeyValue::UInt64(values.value(row)));
    }
    if let Some(values) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(KeyValue::Utf8(values.value(row).to_string()));
    }
    Err(VeinError::internal(format!(
        "grouping by {:?} is not supported yet",
        array.data_type()
    )))
}

/// Appends a key value to a group key's encoding.
///
/// Each value carries a tag, so a `NULL` and an empty string cannot encode the
/// same way, and each string carries its length, so neither can two adjacent
/// columns run together.
fn encode(value: &KeyValue, out: &mut Vec<u8>) {
    match value {
        KeyValue::Null => out.push(0),
        KeyValue::Int64(value) => {
            out.push(1);
            out.extend_from_slice(&value.to_le_bytes());
        }
        KeyValue::UInt64(value) => {
            out.push(3);
            out.extend_from_slice(&value.to_le_bytes());
        }
        KeyValue::Utf8(value) => {
            out.push(2);
            out.extend_from_slice(&(value.len() as u32).to_le_bytes());
            out.extend_from_slice(value.as_bytes());
        }
    }
}

/// One key column, built from every group.
fn key_column(groups: &[Group], column: usize, data_type: &VeinDataType) -> Result<ArrayRef> {
    Ok(match data_type {
        VeinDataType::Int64 => {
            let values: Vec<Option<i64>> = groups
                .iter()
                .map(|group| match group.key[column] {
                    KeyValue::Int64(value) => Some(value),
                    KeyValue::Null => None,
                    KeyValue::UInt64(_) | KeyValue::Utf8(_) => {
                        unreachable!("an int64 key column holds int64 keys")
                    }
                })
                .collect();
            Arc::new(Int64Array::from(values))
        }
        VeinDataType::UInt8 => Arc::new(UInt8Array::from(
            unsigned_key_values(groups, column)
                .into_iter()
                .map(|value| value.map(|value| value as u8))
                .collect::<Vec<_>>(),
        )),
        VeinDataType::UInt16 => Arc::new(UInt16Array::from(
            unsigned_key_values(groups, column)
                .into_iter()
                .map(|value| value.map(|value| value as u16))
                .collect::<Vec<_>>(),
        )),
        VeinDataType::UInt32 => Arc::new(UInt32Array::from(
            unsigned_key_values(groups, column)
                .into_iter()
                .map(|value| value.map(|value| value as u32))
                .collect::<Vec<_>>(),
        )),
        VeinDataType::UInt64 => Arc::new(UInt64Array::from(unsigned_key_values(groups, column))),
        VeinDataType::Utf8 => {
            let values: Vec<Option<&str>> = groups
                .iter()
                .map(|group| match &group.key[column] {
                    KeyValue::Utf8(value) => Some(value.as_str()),
                    KeyValue::Null => None,
                    KeyValue::Int64(_) | KeyValue::UInt64(_) => {
                        unreachable!("a string key column holds string keys")
                    }
                })
                .collect();
            Arc::new(StringArray::from(values))
        }
        other => {
            return Err(VeinError::internal(format!(
                "grouping by {other:?} is not supported yet"
            )));
        }
    })
}

fn unsigned_key_values(groups: &[Group], column: usize) -> Vec<Option<u64>> {
    groups
        .iter()
        .map(|group| match group.key[column] {
            KeyValue::UInt64(value) => Some(value),
            KeyValue::Null => None,
            KeyValue::Int64(_) | KeyValue::Utf8(_) => {
                unreachable!("an unsigned key column holds unsigned keys")
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        Decimal256Array, Float16Array, Float32Array, Float64Array, Int32Array, Int64Array,
        StringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    };
    use arrow::datatypes::i256;
    use common::arrow::SelectionVector;

    use super::*;

    #[test]
    fn decimal256_sum_and_extrema_preserve_wide_values_and_nulls() {
        let large = i256::from(10_i8).checked_pow(40).unwrap();
        let input = Decimal256Array::from(vec![Some(large), None, Some(i256::ONE)])
            .with_precision_and_scale(76, 0)
            .unwrap();
        let plan = AggregatePlan {
            groups: 0,
            grouping_sets: None,
            aggs: vec![AggOpCode::Sum, AggOpCode::Min, AggOpCode::Max],
            aggregate_input_types: vec![VeinDataType::Decimal256(76, 0); 3],
            types: vec![VeinDataType::Decimal256(76, 0); 3],
        };
        let mut sink = AggregateSink::new(plan).unwrap();
        sink.accumulate(DataChunk::from_vectors(vec![
            Arc::new(input.clone()),
            Arc::new(input.clone()),
            Arc::new(input),
        ]))
            .unwrap();
        let output = sink.finish().unwrap();

        let sum = output[0]
            .column(0)
            .as_any()
            .downcast_ref::<Decimal256Array>()
            .unwrap();
        let minimum = output[0]
            .column(1)
            .as_any()
            .downcast_ref::<Decimal256Array>()
            .unwrap();
        let maximum = output[0]
            .column(2)
            .as_any()
            .downcast_ref::<Decimal256Array>()
            .unwrap();
        assert_eq!(sum.value(0), large.checked_add(i256::ONE).unwrap());
        assert_eq!(minimum.value(0), i256::ONE);
        assert_eq!(maximum.value(0), large);
    }

    #[test]
    fn decimal256_average_accumulates_exactly_across_batches_and_applies_scale() {
        let large = i256::from(10_i8).checked_pow(40).unwrap();
        let negative_large = i256::ZERO.checked_sub(large).unwrap();
        let plan = AggregatePlan {
            groups: 0,
            grouping_sets: None,
            aggs: vec![AggOpCode::Avg],
            aggregate_input_types: vec![VeinDataType::Decimal256(76, 2)],
            types: vec![VeinDataType::Float64],
        };
        let mut sink = AggregateSink::new(plan).unwrap();
        for value in [large, i256::ONE, negative_large] {
            let input = Decimal256Array::from(vec![value])
                .with_precision_and_scale(76, 2)
                .unwrap();
            sink.accumulate(DataChunk::from_vectors(vec![Arc::new(input)]))
                .unwrap();
        }
        let output = sink.finish().unwrap();
        let average = output[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(average.value(0), 1.0 / 300.0);
    }

    #[test]
    fn decimal256_sum_enforces_76_digit_precision() {
        let max_decimal_76 = i256::from(10_i8)
            .checked_pow(76)
            .unwrap()
            .checked_sub(i256::ONE)
            .unwrap();
        let input = Decimal256Array::from(vec![max_decimal_76, i256::ONE])
            .with_precision_and_scale(76, 0)
            .unwrap();
        let plan = AggregatePlan {
            groups: 0,
            grouping_sets: None,
            aggs: vec![AggOpCode::Sum],
            aggregate_input_types: vec![VeinDataType::Decimal256(76, 0)],
            types: vec![VeinDataType::Decimal256(76, 0)],
        };
        let mut sink = AggregateSink::new(plan).unwrap();
        let error = sink
            .accumulate(DataChunk::from_vectors(vec![Arc::new(input)]))
            .unwrap_err();
        assert!(error.to_string().contains("overflow while computing SUM"));
    }

    #[test]
    fn float16_aggregates_use_wide_fold_and_preserve_min_max_layout() {
        let source: ArrayRef = Arc::new(Float32Array::from(vec![Some(1.5), None, Some(2.5)]));
        let input = arrow::compute::cast(
            &source,
            &VeinDataType::Float16.to_arrow(),
        )
            .unwrap();
        assert!(input.as_any().is::<Float16Array>());

        let plan = AggregatePlan {
            groups: 0,
            grouping_sets: None,
            aggs: vec![AggOpCode::Sum, AggOpCode::Min, AggOpCode::Max, AggOpCode::Avg],
            aggregate_input_types: vec![VeinDataType::Float16; 4],
            types: vec![
                VeinDataType::Float64,
                VeinDataType::Float16,
                VeinDataType::Float16,
                VeinDataType::Float64,
            ],
        };
        let mut sink = AggregateSink::new(plan).unwrap();
        sink.accumulate(DataChunk::from_vectors(vec![
            input.clone(),
            input.clone(),
            input.clone(),
            input,
        ]))
            .unwrap();
        let output = sink.finish().unwrap();

        assert_eq!(
            output[0]
                .column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            4.0
        );
        for (column, expected) in [(1, 1.5), (2, 2.5)] {
            let value = arrow::compute::cast(
                output[0].column(column),
                &VeinDataType::Float32.to_arrow(),
            )
                .unwrap();
            assert_eq!(
                value
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .unwrap()
                    .value(0),
                expected
            );
        }
        assert_eq!(
            output[0]
                .column(3)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            2.0
        );
    }

    macro_rules! assert_unsigned_layout {
        ($array:ty, $data_type:expr, $sum_type:expr, $sum:expr, $min:expr, $max:expr) => {{
            let input: ArrayRef = Arc::new(<$array>::from(vec![Some(2), None, Some(5)]));
            let plan = AggregatePlan {
                groups: 0,
                grouping_sets: None,
                aggs: vec![AggOpCode::Sum, AggOpCode::Min, AggOpCode::Max, AggOpCode::Avg],
                aggregate_input_types: vec![$data_type.clone(); 4],
                types: vec![
                    $sum_type,
                    $data_type.clone(),
                    $data_type.clone(),
                    VeinDataType::Float64,
                ],
            };
            let mut sink = AggregateSink::new(plan).unwrap();
            sink.accumulate(DataChunk::from_vectors(vec![
                input.clone(),
                input.clone(),
                input.clone(),
                input,
            ]))
            .unwrap();
            let output = sink.finish().unwrap();
            assert_eq!(
                output[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
                    .value(0),
                $sum
            );
            assert_eq!(
                output[0]
                    .column(1)
                    .as_any()
                    .downcast_ref::<$array>()
                    .unwrap()
                    .value(0),
                $min
            );
            assert_eq!(
                output[0]
                    .column(2)
                    .as_any()
                    .downcast_ref::<$array>()
                    .unwrap()
                    .value(0),
                $max
            );
            assert_eq!(
                output[0]
                    .column(3)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(0),
                3.5
            );
        }};
    }

    #[test]
    fn unsigned_layouts_share_typed_states_and_ignore_nulls() {
        assert_unsigned_layout!(UInt8Array, VeinDataType::UInt8, VeinDataType::UInt64, 7, 2, 5);
        assert_unsigned_layout!(UInt16Array, VeinDataType::UInt16, VeinDataType::UInt64, 7, 2, 5);
        assert_unsigned_layout!(UInt32Array, VeinDataType::UInt32, VeinDataType::UInt64, 7, 2, 5);
        assert_unsigned_layout!(UInt64Array, VeinDataType::UInt64, VeinDataType::UInt64, 7, 2, 5);
    }

    macro_rules! assert_unsigned_group_key_layout {
        ($array:ty, $data_type:expr) => {{
            let plan = AggregatePlan {
                groups: 1,
                grouping_sets: None,
                aggs: vec![AggOpCode::Sum],
                aggregate_input_types: vec![VeinDataType::Int64],
                types: vec![$data_type, VeinDataType::Int64],
            };
            let mut sink = AggregateSink::new(plan).unwrap();

            sink.accumulate(DataChunk::from_vectors(vec![
                Arc::new(<$array>::from(vec![Some(2), None])),
                Arc::new(Int64Array::from(vec![1, 2])),
            ]))
            .unwrap();
            sink.accumulate(DataChunk::from_vectors(vec![
                Arc::new(<$array>::from(vec![Some(2), Some(5)])),
                Arc::new(Int64Array::from(vec![3, 4])),
            ]))
            .unwrap();

            let output = sink.finish().unwrap();
            let keys = output[0]
                .column(0)
                .as_any()
                .downcast_ref::<$array>()
                .unwrap();
            let sums = output[0]
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();

            assert_eq!(keys.len(), 3);
            assert_eq!(keys.value(0), 2);
            assert!(keys.is_null(1));
            assert_eq!(keys.value(2), 5);
            assert_eq!(sums.values(), &[4, 2, 4]);
        }};
    }

    #[test]
    fn unsigned_group_keys_preserve_width_nulls_and_cross_batch_groups() {
        assert_unsigned_group_key_layout!(UInt8Array, VeinDataType::UInt8);
        assert_unsigned_group_key_layout!(UInt16Array, VeinDataType::UInt16);
        assert_unsigned_group_key_layout!(UInt32Array, VeinDataType::UInt32);
        assert_unsigned_group_key_layout!(UInt64Array, VeinDataType::UInt64);
    }

    #[test]
    fn uint64_sum_checks_overflow_and_average_uses_wide_exact_state() {
        let sum_plan = AggregatePlan {
            groups: 0,
            grouping_sets: None,
            aggs: vec![AggOpCode::Sum],
            aggregate_input_types: vec![VeinDataType::UInt64],
            types: vec![VeinDataType::UInt64],
        };
        let mut sum_sink = AggregateSink::new(sum_plan).unwrap();
        let overflow = sum_sink
            .accumulate(DataChunk::from_vectors(vec![Arc::new(UInt64Array::from(
                vec![u64::MAX, 1],
            ))]))
            .unwrap_err();
        assert!(overflow.to_string().contains("overflow while computing SUM"));

        let avg_plan = AggregatePlan {
            groups: 0,
            grouping_sets: None,
            aggs: vec![AggOpCode::Avg],
            aggregate_input_types: vec![VeinDataType::UInt64],
            types: vec![VeinDataType::Float64],
        };
        let mut avg_sink = AggregateSink::new(avg_plan).unwrap();
        avg_sink
            .accumulate(DataChunk::from_vectors(vec![Arc::new(UInt64Array::from(vec![
                u64::MAX,
            ]))]))
            .unwrap();
        avg_sink
            .accumulate(DataChunk::from_vectors(vec![Arc::new(UInt64Array::from(vec![
                u64::MAX,
            ]))]))
            .unwrap();
        let output = avg_sink.finish().unwrap();
        let average = output[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(average.value(0), u64::MAX as f64);
    }

    #[test]
    fn accumulation_reads_only_selected_physical_rows() {
        let plan = AggregatePlan {
            groups: 1,
            grouping_sets: None,
            aggs: vec![AggOpCode::Sum],
            aggregate_input_types: vec![VeinDataType::Int64],
            types: vec![VeinDataType::Int64, VeinDataType::Int64],
        };
        let chunk = DataChunk::new(
            vec![
                Arc::new(Int64Array::from(vec![10, 20, 30, 40])),
                Arc::new(Int64Array::from(vec![100, 200, 300, 400])),
            ],
            SelectionVector::new_sparse(vec![1, 3]),
        );
        let mut sink = AggregateSink::new(plan).unwrap();

        sink.accumulate(chunk).unwrap();
        sink.accumulate(DataChunk::from_vectors(vec![
            Arc::new(Int64Array::from(vec![20, 50])),
            Arc::new(Int64Array::from(vec![3, 5])),
        ]))
            .unwrap();
        let output = sink.finish().unwrap();

        let keys = output[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let sums = output[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(keys.values(), &[20, 40, 50]);
        assert_eq!(sums.values(), &[203, 400, 5]);
    }

    #[test]
    fn integer_sum_preserves_values_above_f64_precision() {
        let plan = AggregatePlan {
            groups: 0,
            grouping_sets: None,
            aggs: vec![AggOpCode::Sum, AggOpCode::Min, AggOpCode::Max],
            aggregate_input_types: vec![VeinDataType::Int64; 3],
            types: vec![VeinDataType::Int64; 3],
        };
        let values: ArrayRef = Arc::new(Int64Array::from(vec![
            9_007_199_254_740_992,
            9_007_199_254_740_993,
        ]));
        let input = DataChunk::from_vectors(vec![values.clone(), values.clone(), values]);
        let mut sink = AggregateSink::new(plan).unwrap();

        sink.accumulate(input).unwrap();
        let output = sink.finish().unwrap();
        let sums = output[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let minimum = output[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let maximum = output[0]
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        assert_eq!(sums.value(0), 18_014_398_509_481_985);
        assert_eq!(minimum.value(0), 9_007_199_254_740_992);
        assert_eq!(maximum.value(0), 9_007_199_254_740_993);
    }

    #[test]
    fn decimal_sum_preserves_unscaled_integer_and_scale() {
        let input = Decimal128Array::from(vec![123_456_789_012_345_678_i128, 1])
            .with_precision_and_scale(38, 2)
            .unwrap();
        let plan = AggregatePlan {
            groups: 0,
            grouping_sets: None,
            aggs: vec![AggOpCode::Sum],
            aggregate_input_types: vec![VeinDataType::Decimal128(38, 2)],
            types: vec![VeinDataType::Decimal128(38, 2)],
        };
        let mut sink = AggregateSink::new(plan).unwrap();

        sink.accumulate(DataChunk::from_vectors(vec![Arc::new(input)]))
            .unwrap();
        let output = sink.finish().unwrap();
        let sum = output[0]
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();

        assert_eq!(sum.value(0), 123_456_789_012_345_679_i128);
        assert_eq!(sum.precision(), 38);
        assert_eq!(sum.scale(), 2);
    }

    #[test]
    fn decimal_average_applies_the_input_scale() {
        let input = Decimal128Array::from(vec![1_250_i128, 2_250])
            .with_precision_and_scale(15, 2)
            .unwrap();
        let plan = AggregatePlan {
            groups: 0,
            grouping_sets: None,
            aggs: vec![AggOpCode::Avg],
            aggregate_input_types: vec![VeinDataType::Decimal128(15, 2)],
            types: vec![VeinDataType::Float64],
        };
        let mut sink = AggregateSink::new(plan).unwrap();

        sink.accumulate(DataChunk::from_vectors(vec![Arc::new(input)]))
            .unwrap();
        let output = sink.finish().unwrap();
        let average = output[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        assert_eq!(average.value(0), 17.5);
    }

    #[test]
    fn int64_average_accumulates_exactly_across_batches() {
        let plan = AggregatePlan {
            groups: 0,
            grouping_sets: None,
            aggs: vec![AggOpCode::Avg],
            aggregate_input_types: vec![VeinDataType::Int64],
            types: vec![VeinDataType::Float64],
        };
        let mut sink = AggregateSink::new(plan).unwrap();

        sink.accumulate(DataChunk::from_vectors(vec![Arc::new(Int64Array::from(
            vec![9_007_199_254_740_993_i64],
        ))]))
            .unwrap();
        sink.accumulate(DataChunk::from_vectors(vec![Arc::new(Int64Array::from(
            vec![-9_007_199_254_740_992_i64],
        ))]))
            .unwrap();

        let output = sink.finish().unwrap();
        let average = output[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(average.value(0), 0.5);
    }

    #[test]
    fn decimal_average_accumulates_unscaled_values_exactly() {
        let input = Decimal128Array::from(vec![
            9_007_199_254_740_993_i128,
            -9_007_199_254_740_992_i128,
        ])
            .with_precision_and_scale(19, 0)
            .unwrap();
        let plan = AggregatePlan {
            groups: 0,
            grouping_sets: None,
            aggs: vec![AggOpCode::Avg],
            aggregate_input_types: vec![VeinDataType::Decimal128(19, 0)],
            types: vec![VeinDataType::Float64],
        };
        let mut sink = AggregateSink::new(plan).unwrap();

        sink.accumulate(DataChunk::from_vectors(vec![Arc::new(input)]))
            .unwrap();

        let output = sink.finish().unwrap();
        let average = output[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(average.value(0), 0.5);
    }

    #[test]
    fn float32_average_accumulates_in_float64_state() {
        let input = Float32Array::from(vec![100_000_000.0_f32, 1.0, -100_000_000.0]);
        let plan = AggregatePlan {
            groups: 0,
            grouping_sets: None,
            aggs: vec![AggOpCode::Avg],
            aggregate_input_types: vec![VeinDataType::Float32],
            types: vec![VeinDataType::Float64],
        };
        let mut sink = AggregateSink::new(plan).unwrap();

        sink.accumulate(DataChunk::from_vectors(vec![Arc::new(input)]))
            .unwrap();

        let output = sink.finish().unwrap();
        let average = output[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(average.value(0), 1.0 / 3.0);
    }

    #[test]
    fn float32_sum_reads_input_layout_and_emits_widened_result() {
        let plan = AggregatePlan {
            groups: 0,
            grouping_sets: None,
            aggs: vec![AggOpCode::Sum],
            aggregate_input_types: vec![VeinDataType::Float32],
            types: vec![VeinDataType::Float64],
        };
        let mut sink = AggregateSink::new(plan).unwrap();

        sink.accumulate(DataChunk::from_vectors(vec![Arc::new(Float32Array::from(
            vec![1.25, 2.75],
        ))]))
            .unwrap();
        let output = sink.finish().unwrap();
        let sum = output[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        assert_eq!(sum.value(0), 4.0);
    }

    #[test]
    fn int32_aggregates_widen_sum_and_preserve_min_max_layout() {
        let values: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None, Some(3)]));
        let plan = AggregatePlan {
            groups: 0,
            grouping_sets: None,
            aggs: vec![AggOpCode::Sum, AggOpCode::Min, AggOpCode::Max, AggOpCode::Avg],
            aggregate_input_types: vec![VeinDataType::Int32; 4],
            types: vec![
                VeinDataType::Int64,
                VeinDataType::Int32,
                VeinDataType::Int32,
                VeinDataType::Float64,
            ],
        };
        let mut sink = AggregateSink::new(plan).unwrap();

        sink.accumulate(DataChunk::from_vectors(vec![
            values.clone(),
            values.clone(),
            values.clone(),
            values,
        ]))
            .unwrap();
        let output = sink.finish().unwrap();

        assert_eq!(
            output[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            4
        );
        assert_eq!(
            output[0]
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            1
        );
        assert_eq!(
            output[0]
                .column(2)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            3
        );
        assert_eq!(
            output[0]
                .column(3)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            2.0
        );
    }

    #[test]
    fn int8_and_int16_aggregates_share_int64_state() {
        let int8_values: ArrayRef = Arc::new(Int8Array::from(vec![Some(100), None, Some(-10)]));
        let int16_values: ArrayRef =
            Arc::new(Int16Array::from(vec![Some(1_000), None, Some(-10)]));
        let plan = AggregatePlan {
            groups: 0,
            grouping_sets: None,
            aggs: vec![
                AggOpCode::Sum,
                AggOpCode::Min,
                AggOpCode::Max,
                AggOpCode::Avg,
                AggOpCode::Sum,
                AggOpCode::Min,
                AggOpCode::Max,
                AggOpCode::Avg,
            ],
            aggregate_input_types: vec![
                VeinDataType::Int8,
                VeinDataType::Int8,
                VeinDataType::Int8,
                VeinDataType::Int8,
                VeinDataType::Int16,
                VeinDataType::Int16,
                VeinDataType::Int16,
                VeinDataType::Int16,
            ],
            types: vec![
                VeinDataType::Int64,
                VeinDataType::Int8,
                VeinDataType::Int8,
                VeinDataType::Float64,
                VeinDataType::Int64,
                VeinDataType::Int16,
                VeinDataType::Int16,
                VeinDataType::Float64,
            ],
        };
        let mut sink = AggregateSink::new(plan).unwrap();

        sink.accumulate(DataChunk::from_vectors(vec![
            int8_values.clone(),
            int8_values.clone(),
            int8_values.clone(),
            int8_values,
            int16_values.clone(),
            int16_values.clone(),
            int16_values.clone(),
            int16_values,
        ]))
            .unwrap();
        let output = sink.finish().unwrap();

        assert_eq!(
            output[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            90
        );
        assert_eq!(
            output[0]
                .column(1)
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .value(0),
            -10
        );
        assert_eq!(
            output[0]
                .column(2)
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .value(0),
            100
        );
        assert_eq!(
            output[0]
                .column(3)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            45.0
        );
        assert_eq!(
            output[0]
                .column(4)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            990
        );
        assert_eq!(
            output[0]
                .column(5)
                .as_any()
                .downcast_ref::<Int16Array>()
                .unwrap()
                .value(0),
            -10
        );
        assert_eq!(
            output[0]
                .column(6)
                .as_any()
                .downcast_ref::<Int16Array>()
                .unwrap()
                .value(0),
            1_000
        );
        assert_eq!(
            output[0]
                .column(7)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            495.0
        );
    }

    #[test]
    fn floating_aggregates_share_the_generic_fold() {
        let values: ArrayRef = Arc::new(Float64Array::from(vec![Some(1.5), None, Some(2.5)]));
        let plan = AggregatePlan {
            groups: 0,
            grouping_sets: None,
            aggs: vec![
                AggOpCode::Sum,
                AggOpCode::Min,
                AggOpCode::Max,
                AggOpCode::Avg,
            ],
            aggregate_input_types: vec![VeinDataType::Float64; 4],
            types: vec![VeinDataType::Float64; 4],
        };
        let mut sink = AggregateSink::new(plan).unwrap();

        sink.accumulate(DataChunk::from_vectors(vec![
            values.clone(),
            values.clone(),
            values.clone(),
            values,
        ]))
            .unwrap();
        let output = sink.finish().unwrap();
        let result = (0..4)
            .map(|column| {
                output[0]
                    .column(column)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(0)
            })
            .collect::<Vec<_>>();

        assert_eq!(result, vec![4.0, 1.5, 2.5, 2.0]);
    }

    #[test]
    fn integer_sum_reports_overflow() {
        let plan = AggregatePlan {
            groups: 0,
            grouping_sets: None,
            aggs: vec![AggOpCode::Sum],
            aggregate_input_types: vec![VeinDataType::Int64],
            types: vec![VeinDataType::Int64],
        };
        let input = DataChunk::from_vectors(vec![Arc::new(Int64Array::from(vec![i64::MAX, 1]))]);
        let mut sink = AggregateSink::new(plan).unwrap();

        let error = sink.accumulate(input).unwrap_err();
        assert!(error.to_string().contains("overflow while computing SUM"));
    }

    #[test]
    fn decimal_sum_reports_precision_overflow() {
        let max_decimal_38 = 10_i128.pow(38) - 1;
        let input = Decimal128Array::from(vec![max_decimal_38, 1])
            .with_precision_and_scale(38, 0)
            .unwrap();
        let plan = AggregatePlan {
            groups: 0,
            grouping_sets: None,
            aggs: vec![AggOpCode::Sum],
            aggregate_input_types: vec![VeinDataType::Decimal128(38, 0)],
            types: vec![VeinDataType::Decimal128(38, 0)],
        };
        let mut sink = AggregateSink::new(plan).unwrap();

        let error = sink
            .accumulate(DataChunk::from_vectors(vec![Arc::new(input)]))
            .unwrap_err();
        assert!(error.to_string().contains("overflow while computing SUM"));
    }

    #[test]
    fn null_inputs_are_ignored_and_count_only_counts_non_null_values() {
        let values: ArrayRef = Arc::new(Int64Array::from(vec![Some(3), None, Some(5)]));
        let plan = AggregatePlan {
            groups: 0,
            grouping_sets: None,
            aggs: vec![
                AggOpCode::Count,
                AggOpCode::Sum,
                AggOpCode::Min,
                AggOpCode::Max,
                AggOpCode::Avg,
            ],
            aggregate_input_types: vec![VeinDataType::Int64; 5],
            types: vec![
                VeinDataType::Int64,
                VeinDataType::Int64,
                VeinDataType::Int64,
                VeinDataType::Int64,
                VeinDataType::Float64,
            ],
        };
        let mut sink = AggregateSink::new(plan).unwrap();

        sink.accumulate(DataChunk::from_vectors(vec![
            Arc::new(Int64Array::from(vec![Some(1), None, Some(1)])),
            values.clone(),
            values.clone(),
            values.clone(),
            values,
        ]))
            .unwrap();
        let output = sink.finish().unwrap();
        let row = &output[0];

        assert_eq!(
            row.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            2
        );
        assert_eq!(
            row.column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            8
        );
        assert_eq!(
            row.column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            3
        );
        assert_eq!(
            row.column(3)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            5
        );
        assert_eq!(
            row.column(4)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            4.0
        );
    }

    #[test]
    fn empty_global_aggregate_emits_count_zero_and_null_sum() {
        let plan = AggregatePlan {
            groups: 0,
            grouping_sets: None,
            aggs: vec![AggOpCode::Count, AggOpCode::Sum],
            aggregate_input_types: vec![VeinDataType::Int64, VeinDataType::Float64],
            types: vec![VeinDataType::Int64, VeinDataType::Float64],
        };
        let chunk = DataChunk::from_vectors(vec![
            Arc::new(Int64Array::from(Vec::<i64>::new())),
            Arc::new(Float64Array::from(Vec::<f64>::new())),
        ]);
        let mut sink = AggregateSink::new(plan).unwrap();

        sink.accumulate(chunk).unwrap();
        let output = sink.finish().unwrap();

        assert_eq!(output.len(), 1);
        assert_eq!(output[0].num_rows(), 1);
        let count = output[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let sum = output[0]
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(count.values(), &[0]);
        assert!(sum.is_null(0));
    }

    #[test]
    fn empty_global_average_uses_its_declared_input_type() {
        let plan = AggregatePlan {
            groups: 0,
            grouping_sets: None,
            aggs: vec![AggOpCode::Avg],
            aggregate_input_types: vec![VeinDataType::Float32],
            types: vec![VeinDataType::Float64],
        };
        let chunk = DataChunk::from_vectors(vec![Arc::new(Float32Array::from(Vec::<f32>::new()))]);
        let mut sink = AggregateSink::new(plan).unwrap();

        sink.accumulate(chunk).unwrap();
        let output = sink.finish().unwrap();
        let average = output[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        assert!(average.is_null(0));
    }

    #[test]
    fn grouping_sets_keep_set_identity_and_null_omitted_keys() {
        let plan = AggregatePlan {
            groups: 2,
            grouping_sets: Some(vec![vec![0, 1], vec![0], vec![]]),
            aggs: vec![AggOpCode::Sum],
            aggregate_input_types: vec![VeinDataType::Int64],
            types: vec![VeinDataType::Int64, VeinDataType::Utf8, VeinDataType::Int64],
        };
        let mut sink = AggregateSink::new(plan).unwrap();
        sink.accumulate(DataChunk::from_vectors(vec![
            Arc::new(Int64Array::from(vec![Some(1), None])),
            Arc::new(StringArray::from(vec![Some("x"), Some("z")])),
            Arc::new(Int64Array::from(vec![10, 20])),
        ]))
            .unwrap();

        let output = sink.finish().unwrap();
        let batch = &output[0];
        let first_key = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let second_key = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        let sums = batch.column(2).as_any().downcast_ref::<Int64Array>().unwrap();

        assert_eq!(batch.num_rows(), 5);
        assert_eq!(first_key.value(0), 1);
        assert_eq!(second_key.value(0), "x");
        assert_eq!(first_key.value(1), 1);
        assert!(second_key.is_null(1));
        assert!(first_key.is_null(2));
        assert!(second_key.is_null(2));
        assert_eq!(sums.value(2), 30);
        assert!(first_key.is_null(3));
        assert_eq!(second_key.value(3), "z");
        assert!(first_key.is_null(4));
        assert!(second_key.is_null(4));
        assert_eq!(sums.value(4), 20);
    }

    #[test]
    fn empty_input_emits_only_empty_grouping_sets() {
        let plan = AggregatePlan {
            groups: 1,
            grouping_sets: Some(vec![vec![0], vec![]]),
            aggs: vec![AggOpCode::Count],
            aggregate_input_types: vec![VeinDataType::Int64],
            types: vec![VeinDataType::Int64, VeinDataType::Int64],
        };
        let mut sink = AggregateSink::new(plan).unwrap();
        sink.accumulate(DataChunk::from_vectors(vec![
            Arc::new(Int64Array::from(Vec::<i64>::new())),
            Arc::new(Int64Array::from(Vec::<i64>::new())),
        ]))
            .unwrap();

        let output = sink.finish().unwrap();
        assert_eq!(output[0].num_rows(), 1);
        assert!(output[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .is_null(0));
        assert_eq!(output[0]
                       .column(1)
                       .as_any()
                       .downcast_ref::<Int64Array>()
                       .unwrap()
                       .value(0), 0);
    }
}
