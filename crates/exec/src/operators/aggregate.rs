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

use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use arrow::array::{
    new_null_array, Array, ArrayRef, Decimal128Array, DictionaryArray, Float16Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, StringArray, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array, Decimal256Array,
};
use arrow::datatypes::{i256, Int32Type};
use arrow::buffer::NullBuffer;
use hashbrown::DefaultHashBuilder;

mod group_index;
use group_index::{GroupIndex, GroupIndexEntry, ProbeResolution};
use crate::pipeline::{DataChunk, PipelineNode};
use common::arrow::SelectionVector;
use common::Result;
use common::protocol::{AggOpCode, AggregatePlan};
use error::VeinError;
use types::VeinDataType;

type GroupHasher = <DefaultHashBuilder as BuildHasher>::Hasher;

/// A group key value borrowed from an Arrow array.
#[derive(Clone, Copy, Hash, PartialEq)]
enum KeyValueRef<'a> {
    Null,
    Int64(i64),
    UInt64(u64),
    Utf8(&'a str),
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
            AggOpCode::SingleValue => unreachable!("single-value uses a retained Arrow value"),
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
    Any(&'a ArrayRef),
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
        if op == AggOpCode::SingleValue {
            return Ok(Self::Any(array));
        }
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
    group_rows: GroupRows<'_>,
    value_at: impl Fn(usize) -> Option<T>,
) -> Result<()> {
    for (slot, row) in group_rows {
        states[slot].update(value_at(row))?;
    }
    Ok(())
}

/// Zero-allocation view pairing per-row group IDs with the input selection.
#[derive(Clone, Copy)]
struct GroupRows<'a> {
    group_ids: &'a [usize],
    selection: &'a SelectionVector,
    position: usize,
}

impl<'a> GroupRows<'a> {
    fn new(group_ids: &'a [usize], selection: &'a SelectionVector) -> Self {
        debug_assert_eq!(group_ids.len(), selection.size());
        Self {
            group_ids,
            selection,
            position: 0,
        }
    }
}

impl Iterator for GroupRows<'_> {
    type Item = (usize, usize);

    fn next(&mut self) -> Option<Self::Item> {
        let position = self.position;
        let group_id = *self.group_ids.get(position)?;
        self.position += 1;
        Some((group_id, self.selection.get_index(position)))
    }
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
    SingleValue(Vec<Option<(ArrayRef, usize)>>),
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
        if op == AggOpCode::SingleValue {
            return Ok(Self::SingleValue(Vec::new()));
        }
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
        group_rows: GroupRows<'_>,
    ) -> Result<()> {
        match (self, input) {
            (Self::SingleValue(states), AggregateInput::Any(array)) => {
                for (slot, row) in group_rows {
                    if states[slot].is_some() {
                        return Err(VeinError::stage(
                            "aggregate",
                            "scalar subquery returned more than one row",
                        ));
                    }
                    states[slot] = Some((Arc::clone(array), row));
                }
                Ok(())
            }
            (Self::Count(states), AggregateInput::Count(array)) => {
                for (slot, row) in group_rows {
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
        if let (Self::SingleValue(states), AggOpCode::SingleValue) = (self, op) {
            return match states.as_slice() {
                [Some((array, row))] => Ok(array.slice(*row, 1)),
                [None] => Ok(new_null_array(&data_type.to_arrow(), 1)),
                _ => Err(VeinError::internal(
                    "SINGLE_VALUE requires exactly one global aggregate group",
                )),
            };
        }
        Ok(match (self, op, data_type) {
            (Self::Count(states), AggOpCode::Count, VeinDataType::Int64) => {
                Arc::new(Int64Array::from_iter_values(states.iter().copied()))
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

/// One group's non-key state.
struct Group {
    grouping_set_id: usize,
    key_location: Option<GroupKeyLocation>,
}

/// Locates a group's original key row in retained Arrow-backed key batches.
#[derive(Clone, Copy)]
struct GroupKeyLocation {
    batch: usize,
    row: usize,
}

/// Absorbs rows into groups, then emits one row per group.
pub struct AggregateSink {
    plan: AggregatePlan,
    /// Each grouping set's key positions in the projected key columns.
    grouping_sets: Vec<Vec<u32>>,
    /// Grouping-set membership by key column, prepared once per plan.
    grouping_sets_by_column: Vec<Vec<usize>>,
    /// Prehashed group slots; equality is checked against retained columnar keys.
    index: GroupIndex,
    hash_builder: DefaultHashBuilder,
    /// Reused hash and probe storage for the current input batch.
    group_hashers: Vec<GroupHasher>,
    group_hashes: Vec<u64>,
    batch_group_ids: Vec<usize>,
    /// Per-batch dictionary IDs used by the single-key Utf8 fast path.
    dictionary_ids: Vec<usize>,
    /// First physical row for each dictionary ID in the current batch.
    dictionary_first_rows: Vec<usize>,
    /// Dictionary ID to resolved group slot; the final entry represents a null key.
    dictionary_group_slots: Vec<usize>,
    /// Candidate indexes that will become new groups after this batch resolves.
    batch_new_groups: Vec<usize>,
    /// Arrow-backed grouping columns for batches that contributed new groups.
    group_key_batches: Vec<Vec<Option<GroupKeyColumn>>>,
    /// Reusable group IDs for each grouping set in the current input batch.
    group_ids_by_set: Vec<Vec<usize>>,
    groups: Vec<Group>,
    aggregate_states: Vec<AggregateStateColumn>,
    metrics: AggregateMetrics,
}

/// Low-overhead counters and phase timings for one aggregate sink.
#[derive(Clone, Copy, Default)]
pub(crate) struct AggregateMetrics {
    pub(crate) input_batches: u64,
    pub(crate) input_rows: u64,
    pub(crate) grouping_sets: u64,
    pub(crate) grouping_probes: u64,
    pub(crate) group_index_candidate_checks: u64,
    pub(crate) group_index_reserve_ns: u64,
    pub(crate) group_index_capacity_growth_events: u64,
    pub(crate) group_index_entries_at_capacity_growth: u64,
    pub(crate) group_index_capacity: u64,
    pub(crate) group_index_entries_ns: u64,
    pub(crate) full_hash_matches: u64,
    pub(crate) group_key_comparisons: u64,
    pub(crate) group_collision_retries: u64,
    pub(crate) group_index_hits: u64,
    pub(crate) group_index_misses: u64,
    pub(crate) new_groups: u64,
    pub(crate) retained_key_slots_initialized: u64,
    pub(crate) string_key_bytes_copied: u64,
    pub(crate) hasher_setup_ns: u64,
    pub(crate) key_hash_ns: u64,
    pub(crate) hash_finalize_ns: u64,
    pub(crate) hash_generation_ns: u64,
    /// Resolving candidates to group slots, including collision checks and new groups.
    pub(crate) group_resolution_ns: u64,
    /// Copying each batch's newly discovered key rows into owned group state.
    pub(crate) group_materialize_ns: u64,
    pub(crate) group_ids_distribution_ns: u64,
    pub(crate) state_update_ns: u64,
    pub(crate) output_materialize_ns: u64,
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
        if plan.aggs.contains(&AggOpCode::SingleValue) && groups != 0 {
            return Err(VeinError::internal(
                "SINGLE_VALUE currently requires a global aggregate",
            ));
        }
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
        let mut grouping_sets_by_column = vec![Vec::new(); groups];
        for (set_id, grouping_set) in grouping_sets.iter().enumerate() {
            for &column in grouping_set {
                grouping_sets_by_column[column as usize].push(set_id);
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
        let group_ids_by_set = (0..grouping_sets.len()).map(|_| Vec::new()).collect();
        let mut index = GroupIndex::default();
        let group_index_reserve_ns = plan
            .estimated_groups
            .and_then(|groups| usize::try_from(groups).ok())
            .map(|groups| index.reserve(groups))
            .transpose()?
            .unwrap_or_default();
        Ok(Self {
            plan,
            metrics: AggregateMetrics {
                grouping_sets: grouping_sets.len() as u64,
                group_index_reserve_ns,
                ..AggregateMetrics::default()
            },
            grouping_sets,
            grouping_sets_by_column,
            index,
            hash_builder: DefaultHashBuilder::default(),
            group_hashers: Vec::new(),
            group_hashes: Vec::new(),
            batch_group_ids: Vec::new(),
            dictionary_ids: Vec::new(),
            dictionary_group_slots: Vec::new(),
            batch_new_groups: Vec::new(),
            group_key_batches: Vec::new(),
            dictionary_first_rows: Vec::new(),
            group_ids_by_set,
            groups: Vec::new(),
            aggregate_states,
        })
    }

    /// Adds a group slot and its corresponding state to every aggregate.
    fn push_group(
        groups: &mut Vec<Group>,
        aggregate_states: &mut [AggregateStateColumn],
        grouping_set_id: usize,
        key_location: Option<GroupKeyLocation>,
    ) -> Result<usize> {
        let slot = groups.len();
        groups.push(Group {
            grouping_set_id,
            key_location,
        });
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
        for group_ids in &mut self.group_ids_by_set {
            group_ids.clear();
            if group_ids.capacity() < chunk.selection.size() {
                group_ids.reserve(chunk.selection.size());
            }
        }
        let mut key_columns = std::iter::repeat_with(|| None)
            .take(groups)
            .collect::<Vec<Option<GroupKeyColumn>>>();
        for &column in self.grouping_sets.iter().flatten() {
            let column = column as usize;
            if key_columns[column].is_none() {
                key_columns[column] = Some(GroupKeyColumn::new(chunk.column(column))?);
            }
        }
        self.metrics.input_rows = self
            .metrics
            .input_rows
            .saturating_add(chunk.selection.size() as u64);
        self.metrics.input_batches = self.metrics.input_batches.saturating_add(1);
        let input_rows = chunk.selection.size();
        let grouping_set_count = self.grouping_sets.len();
        // With one Utf8 grouping key, each referenced dictionary entry is a
        // group candidate. The row loop below only fans the resolved IDs out.
        let dictionary_key = (groups == 1
            && self.grouping_sets.len() == 1
            && self.grouping_sets[0].as_slice() == [0])
            .then(|| key_columns[0].as_ref())
            .flatten()
            .and_then(|column| column.dictionary_cardinality().map(|size| (column, size)));
        self.dictionary_ids.clear();
        self.dictionary_first_rows.clear();
        let dictionary_fast_path = dictionary_key.is_some();
        if let Some((column, cardinality)) = dictionary_key {
            self.dictionary_group_slots.resize(cardinality + 1, usize::MAX);
            self.dictionary_group_slots.fill(usize::MAX);
            for logical_row in 0..input_rows {
                let row = chunk.selection.get_index(logical_row);
                let dictionary_id = column
                    .dictionary_id(row)
                    .expect("dictionary fast path has a dictionary key column");
                if self.dictionary_group_slots[dictionary_id] == usize::MAX {
                    self.dictionary_group_slots[dictionary_id] = self.dictionary_ids.len();
                    self.dictionary_ids.push(dictionary_id);
                    self.dictionary_first_rows.push(row);
                }
            }
        }
        let candidate_count = if dictionary_fast_path {
            self.dictionary_ids.len()
        } else {
            input_rows
                .checked_mul(grouping_set_count)
                .ok_or_else(|| VeinError::internal("aggregate batch candidate count overflow"))?
        };
        if self.metrics.input_batches == 1 && self.plan.estimated_groups.is_none() {
            let reserve_ns = self.index.reserve(candidate_count)?;
            self.metrics.group_index_reserve_ns = self
                .metrics
                .group_index_reserve_ns
                .saturating_add(reserve_ns);
        }
        self.group_hashers.clear();
        self.group_hashers
            .try_reserve(candidate_count)
            .map_err(|error| {
                VeinError::internal(format!("cannot reserve aggregate hash states: {error}"))
            })?;
        self.group_hashes.clear();
        self.group_hashes
            .try_reserve(candidate_count)
            .map_err(|error| {
                VeinError::internal(format!("cannot reserve aggregate group hashes: {error}"))
            })?;
        let hash_generation_started = Instant::now();
        let hasher_setup_started = Instant::now();
        for candidate in 0..candidate_count {
            let set_id = candidate % grouping_set_count;
            let mut hasher = self.hash_builder.build_hasher();
            set_id.hash(&mut hasher);
            self.group_hashers.push(hasher);
        }
        self.metrics.hasher_setup_ns = self.metrics.hasher_setup_ns.saturating_add(
            hasher_setup_started.elapsed().as_nanos() as u64,
        );
        let key_hash_started = Instant::now();
        if dictionary_fast_path {
            key_columns[0]
                .as_ref()
                .expect("dictionary key column was initialized")
                .hash_dictionary_ids(0, &self.dictionary_ids, &mut self.group_hashers);
        } else {
            for (column, key_column) in key_columns.iter().enumerate() {
                if let Some(key_column) = key_column {
                    key_column.hash_batch(
                        column as u32,
                        &chunk.selection,
                        &self.grouping_sets_by_column[column],
                        grouping_set_count,
                        &mut self.group_hashers,
                    );
                }
            }
        }
        self.metrics.key_hash_ns = self
            .metrics
            .key_hash_ns
            .saturating_add(key_hash_started.elapsed().as_nanos() as u64);
        let hash_finalize_started = Instant::now();
        self.group_hashes
            .extend(self.group_hashers.iter().map(Hasher::finish));
        self.metrics.hash_finalize_ns = self.metrics.hash_finalize_ns.saturating_add(
            hash_finalize_started.elapsed().as_nanos() as u64,
        );
        let hash_generation_ns = hash_generation_started.elapsed().as_nanos() as u64;
        self.metrics.hash_generation_ns = self
            .metrics
            .hash_generation_ns
            .saturating_add(hash_generation_ns);

        self.metrics.grouping_probes = self
            .metrics
            .grouping_probes
            .saturating_add(candidate_count as u64);
        let hashes = &self.group_hashes;
        let index = &mut self.index;
        let batch_group_ids = &mut self.batch_group_ids;
        let groups_store = &mut self.groups;
        let group_key_batches = &self.group_key_batches;
        let aggregate_states = &mut self.aggregate_states;
        let metrics = &mut self.metrics;
        let new_groups = &mut self.batch_new_groups;
        let dictionary_ids = &self.dictionary_ids;
        new_groups.clear();
        new_groups.try_reserve(candidate_count).map_err(|error| {
            VeinError::internal(format!("cannot reserve aggregate batch groups: {error}"))
        })?;
        let resolution_started = Instant::now();
        let index_metrics = index.resolve_rows(hashes, batch_group_ids, |candidate, existing_slot| {
            let set_id = if dictionary_fast_path { 0 } else { candidate % grouping_set_count };
            let logical_row = candidate / grouping_set_count;
            let grouping_set = &self.grouping_sets[set_id];
            let dictionary_id = dictionary_fast_path.then(|| dictionary_ids[candidate]);
            let row = if dictionary_fast_path {
                self.dictionary_first_rows[candidate]
            } else {
                chunk.selection.get_index(logical_row)
            };
            match existing_slot {
                Some(slot) => {
                    metrics.full_hash_matches = metrics.full_hash_matches.saturating_add(1);
                    let (existing_set_id, keys_match) = if slot < groups_store.len() {
                        let group = &groups_store[slot];
                        let keys_match = grouping_set.iter().all(|&column| {
                            metrics.group_key_comparisons = metrics
                                .group_key_comparisons
                                .saturating_add(1);
                            let column = column as usize;
                            let location = group
                                .key_location
                                .expect("groups with keys retain their source row");
                            let retained = group_key_batches[location.batch][column]
                                .as_ref()
                                .expect("group source batch retains every grouping column");
                            key_columns[column]
                                .as_ref()
                                .expect("grouping set columns have a batch view")
                                .rows_equal(row, retained, location.row)
                        });
                        (group.grouping_set_id, keys_match)
                    } else {
                        let leader = new_groups[slot - groups_store.len()];
                        let leader_set_id = leader % grouping_set_count;
                        let keys_match = if let Some(dictionary_id) = dictionary_id {
                            metrics.group_key_comparisons = metrics
                                .group_key_comparisons
                                .saturating_add(1);
                            let leader_dictionary_id = dictionary_ids[leader];
                            leader_set_id == set_id
                                && key_columns[0]
                                .as_ref()
                                .expect("dictionary key column has a batch view")
                                .dictionary_ids_equal(dictionary_id, leader_dictionary_id)
                        } else {
                            let leader_row = if dictionary_fast_path {
                                self.dictionary_first_rows[leader]
                            } else {
                                chunk
                                    .selection
                                    .get_index(leader / grouping_set_count)
                            };
                            leader_set_id == set_id
                                && grouping_set.iter().all(|&column| {
                                metrics.group_key_comparisons = metrics
                                    .group_key_comparisons
                                    .saturating_add(1);
                                let column = column as usize;
                                let key_column = key_columns[column]
                                    .as_ref()
                                    .expect("grouping set columns have a batch view");
                                key_column.rows_equal(row, key_column, leader_row)
                            })
                        };
                        (leader_set_id, keys_match)
                    };
                    if existing_set_id != set_id {
                        return Ok(ProbeResolution::Retry);
                    }
                    if keys_match {
                        metrics.group_index_hits = metrics.group_index_hits.saturating_add(1);
                        Ok(ProbeResolution::Match(slot))
                    } else {
                        metrics.group_collision_retries =
                            metrics.group_collision_retries.saturating_add(1);
                        Ok(ProbeResolution::Retry)
                    }
                }
                None => {
                    metrics.group_index_misses = metrics.group_index_misses.saturating_add(1);
                    let slot = groups_store.len() + new_groups.len();
                    new_groups.push(candidate);
                    Ok(ProbeResolution::Insert(slot))
                }
            }
        })?;
        metrics.group_index_candidate_checks = metrics
            .group_index_candidate_checks
            .saturating_add(index_metrics.candidate_checks);
        metrics.group_index_reserve_ns = metrics
            .group_index_reserve_ns
            .saturating_add(index_metrics.reserve_ns);
        metrics.group_index_capacity_growth_events = metrics
            .group_index_capacity_growth_events
            .saturating_add(index_metrics.capacity_growth_events);
        metrics.group_index_entries_at_capacity_growth = metrics
            .group_index_entries_at_capacity_growth
            .saturating_add(index_metrics.entries_at_capacity_growth);
        metrics.group_index_capacity = metrics
            .group_index_capacity
            .max(index_metrics.capacity as u64);
        metrics.group_index_entries_ns = metrics
            .group_index_entries_ns
            .saturating_add(index_metrics.entries_ns);
        metrics.group_resolution_ns = metrics
            .group_resolution_ns
            .saturating_add(resolution_started.elapsed().as_nanos() as u64);

        let materialize_started = Instant::now();
        groups_store.try_reserve(new_groups.len()).map_err(|error| {
            VeinError::internal(format!("cannot reserve aggregate groups: {error}"))
        })?;
        let key_batch = if new_groups.is_empty() {
            None
        } else {
            let batch = self.group_key_batches.len();
            self.group_key_batches.push(key_columns.clone());
            Some(batch)
        };
        let group_slot_base = groups_store.len();
        for (new_group_index, &candidate) in new_groups.iter().enumerate() {
            let set_id = candidate % grouping_set_count;
            let logical_row = candidate / grouping_set_count;
            let row = if dictionary_fast_path {
                self.dictionary_first_rows[candidate]
            } else {
                chunk.selection.get_index(logical_row)
            };
            metrics.retained_key_slots_initialized = metrics
                .retained_key_slots_initialized
                .saturating_add(groups as u64);
            let location = key_batch.map(|batch| GroupKeyLocation { batch, row });
            let slot = Self::push_group(groups_store, aggregate_states, set_id, location)?;
            debug_assert_eq!(slot, group_slot_base + new_group_index);
        }
        metrics.new_groups = metrics
            .new_groups
            .saturating_add(new_groups.len() as u64);
        metrics.group_materialize_ns = metrics
            .group_materialize_ns
            .saturating_add(materialize_started.elapsed().as_nanos() as u64);

        let group_ids_distribution_started = Instant::now();
        for group_ids in &mut self.group_ids_by_set {
            group_ids.clear();
        }
        if dictionary_fast_path {
            let key_column = key_columns[0]
                .as_ref()
                .expect("dictionary key column has a batch view");
            for logical_row in 0..input_rows {
                let row = chunk.selection.get_index(logical_row);
                let dictionary_id = key_column
                    .dictionary_id(row)
                    .expect("dictionary fast path has a dictionary key column");
                let candidate = self.dictionary_group_slots[dictionary_id];
                self.group_ids_by_set[0].push(self.batch_group_ids[candidate]);
            }
            for (candidate, &slot) in self.batch_group_ids.iter().enumerate() {
                self.dictionary_group_slots[dictionary_ids[candidate]] = slot;
            }
        } else {
            for (candidate, &slot) in self.batch_group_ids.iter().enumerate() {
                self.group_ids_by_set[candidate % grouping_set_count].push(slot);
            }
        }
        self.metrics.group_ids_distribution_ns = self
            .metrics
            .group_ids_distribution_ns
            .saturating_add(group_ids_distribution_started.elapsed().as_nanos() as u64);

        // Resolve each physical input layout once per aggregate column and
        // batch, then fold the selected rows through the borrowed view.
        let state_update_started = Instant::now();
        for (offset, op) in self.plan.aggs.iter().enumerate() {
            let input = AggregateInput::new(
                *op,
                &self.plan.aggregate_input_types[offset],
                chunk.column(groups + offset),
            )?;
            for group_ids in &self.group_ids_by_set {
                self.aggregate_states[offset]
                    .update_batch(input, GroupRows::new(group_ids, &chunk.selection))?;
            }
        }
        self.metrics.state_update_ns = self
            .metrics
            .state_update_ns
            .saturating_add(state_update_started.elapsed().as_nanos() as u64);
        Ok(())
    }

    pub(crate) fn metrics(&self) -> AggregateMetrics {
        self.metrics
    }

    /// The finished groups, as one chunk in the plan's output types.
    pub fn finish(&mut self) -> Result<Vec<DataChunk>> {
        if self.groups.is_empty() && self.plan.emit_empty_group_on_empty_input {
            let empty_sets: Vec<usize> = self
                .grouping_sets
                .iter()
                .enumerate()
                .filter_map(|(set_id, grouping_set)| grouping_set.is_empty().then_some(set_id))
                .collect();
            for set_id in empty_sets {
                let mut hasher = self.hash_builder.build_hasher();
                set_id.hash(&mut hasher);
                let hash = hasher.finish();
                let exists = self.index.find(hash, |entry| {
                    self.groups[entry.slot].grouping_set_id == set_id
                });
                if exists.is_none() {
                    let slot = Self::push_group(
                        &mut self.groups,
                        &mut self.aggregate_states,
                        set_id,
                        None,
                    )?;
                    self.index
                        .insert_unique(GroupIndexEntry { hash, slot })?;
                    self.metrics.retained_key_slots_initialized = self
                        .metrics
                        .retained_key_slots_initialized
                        .saturating_add(self.plan.groups as u64);
                }
            }
        }
        if self.groups.is_empty() {
            return Ok(Vec::new());
        }
        let output_materialize_started = Instant::now();
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.plan.types.len());
        for (column, data_type) in self.plan.types.iter().enumerate() {
            arrays.push(self.column(column, data_type)?);
        }
        let output = vec![DataChunk::new(
            arrays,
            SelectionVector::new_dense(self.groups.len()),
        )];
        self.metrics.output_materialize_ns = self
            .metrics
            .output_materialize_ns
            .saturating_add(output_materialize_started.elapsed().as_nanos() as u64);
        Ok(output)
    }

    /// One output column, built from every group in group order.
    fn column(&self, column: usize, data_type: &VeinDataType) -> Result<ArrayRef> {
        let groups = self.plan.groups as usize;
        if column < groups {
            let keys = self
                .groups
                .iter()
                .map(|group| {
                    if !self.grouping_sets_by_column[column]
                        .contains(&group.grouping_set_id)
                    {
                        return KeyValueRef::Null;
                    }
                    let Some(location) = group.key_location else {
                        return KeyValueRef::Null;
                    };
                    self.group_key_batches[location.batch][column]
                        .as_ref()
                        .expect("group source batch retains every grouping column")
                        .value(location.row)
                })
                .collect::<Vec<_>>();
            return key_column(&keys, data_type);
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

/// A type-resolved Arrow key column. Cloning an array only clones its shared
/// buffers, so the sink can retain this view without copying key values.
#[derive(Clone)]
enum GroupKeyColumn {
    Int64(Int64Array),
    UInt8(UInt8Array),
    UInt16(UInt16Array),
    UInt32(UInt32Array),
    UInt64(UInt64Array),
    Utf8(StringArray),
    DictionaryUtf8(DictionaryArray<Int32Type>, StringArray),
}

impl GroupKeyColumn {
    fn new(array: &ArrayRef) -> Result<Self> {
        let invalid_type = || {
            VeinError::internal(format!(
                "grouping by {:?} is not supported yet",
                array.data_type()
            ))
        };
        if let Some(values) = array.as_any().downcast_ref::<Int64Array>() {
            return Ok(Self::Int64(values.clone()));
        }
        if let Some(values) = array.as_any().downcast_ref::<UInt8Array>() {
            return Ok(Self::UInt8(values.clone()));
        }
        if let Some(values) = array.as_any().downcast_ref::<UInt16Array>() {
            return Ok(Self::UInt16(values.clone()));
        }
        if let Some(values) = array.as_any().downcast_ref::<UInt32Array>() {
            return Ok(Self::UInt32(values.clone()));
        }
        if let Some(values) = array.as_any().downcast_ref::<UInt64Array>() {
            return Ok(Self::UInt64(values.clone()));
        }
        if let Some(values) = array.as_any().downcast_ref::<StringArray>() {
            return Ok(Self::Utf8(values.clone()));
        }
        if let Some(values) = array.as_any().downcast_ref::<DictionaryArray<Int32Type>>() {
            let strings = values
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(invalid_type)?;
            return Ok(Self::DictionaryUtf8(values.clone(), strings.clone()));
        }
        Err(invalid_type())
    }

    fn dictionary_cardinality(&self) -> Option<usize> {
        match self {
            Self::DictionaryUtf8(_, values) => Some(values.len()),
            _ => None,
        }
    }

    /// Returns a dictionary value index, using `cardinality` for a null key.
    fn dictionary_id(&self, row: usize) -> Option<usize> {
        match self {
            Self::DictionaryUtf8(keys, values) => Some(if keys.keys().is_null(row) {
                values.len()
            } else {
                keys.keys().value(row) as usize
            }),
            _ => None,
        }
    }

    fn dictionary_value(&self, dictionary_id: usize) -> Option<&str> {
        match self {
            Self::DictionaryUtf8(_, values) if dictionary_id < values.len() => {
                (!values.is_null(dictionary_id)).then(|| values.value(dictionary_id))
            }
            Self::DictionaryUtf8(_, values) if dictionary_id == values.len() => None,
            _ => unreachable!("dictionary values are requested only for dictionary keys"),
        }
    }

    fn hash_dictionary_ids<H: Hasher>(&self, column: u32, ids: &[usize], hashers: &mut [H]) {
        for (&dictionary_id, hasher) in ids.iter().zip(hashers) {
            column.hash(hasher);
            self.dictionary_value(dictionary_id).hash(hasher);
        }
    }

    fn dictionary_ids_equal(&self, left: usize, right: usize) -> bool {
        self.dictionary_value(left) == self.dictionary_value(right)
    }

    fn value(&self, row: usize) -> KeyValueRef<'_> {
        match self {
            Self::Int64(values) if values.is_null(row) => KeyValueRef::Null,
            Self::Int64(values) => KeyValueRef::Int64(values.value(row)),
            Self::UInt8(values) if values.is_null(row) => KeyValueRef::Null,
            Self::UInt8(values) => KeyValueRef::UInt64(values.value(row) as u64),
            Self::UInt16(values) if values.is_null(row) => KeyValueRef::Null,
            Self::UInt16(values) => KeyValueRef::UInt64(values.value(row) as u64),
            Self::UInt32(values) if values.is_null(row) => KeyValueRef::Null,
            Self::UInt32(values) => KeyValueRef::UInt64(values.value(row) as u64),
            Self::UInt64(values) if values.is_null(row) => KeyValueRef::Null,
            Self::UInt64(values) => KeyValueRef::UInt64(values.value(row)),
            Self::Utf8(values) if values.is_null(row) => KeyValueRef::Null,
            Self::Utf8(values) => KeyValueRef::Utf8(values.value(row)),
            Self::DictionaryUtf8(keys, _) if keys.keys().is_null(row) => KeyValueRef::Null,
            Self::DictionaryUtf8(keys, values) => {
                let value = keys.keys().value(row) as usize;
                if values.is_null(value) {
                    KeyValueRef::Null
                } else {
                    KeyValueRef::Utf8(values.value(value))
                }
            }
        }
    }

    fn rows_equal(&self, left: usize, other: &Self, right: usize) -> bool {
        self.value(left) == other.value(right)
    }

    fn hash_batch<H: Hasher>(
        &self,
        column: u32,
        selection: &SelectionVector,
        grouping_set_ids: &[usize],
        grouping_set_count: usize,
        hashers: &mut [H],
    ) {
        match self {
            Self::Int64(values) => hash_nullable_column(
                column,
                selection,
                grouping_set_ids,
                grouping_set_count,
                values.nulls(),
                |row| values.value(row),
                hashers,
            ),
            Self::UInt8(values) => hash_nullable_column(
                column,
                selection,
                grouping_set_ids,
                grouping_set_count,
                values.nulls(),
                |row| u64::from(values.value(row)),
                hashers,
            ),
            Self::UInt16(values) => hash_nullable_column(
                column,
                selection,
                grouping_set_ids,
                grouping_set_count,
                values.nulls(),
                |row| u64::from(values.value(row)),
                hashers,
            ),
            Self::UInt32(values) => hash_nullable_column(
                column,
                selection,
                grouping_set_ids,
                grouping_set_count,
                values.nulls(),
                |row| u64::from(values.value(row)),
                hashers,
            ),
            Self::UInt64(values) => hash_nullable_column(
                column,
                selection,
                grouping_set_ids,
                grouping_set_count,
                values.nulls(),
                |row| values.value(row),
                hashers,
            ),
            Self::Utf8(values) => hash_nullable_column(
                column,
                selection,
                grouping_set_ids,
                grouping_set_count,
                values.nulls(),
                |row| values.value(row),
                hashers,
            ),
            Self::DictionaryUtf8(keys, values) => {
                for logical_row in 0..selection.size() {
                    let row = selection.get_index(logical_row);
                    for &set_id in grouping_set_ids {
                        let hasher = &mut hashers[logical_row * grouping_set_count + set_id];
                        column.hash(hasher);
                        let value = if keys.is_null(row) {
                            None
                        } else {
                            let value_index = keys.keys().value(row) as usize;
                            (!values.is_null(value_index)).then(|| values.value(value_index))
                        };
                        value.hash(hasher);
                    }
                }
            }
        }
    }
}

fn hash_nullable_column<T: Hash, H: Hasher>(
    column: u32,
    selection: &SelectionVector,
    grouping_set_ids: &[usize],
    grouping_set_count: usize,
    validity: Option<&NullBuffer>,
    value_at: impl Fn(usize) -> T,
    hashers: &mut [H],
) {
    for logical_row in 0..selection.size() {
        let row = selection.get_index(logical_row);
        for &set_id in grouping_set_ids {
            let hasher = &mut hashers[logical_row * grouping_set_count + set_id];
            column.hash(hasher);
            if validity.is_some_and(|nulls| !nulls.is_valid(row)) {
                Option::<T>::None.hash(hasher);
            } else {
                Some(value_at(row)).hash(hasher);
            }
        }
    }
}

/// One key column, built from every group.
fn key_column(keys: &[KeyValueRef<'_>], data_type: &VeinDataType) -> Result<ArrayRef> {
    Ok(match data_type {
        VeinDataType::Int64 => {
            let values: Vec<Option<i64>> = keys
                .iter()
                .map(|key| match key {
                    KeyValueRef::Int64(value) => Some(*value),
                    KeyValueRef::Null => None,
                    KeyValueRef::UInt64(_) | KeyValueRef::Utf8(_) => {
                        unreachable!("an int64 key column holds int64 keys")
                    }
                })
                .collect();
            Arc::new(Int64Array::from(values))
        }
        VeinDataType::UInt8 => Arc::new(UInt8Array::from(unsigned_key_values(keys, |value| value as u8))),
        VeinDataType::UInt16 => Arc::new(UInt16Array::from(unsigned_key_values(keys, |value| value as u16))),
        VeinDataType::UInt32 => Arc::new(UInt32Array::from(unsigned_key_values(keys, |value| value as u32))),
        VeinDataType::UInt64 => Arc::new(UInt64Array::from(unsigned_key_values(keys, |value| value))),
        VeinDataType::Utf8 => {
            let values: Vec<Option<&str>> = keys
                .iter()
                .map(|key| match key {
                    KeyValueRef::Utf8(value) => Some(*value),
                    KeyValueRef::Null => None,
                    KeyValueRef::Int64(_) | KeyValueRef::UInt64(_) => {
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

fn unsigned_key_values<T>(keys: &[KeyValueRef<'_>], convert: impl Fn(u64) -> T) -> Vec<Option<T>> {
    keys
        .iter()
        .map(|key| match key {
            KeyValueRef::UInt64(value) => Some(convert(*value)),
            KeyValueRef::Null => None,
            KeyValueRef::Int64(_) | KeyValueRef::Utf8(_) => {
                unreachable!("an unsigned key column holds unsigned keys")
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        Decimal256Array, DictionaryArray, Float16Array, Float32Array, Float64Array, Int32Array,
        Int64Array, StringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    };
    use arrow::datatypes::{i256, Int32Type};
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
            estimated_groups: None,
            grouping_sets: None,
            emit_empty_group_on_empty_input: true,
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
            estimated_groups: None,
            grouping_sets: None,
            emit_empty_group_on_empty_input: true,
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
            estimated_groups: None,
            grouping_sets: None,
            emit_empty_group_on_empty_input: true,
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
            estimated_groups: None,
            grouping_sets: None,
            emit_empty_group_on_empty_input: true,
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
                estimated_groups: None,
                grouping_sets: None,
                emit_empty_group_on_empty_input: true,
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
                estimated_groups: None,
                grouping_sets: None,
                emit_empty_group_on_empty_input: true,
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
            estimated_groups: None,
            grouping_sets: None,
            emit_empty_group_on_empty_input: true,
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
            estimated_groups: None,
            grouping_sets: None,
            emit_empty_group_on_empty_input: true,
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
            estimated_groups: None,
            grouping_sets: None,
            emit_empty_group_on_empty_input: true,
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
            estimated_groups: None,
            grouping_sets: None,
            emit_empty_group_on_empty_input: true,
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
            estimated_groups: None,
            grouping_sets: None,
            emit_empty_group_on_empty_input: true,
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
            estimated_groups: None,
            grouping_sets: None,
            emit_empty_group_on_empty_input: true,
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
            estimated_groups: None,
            grouping_sets: None,
            emit_empty_group_on_empty_input: true,
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
            estimated_groups: None,
            grouping_sets: None,
            emit_empty_group_on_empty_input: true,
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
            estimated_groups: None,
            grouping_sets: None,
            emit_empty_group_on_empty_input: true,
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
            estimated_groups: None,
            grouping_sets: None,
            emit_empty_group_on_empty_input: true,
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
            estimated_groups: None,
            grouping_sets: None,
            emit_empty_group_on_empty_input: true,
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
            estimated_groups: None,
            grouping_sets: None,
            emit_empty_group_on_empty_input: true,
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
            estimated_groups: None,
            grouping_sets: None,
            emit_empty_group_on_empty_input: true,
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
            estimated_groups: None,
            grouping_sets: None,
            emit_empty_group_on_empty_input: true,
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
            estimated_groups: None,
            grouping_sets: None,
            emit_empty_group_on_empty_input: true,
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
            estimated_groups: None,
            grouping_sets: None,
            emit_empty_group_on_empty_input: true,
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
            estimated_groups: None,
            grouping_sets: None,
            emit_empty_group_on_empty_input: true,
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
    fn zero_column_distinct_emits_one_row_only_for_nonempty_input() {
        let distinct_plan = || AggregatePlan {
            groups: 0,
            estimated_groups: None,
            grouping_sets: Some(vec![vec![]]),
            emit_empty_group_on_empty_input: false,
            aggs: Vec::new(),
            aggregate_input_types: Vec::new(),
            types: Vec::new(),
        };

        let mut empty = AggregateSink::new(distinct_plan()).expect("create empty distinct sink");
        assert!(empty.finish().expect("finish empty distinct").is_empty());

        let mut nonempty = AggregateSink::new(distinct_plan()).expect("create distinct sink");
        nonempty
            .accumulate(DataChunk::new(
                Vec::new(),
                SelectionVector::new_dense(4),
            ))
            .expect("accumulate zero-column input rows");
        let output = nonempty.finish().expect("finish distinct");

        assert_eq!(output.len(), 1);
        assert_eq!(output[0].num_columns(), 0);
        assert_eq!(output[0].num_rows(), 1);
    }

    #[test]
    fn empty_global_average_uses_its_declared_input_type() {
        let plan = AggregatePlan {
            groups: 0,
            estimated_groups: None,
            grouping_sets: None,
            emit_empty_group_on_empty_input: true,
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
            estimated_groups: None,
            grouping_sets: Some(vec![vec![0, 1], vec![0], vec![]]),
            emit_empty_group_on_empty_input: true,
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

        let metrics = sink.metrics();
        assert_eq!(metrics.input_batches, 1);
        assert_eq!(metrics.input_rows, 2);
        assert_eq!(metrics.grouping_sets, 3);
        assert_eq!(metrics.grouping_probes, 6);
        assert_eq!(metrics.new_groups, 5);
        assert_eq!(metrics.string_key_bytes_copied, 0);
        assert_eq!(metrics.retained_key_slots_initialized, 10);
        assert_eq!(metrics.string_key_bytes_copied, 0);

        let output = sink.finish().unwrap();
        let batch = &output[0];
        let first_key = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let second_key = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        let sums = batch.column(2).as_any().downcast_ref::<Int64Array>().unwrap();

        assert_eq!(batch.num_rows(), 5);
        let mut actual = (0..batch.num_rows())
            .map(|row| {
                (
                    (!first_key.is_null(row)).then(|| first_key.value(row)),
                    (!second_key.is_null(row)).then(|| second_key.value(row)),
                    sums.value(row),
                )
            })
            .collect::<Vec<_>>();
        actual.sort_unstable();
        let mut expected = vec![
            (Some(1), Some("x"), 10),
            (None, Some("z"), 20),
            (Some(1), None, 10),
            (None, None, 20),
            (None, None, 30),
        ];
        expected.sort_unstable();
        assert_eq!(actual, expected);
    }

    #[test]
    fn dictionary_utf8_grouping_handles_nulls_unused_values_and_batch_local_ids() {
        let plan = AggregatePlan {
            groups: 1,
            estimated_groups: None,
            grouping_sets: None,
            emit_empty_group_on_empty_input: true,
            aggs: vec![AggOpCode::Sum],
            aggregate_input_types: vec![VeinDataType::Int64],
            types: vec![VeinDataType::Utf8, VeinDataType::Int64],
        };
        let mut sink = AggregateSink::new(plan).unwrap();
        let first_dictionary = DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![Some(0), Some(1), Some(0), None, Some(2), Some(3)]),
            Arc::new(StringArray::from(vec![
                Some("O"),
                Some("F"),
                None,
                Some("O"),
                Some("unused"),
            ])),
        )
            .unwrap();
        sink.accumulate(DataChunk::from_vectors(vec![
            Arc::new(first_dictionary),
            Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50, 60])),
        ]))
            .unwrap();

        let second_dictionary = DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![Some(0), Some(1)]),
            Arc::new(StringArray::from(vec![Some("F"), Some("O")])),
        )
            .unwrap();
        sink.accumulate(DataChunk::from_vectors(vec![
            Arc::new(second_dictionary),
            Arc::new(Int64Array::from(vec![60, 70])),
        ]))
            .unwrap();

        let output = sink.finish().unwrap();
        let keys = output[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let sums = output[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let actual = (0..output[0].num_rows())
            .map(|row| {
                (
                    (!keys.is_null(row)).then(|| keys.value(row)),
                    sums.value(row),
                )
            })
            .collect::<std::collections::HashMap<_, _>>();

        let metrics = sink.metrics();
        assert_eq!(actual.get(&Some("O")), Some(&170));
        assert_eq!(actual.get(&Some("F")), Some(&80));
        assert_eq!(actual.get(&None), Some(&90));
        assert_eq!(actual.len(), 3, "unused dictionary values must not form groups");
        assert_eq!(metrics.input_rows, 8);
        assert_eq!(metrics.grouping_probes, 7);
        assert_eq!(metrics.new_groups, 3);
        assert_eq!(metrics.string_key_bytes_copied, 0);
    }

    #[test]
    fn empty_input_emits_only_empty_grouping_sets() {
        let plan = AggregatePlan {
            groups: 1,
            estimated_groups: None,
            grouping_sets: Some(vec![vec![0], vec![]]),
            emit_empty_group_on_empty_input: true,
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

    #[test]
    fn hash_collisions_keep_distinct_groups_and_update_matching_group() {
        let plan = AggregatePlan {
            groups: 1,
            estimated_groups: None,
            grouping_sets: None,
            emit_empty_group_on_empty_input: true,
            aggs: vec![AggOpCode::Sum],
            aggregate_input_types: vec![VeinDataType::Int64],
            types: vec![VeinDataType::Int64, VeinDataType::Int64],
        };
        let mut sink = AggregateSink::new(plan).unwrap();
        sink.accumulate(DataChunk::from_vectors(vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![10])),
        ]))
            .unwrap();

        let colliding_chunk = DataChunk::from_vectors(vec![
            Arc::new(Int64Array::from(vec![2, 2])),
            Arc::new(Int64Array::from(vec![20, 30])),
        ]);
        let colliding_key_column = GroupKeyColumn::new(colliding_chunk.column(0)).unwrap();
        let mut colliding_hashers: Vec<_> = (0..colliding_chunk.selection.size())
            .map(|_| sink.hash_builder.build_hasher())
            .collect();
        for hasher in &mut colliding_hashers {
            0usize.hash(hasher);
        }
        colliding_key_column.hash_batch(
            0,
            &colliding_chunk.selection,
            &[0],
            1,
            &mut colliding_hashers,
        );
        let colliding_hash = colliding_hashers[0].finish();
        sink.index
            .insert_unique(GroupIndexEntry {
                hash: colliding_hash,
                slot: 0,
            })
            .unwrap();
        sink.accumulate(colliding_chunk).unwrap();
        let metrics = sink.metrics();
        assert_eq!(metrics.grouping_probes, 3);
        assert!(metrics.group_index_candidate_checks > 0);
        assert_eq!(metrics.full_hash_matches, 3);
        assert_eq!(metrics.group_key_comparisons, 3);
        assert_eq!(metrics.group_collision_retries, 2);
        assert_eq!(metrics.group_index_hits, 1);
        assert_eq!(metrics.group_index_misses, 2);

        let output = sink.finish().unwrap();
        let batch = &output[0];
        let keys = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let sums = batch.column(1).as_any().downcast_ref::<Int64Array>().unwrap();

        assert_eq!(batch.num_rows(), 2);
        assert_eq!((keys.value(0), sums.value(0)), (1, 10));
        assert_eq!((keys.value(1), sums.value(1)), (2, 50));
    }

    #[test]
    fn estimated_group_count_reserves_index_before_accumulation() {
        let plan = AggregatePlan {
            groups: 1,
            estimated_groups: Some(4),
            grouping_sets: None,
            emit_empty_group_on_empty_input: true,
            aggs: vec![AggOpCode::Count],
            aggregate_input_types: vec![VeinDataType::Int64],
            types: vec![VeinDataType::Int64, VeinDataType::Int64],
        };
        let mut sink = AggregateSink::new(plan).expect("create aggregate sink");
        sink.accumulate(DataChunk::from_vectors(vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(Int64Array::from(vec![1, 1, 1, 1])),
        ]))
            .expect("accumulate groups");

        assert_eq!(sink.metrics().group_index_capacity_growth_events, 0);
        let output = sink.finish().expect("finish aggregate");
        assert_eq!(output[0].num_rows(), 4);
    }

    #[test]
    fn first_batch_candidate_bound_reserves_index_without_group_estimate() {
        let plan = AggregatePlan {
            groups: 1,
            estimated_groups: None,
            grouping_sets: None,
            emit_empty_group_on_empty_input: true,
            aggs: vec![AggOpCode::Count],
            aggregate_input_types: vec![VeinDataType::Int64],
            types: vec![VeinDataType::Int64, VeinDataType::Int64],
        };
        let mut sink = AggregateSink::new(plan).expect("create aggregate sink");
        sink.accumulate(DataChunk::from_vectors(vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(Int64Array::from(vec![1, 1, 1, 1])),
        ]))
            .expect("accumulate first batch");

        let metrics = sink.metrics();
        assert!(metrics.group_index_capacity >= 4);
        assert_eq!(metrics.group_index_capacity_growth_events, 0);
        assert_eq!(metrics.new_groups, 4);

        let output = sink.finish().expect("finish aggregate");
        assert_eq!(output[0].num_rows(), 4);
    }

    #[test]
    fn retained_group_keys_share_input_arrow_buffers() {
        let plan = AggregatePlan {
            groups: 1,
            estimated_groups: None,
            grouping_sets: None,
            emit_empty_group_on_empty_input: true,
            aggs: vec![AggOpCode::Count],
            aggregate_input_types: vec![VeinDataType::Int64],
            types: vec![VeinDataType::Int64, VeinDataType::Int64],
        };
        let mut sink = AggregateSink::new(plan).expect("create aggregate sink");
        let keys = Arc::new(Int64Array::from(vec![10, 20, 30]));
        let source_values = keys.values().as_ptr();
        sink.accumulate(DataChunk::from_vectors(vec![
            keys,
            Arc::new(Int64Array::from(vec![1, 1, 1])),
        ]))
            .expect("accumulate keys");

        let Some(GroupKeyColumn::Int64(retained)) = &sink.group_key_batches[0][0] else {
            panic!("retained key batch should contain the int64 Arrow array");
        };
        assert_eq!(retained.values().as_ptr(), source_values);
        assert_eq!(sink.metrics().string_key_bytes_copied, 0);
    }

    fn single_value_plan() -> AggregatePlan {
        AggregatePlan {
            groups: 0,
            estimated_groups: None,
            grouping_sets: None,
            emit_empty_group_on_empty_input: true,
            aggs: vec![AggOpCode::SingleValue],
            aggregate_input_types: vec![VeinDataType::Int64],
            types: vec![VeinDataType::Int64],
        }
    }

    #[test]
    fn single_value_retains_the_only_arrow_scalar_without_copying() {
        let input = Arc::new(Int64Array::from(vec![42]));
        let input_values = input.values().as_ptr();
        let mut sink = AggregateSink::new(single_value_plan()).expect("create scalar aggregate");
        sink.accumulate(DataChunk::from_vectors(vec![Arc::clone(&input) as ArrayRef]))
            .expect("accumulate scalar row");

        let output = sink.finish().expect("finish scalar aggregate");
        let values = output[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("scalar output is int64");
        assert_eq!(values.value(0), 42);
        assert_eq!(values.values().as_ptr(), input_values);
    }

    #[test]
    fn single_value_returns_typed_null_for_empty_input() {
        let mut sink = AggregateSink::new(single_value_plan()).expect("create scalar aggregate");
        sink.accumulate(DataChunk::from_vectors(vec![Arc::new(Int64Array::from(
            Vec::<i64>::new(),
        ))]))
        .expect("accumulate empty input");

        let output = sink.finish().expect("finish empty scalar aggregate");
        let values = output[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("scalar output retains its declared type");
        assert_eq!(values.len(), 1);
        assert!(values.is_null(0));
    }

    #[test]
    fn single_value_errors_when_input_has_multiple_rows_across_batches() {
        let mut sink = AggregateSink::new(single_value_plan()).expect("create scalar aggregate");
        sink.accumulate(DataChunk::from_vectors(vec![Arc::new(Int64Array::from(vec![1]))]))
            .expect("first row is valid");
        let error = sink
            .accumulate(DataChunk::from_vectors(vec![Arc::new(Int64Array::from(vec![2]))]))
            .expect_err("second row must violate scalar cardinality");
        assert!(error.to_string().contains("scalar subquery returned more than one row"));
    }
}
