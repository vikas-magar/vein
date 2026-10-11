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

use std::collections::HashSet;
use std::fs::File;
use std::hash::{BuildHasher, Hash, Hasher};
use std::mem::size_of;
use std::num::NonZeroUsize;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use arrow::array::{
    Array, ArrayRef, Decimal128Array, Decimal256Array, DictionaryArray, Float16Array, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, StringArray,
    UInt16Array, UInt32Array, UInt64Array, UInt8Array, new_null_array,
};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{Int32Type, i256};
use hashbrown::DefaultHashBuilder;

mod group_index;
mod intermediate;
mod spill;
use intermediate::{AggregateIntermediateGroup, AggregateIntermediateState, NumericPartial};
use crate::memory::{MemoryReservation, QueryMemoryTracker};
use crate::pipeline::{DataChunk, PipelineNode};
use common::Result;
use common::arrow::SelectionVector;
use common::protocol::{AggOpCode, AggregatePlan};
use error::VeinError;
use group_index::{GroupIndex, GroupIndexEntry, ProbeResolution};
use spill::AggregateSpillSchema;
use types::VeinDataType;

type GroupHasher = <DefaultHashBuilder as BuildHasher>::Hasher;

/// Maximum group rows materialized into one aggregate output chunk.
pub(crate) const AGGREGATE_BATCH_ROWS: NonZeroUsize =
    NonZeroUsize::new(1024).expect("aggregate batch size must be non-zero");

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

    #[allow(dead_code)] // Used by aggregate spill serialization.
    fn snapshot(&self) -> NumericPartial<T> {
        NumericPartial {
            op: self.op,
            value: self.value,
            count: self.count,
            scale: self.scale,
            decimal_precision_limit: self.decimal_precision_limit,
        }
    }

    #[allow(dead_code)] // Used when restoring aggregate spill state.
    fn merge_partial(&mut self, partial: &NumericPartial<T>) -> Result<()> {
        if self.op != partial.op
            || self.scale != partial.scale
            || self.decimal_precision_limit != partial.decimal_precision_limit
        {
            return Err(VeinError::internal("incompatible numeric aggregate partial state"));
        }
        if partial.count < 0 || (self.op != AggOpCode::Avg && partial.count != 0) {
            return Err(VeinError::internal("invalid numeric aggregate partial count"));
        }
        if (partial.count == 0) != partial.value.is_none() && self.op == AggOpCode::Avg {
            return Err(VeinError::internal("invalid AVG partial state"));
        }

        let value = match (self.op, self.value, partial.value) {
            (_, current, None) => current,
            (_, None, incoming) => incoming,
            (AggOpCode::Sum | AggOpCode::Avg, Some(current), Some(incoming)) => {
                Some(current.checked_add(incoming, self.decimal_precision_limit).ok_or_else(|| {
                    VeinError::internal("numeric overflow while merging aggregate state")
                })?)
            }
            (AggOpCode::Min, Some(current), Some(incoming)) => Some(current.minimum(incoming)),
            (AggOpCode::Max, Some(current), Some(incoming)) => Some(current.maximum(incoming)),
            (op, _, _) => {
                return Err(VeinError::internal(format!("unsupported aggregate operation {op:?}")));
            }
        };
        let count = if self.op == AggOpCode::Avg {
            self.count.checked_add(partial.count).ok_or_else(|| {
                VeinError::internal("numeric overflow while merging AVG count")
            })?
        } else {
            self.count
        };
        self.value = value;
        self.count = count;
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
    fn new(op: AggOpCode, input_type: &VeinDataType, array: &'a ArrayRef) -> Result<Self> {
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
                state
                    .value
                    .expect("a non-empty average has a sum")
                    .to_f64(state.scale)
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
    fn bytes_per_group(&self) -> usize {
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

    fn new(op: AggOpCode, input_type: &VeinDataType, result_type: &VeinDataType) -> Result<Self> {
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
                states.push(NumericAccumulator::new(*op, *scale, *op == AggOpCode::Sum))
            }
            Self::Decimal256 { op, scale, states } => {
                states.push(NumericAccumulator::new(*op, *scale, *op == AggOpCode::Sum))
            }
        }
    }

    #[allow(dead_code)] // Used by aggregate spill serialization.
    fn intermediate_state(&self, slot: usize) -> Result<AggregateIntermediateState> {
        macro_rules! numeric_state {
            ($states:expr, $variant:ident) => {
                AggregateIntermediateState::$variant(
                    $states
                        .get(slot)
                        .ok_or_else(|| VeinError::internal("aggregate state slot is out of bounds"))?
                        .snapshot(),
                )
            };
        }

        Ok(match self {
            Self::SingleValue(states) => {
                let value = states
                    .get(slot)
                    .ok_or_else(|| VeinError::internal("aggregate state slot is out of bounds"))?
                    .clone();
                AggregateIntermediateState::SingleValue {
                    rows: u64::from(value.is_some()),
                    value,
                }
            }
            Self::Count(states) => AggregateIntermediateState::Count(
                *states
                    .get(slot)
                    .ok_or_else(|| VeinError::internal("aggregate state slot is out of bounds"))?,
            ),
            Self::Int64 { states, .. } => numeric_state!(states, Int64),
            Self::Int128Avg { states } => numeric_state!(states, Int128Avg),
            Self::UInt64 { states, .. } => numeric_state!(states, UInt64),
            Self::UInt128Avg { states } => numeric_state!(states, UInt128Avg),
            Self::Float32 { states, .. } => numeric_state!(states, Float32),
            Self::Float64 { states, .. } => numeric_state!(states, Float64),
            Self::Decimal128 { states, .. } => numeric_state!(states, Decimal128),
            Self::Decimal256 { states, .. } => numeric_state!(states, Decimal256),
        })
    }

    #[allow(dead_code)] // Used when restoring aggregate spill state.
    fn merge_intermediate_state(
        &mut self,
        slot: usize,
        partial: &AggregateIntermediateState,
    ) -> Result<()> {
        macro_rules! merge_numeric {
            ($states:expr, $partial:expr) => {{
                $states
                    .get_mut(slot)
                    .ok_or_else(|| VeinError::internal("aggregate state slot is out of bounds"))?
                    .merge_partial($partial)
            }};
        }

        match (self, partial) {
            (Self::SingleValue(states), AggregateIntermediateState::SingleValue { rows, value }) => {
                if *rows > 1 || (*rows == 0) != value.is_none() {
                    return Err(VeinError::internal("invalid SINGLE_VALUE partial state"));
                }
                let current = states
                    .get_mut(slot)
                    .ok_or_else(|| VeinError::internal("aggregate state slot is out of bounds"))?;
                let current_rows = u64::from(current.is_some());
                if current_rows + rows > 1 {
                    return Err(VeinError::stage(
                        "aggregate",
                        "scalar subquery returned more than one row",
                    ));
                }
                if *rows == 1 {
                    *current = value.clone();
                }
                Ok(())
            }
            (Self::Count(states), AggregateIntermediateState::Count(partial)) => {
                let current = states
                    .get_mut(slot)
                    .ok_or_else(|| VeinError::internal("aggregate state slot is out of bounds"))?;
                if *current < 0 || *partial < 0 {
                    return Err(VeinError::internal("invalid COUNT partial state"));
                }
                *current = current
                    .checked_add(*partial)
                    .ok_or_else(|| VeinError::internal("COUNT overflow while merging partial state"))?;
                Ok(())
            }
            (Self::Int64 { states, .. }, AggregateIntermediateState::Int64(partial)) => {
                merge_numeric!(states, partial)
            }
            (Self::Int128Avg { states }, AggregateIntermediateState::Int128Avg(partial)) => {
                merge_numeric!(states, partial)
            }
            (Self::UInt64 { states, .. }, AggregateIntermediateState::UInt64(partial)) => {
                merge_numeric!(states, partial)
            }
            (Self::UInt128Avg { states }, AggregateIntermediateState::UInt128Avg(partial)) => {
                merge_numeric!(states, partial)
            }
            (Self::Float32 { states, .. }, AggregateIntermediateState::Float32(partial)) => {
                merge_numeric!(states, partial)
            }
            (Self::Float64 { states, .. }, AggregateIntermediateState::Float64(partial)) => {
                merge_numeric!(states, partial)
            }
            (Self::Decimal128 { states, .. }, AggregateIntermediateState::Decimal128(partial)) => {
                merge_numeric!(states, partial)
            }
            (Self::Decimal256 { states, .. }, AggregateIntermediateState::Decimal256(partial)) => {
                merge_numeric!(states, partial)
            }
            _ => Err(VeinError::internal("incompatible aggregate partial state variant")),
        }
    }

    fn update_batch(&mut self, input: AggregateInput<'_>, group_rows: GroupRows<'_>) -> Result<()> {
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

    fn len(&self) -> usize {
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

    fn to_array_range(
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

/// One group's non-key state.
struct Group {
    hash: u64,
    grouping_set_id: usize,
    key_location: Option<GroupKeyLocation>,
}

/// Locates a group's original key row in retained Arrow-backed key batches.
#[derive(Clone, Copy)]
pub(super) struct GroupKeyLocation {
    batch: usize,
    row: usize,
}

/// Undo index entries and group-byte reservations when a batch cannot finish
/// retaining its keys or reserving the group output storage.
fn rollback_new_groups(
    index: &mut GroupIndex,
    hashes: &[u64],
    existing_group_count: usize,
    new_groups: &mut Vec<usize>,
    memory: &mut MemoryReservation,
    reserved_bytes_before_batch: usize,
) {
    for (new_group_index, &candidate) in new_groups.iter().enumerate() {
        let removed = index.remove(hashes[candidate], existing_group_count + new_group_index);
        debug_assert!(removed, "every uncommitted group must have an index entry");
    }
    let batch_bytes = memory.bytes().saturating_sub(reserved_bytes_before_batch);
    memory.shrink(batch_bytes);
    new_groups.clear();
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
    /// Memory leases for the distinct source arrays retained by each key batch.
    group_key_batch_leases: Vec<Vec<Arc<MemoryReservation>>>,
    /// Reusable group IDs for each grouping set in the current input batch.
    group_ids_by_set: Vec<Vec<usize>>,
    groups: Vec<Group>,
    aggregate_states: Vec<AggregateStateColumn>,
    memory: MemoryReservation,
    tracker: QueryMemoryTracker,
    spill_schema: AggregateSpillSchema,
    spill_runs: Vec<PathBuf>,
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
        Self::with_memory_tracker(
            plan,
            QueryMemoryTracker::unlimited(common::cancellation::CancellationToken::new()),
        )
    }

    /// An accumulator that charges owned group/index state to `tracker`.
    pub fn with_memory_tracker(plan: AggregatePlan, tracker: QueryMemoryTracker) -> Result<Self> {
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
        let grouping_sets = plan
            .grouping_sets
            .clone()
            .unwrap_or_else(|| vec![(0..plan.groups).collect()]);
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
        let spill_schema = AggregateSpillSchema::try_new(
            plan.types[..groups].to_vec(),
            grouping_sets
                .iter()
                .map(|set| set.iter().map(|&column| column as usize).collect())
                .collect(),
        )?;
        let group_ids_by_set = (0..grouping_sets.len()).map(|_| Vec::new()).collect();
        let mut memory = tracker.reserve(0, "aggregate state")?;
        let mut index = GroupIndex::default();
        let group_index_reserve_ns = plan
            .estimated_groups
            .and_then(|groups| usize::try_from(groups).ok())
            .map(|groups| {
                let bytes = groups
                    .checked_mul(size_of::<GroupIndexEntry>())
                    .ok_or_else(|| VeinError::internal("aggregate index size overflow"))?;
                memory.grow(bytes, "aggregate group index")?;
                index.reserve(groups)
            })
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
            group_key_batch_leases: Vec::new(),
            dictionary_first_rows: Vec::new(),
            group_ids_by_set,
            groups: Vec::new(),
            aggregate_states,
            memory,
            tracker,
            spill_schema,
            spill_runs: Vec::new(),
        })
    }

    /// Adds a group slot and its corresponding state to every aggregate.
    fn push_group(
        groups: &mut Vec<Group>,
        aggregate_states: &mut [AggregateStateColumn],
        hash: u64,
        grouping_set_id: usize,
        key_location: Option<GroupKeyLocation>,
    ) -> Result<usize> {
        let slot = groups.len();
        groups.push(Group {
            hash,
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
        self.accumulate_with_spill(&chunk)
    }

    /// Retries only resource failures, splitting an oversized selection while
    /// keeping every Arrow column shared with the original input chunk.
    fn accumulate_with_spill(&mut self, chunk: &DataChunk) -> Result<()> {
        match self.accumulate_batch(chunk) {
            Ok(()) => Ok(()),
            Err(VeinError::ResourceExhausted { .. }) if !self.groups.is_empty() => {
                self.flush_current_run()?;
                self.accumulate_with_spill(chunk)
            }
            Err(error @ VeinError::ResourceExhausted { .. }) => {
                let rows = chunk.selection.size();
                if rows <= 1 {
                    return Err(error);
                }
                let midpoint = rows / 2;
                let left = DataChunk::new(
                    chunk.vectors.clone(),
                    chunk.selection.slice(0, midpoint),
                );
                let right = DataChunk::new(
                    chunk.vectors.clone(),
                    chunk.selection.slice(midpoint, rows - midpoint),
                );
                self.accumulate_with_spill(&left)?;
                self.accumulate_with_spill(&right)
            }
            Err(error) => Err(error),
        }
    }

    fn accumulate_batch(&mut self, chunk: &DataChunk) -> Result<()> {
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
            self.dictionary_group_slots
                .resize(cardinality + 1, usize::MAX);
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
        if self.metrics.input_batches == 0 && self.plan.estimated_groups.is_none() {
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
        self.metrics.hasher_setup_ns = self
            .metrics
            .hasher_setup_ns
            .saturating_add(hasher_setup_started.elapsed().as_nanos() as u64);
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
        self.metrics.hash_finalize_ns = self
            .metrics
            .hash_finalize_ns
            .saturating_add(hash_finalize_started.elapsed().as_nanos() as u64);
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
        let memory = &mut self.memory;
        let index = &mut self.index;
        let batch_group_ids = &mut self.batch_group_ids;
        let groups_store = &mut self.groups;
        let group_key_batches = &self.group_key_batches;
        let aggregate_states = &mut self.aggregate_states;
        let metrics = &mut self.metrics;
        let new_groups = &mut self.batch_new_groups;
        let dictionary_ids = &self.dictionary_ids;
        let preallocated_groups = self
            .plan
            .estimated_groups
            .and_then(|groups| usize::try_from(groups).ok())
            .filter(|_| self.spill_runs.is_empty())
            .unwrap_or(0);
        let aggregate_state_bytes = aggregate_states
            .iter()
            .try_fold(0usize, |bytes, state| {
                bytes.checked_add(state.bytes_per_group())
            })
            .ok_or_else(|| VeinError::internal("aggregate state size overflow"))?;
        new_groups.clear();
        new_groups.try_reserve(candidate_count).map_err(|error| {
            VeinError::internal(format!("cannot reserve aggregate batch groups: {error}"))
        })?;
        let group_memory_before_batch = memory.bytes();
        let resolution_started = Instant::now();
        let resolution = index.resolve_rows(hashes, batch_group_ids, |candidate, existing_slot| {
            let set_id = if dictionary_fast_path {
                0
            } else {
                candidate % grouping_set_count
            };
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
                            metrics.group_key_comparisons =
                                metrics.group_key_comparisons.saturating_add(1);
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
                            metrics.group_key_comparisons =
                                metrics.group_key_comparisons.saturating_add(1);
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
                                chunk.selection.get_index(leader / grouping_set_count)
                            };
                            leader_set_id == set_id
                                && grouping_set.iter().all(|&column| {
                                metrics.group_key_comparisons =
                                    metrics.group_key_comparisons.saturating_add(1);
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
                    let key_location_bytes = size_of::<Option<GroupKeyLocation>>()
                        .checked_mul(groups)
                        .ok_or_else(|| {
                            VeinError::internal("aggregate key location size overflow")
                        })?;
                    let group_bytes = size_of::<Group>()
                        .checked_add(key_location_bytes)
                        .and_then(|bytes| bytes.checked_add(aggregate_state_bytes))
                        .and_then(|bytes| {
                            if slot >= preallocated_groups {
                                bytes.checked_add(size_of::<GroupIndexEntry>())
                            } else {
                                Some(bytes)
                            }
                        })
                        .ok_or_else(|| VeinError::internal("aggregate group size overflow"))?;
                    memory.grow(group_bytes, "aggregate group state")?;
                    new_groups.push(candidate);
                    Ok(ProbeResolution::Insert(slot))
                }
            }
        });
        let index_metrics = match resolution {
            Ok(metrics) => metrics,
            Err(error) => {
                rollback_new_groups(
                    index,
                    hashes,
                    groups_store.len(),
                    new_groups,
                    memory,
                    group_memory_before_batch,
                );
                return Err(error);
            }
        };
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
        if let Err(error) = groups_store.try_reserve(new_groups.len()) {
            rollback_new_groups(
                index,
                hashes,
                groups_store.len(),
                new_groups,
                memory,
                group_memory_before_batch,
            );
            return Err(VeinError::internal(format!(
                "cannot reserve aggregate groups: {error}"
            )));
        }
        let key_batch = if new_groups.is_empty() {
            None
        } else {
            // Group keys retain their source Arrow arrays. Charge the full array
            // footprint, deduplicated by array identity across operators that
            // share this query tracker, before retaining another Arc to them.
            let mut retained_arrays = HashSet::new();
            let mut key_leases = Vec::new();
            if let Err(error) = key_leases.try_reserve(key_columns.len()) {
                rollback_new_groups(
                    index,
                    hashes,
                    groups_store.len(),
                    new_groups,
                    memory,
                    group_memory_before_batch,
                );
                return Err(VeinError::internal(format!(
                    "cannot reserve aggregate group-key leases: {error}"
                )));
            }
            for (column, key_column) in key_columns.iter().enumerate() {
                if key_column.is_none() {
                    continue;
                }
                let array = chunk.column(column);
                let identity = Arc::as_ptr(array) as *const () as usize;
                if retained_arrays.insert(identity) {
                    let lease = self.tracker.lease_retained_array(
                        identity,
                        array.get_array_memory_size(),
                        "aggregate retained group keys",
                    );
                    match lease {
                        Ok(lease) => key_leases.push(lease),
                        Err(error) => {
                            rollback_new_groups(
                                index,
                                hashes,
                                groups_store.len(),
                                new_groups,
                                memory,
                                group_memory_before_batch,
                            );
                            return Err(error);
                        }
                    }
                }
            }
            if let Err(error) = self.group_key_batches.try_reserve(1) {
                rollback_new_groups(
                    index,
                    hashes,
                    groups_store.len(),
                    new_groups,
                    memory,
                    group_memory_before_batch,
                );
                return Err(VeinError::internal(format!(
                    "cannot reserve aggregate group-key batches: {error}"
                )));
            }
            if let Err(error) = self.group_key_batch_leases.try_reserve(1) {
                rollback_new_groups(
                    index,
                    hashes,
                    groups_store.len(),
                    new_groups,
                    memory,
                    group_memory_before_batch,
                );
                return Err(VeinError::internal(format!(
                    "cannot reserve aggregate group-key lease batches: {error}"
                )));
            }
            let batch = self.group_key_batches.len();
            self.group_key_batches.push(key_columns.clone());
            self.group_key_batch_leases.push(key_leases);
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
            let slot = Self::push_group(groups_store, aggregate_states, hashes[candidate], set_id, location)?;
            debug_assert_eq!(slot, group_slot_base + new_group_index);
        }
        metrics.new_groups = metrics.new_groups.saturating_add(new_groups.len() as u64);
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
        self.metrics.input_rows = self
            .metrics
            .input_rows
            .saturating_add(input_rows as u64);
        self.metrics.input_batches = self.metrics.input_batches.saturating_add(1);
        Ok(())
    }

    /// Writes the current committed groups to one IPC run and then releases
    /// every group-owned allocation and retained input array.
    fn flush_current_run(&mut self) -> Result<()> {
        if self.groups.is_empty() {
            return Ok(());
        }
        self.tracker.check_cancelled()?;
        self.spill_runs.try_reserve(1).map_err(|error| {
            VeinError::internal(format!("cannot reserve aggregate spill run metadata: {error}"))
        })?;
        let (path, file) = self.tracker.create_spill_file()?;
        let written = self.spill_schema.write_run_with(file, self.groups.len(), |slot| {
            self.intermediate_group_state(slot)
        });
        if let Err(error) = written {
            let _ = std::fs::remove_file(&path);
            return Err(error);
        }
        let group_count = self.groups.len();
        self.spill_runs.push(path);
        self.release_group_state();
        tracing::info!(
            target: "vein.audit",
            event = "aggregate_spill_run_written",
            group_count,
            run_count = self.spill_runs.len(),
            "aggregate groups were externalized"
        );
        Ok(())
    }

    /// Drops group state only after its complete run is durable.
    fn release_group_state(&mut self) {
        self.index = GroupIndex::default();
        self.groups = Vec::new();
        self.group_key_batches = Vec::new();
        self.group_key_batch_leases = Vec::new();
        self.aggregate_states = self
            .plan
            .aggs
            .iter()
            .zip(self.plan.aggregate_input_types.iter())
            .zip(self.plan.types.iter().skip(self.plan.groups as usize))
            .map(|((op, input_type), result_type)| {
                AggregateStateColumn::new(*op, input_type, result_type)
            })
            .collect::<Result<Vec<_>>>()
            .expect("validated aggregate plan can recreate empty state columns");
        for group_ids in &mut self.group_ids_by_set {
            *group_ids = Vec::new();
        }
        self.batch_group_ids = Vec::new();
        self.batch_new_groups = Vec::new();
        let reserved = self.memory.bytes();
        self.memory.shrink(reserved);
    }

    pub(crate) fn metrics(&self) -> AggregateMetrics {
        self.metrics
    }

    /// Collects the finished groups for callers that need an owned result list.
    /// Runtime operators should use `finish_with` so output batches are forwarded
    /// and released one at a time.
    pub fn finish(&mut self) -> Result<Vec<DataChunk>> {
        let mut output = Vec::new();
        self.finish_with(AGGREGATE_BATCH_ROWS, |batch| {
            output.push(batch);
            Ok(())
        })?;
        Ok(output)
    }

    /// Emits bounded output chunks while retaining only the aggregate state.
    /// The callback runs synchronously for each chunk and can stop emission by
    /// returning an error.
    pub fn finish_with(
        &mut self,
        batch_rows: NonZeroUsize,
        mut emit: impl FnMut(DataChunk) -> Result<()>,
    ) -> Result<()> {
        if self.spill_runs.is_empty() {
            return self.finish_in_memory_with(batch_rows, &mut emit);
        }
        self.flush_current_run()?;
        let runs = std::mem::take(&mut self.spill_runs);
        Self::finish_spill_partition(
            &self.plan,
            &self.tracker,
            &self.spill_schema,
            runs,
            0,
            batch_rows,
            &mut emit,
        )
    }

    fn finish_in_memory_with(
        &mut self,
        batch_rows: NonZeroUsize,
        emit: &mut impl FnMut(DataChunk) -> Result<()>,
    ) -> Result<()> {
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
                        hash,
                        set_id,
                        None,
                    )?;
                    self.index.insert_unique(GroupIndexEntry { hash, slot })?;
                    self.metrics.retained_key_slots_initialized = self
                        .metrics
                        .retained_key_slots_initialized
                        .saturating_add(self.plan.groups as u64);
                }
            }
        }
        if self.groups.is_empty() {
            return Ok(());
        }
        let mut start = 0;
        while start < self.groups.len() {
            let end = start.saturating_add(batch_rows.get()).min(self.groups.len());
            let output_materialize_started = Instant::now();
            let range = start..end;
            let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.plan.types.len());
            for (column, data_type) in self.plan.types.iter().enumerate() {
                arrays.push(self.column_range(column, data_type, range.clone())?);
            }
            self.metrics.output_materialize_ns = self
                .metrics
                .output_materialize_ns
                .saturating_add(output_materialize_started.elapsed().as_nanos() as u64);
            emit(DataChunk::new(
                arrays,
                SelectionVector::new_dense(end - start),
            ))?;
            start = end;
        }
        Ok(())
    }

    /// Loads one hash partition into bounded group state. If it exceeds the
    /// tracker budget, the input runs are rescanned into two smaller partitions.
    fn finish_spill_partition(
        plan: &AggregatePlan,
        tracker: &QueryMemoryTracker,
        schema: &AggregateSpillSchema,
        runs: Vec<PathBuf>,
        bit: u32,
        batch_rows: NonZeroUsize,
        emit: &mut impl FnMut(DataChunk) -> Result<()>,
    ) -> Result<()> {
        tracker.check_cancelled()?;
        let mut partition_plan = plan.clone();
        // Planner estimates describe the original full input and must not
        // preallocate a partition's hash table beyond its actual memory budget.
        partition_plan.estimated_groups = None;
        let mut sink = AggregateSink::with_memory_tracker(partition_plan, tracker.clone())?;
        let merge_result = (|| {
            for path in &runs {
                tracker.check_cancelled()?;
                let file = File::open(path).map_err(|error| {
                    VeinError::internal(format!(
                        "open aggregate spill run {}: {error}",
                        path.display()
                    ))
                })?;
                let mut reader = schema.open_reader(file)?;
                while let Some(batch) = reader.next_batch()? {
                    tracker.check_cancelled()?;
                    for group in batch {
                        sink.merge_spilled_group(group)?;
                    }
                }
            }
            Ok(())
        })();

        match merge_result {
            Ok(()) => {
                let finish_result = sink.finish_in_memory_with(batch_rows, emit);
                drop(sink);
                let cleanup_result = runs.iter().try_for_each(|path| {
                    std::fs::remove_file(path).map_err(|error| {
                        VeinError::internal(format!(
                            "remove consumed aggregate spill run {}: {error}",
                            path.display()
                        ))
                    })
                });
                finish_result?;
                cleanup_result
            }
            Err(error @ VeinError::ResourceExhausted { .. }) => {
                drop(sink);
                if bit >= u64::BITS {
                    return Err(error);
                }
                let children = schema.partition_runs(&runs, tracker, bit)?;
                tracing::info!(
                    target: "vein.audit",
                    event = "aggregate_spill_partitioned",
                    hash_bit = bit,
                    child_count = children.iter().flatten().count(),
                    "aggregate spill partition exceeded memory and was repartitioned"
                );
                for child in children.into_iter().flatten() {
                    Self::finish_spill_partition(
                        plan,
                        tracker,
                        schema,
                        vec![child],
                        bit + 1,
                        batch_rows,
                        emit,
                    )?;
                }
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    /// One output column for a contiguous group range.
    fn column_range(
        &self,
        column: usize,
        data_type: &VeinDataType,
        range: Range<usize>,
    ) -> Result<ArrayRef> {
        let groups = self.plan.groups as usize;
        if column < groups {
            let keys = self
                .groups
                .get(range.clone())
                .ok_or_else(|| VeinError::internal("aggregate output range is out of bounds"))?
                .iter()
                .map(|group| {
                    if !self.grouping_sets_by_column[column].contains(&group.grouping_set_id) {
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
        self.aggregate_states[offset].to_array_range(self.plan.aggs[offset], data_type, range)
    }

    /// How many groups were found, for accounting.
    pub fn group_count(&self) -> usize {
        self.groups.len()
    }

    #[allow(dead_code)] // Used to serialize one group without finalizing its accumulators.
    fn intermediate_group_state(&self, slot: usize) -> Result<AggregateIntermediateGroup> {
        let group = self
            .groups
            .get(slot)
            .ok_or_else(|| VeinError::internal("aggregate group slot is out of bounds"))?;
        let aggregate_states = self
            .aggregate_states
            .iter()
            .map(|state| state.intermediate_state(slot))
            .collect::<Result<Vec<_>>>()?;
        let keys = (0..self.plan.groups as usize)
            .map(|column| {
                if !self.grouping_sets_by_column[column].contains(&group.grouping_set_id) {
                    return None;
                }
                let location = group.key_location?;
                let key = self.group_key_batches[location.batch][column]
                    .as_ref()
                    .expect("group source batch retains every grouping column");
                Some((key.array_ref(), location.row))
            })
            .collect();
        Ok(AggregateIntermediateGroup {
            hash: group.hash,
            grouping_set_id: group.grouping_set_id,
            keys,
            aggregate_states,
        })
    }

    /// Merges one decoded spill record without converting its key arrays to
    /// scalars. Array wrappers and backing buffers remain shared by reference.
    fn merge_spilled_group(&mut self, group: AggregateIntermediateGroup) -> Result<()> {
        self.tracker.check_cancelled()?;
        let grouping_set = self
            .grouping_sets
            .get(group.grouping_set_id)
            .ok_or_else(|| VeinError::internal("aggregate spill group has invalid set ID"))?;
        if group.keys.len() != self.plan.groups as usize
            || group.aggregate_states.len() != self.aggregate_states.len()
        {
            return Err(VeinError::internal(
                "aggregate spill group layout does not match its plan",
            ));
        }

        let mut key_columns = std::iter::repeat_with(|| None)
            .take(group.keys.len())
            .collect::<Vec<Option<GroupKeyColumn>>>();
        let mut key_row = None;
        for (column, key) in group.keys.iter().enumerate() {
            match (grouping_set.contains(&(column as u32)), key) {
                (true, Some((array, row))) if *row < array.len() => {
                    key_columns[column] = Some(GroupKeyColumn::new(array)?);
                    key_row.get_or_insert(*row);
                }
                (true, _) => {
                    return Err(VeinError::internal(
                        "aggregate spill record is missing an included key",
                    ));
                }
                (false, None) => {}
                (false, Some(_)) => {
                    return Err(VeinError::internal(
                        "aggregate spill record contains an excluded key",
                    ));
                }
            }
        }

        let matching_slot = self.index.find(group.hash, |entry| {
            let Some(existing) = self.groups.get(entry.slot) else {
                return false;
            };
            existing.grouping_set_id == group.grouping_set_id
                && grouping_set.iter().all(|&column| {
                let column = column as usize;
                let Some(location) = existing.key_location else {
                    return false;
                };
                let Some(existing_key) = self.group_key_batches[location.batch][column].as_ref()
                else {
                    return false;
                };
                key_columns[column]
                    .as_ref()
                    .zip(key_row)
                    .is_some_and(|(key, row)| {
                        key.rows_equal(row, existing_key, location.row)
                    })
            })
        });

        let slot = if let Some(entry) = matching_slot {
            entry.slot
        } else {
            let group_bytes = size_of::<Group>()
                .checked_add(
                    size_of::<Option<GroupKeyLocation>>()
                        .checked_mul(self.plan.groups as usize)
                        .ok_or_else(|| VeinError::internal("aggregate spill key size overflow"))?,
                )
                .and_then(|bytes| {
                    self.aggregate_states
                        .iter()
                        .try_fold(bytes, |total, state| {
                            total.checked_add(state.bytes_per_group())
                        })
                })
                .and_then(|bytes| bytes.checked_add(size_of::<GroupIndexEntry>()))
                .ok_or_else(|| VeinError::internal("aggregate spill group size overflow"))?;
            let before = self.memory.bytes();
            self.memory.grow(group_bytes, "aggregate spill merge state")?;
            if let Err(error) = self.index.reserve(1) {
                self.memory.shrink(self.memory.bytes() - before);
                return Err(error);
            }

            let mut retained_arrays = HashSet::new();
            let mut leases = Vec::new();
            if let Err(error) = leases.try_reserve(key_columns.len()) {
                self.memory.shrink(self.memory.bytes() - before);
                return Err(VeinError::internal(format!(
                    "cannot reserve aggregate spill key leases: {error}"
                )));
            }
            for key in group.keys.iter().flatten() {
                let identity = Arc::as_ptr(&key.0) as *const () as usize;
                if retained_arrays.insert(identity) {
                    match self.tracker.lease_retained_array(
                        identity,
                        key.0.get_array_memory_size(),
                        "aggregate spill merge keys",
                    ) {
                        Ok(lease) => leases.push(lease),
                        Err(error) => {
                            self.memory.shrink(self.memory.bytes() - before);
                            return Err(error);
                        }
                    }
                }
            }
            if let Err(error) = self.group_key_batches.try_reserve(1) {
                self.memory.shrink(self.memory.bytes() - before);
                return Err(VeinError::internal(format!(
                    "cannot reserve aggregate spill key batch: {error}"
                )));
            }
            if let Err(error) = self.group_key_batch_leases.try_reserve(1) {
                self.memory.shrink(self.memory.bytes() - before);
                return Err(VeinError::internal(format!(
                    "cannot reserve aggregate spill key leases: {error}"
                )));
            }

            let location = key_row.map(|row| GroupKeyLocation {
                batch: self.group_key_batches.len(),
                row,
            });
            self.group_key_batches.push(key_columns);
            self.group_key_batch_leases.push(leases);
            let slot = Self::push_group(
                &mut self.groups,
                &mut self.aggregate_states,
                group.hash,
                group.grouping_set_id,
                location,
            )?;
            self.index
                .insert_unique(GroupIndexEntry { hash: group.hash, slot })?;
            slot
        };

        for (state, partial) in self
            .aggregate_states
            .iter_mut()
            .zip(&group.aggregate_states)
        {
            state.merge_intermediate_state(slot, partial)?;
        }
        Ok(())
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
    shared_with_memory_tracker(
        plan,
        QueryMemoryTracker::unlimited(common::cancellation::CancellationToken::new()),
    )
}

/// Builds a budgeted accumulator and the node that feeds it.
pub fn shared_with_memory_tracker(
    plan: AggregatePlan,
    tracker: QueryMemoryTracker,
) -> Result<(Arc<Mutex<AggregateSink>>, Box<dyn PipelineNode<DataChunk>>)> {
    let sink = Arc::new(Mutex::new(AggregateSink::with_memory_tracker(
        plan, tracker,
    )?));
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

    /// Clone the typed Arrow wrapper while sharing its underlying buffers.
    fn array_ref(&self) -> ArrayRef {
        match self {
            Self::Int64(values) => Arc::new(values.clone()),
            Self::UInt8(values) => Arc::new(values.clone()),
            Self::UInt16(values) => Arc::new(values.clone()),
            Self::UInt32(values) => Arc::new(values.clone()),
            Self::UInt64(values) => Arc::new(values.clone()),
            Self::Utf8(values) => Arc::new(values.clone()),
            Self::DictionaryUtf8(keys, _) => Arc::new(keys.clone()),
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

    /// Hash one retained key using the same typed and NULL-aware contract as
    /// `hash_batch`. Spill partitioning uses this after decoding keys from IPC,
    /// where dictionary UTF-8 values have already been normalized to strings.
    #[cfg(test)]
    fn hash_row<H: Hasher>(&self, column: u32, row: usize, hasher: &mut H) {
        column.hash(hasher);
        match self {
            Self::Int64(values) => {
                if values.is_null(row) {
                    Option::<i64>::None.hash(hasher);
                } else {
                    Some(values.value(row)).hash(hasher);
                }
            }
            Self::UInt8(values) => {
                hash_optional_unsigned(values.is_null(row), u64::from(values.value(row)), hasher)
            }
            Self::UInt16(values) => {
                hash_optional_unsigned(values.is_null(row), u64::from(values.value(row)), hasher)
            }
            Self::UInt32(values) => {
                hash_optional_unsigned(values.is_null(row), u64::from(values.value(row)), hasher)
            }
            Self::UInt64(values) => {
                hash_optional_unsigned(values.is_null(row), values.value(row), hasher)
            }
            Self::Utf8(values) => {
                if values.is_null(row) {
                    Option::<&str>::None.hash(hasher);
                } else {
                    Some(values.value(row)).hash(hasher);
                }
            }
            Self::DictionaryUtf8(keys, values) => {
                if keys.keys().is_null(row) {
                    Option::<&str>::None.hash(hasher);
                } else {
                    let value_index = keys.keys().value(row) as usize;
                    if values.is_null(value_index) {
                        Option::<&str>::None.hash(hasher);
                    } else {
                        Some(values.value(value_index)).hash(hasher);
                    }
                }
            }
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

#[cfg(test)]
fn hash_optional_unsigned<H: Hasher>(is_null: bool, value: u64, hasher: &mut H) {
    if is_null {
        Option::<u64>::None.hash(hasher);
    } else {
        Some(value).hash(hasher);
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
        VeinDataType::UInt8 => Arc::new(UInt8Array::from(unsigned_key_values(keys, |value| {
            value as u8
        }))),
        VeinDataType::UInt16 => Arc::new(UInt16Array::from(unsigned_key_values(keys, |value| {
            value as u16
        }))),
        VeinDataType::UInt32 => Arc::new(UInt32Array::from(unsigned_key_values(keys, |value| {
            value as u32
        }))),
        VeinDataType::UInt64 => {
            Arc::new(UInt64Array::from(unsigned_key_values(keys, |value| value)))
        }
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
    keys.iter()
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
    use arrow::datatypes::{Int32Type, i256};
    use common::arrow::SelectionVector;

    use super::*;

    fn assert_spill_key_hash_matches_batch(array: ArrayRef) {
        let key = GroupKeyColumn::new(&array).expect("supported grouping key");
        let selection = SelectionVector::new_dense(array.len());
        let hash_builder = DefaultHashBuilder::default();
        let grouping_set_count = 2;
        let mut batch_hashers = (0..array.len() * grouping_set_count)
            .map(|_| hash_builder.build_hasher())
            .collect::<Vec<_>>();
        for (index, hasher) in batch_hashers.iter_mut().enumerate() {
            (index % grouping_set_count).hash(hasher);
        }
        key.hash_batch(
            0,
            &selection,
            &[0, 1],
            grouping_set_count,
            &mut batch_hashers,
        );

        for row in 0..array.len() {
            for set_id in 0..grouping_set_count {
                let mut spill_hasher = hash_builder.build_hasher();
                set_id.hash(&mut spill_hasher);
                key.hash_row(0, row, &mut spill_hasher);
                assert_eq!(
                    spill_hasher.finish(),
                    batch_hashers[row * grouping_set_count + set_id].finish(),
                    "hash differs at row {row}, grouping set {set_id}"
                );
            }
        }
    }

    #[test]
    fn spill_key_hash_matches_live_hash_for_typed_null_and_dictionary_keys() {
        assert_spill_key_hash_matches_batch(Arc::new(Int64Array::from(vec![
            Some(-7), None, Some(42), Some(-7),
        ])));
        assert_spill_key_hash_matches_batch(Arc::new(UInt32Array::from(vec![
            Some(7), None, Some(u32::MAX), Some(7),
        ])));
        assert_spill_key_hash_matches_batch(Arc::new(StringArray::from(vec![
            Some("alpha"), None, Some("beta"), Some("alpha"),
        ])));

        let dictionary = DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![Some(0), None, Some(1), Some(2)]),
            Arc::new(StringArray::from(vec![Some("alpha"), Some("beta"), None])),
        )
            .expect("valid dictionary keys");
        assert_spill_key_hash_matches_batch(Arc::new(dictionary));
    }

    #[test]
    fn aggregate_finish_streams_ordered_groups_in_bounded_chunks() {
        let plan = AggregatePlan {
            groups: 1,
            estimated_groups: None,
            grouping_sets: Some(vec![vec![0]]),
            emit_empty_group_on_empty_input: false,
            aggs: vec![AggOpCode::Sum],
            aggregate_input_types: vec![VeinDataType::Int64],
            types: vec![VeinDataType::Int64, VeinDataType::Int64],
        };
        let mut sink = AggregateSink::new(plan).expect("create grouped sum");
        sink.accumulate(DataChunk::from_vectors(vec![
            Arc::new(Int64Array::from(vec![Some(1), None, Some(1), Some(3), None])) as ArrayRef,
            Arc::new(Int64Array::from(vec![10, 20, 15, 30, 25])) as ArrayRef,
        ]))
            .expect("accumulate grouped values");

        let mut chunks = Vec::new();
        sink.finish_with(NonZeroUsize::new(2).expect("non-zero batch size"), |chunk| {
            chunks.push(chunk);
            Ok(())
        })
            .expect("stream aggregate output");

        assert_eq!(
            chunks
                .iter()
                .map(|chunk| chunk.selection.size())
                .collect::<Vec<_>>(),
            vec![2, 1],
            "each output chunk must respect the requested group row bound"
        );
        let mut actual = Vec::new();
        for chunk in &chunks {
            let keys = chunk.vectors[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("group key output");
            let sums = chunk.vectors[1]
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("sum output");
            for row in 0..chunk.selection.size() {
                actual.push((
                    (!keys.is_null(row)).then(|| keys.value(row)),
                    sums.value(row),
                ));
            }
        }
        assert_eq!(actual, vec![(Some(1), 25), (None, 45), (Some(3), 30)]);
    }

    #[test]
    fn aggregate_finish_callback_error_stops_emission_without_losing_groups() {
        let plan = AggregatePlan {
            groups: 1,
            estimated_groups: None,
            grouping_sets: Some(vec![vec![0]]),
            emit_empty_group_on_empty_input: false,
            aggs: vec![AggOpCode::Count],
            aggregate_input_types: vec![VeinDataType::Int64],
            types: vec![VeinDataType::Int64, VeinDataType::Int64],
        };
        let mut sink = AggregateSink::new(plan).expect("create grouped count");
        sink.accumulate(DataChunk::from_vectors(vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
            Arc::new(Int64Array::from(vec![10, 20, 30])) as ArrayRef,
        ]))
            .expect("accumulate grouped values");

        let mut emitted = 0;
        let error = sink
            .finish_with(NonZeroUsize::new(1).expect("non-zero batch size"), |_| {
                emitted += 1;
                Err(VeinError::internal("downstream stopped"))
            })
            .expect_err("propagate downstream failure");
        assert!(error.to_string().contains("downstream stopped"));
        assert_eq!(emitted, 1, "stop after the first failed push");

        let output = sink.finish().expect("groups remain available for retry");
        assert_eq!(output.iter().map(|chunk| chunk.selection.size()).sum::<usize>(), 3);
    }

    #[test]
    fn aggregate_finish_streaming_preserves_empty_group_semantics() {
        let grouped_plan = AggregatePlan {
            groups: 1,
            estimated_groups: None,
            grouping_sets: Some(vec![vec![0]]),
            emit_empty_group_on_empty_input: false,
            aggs: vec![AggOpCode::Count],
            aggregate_input_types: vec![VeinDataType::Int64],
            types: vec![VeinDataType::Int64, VeinDataType::Int64],
        };
        let mut grouped = AggregateSink::new(grouped_plan).expect("create grouped count");
        let mut grouped_batches = 0;
        grouped
            .finish_with(AGGREGATE_BATCH_ROWS, |_| {
                grouped_batches += 1;
                Ok(())
            })
            .expect("finish empty grouped aggregate");
        assert_eq!(grouped_batches, 0);

        let global_plan = AggregatePlan {
            groups: 0,
            estimated_groups: None,
            grouping_sets: None,
            emit_empty_group_on_empty_input: true,
            aggs: vec![AggOpCode::Count],
            aggregate_input_types: vec![VeinDataType::Int64],
            types: vec![VeinDataType::Int64],
        };
        let mut global = AggregateSink::new(global_plan).expect("create global count");
        let mut global_batches = Vec::new();
        global
            .finish_with(AGGREGATE_BATCH_ROWS, |batch| {
                global_batches.push(batch);
                Ok(())
            })
            .expect("finish empty global aggregate");
        assert_eq!(global_batches.len(), 1);
        assert_eq!(global_batches[0].selection.size(), 1);
        let count = global_batches[0].vectors[0]
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("global COUNT output");
        assert_eq!(count.value(0), 0);
    }

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
        let input = arrow::compute::cast(&source, &VeinDataType::Float16.to_arrow()).unwrap();
        assert!(input.as_any().is::<Float16Array>());

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
            let value =
                arrow::compute::cast(output[0].column(column), &VeinDataType::Float32.to_arrow())
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
                aggs: vec![
                    AggOpCode::Sum,
                    AggOpCode::Min,
                    AggOpCode::Max,
                    AggOpCode::Avg,
                ],
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
        assert_unsigned_layout!(
            UInt8Array,
            VeinDataType::UInt8,
            VeinDataType::UInt64,
            7,
            2,
            5
        );
        assert_unsigned_layout!(
            UInt16Array,
            VeinDataType::UInt16,
            VeinDataType::UInt64,
            7,
            2,
            5
        );
        assert_unsigned_layout!(
            UInt32Array,
            VeinDataType::UInt32,
            VeinDataType::UInt64,
            7,
            2,
            5
        );
        assert_unsigned_layout!(
            UInt64Array,
            VeinDataType::UInt64,
            VeinDataType::UInt64,
            7,
            2,
            5
        );
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
        assert!(
            overflow
                .to_string()
                .contains("overflow while computing SUM")
        );

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
            .accumulate(DataChunk::from_vectors(vec![Arc::new(UInt64Array::from(
                vec![u64::MAX],
            ))]))
            .unwrap();
        avg_sink
            .accumulate(DataChunk::from_vectors(vec![Arc::new(UInt64Array::from(
                vec![u64::MAX],
            ))]))
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
            aggs: vec![
                AggOpCode::Sum,
                AggOpCode::Min,
                AggOpCode::Max,
                AggOpCode::Avg,
            ],
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
        let int16_values: ArrayRef = Arc::new(Int16Array::from(vec![Some(1_000), None, Some(-10)]));
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
            .accumulate(DataChunk::new(Vec::new(), SelectionVector::new_dense(4)))
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
        let first_key = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let second_key = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let sums = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

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
        assert_eq!(
            actual.len(),
            3,
            "unused dictionary values must not form groups"
        );
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
        assert!(
            output[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .is_null(0)
        );
        assert_eq!(
            output[0]
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            0
        );
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
        assert_eq!(sink.metrics().grouping_probes, 1);

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
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let sums = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

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
        assert!(metrics.group_index_reserve_ns > 0);
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
        let tracker = QueryMemoryTracker::unlimited(common::cancellation::CancellationToken::new());
        let mut sink = AggregateSink::with_memory_tracker(plan, tracker.clone())
            .expect("create aggregate sink");
        let keys = Arc::new(Int64Array::from(vec![10, 20, 30]));
        let source_values = keys.values().as_ptr();
        let source_bytes = keys.get_array_memory_size();
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
        assert!(
            tracker.used() >= source_bytes,
            "the retained group-key source array is charged to the query budget"
        );
        drop(sink);
        assert_eq!(
            tracker.used(),
            0,
            "retained key leases release with the sink"
        );
    }

    #[test]
    fn duplicate_only_batch_does_not_retain_or_charge_its_key_array() {
        let plan = AggregatePlan {
            groups: 1,
            estimated_groups: None,
            grouping_sets: None,
            emit_empty_group_on_empty_input: false,
            aggs: vec![AggOpCode::Count],
            aggregate_input_types: vec![VeinDataType::Int64],
            types: vec![VeinDataType::Int64, VeinDataType::Int64],
        };
        let tracker = QueryMemoryTracker::unlimited(common::cancellation::CancellationToken::new());
        let mut sink = AggregateSink::with_memory_tracker(plan, tracker.clone())
            .expect("create aggregate sink");
        sink.accumulate(DataChunk::from_vectors(vec![
            Arc::new(Int64Array::from(vec![10])),
            Arc::new(Int64Array::from(vec![1])),
        ]))
            .expect("create first group");
        let retained_bytes = tracker.used();
        let retained_batches = sink.group_key_batches.len();

        sink.accumulate(DataChunk::from_vectors(vec![
            Arc::new(Int64Array::from(vec![10, 10, 10])),
            Arc::new(Int64Array::from(vec![1, 1, 1])),
        ]))
            .expect("update an existing group");

        assert_eq!(sink.group_key_batches.len(), retained_batches);
        assert_eq!(tracker.used(), retained_bytes);
        drop(sink);
        assert_eq!(tracker.used(), 0);
    }

    #[test]
    fn aggregate_group_growth_respects_budget_and_releases_on_error() {
        let plan = AggregatePlan {
            groups: 1,
            estimated_groups: None,
            grouping_sets: None,
            emit_empty_group_on_empty_input: false,
            aggs: vec![AggOpCode::Count],
            aggregate_input_types: vec![VeinDataType::Int64],
            types: vec![VeinDataType::Int64, VeinDataType::Int64],
        };
        let tracker = QueryMemoryTracker::new(0, common::cancellation::CancellationToken::new());
        let mut sink = AggregateSink::with_memory_tracker(plan, tracker.clone())
            .expect("empty aggregate state fits zero bytes");
        let error = sink
            .accumulate(DataChunk::from_vectors(vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![1])),
            ]))
            .expect_err("creating a group must reserve memory first");
        assert!(matches!(error, VeinError::ResourceExhausted { .. }));
        drop(sink);
        assert_eq!(
            tracker.used(),
            0,
            "failed aggregate state releases reservations"
        );
    }

    #[test]
    fn failed_key_array_reservation_rolls_back_group_index_and_state() {
        let plan = AggregatePlan {
            groups: 1,
            estimated_groups: None,
            grouping_sets: None,
            emit_empty_group_on_empty_input: false,
            aggs: vec![AggOpCode::Count],
            aggregate_input_types: vec![VeinDataType::Int64],
            types: vec![VeinDataType::Int64, VeinDataType::Int64],
        };
        let key_array = Arc::new(Int64Array::from(vec![10]));
        let key_bytes = key_array.get_array_memory_size();
        let baseline_tracker =
            QueryMemoryTracker::unlimited(common::cancellation::CancellationToken::new());
        let mut baseline =
            AggregateSink::with_memory_tracker(plan.clone(), baseline_tracker.clone())
                .expect("create baseline sink");
        baseline
            .accumulate(DataChunk::from_vectors(vec![
                Arc::clone(&key_array) as ArrayRef,
                Arc::new(Int64Array::from(vec![1])) as ArrayRef,
            ]))
            .expect("create baseline group");
        let group_state_bytes = baseline_tracker.used() - key_bytes;
        drop(baseline);

        let tracker = QueryMemoryTracker::new(
            group_state_bytes + key_bytes - 1,
            common::cancellation::CancellationToken::new(),
        );
        let mut sink = AggregateSink::with_memory_tracker(plan, tracker.clone())
            .expect("create budget-limited sink");
        let error = sink
            .accumulate(DataChunk::from_vectors(vec![
                key_array as ArrayRef,
                Arc::new(Int64Array::from(vec![1])) as ArrayRef,
            ]))
            .expect_err("retaining the key array exceeds the remaining budget");

        assert!(matches!(error, VeinError::ResourceExhausted { .. }));
        assert_eq!(sink.group_count(), 0);
        assert_eq!(
            sink.index.len(),
            0,
            "failed batch leaves no dangling index entry"
        );
        assert!(sink.group_key_batches.is_empty());
        assert_eq!(
            tracker.used(),
            0,
            "failed batch releases its partial reservation"
        );
    }

    #[test]
    fn later_group_budget_failure_rolls_back_earlier_groups_in_the_batch() {
        let plan = AggregatePlan {
            groups: 1,
            estimated_groups: None,
            grouping_sets: None,
            emit_empty_group_on_empty_input: false,
            aggs: vec![AggOpCode::Count],
            aggregate_input_types: vec![VeinDataType::Int64],
            types: vec![VeinDataType::Int64, VeinDataType::Int64],
        };
        let key_array = Arc::new(Int64Array::from(vec![10, 20]));
        let key_bytes = key_array.get_array_memory_size();
        let baseline_tracker =
            QueryMemoryTracker::unlimited(common::cancellation::CancellationToken::new());
        let mut baseline =
            AggregateSink::with_memory_tracker(plan.clone(), baseline_tracker.clone())
                .expect("create baseline sink");
        baseline
            .accumulate(DataChunk::new(
                vec![
                    Arc::clone(&key_array) as ArrayRef,
                    Arc::new(Int64Array::from(vec![1, 1])) as ArrayRef,
                ],
                SelectionVector::new_sparse(vec![0]),
            ))
            .expect("create baseline group");
        let one_group_bytes = baseline_tracker.used() - key_bytes;
        drop(baseline);

        let tracker = QueryMemoryTracker::new(
            one_group_bytes,
            common::cancellation::CancellationToken::new(),
        );
        let mut sink = AggregateSink::with_memory_tracker(plan, tracker.clone())
            .expect("create budget-limited sink");
        let error = sink
            .accumulate(DataChunk::from_vectors(vec![
                key_array,
                Arc::new(Int64Array::from(vec![1, 1])),
            ]))
            .expect_err("second distinct group exceeds the budget");

        assert!(matches!(error, VeinError::ResourceExhausted { .. }));
        assert_eq!(sink.group_count(), 0);
        assert_eq!(
            sink.index.len(),
            0,
            "earlier candidates from the batch are rolled back"
        );
        assert!(sink.group_key_batches.is_empty());
        assert_eq!(
            tracker.used(),
            0,
            "batch-local group state is released on failure"
        );
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
        sink.accumulate(DataChunk::from_vectors(
            vec![Arc::clone(&input) as ArrayRef],
        ))
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
        sink.accumulate(DataChunk::from_vectors(vec![Arc::new(Int64Array::from(
            vec![1],
        ))]))
            .expect("first row is valid");
        let error = sink
            .accumulate(DataChunk::from_vectors(vec![Arc::new(Int64Array::from(
                vec![2],
            ))]))
            .expect_err("second row must violate scalar cardinality");
        assert!(
            error
                .to_string()
                .contains("scalar subquery returned more than one row")
        );
    }

    #[test]
    fn intermediate_numeric_states_round_trip_without_finalizing_averages() {
        macro_rules! round_trip_numeric {
            ($variant:ident, $column:expr, $expected:expr) => {{
                let mut column = $column;
                let partial = column
                    .intermediate_state(0)
                    .expect("snapshot numeric aggregate state");
                column.push_group();
                column
                    .merge_intermediate_state(1, &partial)
                    .expect("restore numeric aggregate state");
                let AggregateStateColumn::$variant { states, .. } = column else {
                    panic!("expected {} state column", stringify!($variant));
                };
                assert_eq!(states[1].value, $expected);
                assert_eq!(states[1].count, states[0].count);
                assert_eq!(states[1].op, states[0].op);
                assert_eq!(states[1].scale, states[0].scale);
            }};
        }

        round_trip_numeric!(
            Int64,
            AggregateStateColumn::Int64 {
                op: AggOpCode::Sum,
                states: vec![NumericAccumulator {
                    op: AggOpCode::Sum,
                    value: Some(31_i64),
                    count: 0,
                    scale: 0,
                    decimal_precision_limit: false,
                }],
            },
            Some(31_i64)
        );
        // This sum cannot be represented exactly as Float64. The intermediate
        // AVG state must preserve it together with its row count.
        round_trip_numeric!(
            Int128Avg,
            AggregateStateColumn::Int128Avg {
                states: vec![NumericAccumulator {
                    op: AggOpCode::Avg,
                    value: Some(9_007_199_254_740_993_i128),
                    count: 2,
                    scale: 0,
                    decimal_precision_limit: false,
                }],
            },
            Some(9_007_199_254_740_993_i128)
        );
        round_trip_numeric!(
            UInt64,
            AggregateStateColumn::UInt64 {
                op: AggOpCode::Max,
                states: vec![NumericAccumulator {
                    op: AggOpCode::Max,
                    value: Some(42_u64),
                    count: 0,
                    scale: 0,
                    decimal_precision_limit: false,
                }],
            },
            Some(42_u64)
        );
        round_trip_numeric!(
            UInt128Avg,
            AggregateStateColumn::UInt128Avg {
                states: vec![NumericAccumulator {
                    op: AggOpCode::Avg,
                    value: Some(42_u128),
                    count: 3,
                    scale: 0,
                    decimal_precision_limit: false,
                }],
            },
            Some(42_u128)
        );
        round_trip_numeric!(
            Float32,
            AggregateStateColumn::Float32 {
                op: AggOpCode::Min,
                states: vec![NumericAccumulator {
                    op: AggOpCode::Min,
                    value: Some(1.25_f32),
                    count: 0,
                    scale: 0,
                    decimal_precision_limit: false,
                }],
            },
            Some(1.25_f32)
        );
        round_trip_numeric!(
            Float64,
            AggregateStateColumn::Float64 {
                op: AggOpCode::Sum,
                states: vec![NumericAccumulator {
                    op: AggOpCode::Sum,
                    value: Some(2.5_f64),
                    count: 0,
                    scale: 0,
                    decimal_precision_limit: false,
                }],
            },
            Some(2.5_f64)
        );
        round_trip_numeric!(
            Decimal128,
            AggregateStateColumn::Decimal128 {
                op: AggOpCode::Sum,
                scale: 2,
                states: vec![NumericAccumulator {
                    op: AggOpCode::Sum,
                    value: Some(1_234_i128),
                    count: 0,
                    scale: 2,
                    decimal_precision_limit: true,
                }],
            },
            Some(1_234_i128)
        );
        round_trip_numeric!(
            Decimal256,
            AggregateStateColumn::Decimal256 {
                op: AggOpCode::Max,
                scale: 3,
                states: vec![NumericAccumulator {
                    op: AggOpCode::Max,
                    value: Some(i256::from(12_i64)),
                    count: 0,
                    scale: 3,
                    decimal_precision_limit: false,
                }],
            },
            Some(i256::from(12_i64))
        );

        let mut count = AggregateStateColumn::Count(vec![7]);
        let partial = count.intermediate_state(0).expect("snapshot COUNT state");
        count.push_group();
        count
            .merge_intermediate_state(1, &partial)
            .expect("restore COUNT state");
        let AggregateStateColumn::Count(counts) = count else {
            panic!("expected COUNT state column");
        };
        assert_eq!(counts, vec![7, 7]);
    }

    #[test]
    fn intermediate_numeric_merge_combines_partials_and_is_atomic_on_error() {
        let sum_source = AggregateStateColumn::Int64 {
            op: AggOpCode::Sum,
            states: vec![NumericAccumulator {
                op: AggOpCode::Sum,
                value: Some(5_i64),
                count: 0,
                scale: 0,
                decimal_precision_limit: false,
            }],
        };
        let sum_partial = sum_source
            .intermediate_state(0)
            .expect("snapshot SUM partial");
        let mut sum_target = AggregateStateColumn::Int64 {
            op: AggOpCode::Sum,
            states: vec![NumericAccumulator {
                op: AggOpCode::Sum,
                value: Some(10_i64),
                count: 0,
                scale: 0,
                decimal_precision_limit: false,
            }],
        };
        sum_target
            .merge_intermediate_state(0, &sum_partial)
            .expect("merge SUM partial");
        let AggregateStateColumn::Int64 { states, .. } = sum_target else {
            panic!("expected Int64 SUM state");
        };
        assert_eq!(states[0].value, Some(15));

        let avg_partial = AggregateIntermediateState::Int128Avg(NumericPartial {
            op: AggOpCode::Avg,
            value: Some(300_i128),
            count: 3,
            scale: 0,
            decimal_precision_limit: false,
        });
        let mut avg_target = AggregateStateColumn::Int128Avg {
            states: vec![NumericAccumulator {
                op: AggOpCode::Avg,
                value: Some(100_i128),
                count: 2,
                scale: 0,
                decimal_precision_limit: false,
            }],
        };
        avg_target
            .merge_intermediate_state(0, &avg_partial)
            .expect("merge AVG sum and count");
        let AggregateStateColumn::Int128Avg { states } = avg_target else {
            panic!("expected Int128 AVG state");
        };
        assert_eq!(states[0].value, Some(400));
        assert_eq!(states[0].count, 5);

        let overflow_partial = AggregateIntermediateState::Int64(NumericPartial {
            op: AggOpCode::Sum,
            value: Some(1_i64),
            count: 0,
            scale: 0,
            decimal_precision_limit: false,
        });
        let mut overflow_target = AggregateStateColumn::Int64 {
            op: AggOpCode::Sum,
            states: vec![NumericAccumulator {
                op: AggOpCode::Sum,
                value: Some(i64::MAX),
                count: 0,
                scale: 0,
                decimal_precision_limit: false,
            }],
        };
        assert!(overflow_target
            .merge_intermediate_state(0, &overflow_partial)
            .is_err());
        let AggregateStateColumn::Int64 { states, .. } = &overflow_target else {
            panic!("expected Int64 SUM state");
        };
        assert_eq!(states[0].value, Some(i64::MAX));

        let incompatible_partial = AggregateIntermediateState::Int64(NumericPartial {
            op: AggOpCode::Min,
            value: Some(3_i64),
            count: 0,
            scale: 0,
            decimal_precision_limit: false,
        });
        assert!(overflow_target
            .merge_intermediate_state(0, &incompatible_partial)
            .is_err());
        let AggregateStateColumn::Int64 { states, .. } = overflow_target else {
            panic!("expected Int64 SUM state");
        };
        assert_eq!(states[0].value, Some(i64::MAX));
    }

    #[test]
    fn intermediate_single_value_preserves_null_row_and_cardinality() {
        let input: ArrayRef = Arc::new(Int64Array::from(vec![None]));
        let mut column = AggregateStateColumn::SingleValue(vec![Some((Arc::clone(&input), 0))]);
        let partial = column
            .intermediate_state(0)
            .expect("snapshot one NULL row");
        column.push_group();
        column
            .merge_intermediate_state(1, &partial)
            .expect("restore one NULL row");

        let AggregateStateColumn::SingleValue(states) = &mut column else {
            panic!("expected SINGLE_VALUE state column");
        };
        let (retained, row) = states[1].as_ref().expect("NULL is still an input row");
        assert!(retained.is_null(*row));
        assert!(Arc::ptr_eq(retained, &input), "Arrow payload remains shared");

        let error = column
            .merge_intermediate_state(1, &partial)
            .expect_err("two NULL rows still violate scalar cardinality");
        assert!(error.to_string().contains("scalar subquery returned more than one row"));
        let AggregateStateColumn::SingleValue(states) = column else {
            panic!("expected SINGLE_VALUE state column");
        };
        assert!(states[1].as_ref().expect("first row remains").0.is_null(0));
    }

    #[test]
    fn intermediate_group_state_preserves_grouping_set_and_null_key_identity() {
        let plan = AggregatePlan {
            groups: 1,
            estimated_groups: None,
            grouping_sets: Some(vec![vec![0], vec![]]),
            emit_empty_group_on_empty_input: false,
            aggs: vec![AggOpCode::Count],
            aggregate_input_types: vec![VeinDataType::Int64],
            types: vec![VeinDataType::Int64, VeinDataType::Int64],
        };
        let mut sink = AggregateSink::new(plan).expect("create grouping-set aggregate");
        let input_keys = Arc::new(Int64Array::from(vec![None, Some(1)]));
        let input_key_values = input_keys.values().as_ptr();
        sink.accumulate(DataChunk::from_vectors(vec![
            input_keys,
            Arc::new(Int64Array::from(vec![10, 20])) as ArrayRef,
        ]))
            .expect("accumulate grouped and NULL keys");

        let states = (0..sink.group_count())
            .map(|slot| sink.intermediate_group_state(slot).expect("snapshot group"))
            .collect::<Vec<_>>();
        let real_null_group = states
            .iter()
            .find(|state| state.grouping_set_id == 0)
            .expect("real NULL key group has its own grouping-set ID");
        let (retained_keys, row) = real_null_group.keys[0]
            .as_ref()
            .expect("group key snapshot retains its Arrow column");
        let retained_keys = retained_keys
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("snapshot preserves the key array type");
        assert!(retained_keys.is_null(*row));
        assert_eq!(
            retained_keys.values().as_ptr(),
            input_key_values,
            "snapshot shares the source Arrow value buffer"
        );
        assert!(states
            .iter()
            .find(|state| state.grouping_set_id == 1)
            .expect("empty grouping set state")
            .keys[0]
            .is_none());
        assert!(
            states.iter().any(|state| state.grouping_set_id == 1),
            "the empty grouping set keeps its identity even when another set retains the source batch"
        );
        assert!(states.iter().all(|state| state.aggregate_states.len() == 1));
        drop(sink);
        assert!(
            retained_keys.is_null(*row),
            "the intermediate group keeps its shared key view after the sink drops"
        );
    }

    #[test]
    fn aggregate_spill_flushes_retries_and_merges_repeated_keys() {
        let plan = AggregatePlan {
            groups: 1,
            estimated_groups: None,
            grouping_sets: None,
            emit_empty_group_on_empty_input: false,
            aggs: vec![AggOpCode::Count],
            aggregate_input_types: vec![VeinDataType::Int64],
            types: vec![VeinDataType::Int64, VeinDataType::Int64],
        };
        let tracker = QueryMemoryTracker::new(
            16 * 1024,
            common::cancellation::CancellationToken::new(),
        );
        let mut sink = AggregateSink::with_memory_tracker(plan, tracker.clone())
            .expect("create budgeted aggregate");
        let input_rows = 1024_i64;
        let keys = (0..input_rows).map(|row| row % 257).collect::<Vec<_>>();
        let values = (0..input_rows).collect::<Vec<_>>();
        sink.accumulate(DataChunk::from_vectors(vec![
            Arc::new(Int64Array::from(keys.clone())) as ArrayRef,
            Arc::new(Int64Array::from(values)) as ArrayRef,
        ]))
            .expect("large input selection is split and retried after spill");
        assert!(
            !sink.spill_runs.is_empty(),
            "the small tracker budget must externalize groups"
        );
        let spill_paths = sink.spill_runs.clone();

        let output = sink.finish().expect("merge spill runs");
        assert!(
            spill_paths.iter().all(|path| !path.exists()),
            "successfully consumed aggregate runs are removed immediately"
        );
        let mut counts = std::collections::BTreeMap::new();
        for batch in output {
            let keys = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let values = batch.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
            for row in 0..batch.num_rows() {
                counts.insert(keys.value(row), values.value(row));
            }
        }
        let expected = keys.into_iter().fold(
            std::collections::BTreeMap::new(),
            |mut counts, key| {
                *counts.entry(key).or_insert(0_i64) += 1;
                counts
            },
        );
        assert_eq!(counts, expected);
        assert!(tracker.peak() <= tracker.limit());
    }

    #[test]
    fn aggregate_spill_preserves_null_keys_and_grouping_set_identity() {
        let plan = AggregatePlan {
            groups: 1,
            estimated_groups: None,
            grouping_sets: Some(vec![vec![0], vec![]]),
            emit_empty_group_on_empty_input: false,
            aggs: vec![AggOpCode::Count],
            aggregate_input_types: vec![VeinDataType::Int64],
            types: vec![VeinDataType::Int64, VeinDataType::Int64],
        };
        let tracker = QueryMemoryTracker::new(
            16 * 1024,
            common::cancellation::CancellationToken::new(),
        );
        let mut sink = AggregateSink::with_memory_tracker(plan, tracker)
            .expect("create budgeted grouping-set aggregate");
        let input_rows = 1024_i64;
        let keys = (0..input_rows)
            .map(|row| (row % 100 != 0).then_some(row % 257))
            .collect::<Vec<_>>();
        sink.accumulate(DataChunk::from_vectors(vec![
            Arc::new(Int64Array::from(keys.clone())) as ArrayRef,
            Arc::new(Int64Array::from_iter_values(0..input_rows)) as ArrayRef,
        ]))
            .expect("spill grouping-set state");
        assert!(
            !sink.spill_runs.is_empty(),
            "the constrained budget must create a spill run"
        );
        let output = sink.finish().expect("merge grouping-set spill runs");
        let mut null_key_counts = Vec::new();
        for batch in output {
            let keys = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let values = batch.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
            for row in 0..batch.num_rows() {
                if keys.is_null(row) {
                    null_key_counts.push(values.value(row));
                }
            }
        }
        null_key_counts.sort_unstable();
        let null_rows = keys.iter().filter(|key| key.is_none()).count() as i64;
        assert_eq!(null_key_counts, [null_rows, input_rows]);
    }
}
