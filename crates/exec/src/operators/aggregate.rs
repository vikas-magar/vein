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
    Array, ArrayRef, Decimal128Array, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, StringArray,
};

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
    Utf8(String),
}

/// Operations shared by every numeric accumulator type.
trait AggregateValue: Copy + PartialOrd {
    fn checked_add(self, other: Self) -> Option<Self>;
    fn minimum(self, other: Self) -> Self;
    fn maximum(self, other: Self) -> Self;
    fn to_f64(self, scale: i8) -> f64;
}

impl AggregateValue for i64 {
    fn checked_add(self, other: Self) -> Option<Self> {
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
    fn checked_add(self, other: Self) -> Option<Self> {
        i128::checked_add(self, other).filter(|sum| {
            let max_decimal_38 = 10_i128.pow(38) - 1;
            (-max_decimal_38..=max_decimal_38).contains(sum)
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

impl AggregateValue for f64 {
    fn checked_add(self, other: Self) -> Option<Self> {
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
    fn checked_add(self, other: Self) -> Option<Self> {
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
    avg_sum: f64,
    count: i64,
    scale: i8,
}

impl<T: AggregateValue> NumericAccumulator<T> {
    fn new(op: AggOpCode, scale: i8) -> Self {
        Self {
            op,
            value: None,
            avg_sum: 0.0,
            count: 0,
            scale,
        }
    }

    fn update(&mut self, value: Option<T>) -> Result<()> {
        let Some(value) = value else {
            return Ok(());
        };
        match self.op {
            AggOpCode::Count => self.count += 1,
            AggOpCode::Sum => {
                self.value = Some(match self.value {
                    Some(current) => current.checked_add(value).ok_or_else(|| {
                        VeinError::internal("numeric overflow while computing SUM")
                    })?,
                    None => value,
                });
            }
            AggOpCode::Min => {
                self.value = Some(self.value.map_or(value, |current| current.minimum(value)));
            }
            AggOpCode::Max => {
                self.value = Some(self.value.map_or(value, |current| current.maximum(value)));
            }
            AggOpCode::Avg => {
                self.avg_sum += value.to_f64(self.scale);
                self.count += 1;
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
    Float32(&'a Float32Array),
    Float64(&'a Float64Array),
    Decimal128(&'a Decimal128Array),
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

fn average_values<T>(states: &[NumericAccumulator<T>]) -> Vec<Option<f64>> {
    states
        .iter()
        .map(|state| (state.count > 0).then(|| state.avg_sum / state.count as f64))
        .collect()
}

/// A typed state vector selected once when the aggregate sink is created.
enum AggregateStateColumn {
    Count(Vec<i64>),
    Int64 {
        op: AggOpCode,
        states: Vec<NumericAccumulator<i64>>,
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
        // Signed integer operations share an Int64 state, while AVG follows
        // its input layout and other operations follow their bound result.
        if matches!(
            input_type,
            VeinDataType::Int8
                | VeinDataType::Int16
                | VeinDataType::Int32
                | VeinDataType::Int64
        ) {
            return Ok(Self::Int64 {
                op,
                states: Vec::new(),
            });
        }
        let state_type = if op == AggOpCode::Avg {
            input_type
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
            Self::Int64 { op, states } => states.push(NumericAccumulator::new(*op, 0)),
            Self::Float32 { op, states } => states.push(NumericAccumulator::new(*op, 0)),
            Self::Float64 { op, states } => states.push(NumericAccumulator::new(*op, 0)),
            Self::Decimal128 { op, scale, states } => {
                states.push(NumericAccumulator::new(*op, *scale))
            }
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
            (Self::Float32 { states, .. }, AggOpCode::Sum | AggOpCode::Min | AggOpCode::Max, VeinDataType::Float32) => {
                Arc::new(Float32Array::from(numeric_values(states)))
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
            (Self::Int64 { states, .. }, AggOpCode::Avg, VeinDataType::Float64) => {
                Arc::new(Float64Array::from(average_values(states)))
            }
            (Self::Float32 { states, .. }, AggOpCode::Avg, VeinDataType::Float64) => {
                Arc::new(Float64Array::from(average_values(states)))
            }
            (Self::Float64 { states, .. }, AggOpCode::Avg, VeinDataType::Float64) => {
                Arc::new(Float64Array::from(average_values(states)))
            }
            (Self::Decimal128 { states, .. }, AggOpCode::Avg, VeinDataType::Float64) => {
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
            index: HashMap::new(),
            groups: Vec::new(),
            aggregate_states,
        })
    }

    /// Adds a group slot and its corresponding state to every aggregate.
    fn push_group(&mut self, key: Vec<KeyValue>) -> Result<usize> {
        let slot = self.groups.len();
        self.groups.push(Group { key });
        for column in &mut self.aggregate_states {
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
        let mut group_rows = Vec::with_capacity(chunk.selection.size());
        for logical_row in 0..chunk.selection.size() {
            let row = chunk.selection.get_index(logical_row);
            key.clear();
            let mut values = Vec::with_capacity(groups);
            for column in 0..groups {
                let value = key_value(chunk.column(column), row)?;
                encode(&value, &mut key);
                values.push(value);
            }

            // A new key takes a fresh slot; an existing one folds into its own.
            let existing_slot = self.index.get(&key).copied();
            let slot = match existing_slot {
                Some(slot) => slot,
                None => {
                    let slot = self.push_group(values)?;
                    self.index.insert(key.clone(), slot);
                    slot
                }
            };

            group_rows.push((slot, row));
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
            if self.plan.groups != 0 {
                return Ok(Vec::new());
            }
            self.push_group(Vec::new())?;
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
                    KeyValue::Utf8(_) => unreachable!("an int64 key column holds int64 keys"),
                })
                .collect();
            Arc::new(Int64Array::from(values))
        }
        VeinDataType::Utf8 => {
            let values: Vec<Option<&str>> = groups
                .iter()
                .map(|group| match &group.key[column] {
                    KeyValue::Utf8(value) => Some(value.as_str()),
                    KeyValue::Null => None,
                    KeyValue::Int64(_) => unreachable!("a string key column holds string keys"),
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Float64Array, Int32Array, Int64Array};
    use common::arrow::SelectionVector;

    use super::*;

    #[test]
    fn accumulation_reads_only_selected_physical_rows() {
        let plan = AggregatePlan {
            groups: 1,
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
    fn float32_sum_reads_input_layout_and_emits_widened_result() {
        let plan = AggregatePlan {
            groups: 0,
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
}
