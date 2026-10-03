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
    Array, ArrayRef, Decimal128Array, Float64Array, Int64Array, StringArray,
};
use arrow::datatypes::DataType;

use crate::pipeline::{DataChunk, PipelineNode};
use common::protocol::{AggOpCode, AggregatePlan};
use common::Result;
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

/// Physical type dispatch around the one generic accumulator implementation.
#[derive(Debug, Clone, Copy)]
enum Accumulator {
    Count(i64),
    Uninitialized(AggOpCode),
    Int64(NumericAccumulator<i64>),
    Float64(NumericAccumulator<f64>),
    Decimal128(NumericAccumulator<i128>),
}

impl Accumulator {
    fn new(op: AggOpCode, data_type: &VeinDataType) -> Result<Self> {
        if op == AggOpCode::Count {
            return Ok(Self::Count(0));
        }
        if op == AggOpCode::Avg {
            return Ok(Self::Uninitialized(op));
        }
        Ok(match data_type {
            VeinDataType::Int64 => Self::Int64(NumericAccumulator::new(op, 0)),
            VeinDataType::Float64 => Self::Float64(NumericAccumulator::new(op, 0)),
            VeinDataType::Decimal128(_, scale) => Self::Decimal128(NumericAccumulator::new(op, *scale)),
            other => {
                return Err(VeinError::internal(format!(
                    "aggregate result type {other:?} is not supported yet"
                )))
            }
        })
    }

    fn update(&mut self, array: &ArrayRef, row: usize) -> Result<()> {
        if let Self::Count(count) = self {
            if !array.is_null(row) {
                *count += 1;
            }
            return Ok(());
        }
        if array.is_null(row) {
            return Ok(());
        }
        if let Self::Uninitialized(op) = self {
            *self = match array.data_type() {
                DataType::Int64 => Self::Int64(NumericAccumulator::new(*op, 0)),
                DataType::Float64 => Self::Float64(NumericAccumulator::new(*op, 0)),
                DataType::Decimal128(_, scale) => Self::Decimal128(NumericAccumulator::new(*op, *scale)),
                other => {
                    return Err(VeinError::internal(format!(
                        "aggregating {other:?} is not supported yet"
                    )))
                }
            };
        }
        match self {
            Self::Int64(accumulator) => accumulator.update(Some(
                array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                    VeinError::internal("aggregate input type does not match Int64 result")
                })?.value(row),
            )),
            Self::Float64(accumulator) => accumulator.update(Some(
                array.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                    VeinError::internal("aggregate input type does not match Float64 result")
                })?.value(row),
            )),
            Self::Decimal128(accumulator) => accumulator.update(Some(
                array.as_any().downcast_ref::<Decimal128Array>().ok_or_else(|| {
                    VeinError::internal("aggregate input type does not match Decimal128 result")
                })?.value(row),
            )),
            Self::Count(_) | Self::Uninitialized(_) => unreachable!(),
        }
    }
}

/// One group: its key and every accumulator.
struct Group {
    key: Vec<KeyValue>,
    accumulators: Vec<Accumulator>,
}

/// Absorbs rows into groups, then emits one row per group.
pub struct AggregateSink {
    plan: AggregatePlan,
    /// Encoded key to index in `groups`: encoding each key once makes a group
    /// lookup a hash of bytes rather than a comparison of typed columns.
    index: HashMap<Vec<u8>, usize>,
    groups: Vec<Group>,
}

impl AggregateSink {
    /// An accumulator for `plan`, refusing a shape it cannot emit.
    pub fn new(plan: AggregatePlan) -> Result<Self> {
        let expected = plan.groups as usize + plan.aggs.len();
        if plan.types.len() != expected {
            return Err(VeinError::internal(format!(
                "aggregate plan describes {} output columns but {} types",
                expected,
                plan.types.len()
            )));
        }
        Ok(Self {
            plan,
            index: HashMap::new(),
            groups: Vec::new(),
        })
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
            let slot = match self.index.get(&key) {
                Some(slot) => *slot,
                None => {
                    let slot = self.groups.len();
                    self.index.insert(key.clone(), slot);
                    self.groups.push(Group {
                        key: values,
                        accumulators: self
                            .plan
                            .aggs
                            .iter()
                            .zip(self.plan.types.iter().skip(groups))
                            .map(|(op, data_type)| Accumulator::new(*op, data_type))
                            .collect::<Result<Vec<_>>>()?,
                    });
                    slot
                }
            };

            for (offset, _) in self.plan.aggs.iter().enumerate() {
                self.groups[slot].accumulators[offset]
                    .update(chunk.column(groups + offset), row)?;
            }
        }
        Ok(())
    }

    /// The finished groups, as one chunk in the plan's output types.
    pub fn finish(&mut self) -> Result<Vec<DataChunk>> {
        if self.groups.is_empty() {
            if self.plan.groups != 0 {
                return Ok(Vec::new());
            }
            self.groups.push(Group {
                key: Vec::new(),
                accumulators: self
                    .plan
                    .aggs
                    .iter()
                    .zip(self.plan.types.iter())
                    .map(|(op, data_type)| Accumulator::new(*op, data_type))
                    .collect::<Result<Vec<_>>>()?,
            });
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
        Ok(match self.plan.aggs[offset] {
            AggOpCode::Count => {
                let values: Vec<i64> = self
                    .groups
                    .iter()
                    .map(|group| match group.accumulators[offset] {
                        Accumulator::Count(count) => count,
                        _ => unreachable!("a count column holds a count"),
                    })
                    .collect();
                Arc::new(Int64Array::from(values))
            }
            AggOpCode::Sum | AggOpCode::Min | AggOpCode::Max => match data_type {
                VeinDataType::Int64 => Arc::new(Int64Array::from(
                    self.groups.iter().map(|group| match group.accumulators[offset] {
                        Accumulator::Int64(accumulator) => accumulator.value,
                        _ => unreachable!("an Int64 aggregate holds an Int64 accumulator"),
                    }).collect::<Vec<_>>(),
                )),
                VeinDataType::Float64 => Arc::new(Float64Array::from(
                    self.groups.iter().map(|group| match group.accumulators[offset] {
                        Accumulator::Float64(accumulator) => accumulator.value,
                        _ => unreachable!("a Float64 aggregate holds a Float64 accumulator"),
                    }).collect::<Vec<_>>(),
                )),
                VeinDataType::Decimal128(precision, scale) => {
                    let values = self.groups.iter().map(|group| match group.accumulators[offset] {
                        Accumulator::Decimal128(accumulator) => accumulator.value,
                        _ => unreachable!("a Decimal128 aggregate holds a Decimal128 accumulator"),
                    }).collect::<Vec<_>>();
                    Arc::new(Decimal128Array::from(values)
                        .with_precision_and_scale(*precision, *scale)
                        .map_err(|error| VeinError::internal(format!("invalid decimal aggregate type: {error}")))?)
                }
                other => return Err(VeinError::internal(format!("aggregate output type {other:?} is not supported yet"))),
            },
            AggOpCode::Avg => {
                let values: Vec<Option<f64>> = self.groups.iter().map(|group| match group.accumulators[offset] {
                    Accumulator::Int64(accumulator) => (accumulator.count > 0).then(|| accumulator.avg_sum / accumulator.count as f64),
                    Accumulator::Float64(accumulator) => (accumulator.count > 0).then(|| accumulator.avg_sum / accumulator.count as f64),
                    Accumulator::Decimal128(accumulator) => (accumulator.count > 0).then(|| accumulator.avg_sum / accumulator.count as f64),
                    Accumulator::Uninitialized(_) => None,
                    _ => unreachable!("an average column holds a numeric accumulator"),
                }).collect();
                Arc::new(Float64Array::from(values))
            }
        })
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
pub fn shared(plan: AggregatePlan) -> Result<(Arc<Mutex<AggregateSink>>, Box<dyn PipelineNode<DataChunk>>)> {
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
            )))
        }
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Float64Array, Int64Array};
    use common::arrow::SelectionVector;

    use super::*;

    #[test]
    fn accumulation_reads_only_selected_physical_rows() {
        let plan = AggregatePlan {
            groups: 1,
            aggs: vec![AggOpCode::Sum],
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
        assert_eq!(keys.values(), &[20, 40]);
        assert_eq!(sums.values(), &[200, 400]);
    }

    #[test]
    fn integer_sum_preserves_values_above_f64_precision() {
        let plan = AggregatePlan {
            groups: 0,
            aggs: vec![AggOpCode::Sum, AggOpCode::Min, AggOpCode::Max],
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
        let sums = output[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let minimum = output[0].column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        let maximum = output[0].column(2).as_any().downcast_ref::<Int64Array>().unwrap();

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
            types: vec![VeinDataType::Decimal128(38, 2)],
        };
        let mut sink = AggregateSink::new(plan).unwrap();

        sink.accumulate(DataChunk::from_vectors(vec![Arc::new(input)])).unwrap();
        let output = sink.finish().unwrap();
        let sum = output[0].column(0).as_any().downcast_ref::<Decimal128Array>().unwrap();

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
    fn floating_aggregates_share_the_generic_fold() {
        let values: ArrayRef = Arc::new(Float64Array::from(vec![Some(1.5), None, Some(2.5)]));
        let plan = AggregatePlan {
            groups: 0,
            aggs: vec![AggOpCode::Sum, AggOpCode::Min, AggOpCode::Max, AggOpCode::Avg],
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
            types: vec![VeinDataType::Int64],
        };
        let input = DataChunk::from_vectors(vec![Arc::new(Int64Array::from(vec![
            i64::MAX,
            1,
        ]))]);
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

        assert_eq!(row.column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0), 2);
        assert_eq!(row.column(1).as_any().downcast_ref::<Int64Array>().unwrap().value(0), 8);
        assert_eq!(row.column(2).as_any().downcast_ref::<Int64Array>().unwrap().value(0), 3);
        assert_eq!(row.column(3).as_any().downcast_ref::<Int64Array>().unwrap().value(0), 5);
        assert_eq!(row.column(4).as_any().downcast_ref::<Float64Array>().unwrap().value(0), 4.0);
    }

    #[test]
    fn empty_global_aggregate_emits_count_zero_and_null_sum() {
        let plan = AggregatePlan {
            groups: 0,
            aggs: vec![AggOpCode::Count, AggOpCode::Sum],
            types: vec![VeinDataType::Int64, VeinDataType::Float64],
        };
        let chunk = DataChunk::from_vectors(vec![
            Arc::new(Int64Array::from(Vec::<i64>::new())),
            Arc::new(Int64Array::from(Vec::<i64>::new())),
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
}
