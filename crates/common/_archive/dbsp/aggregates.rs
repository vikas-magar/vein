#![allow(missing_docs)]

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use error::{Result, VeinError};

use super::chunk::Chunk;
use super::r#operator::{DbspOperator, OperatorMeta, UnaryOperator};
use crate::identifiers::OperatorId;

/// Supported aggregate functions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFunction {
    Count,
    Sum,
    Min,
    Max,
    Avg,
}

impl AggregateFunction {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Count => "count",
            Self::Sum => "sum",
            Self::Min => "min",
            Self::Max => "max",
            Self::Avg => "avg",
        }
    }

    pub fn output_type(&self) -> DataType {
        match self {
            Self::Count | Self::Sum | Self::Min | Self::Max => DataType::Int64,
            Self::Avg => DataType::Float64,
        }
    }
}

// ── Enum-based accumulator ──

#[derive(Debug, Clone)]
enum AccumState {
    Count { count: i64 },
    Sum { sum: i64 },
    Min { multiset: BTreeMap<i64, i64> },
    Max { multiset: BTreeMap<i64, i64> },
    Avg { sum: i64, count: i64 },
}

impl AccumState {
    fn new(func: AggregateFunction) -> Self {
        match func {
            AggregateFunction::Count => Self::Count { count: 0 },
            AggregateFunction::Sum => Self::Sum { sum: 0 },
            AggregateFunction::Min => Self::Min { multiset: BTreeMap::new() },
            AggregateFunction::Max => Self::Max { multiset: BTreeMap::new() },
            AggregateFunction::Avg => Self::Avg { sum: 0, count: 0 },
        }
    }

    fn add(&mut self, value: i64, weight: i64) {
        match self {
            Self::Count { count } => *count += weight,
            Self::Sum { sum } => *sum += value * weight,
            Self::Min { multiset } | Self::Max { multiset } => {
                *multiset.entry(value).or_insert(0) += weight;
                if *multiset.get(&value).unwrap() == 0 {
                    multiset.remove(&value);
                }
            }
            Self::Avg { sum, count } => {
                *sum += value * weight;
                *count += weight;
            }
        }
    }

    fn result_int(&self) -> i64 {
        match self {
            Self::Count { count } => *count,
            Self::Sum { sum } => *sum,
            Self::Min { multiset } => multiset.keys().next().copied().unwrap_or(0),
            Self::Max { multiset } => multiset.keys().last().copied().unwrap_or(0),
            Self::Avg { .. } => 0,
        }
    }

    fn result_float(&self) -> f64 {
        match self {
            Self::Avg { sum, count } => {
                if *count == 0 { 0.0 } else { *sum as f64 / *count as f64 }
            }
            _ => self.result_int() as f64,
        }
    }

    fn is_zero(&self) -> bool {
        match self {
            Self::Count { count } => *count == 0,
            Self::Sum { sum } => *sum == 0,
            Self::Min { multiset } | Self::Max { multiset } => multiset.is_empty(),
            Self::Avg { sum, count } => *sum == 0 && *count == 0,
        }
    }
}

// ── AggregateOperator ──

pub struct AggregateOperator {
    id: OperatorId,
    key_indices: Vec<usize>,
    groups: BTreeMap<Vec<i64>, Vec<AccumState>>,
    functions: Vec<AggregateFunction>,
    output_schema: SchemaRef,
    meta: OperatorMeta,
}

impl AggregateOperator {
    pub fn new(
        id: OperatorId,
        key_indices: Vec<usize>,
        functions: Vec<AggregateFunction>,
        input_schema: SchemaRef,
    ) -> Self {
        let key_fields: Vec<Field> = key_indices
            .iter()
            .map(|&i| input_schema.field(i).as_ref().clone())
            .collect();
        let mut out_fields = key_fields;
        for func in &functions {
            out_fields.push(Field::new(func.name(), func.output_type(), true));
        }
        out_fields.push(Field::new("__weight", DataType::Int64, false));
        let output_schema = Arc::new(Schema::new(out_fields));

        Self {
            id,
            key_indices,
            groups: BTreeMap::new(),
            functions,
            output_schema,
            meta: OperatorMeta::default(),
        }
    }

    fn row_key(&self, batch: &RecordBatch, row: usize) -> Vec<i64> {
        self.key_indices
            .iter()
            .map(|&i| {
                batch
                    .column(i)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .map(|a| a.value(row))
                    .unwrap_or(0)
            })
            .collect()
    }

    fn value_col_idx(&self) -> usize {
        for i in 0..self.key_indices.len() + 1 {
            if !self.key_indices.contains(&i) {
                return i;
            }
        }
        self.key_indices.len()
    }

    fn snapshot_groups(&self) -> Result<Chunk> {
        if self.groups.is_empty() {
            return Ok(Chunk::empty(self.output_schema.clone()));
        }

        let n = self.groups.len();
        let mut key_cols: Vec<Vec<i64>> =
            (0..self.key_indices.len()).map(|_| Vec::with_capacity(n)).collect();
        let mut int_cols: Vec<Vec<i64>> = Vec::new();
        let mut float_cols: Vec<Vec<f64>> = Vec::new();
        for (i, func) in self.functions.iter().enumerate() {
            if *func == AggregateFunction::Avg {
                float_cols.push(Vec::with_capacity(n));
                int_cols.push(Vec::new());
            } else {
                int_cols.push(Vec::with_capacity(n));
                float_cols.push(Vec::new());
            }
        }
        let mut weights: Vec<i64> = Vec::with_capacity(n);

        for (key, accs) in &self.groups {
            for (i, k) in key.iter().enumerate() {
                key_cols[i].push(*k);
            }
            let mut int_idx = 0;
            let mut float_idx = 0;
            for (i, acc) in accs.iter().enumerate() {
                if self.functions[i] == AggregateFunction::Avg {
                    float_cols[float_idx].push(acc.result_float());
                    float_idx += 1;
                } else {
                    int_cols[int_idx].push(acc.result_int());
                    int_idx += 1;
                }
            }
            weights.push(1);
        }

        let total_data = self.key_indices.len() + self.functions.len();
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(total_data + 1);

        for kc in key_cols {
            columns.push(Arc::new(Int64Array::from(kc)));
        }
        let mut int_idx = 0;
        let mut float_idx = 0;
        for func in &self.functions {
            if *func == AggregateFunction::Avg {
                columns.push(Arc::new(Float64Array::from(
                    std::mem::take(&mut float_cols[float_idx]),
                )));
                float_idx += 1;
            } else {
                columns.push(Arc::new(Int64Array::from(
                    std::mem::take(&mut int_cols[int_idx]),
                )));
                int_idx += 1;
            }
        }
        columns.push(Arc::new(Int64Array::from(weights)));

        let batch = RecordBatch::try_new(self.output_schema.clone(), columns)
            .map_err(|e| VeinError::Arrow(e))?;
        Chunk::new(batch)
    }

    fn apply_delta(&mut self, delta: &Chunk) -> Result<()> {
        let batch = delta.as_batch();
        let w = delta.weights();
        let vcol = self.value_col_idx();

        for row in 0..batch.num_rows() {
            let weight = w.value(row);
            if weight == 0 {
                continue;
            }
            let key = self.row_key(batch, row);
            let value = batch
                .column(vcol)
                .as_any()
                .downcast_ref::<Int64Array>()
                .map(|a| a.value(row))
                .unwrap_or(0);

            let accs = self.groups.entry(key).or_insert_with(|| {
                self.functions.iter().map(|&f| AccumState::new(f)).collect()
            });
            for acc in accs.iter_mut() {
                acc.add(value, weight);
            }

            let all_zero = accs.iter().all(|a| a.is_zero());
            if all_zero {
                let weak_key = self.row_key(batch, row);
                self.groups.remove(&weak_key);
            }
        }
        Ok(())
    }
}

#[async_trait]
impl DbspOperator for AggregateOperator {
    fn name(&self) -> &'static str { "AggregateOperator" }
    fn operator_id(&self) -> OperatorId { self.id }
    async fn init(&mut self) -> Result<()> { Ok(()) }
    fn metadata(&self) -> OperatorMeta { self.meta.clone() }
}

#[async_trait]
impl UnaryOperator for AggregateOperator {
    async fn eval(&mut self, input: &Chunk) -> Result<Chunk> {
        let old_snapshot = self.snapshot_groups()?;
        self.apply_delta(input)?;
        let new_snapshot = self.snapshot_groups()?;

        if old_snapshot.is_empty() {
            return Ok(new_snapshot);
        }
        new_snapshot.minus(&old_snapshot)
    }
}

#[cfg(test)]
#[allow(missing_docs)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]))
    }

    fn make_chunk(keys: &[i64], vals: &[i64], weight: i64) -> Chunk {
        let s = schema();
        let kc = Int64Array::from(keys.to_vec());
        let vc = Int64Array::from(vals.to_vec());
        let batch = RecordBatch::try_new(s, vec![Arc::new(kc), Arc::new(vc)]).unwrap();
        Chunk::from_records(batch, weight)
    }

    #[tokio::test]
    async fn test_aggregate_count() {
        let mut op = AggregateOperator::new(
            OperatorId::from_raw(1),
            vec![0],
            vec![AggregateFunction::Count],
            schema(),
        );
        let input = make_chunk(&[1, 2, 1], &[10, 20, 30], 1);
        let r = op.eval(&input).await.unwrap();
        assert_eq!(r.len(), 2);
    }

    #[tokio::test]
    async fn test_aggregate_sum() {
        let mut op = AggregateOperator::new(
            OperatorId::from_raw(1),
            vec![0],
            vec![AggregateFunction::Sum],
            schema(),
        );
        let input = make_chunk(&[1, 2, 1], &[10, 20, 30], 1);
        let r = op.eval(&input).await.unwrap();
        assert_eq!(r.len(), 2);
        assert_eq!(r.weighted_count(), 2);
    }

    #[tokio::test]
    async fn test_aggregate_avg() {
        let mut op = AggregateOperator::new(
            OperatorId::from_raw(1),
            vec![0],
            vec![AggregateFunction::Avg],
            schema(),
        );
        let input = make_chunk(&[1, 1], &[10, 30], 1);
        let r = op.eval(&input).await.unwrap();
        assert_eq!(r.len(), 1);
    }

    #[tokio::test]
    async fn test_aggregate_min_max() {
        let mut op = AggregateOperator::new(
            OperatorId::from_raw(1),
            vec![0],
            vec![AggregateFunction::Min, AggregateFunction::Max],
            schema(),
        );
        let input = make_chunk(&[1, 1, 1], &[10, 30, 5], 1);
        let r = op.eval(&input).await.unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r.weighted_count(), 1);
    }

    #[tokio::test]
    async fn test_aggregate_incremental() {
        let mut op = AggregateOperator::new(
            OperatorId::from_raw(1),
            vec![0],
            vec![AggregateFunction::Sum],
            schema(),
        );

        let input = make_chunk(&[1, 2], &[10, 20], 1);
        let r1 = op.eval(&input).await.unwrap();
        assert_eq!(r1.weighted_count(), 2);

        let input2 = make_chunk(&[1], &[10], -1);
        let r2 = op.eval(&input2).await.unwrap();
        // Delta: k=1 sum changed from 10 to 0 (removed)
        assert_eq!(r2.len(), 1);
        assert_eq!(r2.weighted_count(), -1);
    }
}
