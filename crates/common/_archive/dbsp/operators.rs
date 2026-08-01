#![allow(missing_docs)]

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, Int64Array};
use arrow::compute::filter_record_batch;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use error::{Result, VeinError};

use super::chunk::Chunk;
use super::r#operator::{
    BinaryOperator, DbspOperator, OperatorMeta, StrictOperator, UnaryOperator,
};
use crate::identifiers::OperatorId;

// ── FilterOperator ──

pub struct FilterOperator {
    id: OperatorId,
    predicate: Arc<dyn Fn(&RecordBatch) -> Result<BooleanArray> + Send + Sync>,
    meta: OperatorMeta,
}

impl FilterOperator {
    pub fn new(
        id: OperatorId,
        predicate: impl Fn(&RecordBatch) -> Result<BooleanArray> + Send + Sync + 'static,
    ) -> Self {
        Self {
            id,
            predicate: Arc::new(predicate),
            meta: OperatorMeta::default(),
        }
    }
}

#[async_trait]
impl DbspOperator for FilterOperator {
    fn name(&self) -> &'static str {
        "FilterOperator"
    }
    fn operator_id(&self) -> OperatorId { self.id }
    async fn init(&mut self) -> Result<()> { Ok(()) }
    fn metadata(&self) -> OperatorMeta { self.meta.clone() }
}

#[async_trait]
impl UnaryOperator for FilterOperator {
    async fn eval(&mut self, input: &Chunk) -> Result<Chunk> {
        let mask = (self.predicate)(input.as_batch())?;
        let filtered = filter_record_batch(input.as_batch(), &mask)
            .map_err(|e| VeinError::Arrow(e))?;
        Chunk::new(filtered)
    }
}

// ── ProjectOperator ──

pub struct ProjectOperator {
    id: OperatorId,
    columns: Vec<usize>,
    meta: OperatorMeta,
}

impl ProjectOperator {
    pub fn new(id: OperatorId, columns: Vec<usize>) -> Self {
        Self { id, columns, meta: OperatorMeta::default() }
    }
}

#[async_trait]
impl DbspOperator for ProjectOperator {
    fn name(&self) -> &'static str { "ProjectOperator" }
    fn operator_id(&self) -> OperatorId { self.id }
    async fn init(&mut self) -> Result<()> { Ok(()) }
    fn metadata(&self) -> OperatorMeta { self.meta.clone() }
}

#[async_trait]
impl UnaryOperator for ProjectOperator {
    async fn eval(&mut self, input: &Chunk) -> Result<Chunk> {
        let batch = input.as_batch();
        let weight_idx = batch.schema().index_of("__weight").expect("Chunk has __weight");
        let mut indices = self.columns.clone();
        if !indices.contains(&weight_idx) {
            indices.push(weight_idx);
        }
        let cols: Vec<ArrayRef> = indices.iter().map(|&i| batch.column(i).clone()).collect();
        let fields: Vec<Field> = indices.iter()
            .map(|&i| batch.schema().field(i).as_ref().clone()).collect();
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema, cols).map_err(|e| VeinError::Arrow(e))?;
        Chunk::new(batch)
    }
}

// ── MapOperator ──

pub struct MapOperator {
    id: OperatorId,
    func: Arc<dyn Fn(&RecordBatch) -> Result<RecordBatch> + Send + Sync>,
    meta: OperatorMeta,
}

impl MapOperator {
    pub fn new(
        id: OperatorId,
        func: impl Fn(&RecordBatch) -> Result<RecordBatch> + Send + Sync + 'static,
    ) -> Self {
        Self { id, func: Arc::new(func), meta: OperatorMeta::default() }
    }
}

#[async_trait]
impl DbspOperator for MapOperator {
    fn name(&self) -> &'static str { "MapOperator" }
    fn operator_id(&self) -> OperatorId { self.id }
    async fn init(&mut self) -> Result<()> { Ok(()) }
    fn metadata(&self) -> OperatorMeta { self.meta.clone() }
}

#[async_trait]
impl UnaryOperator for MapOperator {
    async fn eval(&mut self, input: &Chunk) -> Result<Chunk> {
        let data = input.strip_weights();
        let result = (self.func)(&data)?;
        let weights = input.weights();
        let mut cols: Vec<ArrayRef> = result.columns().to_vec();
        cols.push(Arc::new(weights.clone()));
        let mut fields: Vec<Field> = result.schema().fields().iter().map(|f| f.as_ref().clone()).collect();
        fields.push(Field::new("__weight", DataType::Int64, false));
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema, cols).map_err(|e| VeinError::Arrow(e))?;
        Chunk::new(batch)
    }
}

// ── NegateOperator ──

pub struct NegateOperator {
    id: OperatorId,
    meta: OperatorMeta,
}

impl NegateOperator {
    pub fn new(id: OperatorId) -> Self {
        Self { id, meta: OperatorMeta::default() }
    }
}

#[async_trait]
impl DbspOperator for NegateOperator {
    fn name(&self) -> &'static str { "NegateOperator" }
    fn operator_id(&self) -> OperatorId { self.id }
    async fn init(&mut self) -> Result<()> { Ok(()) }
    fn metadata(&self) -> OperatorMeta { self.meta.clone() }
}

#[async_trait]
impl UnaryOperator for NegateOperator {
    async fn eval(&mut self, input: &Chunk) -> Result<Chunk> {
        Ok(input.negate())
    }
}

// ── Z1Operator (z⁻¹ delay) ──

pub struct Z1Operator {
    id: OperatorId,
    prev: Option<Chunk>,
    meta: OperatorMeta,
}

impl Z1Operator {
    pub fn new(id: OperatorId) -> Self {
        Self { id, prev: None, meta: OperatorMeta::default() }
    }
    pub fn set_current(&mut self, chunk: Chunk) {
        self.prev = Some(chunk);
    }
}

#[async_trait]
impl DbspOperator for Z1Operator {
    fn name(&self) -> &'static str { "Z1Operator" }
    fn operator_id(&self) -> OperatorId { self.id }
    async fn init(&mut self) -> Result<()> { Ok(()) }
    fn metadata(&self) -> OperatorMeta { self.meta.clone() }
}

#[async_trait]
impl StrictOperator for Z1Operator {
    async fn eval_strict(&mut self) -> Result<Chunk> {
        match self.prev.take() {
            Some(chunk) => Ok(chunk),
            None => Ok(Chunk::empty(SchemaRef::new(Schema::new(Vec::<Field>::new())))),
        }
    }
}

// ── DistinctOperator ──

pub struct DistinctOperator {
    id: OperatorId,
    meta: OperatorMeta,
}

impl DistinctOperator {
    pub fn new(id: OperatorId) -> Self {
        Self { id, meta: OperatorMeta::default() }
    }
}

#[async_trait]
impl DbspOperator for DistinctOperator {
    fn name(&self) -> &'static str { "DistinctOperator" }
    fn operator_id(&self) -> OperatorId { self.id }
    async fn init(&mut self) -> Result<()> { Ok(()) }
    fn metadata(&self) -> OperatorMeta { self.meta.clone() }
}

#[async_trait]
impl UnaryOperator for DistinctOperator {
    async fn eval(&mut self, input: &Chunk) -> Result<Chunk> {
        Ok(input.group_by())
    }
}

// ── Helper: hash a row across column arrays ──

fn row_hash(cols: &[ArrayRef], row: usize, num_cols: usize) -> u64 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    for c in 0..num_cols {
        hash_value(cols[c].as_ref(), row, &mut h);
    }
    h.finish()
}

fn hash_value(arr: &dyn Array, row: usize, h: &mut impl Hasher) {
    use arrow::array::*;
    if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        a.value(row).hash(h);
    } else if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
        a.value(row).to_bits().hash(h);
    } else if let Some(a) = arr.as_any().downcast_ref::<BooleanArray>() {
        a.value(row).hash(h);
    } else if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
        a.value(row).hash(h);
    }
}

fn rows_equal(
    l_cols: &[ArrayRef], l_row: usize,
    r_cols: &[ArrayRef], r_row: usize,
    num_cols: usize,
) -> bool {
    use arrow::array::*;
    for c in 0..num_cols {
        let l = l_cols[c].as_ref();
        let r = r_cols[c].as_ref();
        let eq = match (l.as_any().downcast_ref::<Int64Array>(), r.as_any().downcast_ref::<Int64Array>()) {
            (Some(a), Some(b)) => a.value(l_row) == b.value(r_row),
            _ => {
                // fallback: compare string via Debug
                format!("{:?}", l) == format!("{:?}", r)
            }
        };
        if !eq {
            return false;
        }
    }
    true
}

// ── HashJoinOperator ──

pub struct HashJoinOperator {
    id: OperatorId,
    left_state: super::arrow_hash_index::ArrowHashIndex,
    right_state: super::arrow_hash_index::ArrowHashIndex,
    output_schema: SchemaRef,
    meta: OperatorMeta,
}

impl HashJoinOperator {
    pub fn new(
        id: OperatorId,
        left_schema: SchemaRef,
        right_schema: SchemaRef,
    ) -> Self {
        let lf: Vec<Field> = left_schema.fields().iter().map(|f| f.as_ref().clone()).collect();
        let rf: Vec<Field> = right_schema.fields().iter().map(|f| f.as_ref().clone()).collect();
        let mut out = lf;
        out.extend(rf);
        out.push(Field::new("__weight", DataType::Int64, false));
        let output_schema = Arc::new(Schema::new(out));

        Self {
            id,
            left_state: super::arrow_hash_index::ArrowHashIndex::new(left_schema),
            right_state: super::arrow_hash_index::ArrowHashIndex::new(right_schema),
            output_schema,
            meta: OperatorMeta::default(),
        }
    }

    fn build_join_rows(
        l_batch: &RecordBatch, l_cols: usize,
        r_batch: &RecordBatch, r_cols: usize,
        out_schema: SchemaRef,
        pairs: &[(u32, u32, i64)],
    ) -> Result<Chunk> {
        if pairs.is_empty() {
            return Ok(Chunk::empty(out_schema));
        }
        let n = pairs.len();
        let mut weights = Vec::with_capacity(n);
        let mut l_indices = Vec::with_capacity(n);
        let mut r_indices = Vec::with_capacity(n);
        for (li, ri, w) in pairs {
            weights.push(*w);
            l_indices.push(*li);
            r_indices.push(*ri);
        }

        let l_idx_arr = arrow::array::UInt32Array::from(l_indices);
        let r_idx_arr = arrow::array::UInt32Array::from(r_indices);

        let mut out_cols: Vec<ArrayRef> = Vec::with_capacity(l_cols + r_cols + 1);
        for c in 0..l_cols {
            let taken = arrow::compute::take(l_batch.column(c).as_ref(), &l_idx_arr, None)
                .map_err(|e| VeinError::Arrow(e))?;
            out_cols.push(taken);
        }
        for c in 0..r_cols {
            let taken = arrow::compute::take(r_batch.column(c).as_ref(), &r_idx_arr, None)
                .map_err(|e| VeinError::Arrow(e))?;
            out_cols.push(taken);
        }
        out_cols.push(Arc::new(Int64Array::from(weights)));

        let batch = RecordBatch::try_new(out_schema, out_cols)
            .map_err(|e| VeinError::Arrow(e))?;
        Chunk::new(batch)
    }

    fn join_two_chunks(
        &self, left: &Chunk, right: &Chunk, scale: i64,
    ) -> Result<Chunk> {
        let lb = left.as_batch();
        let rb = right.as_batch();
        let lr = lb.num_rows();
        let rr = rb.num_rows();
        if lr == 0 || rr == 0 {
            return Ok(Chunk::empty(self.output_schema.clone()));
        }
        let lc = lb.num_columns() - 1;
        let rc = rb.num_columns() - 1;
        let cc = lc.min(rc);

        let lw = left.weights();
        let rw = right.weights();
        let mut pairs = Vec::new();

        for i in 0..lr {
            let wl = lw.value(i);
            if wl == 0 { continue; }
            let hl = row_hash(lb.columns(), i, lc);
            for j in 0..rr {
                let wr = rw.value(j);
                if wr == 0 { continue; }
                if hl != row_hash(rb.columns(), j, rc) { continue; }
                if !rows_equal(lb.columns(), i, rb.columns(), j, cc) { continue; }
                let w = wl * wr * scale;
                if w != 0 {
                    pairs.push((i as u32, j as u32, w));
                }
            }
        }

        Self::build_join_rows(lb, lc, rb, rc, self.output_schema.clone(), &pairs)
    }

}

#[async_trait]
impl DbspOperator for HashJoinOperator {
    fn name(&self) -> &'static str { "HashJoinOperator" }
    fn operator_id(&self) -> OperatorId { self.id }
    async fn init(&mut self) -> Result<()> { Ok(()) }
    fn metadata(&self) -> OperatorMeta { self.meta.clone() }
}

#[async_trait]
impl BinaryOperator for HashJoinOperator {
    async fn eval(&mut self, lhs: &Chunk, rhs: &Chunk) -> Result<Chunk> {
        let old_left = self.left_state.snapshot()?;
        let old_right = self.right_state.snapshot()?;

        let mut parts = Vec::new();

        if !old_left.is_empty() && !rhs.is_empty() {
            parts.push(self.join_two_chunks(&old_left, rhs, 1)?);
        }
        if !lhs.is_empty() && !old_right.is_empty() {
            parts.push(self.join_two_chunks(lhs, &old_right, 1)?);
        }
        if !lhs.is_empty() && !rhs.is_empty() {
            parts.push(self.join_two_chunks(lhs, rhs, 1)?);
        }

        self.left_state.apply_diff(lhs)?;
        self.right_state.apply_diff(rhs)?;

        if parts.is_empty() {
            return Ok(Chunk::empty(self.output_schema.clone()));
        }
        let mut combined = parts.remove(0);
        for p in parts {
            combined = combined.plus(&p)?;
        }
        Ok(combined)
    }
}

// ── HashAggregateOperator (count) ──

pub struct HashAggregateOperator {
    id: OperatorId,
    #[allow(dead_code)]
    key_indices: Vec<usize>,
    state: super::arrow_hash_index::ArrowHashIndex,
    meta: OperatorMeta,
}

impl HashAggregateOperator {
    pub fn new(id: OperatorId, key_indices: Vec<usize>, schema: SchemaRef) -> Self {
        Self {
            id,
            key_indices,
            state: super::arrow_hash_index::ArrowHashIndex::new(schema),
            meta: OperatorMeta::default(),
        }
    }
}

#[async_trait]
impl DbspOperator for HashAggregateOperator {
    fn name(&self) -> &'static str { "HashAggregateOperator" }
    fn operator_id(&self) -> OperatorId { self.id }
    async fn init(&mut self) -> Result<()> { Ok(()) }
    fn metadata(&self) -> OperatorMeta { self.meta.clone() }
}

#[async_trait]
impl UnaryOperator for HashAggregateOperator {
    async fn eval(&mut self, input: &Chunk) -> Result<Chunk> {
        let old_state = self.state.snapshot()?;
        self.state.apply_diff(input)?;
        let new_state = self.state.snapshot()?;

        // Output: new_state - old_state
        new_state.minus(&old_state)
    }
}

#[cfg(test)]
#[allow(missing_docs)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int64, false),
            Field::new("y", DataType::Utf8, false),
        ]))
    }

    fn make_chunk(keys: &[i64], vals: &[&str], weight: i64) -> Chunk {
        let s = schema();
        let key_col = Int64Array::from(keys.to_vec());
        let val_col = arrow::array::StringArray::from(vals.to_vec());
        let batch = RecordBatch::try_new(s, vec![Arc::new(key_col), Arc::new(val_col)]).unwrap();
        Chunk::from_records(batch, weight)
    }

    fn single_col_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
        ]))
    }

    fn make_single_chunk(keys: &[i64], weight: i64) -> Chunk {
        let s = single_col_schema();
        let col = Int64Array::from(keys.to_vec());
        let batch = RecordBatch::try_new(s, vec![Arc::new(col)]).unwrap();
        Chunk::from_records(batch, weight)
    }

    #[tokio::test]
    async fn test_filter_operator() {
        let mut op = FilterOperator::new(OperatorId::from_raw(1), |batch: &RecordBatch| {
            let col = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            Ok(BooleanArray::from((0..col.len()).map(|i| col.value(i) > 1).collect::<Vec<_>>()))
        });
        let input = make_chunk(&[1, 2, 3], &["a", "b", "c"], 1);
        let result = op.eval(&input).await.unwrap();
        assert_eq!(result.len(), 2);
    }

    #[tokio::test]
    async fn test_project_operator() {
        let mut op = ProjectOperator::new(OperatorId::from_raw(1), vec![0]);
        let input = make_chunk(&[1, 2], &["a", "b"], 1);
        let result = op.eval(&input).await.unwrap();
        assert_eq!(result.as_batch().num_columns(), 2);
        assert_eq!(result.len(), 2);
    }

    #[tokio::test]
    async fn test_negate_operator() {
        let mut op = NegateOperator::new(OperatorId::from_raw(1));
        let input = make_chunk(&[1, 2], &["a", "b"], 5);
        let result = op.eval(&input).await.unwrap();
        let w = result.weights();
        assert_eq!(w.value(0), -5);
        assert_eq!(w.value(1), -5);
    }

    #[tokio::test]
    async fn test_z1_operator() {
        let mut op = Z1Operator::new(OperatorId::from_raw(1));
        let r = op.eval_strict().await.unwrap();
        assert!(r.is_empty());
        let input = make_chunk(&[1, 2], &["a", "b"], 1);
        op.set_current(input.clone());
        let r = op.eval_strict().await.unwrap();
        assert_eq!(r.len(), 2);
    }

    #[tokio::test]
    async fn test_distinct_operator() {
        let mut op = DistinctOperator::new(OperatorId::from_raw(1));
        let input = make_chunk(&[1, 1, 2], &["a", "a", "b"], 1);
        let result = op.eval(&input).await.unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result.weighted_count(), 3);
    }

    #[tokio::test]
    async fn test_hash_join_operator() {
        let left_schema = single_col_schema();
        let right_schema = single_col_schema();
        let mut op = HashJoinOperator::new(
            OperatorId::from_raw(10),
            left_schema.clone(),
            right_schema.clone(),
        );

        // First epoch: left has (1,2), right has (2,3) → join on 2 → 1 row
        let lhs = make_single_chunk(&[1, 2], 1);
        let rhs = make_single_chunk(&[2, 3], 1);
        let r = op.eval(&lhs, &rhs).await.unwrap();
        assert_eq!(r.len(), 1, "first epoch should match 1 row (k=2)");

        // Second epoch: add (1) to left, add (1) to right
        // T1: old_left {1,2} ⋈ rhs [1] → (1,1) w=1
        // T2: lhs [1] ⋈ old_right {2,3} → no match
        // T3: lhs [1] ⋈ rhs [1] → (1,1) w=1
        // Combined: (1,1) w=2 (identical rows grouped by plus)
        let lhs2 = make_single_chunk(&[1], 1);
        let rhs2 = make_single_chunk(&[1], 1);
        let r2 = op.eval(&lhs2, &rhs2).await.unwrap();
        assert_eq!(r2.len(), 1, "identical rows group into 1 row");
        assert_eq!(r2.weighted_count(), 2, "total delta weight is 2");

        // Third epoch: remove (1) from left, remove (1) from right
        let lhs3 = make_single_chunk(&[1], -1);
        let rhs3 = make_single_chunk(&[1], -1);
        let r3 = op.eval(&lhs3, &rhs3).await.unwrap();
        assert_eq!(r3.weighted_count(), -2, "deletion delta weight is -2");
    }

    #[tokio::test]
    async fn test_hash_aggregate_operator() {
        let schema = single_col_schema();
        let mut op = HashAggregateOperator::new(
            OperatorId::from_raw(20),
            vec![0],
            schema,
        );

        // First epoch: rows k=1, k=2, k=1 → diff adds both
        let input = make_single_chunk(&[1, 2, 1], 1);
        let r = op.eval(&input).await.unwrap();
        // Output should have: (1, count=2), (2, count=1)
        assert_eq!(r.len(), 2);

        // Second epoch: remove k=1
        let input2 = make_single_chunk(&[1], -1);
        let r2 = op.eval(&input2).await.unwrap();
        // Output should have: (1, count=-1)
        assert_eq!(r2.len(), 1);
        assert_eq!(r2.weighted_count(), -1);
    }
}
