use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use error::{Result, VeinError};

use super::Chunk;

const WEIGHT_COLUMN: &str = "__weight";

/// Columnar hash index for z⁻¹ state storage.
///
/// All data columns are stored contiguously with a parallel weight vector
/// and a hash index mapping row hashes to offsets.
pub struct ArrowHashIndex {
    schema: SchemaRef,
    columns: Vec<ArrayRef>,
    weights: Vec<i64>,
    hash_index: HashMap<u64, Vec<u32>>,
}

impl ArrowHashIndex {
    /// Create a new empty index with the given schema.
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
            columns: Vec::new(),
            weights: Vec::new(),
            hash_index: HashMap::new(),
        }
    }

    /// Apply a diff: rows with positive weights are upserted, negative are deleted.
    pub fn apply_diff(&mut self, diff: &Chunk) -> Result<()> {
        let batch = diff.as_batch();
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Ok(());
        }

        let weight_col = diff.weights();
        let num_data = batch.num_columns() - 1;

        for row in 0..num_rows {
            let weight = weight_col.value(row);
            let hash = row_hash(batch.columns(), row, num_data);

            if weight > 0 {
                self.upsert(batch, row, hash, weight, num_data);
            } else if weight < 0 {
                self.delete(batch, row, hash, -weight, num_data);
            }
        }
        Ok(())
    }

    /// Apply a diff and enforce a memory budget.
    /// If the memory used exceeds `budget_bytes`, compaction is triggered.
    /// Returns `true` if compaction was performed.
    pub fn apply_diff_with_budget(&mut self, diff: &Chunk, budget_bytes: usize) -> Result<bool> {
        self.apply_diff(diff)?;
        if self.memory_used() > budget_bytes {
            self.compact()?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Full snapshot as a Chunk.
    pub fn snapshot(&self) -> Result<Chunk> {
        if self.weights.is_empty() {
            return Ok(Chunk::empty(self.schema.clone()));
        }

        let mut columns = self.columns.clone();
        let weight_array = Int64Array::from(self.weights.clone());
        columns.push(Arc::new(weight_array));

        let data_fields: Vec<Field> =
            self.schema.fields().iter().map(|f| f.as_ref().clone()).collect();
        let mut all_fields = data_fields;
        all_fields.push(Field::new(WEIGHT_COLUMN, DataType::Int64, false));
        let schema = Arc::new(Schema::new(all_fields));

        let batch =
            RecordBatch::try_new(schema, columns).map_err(|e| VeinError::Arrow(e))?;
        Chunk::new(batch)
    }

    /// Probe the hash index: given probe keys, return matching row offsets.
    pub fn probe(&self, keys: &RecordBatch, _key_columns: &[usize]) -> Result<Vec<Option<u32>>> {
        let num_rows = keys.num_rows();
        let num_probe_cols = keys.num_columns();
        let mut results = Vec::with_capacity(num_rows);

        for row in 0..num_rows {
            let hash = row_hash(keys.columns(), row, num_probe_cols);
            let matched = self.hash_index.get(&hash).and_then(|offsets| {
                offsets.iter().copied().find(|&off| {
                    rows_equal(keys, row, &self.columns, off as usize, num_probe_cols)
                })
            });
            results.push(matched);
        }
        Ok(results)
    }

    /// Probe returning ALL matching row offsets per probe row.
    /// Used by HashJoinOperator where one probe key may match many state rows.
    pub fn probe_matches(&self, keys: &RecordBatch) -> Result<Vec<Vec<u32>>> {
        let num_rows = keys.num_rows();
        let num_probe_cols = keys.num_columns();
        let mut results = Vec::with_capacity(num_rows);

        for row in 0..num_rows {
            let hash = row_hash(keys.columns(), row, num_probe_cols);
            let matched: Vec<u32> = self
                .hash_index
                .get(&hash)
                .map(|offsets| {
                    offsets
                        .iter()
                        .copied()
                        .filter(|&off| {
                            rows_equal(keys, row, &self.columns, off as usize, num_probe_cols)
                        })
                        .collect()
                })
                .unwrap_or_default();
            results.push(matched);
        }
        Ok(results)
    }

    /// Compact: remove zero-weight rows, rebuild hash index.
    pub fn compact(&mut self) -> Result<()> {
        let keep: Vec<usize> = self
            .weights
            .iter()
            .enumerate()
            .filter(|&(_, w)| *w != 0)
            .map(|(i, _)| i)
            .collect();

        if keep.len() == self.weights.len() {
            return Ok(());
        }

        if keep.is_empty() {
            self.columns.clear();
            self.weights.clear();
            self.hash_index.clear();
            return Ok(());
        }

        self.columns = self
            .columns
            .iter()
            .map(|col| take_rows(col.as_ref(), &keep))
            .collect();
        self.weights = keep.iter().map(|&i| self.weights[i]).collect();
        self.rebuild_hash_index();
        Ok(())
    }

    /// Memory used in bytes.
    pub fn memory_used(&self) -> usize {
        self.columns
            .iter()
            .map(|c| c.get_array_memory_size())
            .sum::<usize>()
    }

    /// Access count for priority calculation.
    pub fn access_count(&self) -> u64 {
        0
    }

    // ── Private ──

    fn upsert(&mut self, batch: &RecordBatch, row: usize, hash: u64, weight: i64, num_data: usize) {
        if let Some(offsets) = self.hash_index.get(&hash) {
            for &off in offsets {
                if rows_equal(batch, row, &self.columns, off as usize, num_data) {
                    self.weights[off as usize] += weight;
                    return;
                }
            }
        }

        // New row: append.
        let new_idx = self.weights.len() as u32;
        for col_idx in 0..num_data {
            let array = batch.column(col_idx);
            let taken = take_rows(array.as_ref(), &[row]);
            if self.columns.len() <= col_idx {
                self.columns.push(taken);
            } else {
                self.columns[col_idx] = concat_arrays(&self.columns[col_idx], &taken);
            }
        }
        self.weights.push(weight);
        self.hash_index.entry(hash).or_default().push(new_idx);
    }

    fn delete(&mut self, batch: &RecordBatch, row: usize, hash: u64, weight: i64, num_data: usize) {
        if let Some(offsets) = self.hash_index.get(&hash).cloned() {
            for &off in &offsets {
                if rows_equal(batch, row, &self.columns, off as usize, num_data) {
                    self.weights[off as usize] -= weight;
                    return;
                }
            }
        }
    }

    fn rebuild_hash_index(&mut self) {
        self.hash_index.clear();
        let num_data = self.columns.len();
        for row in 0..self.weights.len() {
            if self.weights[row] == 0 {
                continue;
            }
            let hash = row_hash_row(&self.columns, row, num_data);
            self.hash_index.entry(hash).or_default().push(row as u32);
        }
    }
}

// ── Helper functions ──

fn row_hash(columns: &[ArrayRef], row: usize, num_cols: usize) -> u64 {
    use std::hash::Hasher;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for col_idx in 0..num_cols {
        hash_value(columns[col_idx].as_ref(), row, &mut hasher);
    }
    hasher.finish()
}

fn row_hash_row(columns: &[ArrayRef], row: usize, num_cols: usize) -> u64 {
    row_hash(columns, row, num_cols)
}

fn hash_value(array: &dyn Array, row: usize, hasher: &mut impl std::hash::Hasher) {
    use std::hash::Hash;
    use arrow::array::*;
    if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
        arr.value(row).hash(hasher);
    } else if let Some(arr) = array.as_any().downcast_ref::<arrow::array::StringArray>() {
        arr.value(row).hash(hasher);
    } else if let Some(arr) = array.as_any().downcast_ref::<arrow::array::Float64Array>() {
        arr.value(row).to_bits().hash(hasher);
    } else if let Some(arr) = array.as_any().downcast_ref::<arrow::array::BooleanArray>() {
        arr.value(row).hash(hasher);
    }
}

fn rows_equal(
    batch: &RecordBatch,
    batch_row: usize,
    index_cols: &[ArrayRef],
    index_row: usize,
    num_cols: usize,
) -> bool {
    for col_idx in 0..num_cols {
        let a = batch.column(col_idx);
        let b = &index_cols[col_idx];
        let eq = match (a.data_type(), b.data_type()) {
            (dt1, dt2) if dt1 != dt2 => false,
            _ => {
                use arrow::array::*;
                if let (Some(aa), Some(bb)) =
                    (a.as_any().downcast_ref::<Int64Array>(), b.as_any().downcast_ref::<Int64Array>())
                {
                    aa.value(batch_row) == bb.value(index_row)
                } else if let (Some(aa), Some(bb)) = (
                    a.as_any().downcast_ref::<StringArray>(),
                    b.as_any().downcast_ref::<StringArray>(),
                ) {
                    aa.value(batch_row) == bb.value(index_row)
                } else if let (Some(aa), Some(bb)) = (
                    a.as_any().downcast_ref::<Float64Array>(),
                    b.as_any().downcast_ref::<Float64Array>(),
                ) {
                    aa.value(batch_row) == bb.value(index_row)
                } else if let (Some(aa), Some(bb)) = (
                    a.as_any().downcast_ref::<BooleanArray>(),
                    b.as_any().downcast_ref::<BooleanArray>(),
                ) {
                    aa.value(batch_row) == bb.value(index_row)
                } else {
                    false
                }
            }
        };
        if !eq {
            return false;
        }
    }
    true
}

fn take_rows(array: &dyn Array, indices: &[usize]) -> ArrayRef {
    let idx_array = arrow::array::UInt32Array::from(
        indices.iter().map(|&i| i as u32).collect::<Vec<_>>(),
    );
    arrow::compute::take(array, &idx_array, None).expect("take_rows")
}

fn concat_arrays(a: &ArrayRef, b: &ArrayRef) -> ArrayRef {
    let concatenated = arrow::compute::concat(&[a.as_ref(), b.as_ref()]).expect("concat_arrays");
    concatenated
}

#[cfg(test)]
#[allow(missing_docs)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;

    fn make_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, false),
            Field::new("value", DataType::Utf8, false),
        ]))
    }

    fn make_chunk(keys: &[i64], values: &[&str], weight: i64) -> Chunk {
        let schema = make_schema();
        let key_col = Int64Array::from(keys.to_vec());
        let val_col = arrow::array::StringArray::from(values.to_vec());
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(key_col), Arc::new(val_col)]).unwrap();
        Chunk::from_records(batch, weight)
    }

    #[test]
    fn test_apply_diff_insert() {
        let mut idx = ArrowHashIndex::new(make_schema());
        let chunk = make_chunk(&[1, 2], &["a", "b"], 1);
        idx.apply_diff(&chunk).unwrap();
        let snap = idx.snapshot().unwrap();
        assert_eq!(snap.len(), 2);
    }

    #[test]
    fn test_apply_diff_delete() {
        let mut idx = ArrowHashIndex::new(make_schema());
        idx.apply_diff(&make_chunk(&[1, 2], &["a", "b"], 1)).unwrap();
        idx.apply_diff(&make_chunk(&[1], &["a"], -1)).unwrap();
        let snap = idx.snapshot().unwrap();
        assert_eq!(snap.len(), 1);
    }

    #[test]
    fn test_snapshot_matches() {
        let mut idx = ArrowHashIndex::new(make_schema());
        idx.apply_diff(&make_chunk(&[1, 2, 3], &["a", "b", "c"], 1))
            .unwrap();
        let snap = idx.snapshot().unwrap();
        assert_eq!(snap.len(), 3);
        assert_eq!(snap.weighted_count(), 3);
    }

    #[test]
    fn test_compact_zero_weight() {
        let mut idx = ArrowHashIndex::new(make_schema());
        idx.apply_diff(&make_chunk(&[1, 2], &["a", "b"], 1)).unwrap();
        idx.apply_diff(&make_chunk(&[1], &["a"], -1)).unwrap();
        idx.compact().unwrap();
        let snap = idx.snapshot().unwrap();
        assert_eq!(snap.len(), 1);
    }
}
