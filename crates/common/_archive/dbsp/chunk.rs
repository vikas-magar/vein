use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use error::{Result, VeinError};

const WEIGHT_COLUMN: &str = "__weight";

/// A Z-set (ℤ-set) over columnar Arrow data.
///
/// Each row has an integer weight. Positive = tuple exists (inserted),
/// negative = tuple is anti-exists (deleted), zero = removed on compact().
///
/// The weight is stored as the LAST column of the RecordBatch, named "__weight"
/// (hidden from schema introspection). This enables zero-copy IPC transport
/// and SIMD-accelerated algebraic operations.
#[derive(Clone)]
pub struct Chunk {
    batch: RecordBatch,
}

impl Chunk {
    /// Create from a RecordBatch that already has a "__weight" Int64Array column.
    pub fn new(batch: RecordBatch) -> Result<Self> {
        let schema = batch.schema();
        let weight_idx = schema.index_of(WEIGHT_COLUMN).map_err(|_| {
            VeinError::internal(format!(
                "Chunk requires a '{}' column of type Int64",
                WEIGHT_COLUMN
            ))
        })?;
        let weight_field = schema.field(weight_idx);
        if !weight_field.data_type().equals_datatype(&DataType::Int64) {
            return Err(VeinError::internal(format!(
                "Chunk weight column must be Int64, got {:?}",
                weight_field.data_type()
            )));
        }
        Ok(Self { batch })
    }

    /// Create from records with a uniform weight.
    pub fn from_records(batch: RecordBatch, weight: i64) -> Self {
        let num_rows = batch.num_rows();
        let weight_array = Int64Array::from_value(weight, num_rows);

        let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
        columns.push(Arc::new(weight_array));

        let data_fields: Vec<Field> =
            batch.schema().fields().iter().map(|f| f.as_ref().clone()).collect();
        let mut all_fields = data_fields;
        all_fields.push(Field::new(WEIGHT_COLUMN, DataType::Int64, false));
        let schema = Arc::new(Schema::new(all_fields));

        let batch = RecordBatch::try_new(schema, columns).expect("valid Chunk batch");
        Self { batch }
    }

    /// Create an empty Z-set with the given schema.
    pub fn empty(schema: SchemaRef) -> Self {
        let data_fields: Vec<Field> =
            schema.fields().iter().map(|f| f.as_ref().clone()).collect();
        let mut all_fields = data_fields;
        all_fields.push(Field::new(WEIGHT_COLUMN, DataType::Int64, false));
        let schema = Arc::new(Schema::new(all_fields));

        let columns: Vec<ArrayRef> = schema
            .fields()
            .iter()
            .map(|f| arrow::array::new_empty_array(f.data_type()))
            .collect();
        let batch = RecordBatch::try_new(schema, columns).expect("empty Chunk batch");
        Self { batch }
    }

    /// Pointwise addition: weights are added element-by-element.
    ///
    /// Uses a hash-based grouping: each row is hashed by all data columns,
    /// matching rows have their weights summed. O(n) expected.
    pub fn plus(&self, other: &Self) -> Result<Self> {
        if self.batch.num_rows() == 0 {
            return Ok(other.clone());
        }
        if other.batch.num_rows() == 0 {
            return Ok(self.clone());
        }

        // Group by hash, storing (source_enum, row_index, accumulated_weight).
        let mut groups: HashMap<u64, (u8, usize, i64)> = HashMap::new();
        let num_data = self.num_data_columns();

        for row in 0..self.batch.num_rows() {
            let hash = row_hash(self.batch.columns(), row, num_data);
            let weight = self.weights().value(row);
            groups
                .entry(hash)
                .and_modify(|(_, _, w)| *w += weight)
                .or_insert((0, row, weight));
        }

        for row in 0..other.batch.num_rows() {
            let hash = row_hash(other.batch.columns(), row, num_data);
            let weight = other.weights().value(row);
            match groups.entry(hash) {
                std::collections::hash_map::Entry::Occupied(mut oc) => {
                    let (_, _, w) = oc.get_mut();
                    *w += weight;
                }
                std::collections::hash_map::Entry::Vacant(v) => {
                    v.insert((1, row, weight));
                }
            }
        }

        // Collect non-zero groups.
        let active: Vec<(u8, usize, i64)> = groups
            .into_values()
            .filter(|(_, _, w)| *w != 0)
            .collect();

        if active.is_empty() {
            return Ok(Self::empty(self.batch.schema()));
        }

        // Build output by taking rows from source batches at stored indices.
        let num_out = active.len();
        let mut out_weights: Vec<i64> = Vec::with_capacity(num_out);
        let mut out_indices_per_source: [Vec<u32>; 2] = [Vec::new(), Vec::new()];

        // First pass: separate indices by source.
        for (src, row, weight) in &active {
            out_weights.push(*weight);
            out_indices_per_source[*src as usize].push(*row as u32);
        }

        // Second pass: take rows from each source batch.
        let mut source_taken: [Option<RecordBatch>; 2] = [None, None];
        for src in 0..2 {
            if out_indices_per_source[src].is_empty() {
                continue;
            }
            let src_batch = if src == 0 { &self.batch } else { &other.batch };
            let idx_array = arrow::array::UInt32Array::from(out_indices_per_source[src].clone());
            let taken_cols: Vec<ArrayRef> = src_batch
                .columns()
                .iter()
                .map(|col| arrow::compute::take(col.as_ref(), &idx_array, None).expect("take in plus"))
                .collect();
            source_taken[src] = Some(
                RecordBatch::try_new(src_batch.schema(), taken_cols)
                    .expect("taken batch"),
            );
        }

        // Concat and correct weights for the last (weight) column.
        let taken_batches: Vec<&RecordBatch> = source_taken.iter().filter_map(|b| b.as_ref()).collect();
        let mut concat = arrow::compute::concat_batches(&self.batch.schema(), taken_batches.into_iter())
            .map_err(|e| VeinError::Arrow(e))?;

        // Replace the weight column with our computed weights.
        let weight_col_idx = concat.schema().index_of(WEIGHT_COLUMN).expect("has weight");
        let mut final_cols: Vec<ArrayRef> = concat.columns().to_vec();
        final_cols[weight_col_idx] = Arc::new(Int64Array::from(out_weights));
        concat = RecordBatch::try_new(concat.schema(), final_cols)
            .map_err(|e| VeinError::Arrow(e))?;

        Ok(Self { batch: concat })
    }

    /// Unary negation: flip all weights.
    pub fn negate(&self) -> Self {
        let weights = self.weights();
        let negated: Vec<i64> = (0..weights.len()).map(|i| -weights.value(i)).collect();
        let mut columns: Vec<ArrayRef> = self.batch.columns()[..self.num_data_columns()].to_vec();
        columns.push(Arc::new(Int64Array::from(negated)));
        let batch = RecordBatch::try_new(self.batch.schema(), columns).expect("negate Chunk");
        Self { batch }
    }

    /// Z-set subtraction: self - other = self.plus(other.negate()).
    pub fn minus(&self, other: &Self) -> Result<Self> {
        self.plus(&other.negate())
    }

    /// Remove rows with zero weight.
    pub fn compact(&self) -> Self {
        let weights = self.weights();
        let indices: Vec<u32> = (0..weights.len())
            .filter(|&i| weights.value(i) != 0)
            .map(|i| i as u32)
            .collect();

        if indices.is_empty() {
            return Self::empty(self.batch.schema());
        }
        if indices.len() == weights.len() {
            return self.clone();
        }

        let columns: Vec<ArrayRef> = self
            .batch
            .columns()
            .iter()
            .map(|col| {
                arrow::compute::take(
                    col.as_ref(),
                    &arrow::array::UInt32Array::from(indices.clone()),
                    None,
                )
                .expect("take in compact")
            })
            .collect();

        let batch =
            RecordBatch::try_new(self.batch.schema(), columns).expect("compact Chunk");
        Self { batch }
    }

    /// Group by all data columns, summing weights for duplicate rows.
    /// Returns a new Chunk with deduplicated rows.
    pub fn group_by(&self) -> Self {
        let num_data = self.num_data_columns();
        if self.batch.num_rows() <= 1 {
            return self.clone();
        }

        let mut groups: HashMap<u64, (usize, i64)> = HashMap::new();

        for row in 0..self.batch.num_rows() {
            let hash = row_hash(self.batch.columns(), row, num_data);
            let weight = self.weights().value(row);
            groups
                .entry(hash)
                .and_modify(|(_, w)| *w += weight)
                .or_insert((row, weight));
        }

        let active: Vec<(usize, i64)> = groups
            .into_values()
            .filter(|(_, w)| *w != 0)
            .collect();

        if active.is_empty() {
            return Self::empty(self.batch.schema());
        }

        let num_out = active.len();
        let mut out_weights: Vec<i64> = Vec::with_capacity(num_out);
        let mut out_indices: Vec<u32> = Vec::with_capacity(num_out);

        for (row, weight) in &active {
            out_weights.push(*weight);
            out_indices.push(*row as u32);
        }

        let idx_array = arrow::array::UInt32Array::from(out_indices);
        let columns: Vec<ArrayRef> = self
            .batch
            .columns()
            .iter()
            .map(|col| {
                arrow::compute::take(col.as_ref(), &idx_array, None)
                    .expect("take in group_by")
            })
            .collect();

        let weight_idx = self.batch.schema().index_of(WEIGHT_COLUMN).expect("has weight");
        let mut batch = RecordBatch::try_new(self.batch.schema(), columns)
            .expect("group_by batch");
        let mut final_cols: Vec<ArrayRef> = batch.columns().to_vec();
        final_cols[weight_idx] = Arc::new(Int64Array::from(out_weights));
        batch = RecordBatch::try_new(batch.schema(), final_cols)
            .expect("group_by batch with weights");
        Self { batch }
    }

    /// True if all weights are zero (or row count is zero).
    pub fn is_empty(&self) -> bool {
        let weights = self.weights();
        weights.is_empty() || (0..weights.len()).all(|i| weights.value(i) == 0)
    }

    /// Sum of all weights (useful for COUNT queries).
    pub fn weighted_count(&self) -> i64 {
        let weights = self.weights();
        (0..weights.len()).map(|i| weights.value(i)).sum()
    }

    /// Number of non-zero-weight rows.
    pub fn len(&self) -> usize {
        let weights = self.weights();
        (0..weights.len())
            .filter(|&i| weights.value(i) != 0)
            .count()
    }

    /// Access the underlying RecordBatch.
    pub fn as_batch(&self) -> &RecordBatch {
        &self.batch
    }

    /// Access the weight column.
    pub fn weights(&self) -> &Int64Array {
        let weight_idx = self
            .batch
            .schema()
            .index_of(WEIGHT_COLUMN)
            .expect("Chunk always has __weight column");
        self.batch
            .column(weight_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("__weight is always Int64")
    }

    /// Convert to a RecordBatch WITHOUT the weight column.
    pub fn strip_weights(&self) -> RecordBatch {
        let data_cols: Vec<ArrayRef> = self.batch.columns()[..self.num_data_columns()].to_vec();
        let data_fields: Vec<Field> = self.batch.schema().fields()[..self.num_data_columns()]
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        let schema = Arc::new(Schema::new(data_fields));
        RecordBatch::try_new(schema, data_cols).expect("stripped batch")
    }

    /// Append weights to a RecordBatch (inverse of strip_weights).
    pub fn from_record_batch(batch: RecordBatch) -> Result<Self> {
        let num_rows = batch.num_rows();
        let weight_array = Int64Array::from_value(1, num_rows);

        let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
        columns.push(Arc::new(weight_array));

        let data_fields: Vec<Field> =
            batch.schema().fields().iter().map(|f| f.as_ref().clone()).collect();
        let mut all_fields = data_fields;
        all_fields.push(Field::new(WEIGHT_COLUMN, DataType::Int64, false));
        let schema = Arc::new(Schema::new(all_fields));
        let batch =
            RecordBatch::try_new(schema, columns).map_err(|e| VeinError::Arrow(e))?;
        Ok(Self { batch })
    }

    /// Serialize to Arrow IPC bytes (File format).
    /// Returns a `Vec<u8>` that can be written to disk or sent over the wire.
    pub fn to_ipc_bytes(&self) -> Result<Vec<u8>> {
        use std::io::Cursor;
        let mut buffer = Cursor::new(Vec::new());
        {
            let options = arrow::ipc::writer::IpcWriteOptions::default();
            let mut writer =
                FileWriter::try_new_with_options(&mut buffer, &self.batch.schema(), options)
                    .map_err(|e| VeinError::Arrow(e))?;
            writer
                .write(&self.batch)
                .map_err(|e| VeinError::Arrow(e))?;
            writer.finish().map_err(|e| VeinError::Arrow(e))?;
        }
        Ok(buffer.into_inner())
    }

    /// Deserialize from Arrow IPC bytes.
    pub fn from_ipc_bytes(data: &[u8]) -> Result<Self> {
        use std::io::Cursor;
        let cursor = Cursor::new(data);
        let reader =
            FileReader::try_new(cursor, None).map_err(|e| VeinError::Arrow(e))?;
        let schema = reader.schema();
        let mut batches: Vec<RecordBatch> = Vec::new();
        for maybe_batch in reader {
            let batch = maybe_batch.map_err(|e| VeinError::Arrow(e))?;
            batches.push(batch);
        }
        if batches.is_empty() {
            return Ok(Self::empty(schema));
        }
        let batch = arrow::compute::concat_batches(&schema, batches.iter())
            .map_err(|e| VeinError::Arrow(e))?;
        Self::new(batch)
    }

    // ── Private helpers ──

    fn num_data_columns(&self) -> usize {
        self.batch
            .schema()
            .index_of(WEIGHT_COLUMN)
            .expect("Chunk always has __weight")
    }
}

impl std::fmt::Debug for Chunk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Chunk({} rows, {} cols)",
            self.batch.num_rows(),
            self.batch.num_columns()
        )
    }
}

// ── Hashing helpers ──

/// Hash a single row across the first `num_cols` columns, returning a u64.
fn row_hash(columns: &[ArrayRef], row: usize, num_cols: usize) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for col_idx in 0..num_cols {
        hash_column_value(columns[col_idx].as_ref(), row, &mut hasher);
    }
    hasher.finish()
}

/// Hash one cell value into the hasher.
fn hash_column_value(array: &dyn Array, row: usize, hasher: &mut impl Hasher) {
    use arrow::array::*;
    if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
        arr.value(row).hash(hasher);
    } else if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
        arr.value(row).hash(hasher);
    } else if let Some(arr) = array.as_any().downcast_ref::<Float64Array>() {
        arr.value(row).to_bits().hash(hasher);
    } else if let Some(arr) = array.as_any().downcast_ref::<BooleanArray>() {
        arr.value(row).hash(hasher);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;

    fn make_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, false),
            Field::new("value", DataType::Utf8, false),
        ]))
    }

    fn make_batch(schema: &SchemaRef, keys: &[i64], values: &[&str]) -> RecordBatch {
        let key_col = Int64Array::from(keys.to_vec());
        let val_col = arrow::array::StringArray::from(values.to_vec());
        RecordBatch::try_new(schema.clone(), vec![Arc::new(key_col), Arc::new(val_col)])
            .unwrap()
    }

    #[test]
    fn test_chunk_plus() {
        let schema = make_schema();
        let batch1 = make_batch(&schema, &[1, 2], &["a", "b"]);
        let batch2 = make_batch(&schema, &[1, 3], &["a", "c"]);

        let c1 = Chunk::from_records(batch1, 1);
        let c2 = Chunk::from_records(batch2, 1);
        let result = c1.plus(&c2).unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result.weighted_count(), 4);
    }

    #[test]
    fn test_chunk_negate() {
        let schema = make_schema();
        let batch = make_batch(&schema, &[1, 2], &["a", "b"]);
        let c = Chunk::from_records(batch, 5);
        let neg = c.negate();
        let w = neg.weights();
        assert_eq!(w.value(0), -5);
        assert_eq!(w.value(1), -5);
    }

    #[test]
    fn test_chunk_compact() {
        let schema = make_schema();
        let batch1 = make_batch(&schema, &[1, 2], &["a", "b"]);
        let batch2 = make_batch(&schema, &[1], &["a"]);

        let c1 = Chunk::from_records(batch1, 1);
        let c2 = Chunk::from_records(batch2, -1);
        let result = c1.plus(&c2).unwrap();

        let compacted = result.compact();
        assert_eq!(compacted.len(), 1);
    }

    #[test]
    fn test_chunk_is_empty() {
        let schema = make_schema();
        let c = Chunk::empty(schema);
        assert!(c.is_empty());
        assert_eq!(c.len(), 0);
    }

    #[test]
    fn test_chunk_strip_weights() {
        let schema = make_schema();
        let batch = make_batch(&schema, &[1, 2], &["a", "b"]);
        let c = Chunk::from_records(batch, 1);
        let stripped = c.strip_weights();
        assert!(stripped.schema().index_of(WEIGHT_COLUMN).is_err());
        assert_eq!(stripped.num_rows(), 2);
    }

    #[test]
    fn test_chunk_from_record_batch() {
        let schema = make_schema();
        let batch = make_batch(&schema, &[1, 2], &["a", "b"]);
        let c = Chunk::from_record_batch(batch).unwrap();
        let w = c.weights();
        assert_eq!(w.value(0), 1);
        assert_eq!(w.value(1), 1);
    }
}
