//! Source-independent join key extraction.
//!
//! EXO can later implement the same trait by returning canonical addresses
//! directly. Arrow-backed sources use this module as their value-native path.

use arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, StringArray,
    BinaryViewArray, StringViewArray,
    TimestampMicrosecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use common::Result;
use error::VeinError;
use exo::{
    AddressBatch, canonical_from_f32_native, canonical_from_f64, canonical_from_i8_native,
    canonical_from_i16_native, canonical_from_i32, canonical_from_i32_native, canonical_from_i64,
    classify_storage,
};
use std::hash::{Hash, Hasher};

use crate::pipeline::DataChunk;

/// One typed component of a join key.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum JoinKeyPart {
    /// A canonical address for a fixed-width value.
    Canonical(u64),
    /// Exact bytes for a value-native variable-width key.
    Bytes(Vec<u8>),
    /// A hashed address with exact bytes retained for collision verification.
    CanonicalWithVerification {
        /// Canonical address used for partitioning and candidate lookup.
        address: u64,
        /// Original variable-width value used for exact verification.
        value: Vec<u8>,
    },
}

/// Estimates heap memory retained by one join-key component.
impl JoinKeyPart {
    fn estimated_heap_size(&self) -> usize {
        std::mem::size_of::<Self>()
            + match self {
                Self::Canonical(_) => 0,
                Self::Bytes(value) => value.len(),
                Self::CanonicalWithVerification { value, .. } => value.len(),
            }
    }
}

/// A composite key supporting one or more join columns.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct JoinKey {
    /// Key components in planner-defined order.
    pub parts: Vec<JoinKeyPart>,
}

impl JoinKey {
    /// Creates a composite key from its components.
    pub fn new(parts: Vec<JoinKeyPart>) -> Self {
        Self { parts }
    }

    /// Returns whether every component is a canonical fixed-width value.
    pub fn is_address_native(&self) -> bool {
        self.parts.iter().all(|part| {
            matches!(
                part,
                JoinKeyPart::Canonical(_) | JoinKeyPart::CanonicalWithVerification { .. }
            )
        })
    }

    /// Estimates heap bytes retained by this key in the build hash table.
    pub(crate) fn estimated_heap_size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self
                .parts
                .iter()
                .map(JoinKeyPart::estimated_heap_size)
                .sum::<usize>()
    }

    /// Computes a stable hash shared by local hash tables and shuffle routing.
    ///
    /// Verification bytes are deliberately excluded from the hash. They are
    /// compared only after an address candidate lands in the same bucket.
    pub fn stable_hash(&self) -> u64 {
        let mut hasher = StableKeyHasher::default();
        for part in &self.parts {
            match part {
                JoinKeyPart::Canonical(address) => {
                    hasher.write_u8(0);
                    hasher.write(&address.to_le_bytes());
                }
                JoinKeyPart::CanonicalWithVerification { address, .. } => {
                    hasher.write_u8(0);
                    hasher.write(&address.to_le_bytes());
                }
                JoinKeyPart::Bytes(value) => {
                    hasher.write_u8(1);
                    hasher.write(&(value.len() as u64).to_le_bytes());
                    hasher.write(value);
                }
            }
        }
        hasher.finish()
    }
}

/// One input row and its extracted key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JoinKeyRow {
    /// Physical row index in the originating DataChunk.
    pub row_index: u32,
    /// Null when at least one join component is SQL NULL.
    pub key: Option<JoinKey>,
    /// Stable hash, or zero for a null key.
    pub hash: u64,
}

/// Extracted keys for one input DataChunk.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct JoinKeyBatch {
    rows: Vec<JoinKeyRow>,
}

impl JoinKeyBatch {
    /// Creates a key batch from extracted rows.
    pub fn new(rows: Vec<JoinKeyRow>) -> Self {
        Self { rows }
    }

    /// Builds canonical join keys directly from an EXO address batch.
    pub fn from_address_batch(batch: &AddressBatch, row_indices: &[u32]) -> Result<Self> {
        if batch.len() != row_indices.len() {
            return Err(VeinError::internal(format!(
                "address join batch mismatch: {} rows != {} row indices",
                batch.len(),
                row_indices.len()
            )));
        }

        let rows = batch
            .iter()
            .zip(row_indices.iter().copied())
            .map(|((address, valid), row_index)| {
                let key = valid.then(|| JoinKey::new(vec![JoinKeyPart::Canonical(address)]));
                let hash = key.as_ref().map(JoinKey::stable_hash).unwrap_or(0);
                JoinKeyRow {
                    row_index,
                    key,
                    hash,
                }
            })
            .collect();
        Ok(Self::new(rows))
    }

    /// Builds composite canonical keys from per-column EXO address sidecars.
    pub fn from_address_batches(batches: &[&AddressBatch], row_indices: &[u32]) -> Result<Self> {
        if batches.is_empty() {
            return Err(VeinError::internal(
                "address-native join requires at least one key column",
            ));
        }
        if batches.iter().any(|batch| batch.len() != row_indices.len()) {
            return Err(VeinError::internal(
                "address join sidecars do not match row indices",
            ));
        }

        let rows = row_indices
            .iter()
            .copied()
            .map(|row_index| {
                let mut parts = Vec::with_capacity(batches.len());
                let mut is_null = false;
                for batch in batches {
                    let Some((address, valid)) = batch.get(row_index as usize) else {
                        is_null = true;
                        break;
                    };
                    if !valid {
                        is_null = true;
                        break;
                    }
                    parts.push(JoinKeyPart::Canonical(address));
                }
                let key = (!is_null).then(|| JoinKey::new(parts));
                let hash = key.as_ref().map(JoinKey::stable_hash).unwrap_or(0);
                JoinKeyRow {
                    row_index,
                    key,
                    hash,
                }
            })
            .collect();
        Ok(Self::new(rows))
    }

    /// Number of input rows represented by this batch.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Whether this batch contains no rows.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Returns all rows, including null-key rows.
    pub fn rows(&self) -> &[JoinKeyRow] {
        &self.rows
    }

    /// Iterates only rows eligible for ordinary SQL equi-join matching.
    pub fn matching_rows(&self) -> impl Iterator<Item = &JoinKeyRow> {
        self.rows.iter().filter(|row| row.key.is_some())
    }
}

/// Extracts join keys from an execution batch.
pub trait JoinKeyExtractor: Send + Sync {
    /// Extracts keys from the selected physical rows of a DataChunk.
    fn extract(&self, chunk: &DataChunk, key_columns: &[usize]) -> Result<JoinKeyBatch>;
}

/// Attempts address-native extraction from all requested sidecars.
pub fn try_extract_address_keys(
    chunk: &DataChunk,
    key_columns: &[usize],
) -> Result<Option<JoinKeyBatch>> {
    if key_columns.is_empty() {
        return Err(VeinError::internal("join requires at least one key column"));
    }
    let Some(batches) = key_columns
        .iter()
        .map(|&column| chunk.address_columns.get(column).and_then(Option::as_ref))
        .collect::<Option<Vec<_>>>()
    else {
        return Ok(None);
    };
    let mut rows = Vec::with_capacity(chunk.num_rows());
    for logical_row in 0..chunk.num_rows() {
        let row_index = chunk.selection.get_index(logical_row) as u32;
        let mut parts = Vec::with_capacity(batches.len());
        let mut is_null = false;
        for (&column, batch) in key_columns.iter().zip(batches.iter()) {
            let Some((address, valid)) = batch.get(row_index as usize) else {
                is_null = true;
                break;
            };
            if !valid {
                is_null = true;
                break;
            }
            let array = chunk.vectors.get(column).ok_or_else(|| {
                VeinError::internal(format!("join key column {column} is out of bounds"))
            })?;
            let part = address_key_part(array.as_ref(), row_index as usize, address)?;
            parts.push(part);
        }
        let key = (!is_null).then(|| JoinKey::new(parts));
        let hash = key.as_ref().map(JoinKey::stable_hash).unwrap_or(0);
        rows.push(JoinKeyRow {
            row_index,
            key,
            hash,
        });
    }
    Ok(Some(JoinKeyBatch::new(rows)))
}

fn address_key_part(array: &dyn Array, row: usize, address: u64) -> Result<JoinKeyPart> {
    let storage = classify_storage(array.data_type()).ok_or_else(|| {
        VeinError::internal(format!(
            "EXO has no storage classification for address join key type {:?}",
            array.data_type()
        ))
    })?;
    if !storage.requires_collision_verification() {
        return Ok(JoinKeyPart::Canonical(address));
    }
    if array.is_null(row) {
        return Err(VeinError::internal(
            "EXO address sidecar marks a null join value as valid",
        ));
    }
    let value = exact_variable_bytes(array, row).ok_or_else(|| {
        VeinError::internal(format!(
            "EXO join key type {:?} requires collision verification but has no exact-value codec",
            array.data_type()
        ))
    })?;
    Ok(JoinKeyPart::CanonicalWithVerification { address, value })
}

fn exact_variable_bytes(array: &dyn Array, row: usize) -> Option<Vec<u8>> {
    if let Some(values) = array.as_any().downcast_ref::<StringArray>() {
        return Some(values.value(row).as_bytes().to_vec());
    }
    if let Some(values) = array.as_any().downcast_ref::<LargeStringArray>() {
        return Some(values.value(row).as_bytes().to_vec());
    }
    if let Some(values) = array.as_any().downcast_ref::<BinaryArray>() {
        return Some(values.value(row).to_vec());
    }
    if let Some(values) = array.as_any().downcast_ref::<LargeBinaryArray>() {
        return Some(values.value(row).to_vec());
    }
    if let Some(values) = array.as_any().downcast_ref::<StringViewArray>() {
        return Some(values.value(row).as_bytes().to_vec());
    }
    if let Some(values) = array.as_any().downcast_ref::<BinaryViewArray>() {
        return Some(values.value(row).to_vec());
    }
    None
}

/// Arrow value-native key extraction for Parquet, CSV, JSON, and other sources.
#[derive(Debug, Default, Clone, Copy)]
pub struct ArrowJoinKeyExtractor;

impl JoinKeyExtractor for ArrowJoinKeyExtractor {
    fn extract(&self, chunk: &DataChunk, key_columns: &[usize]) -> Result<JoinKeyBatch> {
        if key_columns.is_empty() {
            return Err(VeinError::internal("join requires at least one key column"));
        }

        let arrays: Vec<_> = key_columns
            .iter()
            .map(|&column| {
                chunk.vectors.get(column).ok_or_else(|| {
                    VeinError::internal(format!("join key column {column} is out of bounds"))
                })
            })
            .collect::<Result<_>>()?;

        let mut rows = Vec::with_capacity(chunk.num_rows());
        for logical_row in 0..chunk.num_rows() {
            let row_index = chunk.selection.get_index(logical_row) as u32;
            let mut parts = Vec::with_capacity(arrays.len());
            let mut is_null = false;

            for array in &arrays {
                match extract_part(array.as_ref(), row_index as usize)? {
                    Some(part) => parts.push(part),
                    None => {
                        is_null = true;
                        break;
                    }
                }
            }

            let key = (!is_null).then(|| JoinKey::new(parts));
            let hash = key.as_ref().map(JoinKey::stable_hash).unwrap_or(0);
            rows.push(JoinKeyRow {
                row_index,
                key,
                hash,
            });
        }

        Ok(JoinKeyBatch::new(rows))
    }
}

fn extract_part(array: &dyn Array, row: usize) -> Result<Option<JoinKeyPart>> {
    if array.is_null(row) {
        return Ok(None);
    }

    if let Some(value) = exact_variable_bytes(array, row) {
        return Ok(Some(JoinKeyPart::Bytes(value)));
    }

    if let Some(values) = array.as_any().downcast_ref::<Int8Array>() {
        return Ok(Some(JoinKeyPart::Canonical(
            canonical_from_i8_native(values.value(row)) as u64,
        )));
    }
    if let Some(values) = array.as_any().downcast_ref::<Int16Array>() {
        return Ok(Some(JoinKeyPart::Canonical(
            canonical_from_i16_native(values.value(row)) as u64,
        )));
    }
    if let Some(values) = array.as_any().downcast_ref::<Int32Array>() {
        return Ok(Some(JoinKeyPart::Canonical(
            canonical_from_i32_native(values.value(row)) as u64,
        )));
    }
    if let Some(values) = array.as_any().downcast_ref::<Int64Array>() {
        return Ok(Some(JoinKeyPart::Canonical(canonical_from_i64(
            values.value(row),
        ))));
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt8Array>() {
        return Ok(Some(JoinKeyPart::Canonical(values.value(row) as u64)));
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt16Array>() {
        return Ok(Some(JoinKeyPart::Canonical(values.value(row) as u64)));
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt32Array>() {
        return Ok(Some(JoinKeyPart::Canonical(values.value(row) as u64)));
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt64Array>() {
        return Ok(Some(JoinKeyPart::Canonical(values.value(row))));
    }
    if let Some(values) = array.as_any().downcast_ref::<Float32Array>() {
        return Ok(Some(JoinKeyPart::Canonical(
            canonical_from_f32_native(values.value(row)) as u64,
        )));
    }
    if let Some(values) = array.as_any().downcast_ref::<Float64Array>() {
        return Ok(Some(JoinKeyPart::Canonical(canonical_from_f64(
            values.value(row),
        ))));
    }
    if let Some(values) = array.as_any().downcast_ref::<BooleanArray>() {
        return Ok(Some(JoinKeyPart::Canonical(values.value(row) as u64)));
    }
    if let Some(values) = array.as_any().downcast_ref::<Date32Array>() {
        return Ok(Some(JoinKeyPart::Canonical(canonical_from_i32(
            values.value(row),
        ))));
    }
    if let Some(values) = array.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        return Ok(Some(JoinKeyPart::Canonical(canonical_from_i64(
            values.value(row),
        ))));
    }
    Err(VeinError::internal(format!(
        "unsupported Arrow join key type: {:?}",
        array.data_type()
    )))
}

#[derive(Default)]
struct StableKeyHasher {
    state: u64,
}

impl Hasher for StableKeyHasher {
    fn finish(&self) -> u64 {
        self.state
    }

    fn write(&mut self, bytes: &[u8]) {
        if self.state == 0 {
            self.state = 0xcbf29ce484222325;
        }
        for byte in bytes {
            self.state ^= *byte as u64;
            self.state = self.state.wrapping_mul(0x100000001b3);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ArrowJoinKeyExtractor, JoinKeyExtractor, JoinKeyPart, try_extract_address_keys};
    use crate::pipeline::DataChunk;
    use arrow::array::{ArrayRef, Decimal128Array, Int64Array, StringArray, StringViewArray};
    use common::arrow::SelectionVector;
    use exo::AddressBatch;
    use std::sync::Arc;

    #[test]
    fn preserves_selected_physical_rows_and_null_keys() {
        let values: ArrayRef = Arc::new(Int64Array::from(vec![Some(10), None, Some(30)]));
        let chunk = DataChunk::new(vec![values], SelectionVector::new_sparse(vec![2, 1]));

        let batch = ArrowJoinKeyExtractor
            .extract(&chunk, &[0])
            .expect("key extraction succeeds");

        assert_eq!(batch.rows()[0].row_index, 2);
        assert_eq!(
            batch.rows()[0].key.as_ref().unwrap().parts,
            vec![JoinKeyPart::Canonical(exo::canonical_from_i64(30)),]
        );
        assert!(batch.rows()[1].key.is_none());
        assert_eq!(batch.matching_rows().count(), 1);
    }

    #[test]
    fn retains_exact_bytes_for_variable_width_keys() {
        let values: ArrayRef = Arc::new(StringArray::from(vec![Some("alpha"), Some("beta")]));
        let chunk = DataChunk::from_vectors(vec![values]);

        let batch = ArrowJoinKeyExtractor
            .extract(&chunk, &[0])
            .expect("key extraction succeeds");

        assert_eq!(
            batch.rows()[0].key.as_ref().unwrap().parts,
            vec![JoinKeyPart::Bytes(b"alpha".to_vec())]
        );
        assert_ne!(batch.rows()[0].hash, batch.rows()[1].hash);
    }

    #[test]
    fn verifies_string_view_address_candidates_with_exact_values() {
        let values: ArrayRef = Arc::new(StringViewArray::from(vec![Some("alpha"), Some("omega")]));
        let chunk = DataChunk::from_vectors_with_addresses(
            vec![values],
            vec![Some(AddressBatch::from_addresses(vec![17, 17]))],
        )
        .expect("aligned address sidecar");

        let batch = try_extract_address_keys(&chunk, &[0])
            .expect("address key extraction succeeds")
            .expect("EXO address sidecar is present");

        let first = batch.rows()[0].key.as_ref().expect("non-null first key");
        let second = batch.rows()[1].key.as_ref().expect("non-null second key");
        assert_ne!(first, second);
        assert_eq!(first.parts[0], JoinKeyPart::CanonicalWithVerification {
            address: 17,
            value: b"alpha".to_vec(),
        });
        assert_eq!(second.parts[0], JoinKeyPart::CanonicalWithVerification {
            address: 17,
            value: b"omega".to_vec(),
        });
        assert_eq!(batch.rows()[0].hash, batch.rows()[1].hash);
    }

    #[test]
    fn address_extraction_fails_closed_for_unimplemented_collision_verifier() {
        let values: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(123_i128)])
                .with_precision_and_scale(22, 1)
                .expect("valid decimal type"),
        );
        let chunk = DataChunk::from_vectors_with_addresses(
            vec![values],
            vec![Some(AddressBatch::from_addresses(vec![17]))],
        )
        .expect("aligned address sidecar");

        assert!(try_extract_address_keys(&chunk, &[0]).is_err());
    }

    #[test]
    fn extracts_string_view_values_on_the_value_native_path() {
        let values: ArrayRef = Arc::new(StringViewArray::from(vec![Some("alpha"), Some("beta")]));
        let chunk = DataChunk::from_vectors(vec![values]);

        let batch = ArrowJoinKeyExtractor
            .extract(&chunk, &[0])
            .expect("view key extraction succeeds");

        assert_eq!(
            batch.rows()[0].key.as_ref().unwrap().parts,
            vec![JoinKeyPart::Bytes(b"alpha".to_vec())]
        );
    }

    #[test]
    fn composite_key_order_is_significant() {
        let first = super::JoinKey::new(vec![
            JoinKeyPart::Canonical(1),
            JoinKeyPart::Bytes(b"x".to_vec()),
        ]);
        let second = super::JoinKey::new(vec![
            JoinKeyPart::Bytes(b"x".to_vec()),
            JoinKeyPart::Canonical(1),
        ]);

        assert_ne!(first, second);
        assert_ne!(first.stable_hash(), second.stable_hash());
    }
}
