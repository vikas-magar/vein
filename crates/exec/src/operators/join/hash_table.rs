//! The build side of a hash join, as a table of references.
//!
//! Build payloads stay in reference-backed views. Key resolution shares contiguous
//! Arrow buffers or gathers mapped key rows; the single-Int64 fast path also
//! copies keys into the hash index. These allocations are distinct from payload
//! copying. Probe keys are execution-local, keeping the shared table immutable.
//!
//! Matching produces row references. The join operator composes output views;
//! ordinary Arrow payload arrays are requested only at an explicit boundary.

use arrow::array::{Array, Int64Array, StringArray};
use std::mem::size_of;
use std::time::Instant;

use crate::memory::{MemoryReservation, QueryMemoryTracker};
use crate::pipeline::{DataChunk, DataChunkView, MaterializationReport};
use common::Result;
use error::VeinError;
use types::{HashJoinKeyType, VeinDataType};

/// Which columns of a chunk form the join key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinKeys {
    /// Key columns, in order.
    pub columns: Vec<usize>,
}

impl JoinKeys {
    /// A key over `columns`.
    pub fn new(columns: Vec<usize>) -> Self {
        Self { columns }
    }
}

/// "No entry", since entry 0 is a valid index.
const NONE: u32 = u32::MAX;

/// Buckets before growth; a power of two so the modulo is a mask.
const INITIAL_BUCKETS: usize = 1 << 12;

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

#[inline]
fn fnv1a(bytes: &[u8], mut hash: u64) -> u64 {
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

/// The bucket a hash lands in.
///
/// `buckets.len()` is always a power of two — [`HashTable::rebucket`] rounds up
/// and every growth doubles — so this is a mask. The `%` it replaces was a real
/// 64-bit division on every probe row, because the compiler cannot see that
/// invariant in a `Vec`'s length.
#[inline]
fn bucket_of(hash: u64, buckets: usize) -> usize {
    (hash as usize) & (buckets - 1)
}

/// A key column resolved to its concrete Arrow type.
///
/// The array is an `Arc` clone rather than a borrow, so the resolved keys can be
/// owned beside the chunk they describe without the table becoming
/// self-referential.
#[derive(Debug, Clone)]
enum KeyArray {
    Int64(Int64Array),
    Utf8(StringArray),
}

impl KeyArray {
    #[inline]
    fn is_null(&self, row: usize) -> bool {
        match self {
            KeyArray::Int64(array) => array.is_null(row),
            KeyArray::Utf8(array) => array.is_null(row),
        }
    }

    #[inline]
    fn hash_into(&self, row: usize, hash: u64) -> u64 {
        match self {
            KeyArray::Int64(array) => fnv1a(&array.value(row).to_ne_bytes(), hash),
            KeyArray::Utf8(array) => fnv1a(array.value(row).as_bytes(), hash),
        }
    }

    #[inline]
    fn value_eq(&self, row: usize, other: &KeyArray, other_row: usize) -> bool {
        match (self, other) {
            (KeyArray::Int64(left), KeyArray::Int64(right)) => {
                left.value(row) == right.value(other_row)
            }
            (KeyArray::Utf8(left), KeyArray::Utf8(right)) => {
                left.value(row) == right.value(other_row)
            }
            _ => false,
        }
    }
}

/// The key columns of one chunk, resolved once rather than once per row.
#[derive(Debug, Clone)]
struct ChunkKeys {
    columns: Vec<KeyArray>,
}

impl ChunkKeys {
    fn resolve(keys: &JoinKeys, chunk: &DataChunkView) -> Result<(Self, MaterializationReport, u64)> {
        let started = Instant::now();
        let mut report = MaterializationReport::default();
        let mut columns = Vec::with_capacity(keys.columns.len());
        for index in &keys.columns {
            // Only the join keys cross an Arrow-kernel boundary; payload arrays
            // stay retained by the reference-backed view.
            let (array, column_report) = chunk.materialize_column_with_report(*index)?;
            report.accumulate(column_report);
            let data_type = VeinDataType::try_from(array.data_type()).map_err(|error| {
                VeinError::internal(format!(
                    "join key column {index} has an unsupported Arrow type {:?}: {error}",
                    array.data_type()
                ))
            })?;
            let resolved = match data_type.hash_join_key_type() {
                Some(HashJoinKeyType::Int64) => {
                    let ints = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                        VeinError::internal(format!(
                            "join key column {index} does not match its declared Int64 type"
                        ))
                    })?;
                    KeyArray::Int64(ints.clone())
                }
                Some(HashJoinKeyType::Utf8) => {
                    let strings = array.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                        VeinError::internal(format!(
                            "join key column {index} does not match its declared Utf8 type"
                        ))
                    })?;
                    KeyArray::Utf8(strings.clone())
                }
                None => {
                    return Err(VeinError::internal(format!(
                        "join key column {index} has type {:?}, which is not supported by hash join",
                        array.data_type()
                    )));
                }
            };
            columns.push(resolved);
        }
        Ok((Self { columns }, report, u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX)))
    }

    /// Whether any key column is null at `row`. A null key matches nothing.
    fn any_null(&self, row: usize) -> bool {
        self.columns.iter().any(|column| column.is_null(row))
    }

    fn hash(&self, row: usize) -> u64 {
        self.columns
            .iter()
            .fold(FNV_OFFSET, |hash, column| column.hash_into(row, hash))
    }

    /// Whether this is a single `Int64` key, the shape that can be compared as a
    /// plain value instead of through the encoding.
    fn is_single_i64(&self) -> bool {
        matches!(self.columns.as_slice(), [KeyArray::Int64(_)])
    }

    /// The single `Int64` key at `row`; only meaningful when [`Self::is_single_i64`].
    fn single_i64(&self, row: usize) -> Option<i64> {
        match self.columns.as_slice() {
            [KeyArray::Int64(array)] => Some(array.value(row)),
            _ => None,
        }
    }

    fn equals(&self, row: usize, other: &Self, other_row: usize) -> bool {
        self.columns
            .iter()
            .zip(&other.columns)
            .all(|(left, right)| left.value_eq(row, right, other_row))
    }
}

/// One build row: a reference into a retained chunk.
#[derive(Debug, Clone, Copy)]
struct Entry {
    chunk: u32,
    row: u32,
    next: u32,
}

/// A build side indexed by key hash, holding references rather than rows.
#[derive(Debug)]
pub struct HashTable {
    /// Key columns of a build chunk.
    build_keys: JoinKeys,
    /// Key columns of a probe chunk, resolved separately because the key need
    /// not sit at the same position on each side.
    probe_keys: JoinKeys,
    chunks: Vec<DataChunkView>,
    keys_by_chunk: Vec<ChunkKeys>,
    entries: Vec<Entry>,
    hashes: Vec<u64>,
    buckets: Vec<u32>,
    /// Build rows whose key is null: entered, but never bucketed, because no
    /// probe row may match them and an outer join still has to emit them.
    null_keyed: Vec<(u32, u32)>,
    /// The build key of each entry, when the key is a single `Int64`. Comparing
    /// these is one array read instead of resolving the key's encoding and
    /// dereferencing an Arrow array per chain candidate — DuckDB's trick of
    /// keeping the key beside the hash (`tuple_data_layout.cpp`: "NULL byte +
    /// fixed-width type"). Only the key is copied; payload columns stay
    /// columnar and shared.
    keys: Vec<i64>,
    /// Build key array conversions, excluding inline-key/index allocations.
    build_key_materialization: MaterializationReport,
    build_key_materialize_us: u64,
    tracker: QueryMemoryTracker,
    memory: MemoryReservation,
}

impl HashTable {
    /// An empty table.
    ///
    /// `build_keys` index a build chunk; `probe_keys` index a probe chunk. They
    /// are separate because a join's key need not sit at the same column
    /// position on both sides — `a.id = b.other_id` is legal, and resolving the
    /// probe through the build's positions would compare the wrong columns and
    /// return wrong rows.
    pub fn new(build_keys: JoinKeys, probe_keys: JoinKeys) -> Self {
        Self::with_memory_tracker(
            build_keys,
            probe_keys,
            QueryMemoryTracker::unlimited(common::cancellation::CancellationToken::new()),
        )
            .expect("unlimited join budget must fit initial buckets")
    }

    /// Create a hash table that charges owned index state to `tracker`.
    pub fn with_memory_tracker(
        build_keys: JoinKeys,
        probe_keys: JoinKeys,
        tracker: QueryMemoryTracker,
    ) -> Result<Self> {
        let bucket_bytes = INITIAL_BUCKETS
            .checked_mul(size_of::<u32>())
            .ok_or_else(|| VeinError::internal("initial join bucket size overflow"))?;
        let memory = tracker.reserve(bucket_bytes, "hash join buckets")?;
        let mut buckets = Vec::new();
        buckets.try_reserve_exact(INITIAL_BUCKETS).map_err(|error| {
            VeinError::internal(format!("cannot allocate initial hash join buckets: {error}"))
        })?;
        buckets.resize(INITIAL_BUCKETS, NONE);
        Ok(Self {
            build_keys,
            probe_keys,
            chunks: Vec::new(),
            keys_by_chunk: Vec::new(),
            entries: Vec::new(),
            hashes: Vec::new(),
            buckets,
            null_keyed: Vec::new(),
            keys: Vec::new(),
            build_key_materialization: MaterializationReport::default(),
            build_key_materialize_us: 0,
            tracker: tracker.clone(),
            memory,
        })
    }

    /// Whether the build key is a single `Int64`, so [`Self::keys`] is usable.
    fn inline_keys(&self) -> bool {
        self.keys.len() == self.entries.len()
    }

    /// How many build rows are entered, null-keyed ones included.
    pub fn len(&self) -> usize {
        self.entries.len() + self.null_keyed.len()
    }

    /// Whether nothing has been built.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty() && self.null_keyed.is_empty()
    }

    /// Adds a build chunk.
    ///
    /// The chunk is retained: entries point into its columns, so it must outlive
    /// the probe. A row with a null key is entered — an outer join still has to
    /// emit it — but is left out of the buckets, so no probe can ever match it.
    pub fn insert(&mut self, chunk: DataChunk) -> Result<()> {
        self.insert_view(DataChunkView::from_chunk(chunk)?)
    }

    /// Retains a reference-backed build batch and indexes its active rows.
    pub fn insert_view(&mut self, mut chunk: DataChunkView) -> Result<()> {
        let tracker_id = self.tracker.identity();
        chunk.account_source_arrays(tracker_id, |identity, bytes| {
            let lease: std::sync::Arc<dyn crate::pipeline::view::ViewMemoryLease> =
                self.tracker.lease_retained_array(identity, bytes, "hash join retained arrays")?;
            Ok(lease)
        })?;
        let (keys, report, materialize_us) = ChunkKeys::resolve(&self.build_keys, &chunk)?;
        self.build_key_materialization.accumulate(report);
        self.build_key_materialize_us = self.build_key_materialize_us.saturating_add(materialize_us);
        let chunk_index = self.chunks.len() as u32;
        // The keys stay a local, so the loop below can mutate the table's
        // vectors while still reading the chunk's resolved keys. Building a
        // `(row, hash, is_null)` scratch vector instead cost 24 bytes per build
        // row — three times what the table itself stores.
        self.memory.grow(size_of::<DataChunkView>(), "hash join chunk references")?;
        self.chunks.push(chunk);

        for position in 0..self.chunks[chunk_index as usize].num_rows() {
            let row = position as u32;
            if keys.any_null(row as usize) {
                self.memory.grow(size_of::<(u32, u32)>(), "hash join null-key index")?;
                self.null_keyed.push((chunk_index, row));
                continue;
            }
            // Grown per row rather than sized up front, so no count pass is
            // needed; the doubling makes it amortised constant.
            self.grow_if_needed(1)?;
            let inline_key = keys.single_i64(row as usize);
            let entry_bytes = size_of::<Entry>()
                .checked_add(size_of::<u64>())
                .and_then(|bytes| {
                    bytes.checked_add(inline_key.map_or(0, |_| size_of::<i64>()))
                })
                .ok_or_else(|| VeinError::internal("join index entry size overflow"))?;
            self.memory.grow(entry_bytes, "hash join row index")?;
            let hash = keys.hash(row as usize);
            let index = self.entries.len() as u32;
            let bucket = bucket_of(hash, self.buckets.len());
            let next = self.buckets[bucket];
            self.buckets[bucket] = index;
            self.entries.push(Entry {
                chunk: chunk_index,
                row,
                next,
            });
            self.hashes.push(hash);
            if let Some(value) = inline_key {
                self.keys.push(value);
            }
        }
        // A chunk whose key is not a single Int64 invalidates the inline array:
        // drop it rather than leave a partially filled one behind.
        if self.keys.len() != self.entries.len() {
            self.keys.clear();
        }
        self.keys_by_chunk.push(keys);
        Ok(())
    }

    /// Grows the bucket table so chains stay short.
    ///
    /// Called once per chunk, sized for the entries about to arrive, so a large
    /// build side does not degrade into one long chain per bucket. Null-keyed
    /// rows are not in the table to rehash.
    fn grow_if_needed(&mut self, additional: usize) -> Result<()> {
        let wanted = self.entries.len().checked_add(additional)
            .and_then(|entries| entries.checked_mul(2))
            .ok_or_else(|| VeinError::internal("hash join bucket count overflow"))?;
        if wanted <= self.buckets.len() {
            return Ok(());
        }
        let mut size = self.buckets.len();
        while size < wanted {
            size = size.checked_mul(2)
                .ok_or_else(|| VeinError::internal("hash join bucket count overflow"))?;
        }
        let new_bytes = size.checked_mul(size_of::<u32>())
            .ok_or_else(|| VeinError::internal("hash join bucket bytes overflow"))?;
        let old_bytes = self.buckets.len().checked_mul(size_of::<u32>())
            .ok_or_else(|| VeinError::internal("hash join bucket bytes overflow"))?;
        self.memory.grow(new_bytes, "hash join buckets")?;
        if let Err(error) = self.rebucket(size) {
            self.memory.shrink(new_bytes);
            return Err(error);
        }
        self.memory.shrink(old_bytes);
        Ok(())
    }

    /// Rebuilds the buckets from the stored hashes.
    ///
    /// O(entries) and allocation-free apart from the bucket vector itself: the
    /// hashes are already computed, so nothing is re-hashed. Null-keyed rows are
    /// not in `entries`, so they are correctly left out.
    fn rebucket(&mut self, size: usize) -> Result<()> {
        // `bucket_of` masks, so the count must be a power of two. A caller's
        // "entries times two" is not one, and rounding up here keeps the
        // invariant in a single place rather than at every call site.
        let size = size.max(1).next_power_of_two();
        let mut buckets = Vec::new();
        buckets.try_reserve_exact(size).map_err(|error| {
            VeinError::internal(format!("cannot allocate hash join buckets: {error}"))
        })?;
        buckets.resize(size, NONE);
        self.buckets = buckets;
        for index in 0..self.entries.len() {
            let bucket = bucket_of(self.hashes[index], size);
            self.entries[index].next = self.buckets[bucket];
            self.buckets[bucket] = index as u32;
        }
        Ok(())
    }

    /// Absorbs `other` into this table.
    ///
    /// This is what a parallel build needs: each worker fills its own table over
    /// its own chunks, and the results are combined here. Entries reference their
    /// chunks by index, so every reference from `other` is rebased, and the
    /// buckets are rebuilt from the stored hashes rather than re-hashed.
    ///
    /// Both tables must have been built on the same key; combining different
    /// keys would silently compare the wrong columns.
    pub fn merge(&mut self, mut other: HashTable) -> Result<()> {
        if self.build_keys != other.build_keys || self.probe_keys != other.probe_keys {
            return Err(VeinError::internal(
                "cannot merge hash tables built on different keys",
            ));
        }
        if self.tracker.identity() != other.tracker.identity() {
            for chunk in &mut other.chunks {
                let tracker_id = self.tracker.identity();
                chunk.account_source_arrays(tracker_id, |identity, bytes| {
                    let lease: std::sync::Arc<dyn crate::pipeline::view::ViewMemoryLease> = self
                        .tracker
                        .lease_retained_array(identity, bytes, "merged hash join retained arrays")?;
                    Ok(lease)
                })?;
            }
        }
        let self_inline = self.inline_keys();
        let other_inline = other.inline_keys();
        let replaced_bucket_bytes = self
            .buckets
            .len()
            .checked_add(other.buckets.len())
            .and_then(|count| count.checked_mul(size_of::<u32>()))
            .ok_or_else(|| VeinError::internal("merged join bucket bytes overflow"))?;
        self.memory.absorb(other.memory)?;
        self.build_key_materialization.accumulate(other.build_key_materialization);
        self.build_key_materialize_us = self.build_key_materialize_us.saturating_add(other.build_key_materialize_us);
        let offset = self.chunks.len() as u32;

        self.chunks.extend(other.chunks);
        self.keys_by_chunk.extend(other.keys_by_chunk);
        self.entries
            .extend(other.entries.into_iter().map(|entry| Entry {
                chunk: entry.chunk + offset,
                row: entry.row,
                next: NONE,
            }));
        self.hashes.extend(other.hashes);
        self.null_keyed.extend(
            other
                .null_keyed
                .into_iter()
                .map(|(chunk, row)| (chunk + offset, row)),
        );
        // The inline key array is only meaningful if *both* sides kept one.
        if self_inline && other_inline {
            self.keys.extend(other.keys);
        } else {
            self.keys.clear();
        }

        let wanted_buckets = self.entries.len()
            .checked_mul(2)
            .ok_or_else(|| VeinError::internal("merged join bucket count overflow"))?;
        let bucket_count = self
            .buckets
            .len()
            .max(wanted_buckets)
            .max(INITIAL_BUCKETS)
            .max(1)
            .next_power_of_two();
        let new_bucket_bytes = bucket_count
            .checked_mul(size_of::<u32>())
            .ok_or_else(|| VeinError::internal("merged join bucket bytes overflow"))?;
        self.memory.grow(new_bucket_bytes, "hash join buckets")?;
        if let Err(error) = self.rebucket(bucket_count) {
            self.memory.shrink(new_bucket_bytes);
            self.memory.shrink(other.buckets.len() * size_of::<u32>());
            return Err(error);
        }
        self.memory.shrink(replaced_bucket_bytes);
        Ok(())
    }

    /// Appends `(probe row, (build chunk, build row))` for every match.
    ///
    /// Rows are physical indices, so the caller can gather columns by them. The
    /// build reference is resolved here, where the entry is already in hand:
    /// returning entry indices and resolving them in the caller was a second
    /// pass over every match, and the pair list it filled was another ~8 bytes
    /// per row of scratch.
    ///
    /// A probe row with a null key contributes nothing.
    pub fn probe(&self, probe: &DataChunk, matches: &mut Vec<(u32, (u32, u32))>) -> Result<()> {
        self.probe_view(&DataChunkView::from_chunk(probe.clone())?, matches).map(|_| ())
    }

    /// Probes a reference-backed batch. Returned rows are logical positions in
    /// the selected input batches.
    /// Returns the probe key conversion report and resolution time in microseconds.
    /// The report excludes hash/index allocations and never mutates build metrics.
    pub fn probe_view(&self, probe: &DataChunkView, matches: &mut Vec<(u32, (u32, u32))>) -> Result<(MaterializationReport, u64)> {
        let (probe_keys, report, materialize_us) = ChunkKeys::resolve(&self.probe_keys, probe)?;
        let inline = self.inline_keys() && probe_keys.is_single_i64();
        for position in 0..probe.num_rows() {
            let row = position as u32;
            if probe_keys.any_null(row as usize) {
                continue;
            }
            let hash = probe_keys.hash(row as usize);
            let value = inline.then(|| probe_keys.single_i64(row as usize).unwrap_or_default());
            let mut head = self.buckets[bucket_of(hash, self.buckets.len())];
            while head != NONE {
                let entry = self.entries[head as usize];
                let same = self.hashes[head as usize] == hash
                    && match value {
                    Some(value) => self.keys[head as usize] == value,
                    None => self.keys_by_chunk[entry.chunk as usize].equals(
                        entry.row as usize,
                        &probe_keys,
                        row as usize,
                    ),
                };
                if same {
                    matches.push((row, (entry.chunk, entry.row)));
                }
                head = entry.next;
            }
        }
        Ok((report, materialize_us))
    }

    /// Accumulated build key conversion work, including merged build tables.
    pub(crate) fn build_key_materialization(&self) -> (MaterializationReport, u64) {
        (self.build_key_materialization, self.build_key_materialize_us)
    }

    /// Every build row the table holds, as `(chunk, row)`.
    ///
    /// Which of these went unmatched is the *operator's* business, not the
    /// table's: a probe fan-out shares one table read-only across tasks, so it
    /// cannot record what each task consumed. The operator subtracts what it
    /// saw.
    pub fn all_build_rows(&self) -> Vec<(u32, u32)> {
        let mut rows: Vec<(u32, u32)> = self
            .entries
            .iter()
            .map(|entry| (entry.chunk, entry.row))
            .collect();
        rows.extend(self.null_keyed.iter().copied());
        rows
    }

    /// The retained chunk an entry belongs to, for gathering build columns.
    pub fn build_chunk(&self, chunk: u32) -> &DataChunkView {
        &self.chunks[chunk as usize]
    }

    /// How many build chunks the table retains.
    pub fn chunk_count(&self) -> usize {
        self.chunks.len()
    }

    /// The build row a matched entry refers to.
    pub fn build_row(&self, entry: u32) -> (u32, u32) {
        let entry = self.entries[entry as usize];
        (entry.chunk, entry.row)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int32Array, Int64Array, StringArray};

    use super::*;

    fn ints(values: &[i64]) -> ArrayRef {
        Arc::new(Int64Array::from(values.to_vec()))
    }

    fn chunk(column: ArrayRef) -> DataChunk {
        DataChunk::from_vectors(vec![column])
    }

    #[test]
    fn key_reports_cover_composite_null_keys_sparse_probes_and_table_merges() {
        use common::arrow::SelectionVector;

        let numbers: ArrayRef = Arc::new(Int64Array::from(vec![Some(7), None, Some(8)]));
        let strings: ArrayRef = Arc::new(StringArray::from(vec!["A", "B", "C"]));
        let payload = ints(&[70, 0, 80]);
        let make_view = |selection| DataChunkView::from_chunk(DataChunk::new(
            vec![Arc::clone(&numbers), Arc::clone(&strings), Arc::clone(&payload)], selection,
        )).unwrap();
        let mut left = HashTable::new(JoinKeys::new(vec![0, 1]), JoinKeys::new(vec![0, 1]));
        left.insert_view(make_view(SelectionVector::new_dense(3))).unwrap();
        let (shared, shared_us) = left.build_key_materialization();
        assert_eq!(shared.shared_columns, 2);
        assert_eq!(shared.gathered_columns, 0);
        let mut right = HashTable::new(JoinKeys::new(vec![0, 1]), JoinKeys::new(vec![0, 1]));
        let sparse = make_view(SelectionVector::new_sparse(vec![2, 1, 0]));
        let (_, first) = sparse.materialize_column_with_report(0).unwrap();
        let (_, second) = sparse.materialize_column_with_report(1).unwrap();
        let expected_bytes = first.gathered_output_buffer_bytes + second.gathered_output_buffer_bytes;
        right.insert_view(sparse.clone()).unwrap();
        let (gathered, gathered_us) = right.build_key_materialization();
        assert_eq!(gathered.gathered_columns, 2);
        assert_eq!(gathered.gathered_output_buffer_bytes, expected_bytes);
        assert!(expected_bytes > 0);
        left.merge(right).unwrap();
        let mut expected = shared;
        expected.accumulate(gathered);
        assert_eq!(left.build_key_materialization(), (expected, shared_us.saturating_add(gathered_us)));
        for index in 0..2 {
            assert!(Arc::ptr_eq(left.build_chunk(index).resolve(2, 0).unwrap().unwrap().0, &payload));
        }

        let before = left.build_key_materialization();
        let mut matches = Vec::new();
        let (report, _) = left.probe_view(&sparse, &mut matches).unwrap();
        assert_eq!(report, gathered);
        matches.sort_unstable();
        assert_eq!(matches, vec![(0, (0, 2)), (0, (1, 0)), (2, (0, 0)), (2, (1, 2))]);
        assert_eq!(left.build_key_materialization(), before, "probing must not mutate the shared build table");
        matches.clear();
        let (report, _) = left.probe_view(&make_view(SelectionVector::new_dense(3)), &mut matches).unwrap();
        assert_eq!(report, shared);
        let mut incompatible = HashTable::new(JoinKeys::new(vec![2]), JoinKeys::new(vec![2]));
        incompatible.insert_view(make_view(SelectionVector::new_dense(3))).unwrap();
        assert!(left.merge(incompatible).is_err());
        assert_eq!(left.build_key_materialization(), before);
    }

    #[test]
    fn key_reports_handle_empty_batches_and_keyless_joins() {
        use common::arrow::SelectionVector;

        let empty = DataChunkView::from_chunk(DataChunk::new(vec![ints(&[1])], SelectionVector::new_sparse(vec![]))).unwrap();
        let mut table = HashTable::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
        table.insert_view(empty.clone()).unwrap();
        let expected = MaterializationReport { empty_columns: 1, ..Default::default() };
        assert_eq!(table.build_key_materialization().0, expected);
        let mut matches = Vec::new();
        assert_eq!(table.probe_view(&empty, &mut matches).unwrap().0, expected);
        assert!(matches.is_empty());

        let mut keyless = HashTable::new(JoinKeys::new(vec![]), JoinKeys::new(vec![]));
        let view = DataChunkView::from_chunk(chunk(ints(&[1, 2]))).unwrap();
        keyless.insert_view(view.clone()).unwrap();
        assert_eq!(keyless.build_key_materialization().0, MaterializationReport::default());
        assert_eq!(keyless.probe_view(&view, &mut matches).unwrap().0, MaterializationReport::default());
        assert_eq!(matches.len(), 4);
    }

    /// A probe row finds exactly the build rows whose key equals it.
    #[test]
    fn probe_finds_equal_keys() {
        let mut table = HashTable::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
        table.insert(chunk(ints(&[10, 20, 30]))).expect("insert");
        table.insert(chunk(ints(&[20, 40]))).expect("insert");

        let mut pairs = Vec::new();
        table
            .probe(&chunk(ints(&[20, 30, 50])), &mut pairs)
            .expect("probe");

        let mut matches: Vec<(u32, u32, u32)> = pairs
            .iter()
            .map(|(probe_row, (build_chunk, build_row))| (*probe_row, *build_chunk, *build_row))
            .collect();
        matches.sort();
        // Probe row 0 (key 20) matches chunk 0 row 1 and chunk 1 row 0; probe
        // row 1 (key 30) matches chunk 0 row 2; probe row 2 (50) matches nothing.
        assert_eq!(matches, vec![(0, 0, 1), (0, 1, 0), (1, 0, 2)]);
    }

    #[test]
    fn insert_rejects_key_types_without_a_hash_join_representation() {
        let mut table = HashTable::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
        let unsupported: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));

        let error = table
            .insert(chunk(unsupported))
            .expect_err("unsupported key representations must fail before probing");

        assert!(error.to_string().contains("not supported by hash join"));
        assert!(error.to_string().contains("Int32"));
        assert_eq!(table.build_key_materialization(), (MaterializationReport::default(), 0));
    }

    /// A null key never matches, on either side of the join.
    #[test]
    fn null_keys_never_match() {
        let build: ArrayRef = Arc::new(Int64Array::from(vec![Some(10), None]));
        let mut table = HashTable::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
        table
            .insert(DataChunk::from_vectors(vec![build]))
            .expect("insert");

        let probe: ArrayRef = Arc::new(Int64Array::from(vec![None, Some(10)]));
        let mut pairs = Vec::new();
        table
            .probe(&DataChunk::from_vectors(vec![probe]), &mut pairs)
            .expect("probe");

        assert_eq!(pairs.len(), 1, "only the non-null key may match");
        assert_eq!(pairs[0].0, 1, "and it is probe row 1");
    }

    /// The probe reports what it matched, and the table reports every build row:
    /// an outer join subtracts one from the other.
    #[test]
    fn the_probe_reports_its_matches_and_the_table_every_row() {
        let mut table = HashTable::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
        table.insert(chunk(ints(&[10, 20]))).expect("insert");

        let mut pairs = Vec::new();
        table.probe(&chunk(ints(&[10])), &mut pairs).expect("probe");

        assert_eq!(pairs, vec![(0, (0, 0))]);
        assert_eq!(table.all_build_rows(), vec![(0, 0), (0, 1)]);
    }

    /// Several key columns must all agree, and strings compare by value.
    #[test]
    fn multi_column_keys_and_strings_match() {
        let mut table = HashTable::new(JoinKeys::new(vec![0, 1]), JoinKeys::new(vec![0, 1]));
        table
            .insert(DataChunk::from_vectors(vec![
                ints(&[1, 1]),
                Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
            ]))
            .expect("insert");

        let mut pairs = Vec::new();
        table
            .probe(
                &DataChunk::from_vectors(vec![
                    ints(&[1]),
                    Arc::new(StringArray::from(vec!["b"])) as ArrayRef,
                ]),
                &mut pairs,
            )
            .expect("probe");
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].1, (0, 1));

        let mut none = Vec::new();
        table
            .probe(
                &DataChunk::from_vectors(vec![
                    ints(&[1]),
                    Arc::new(StringArray::from(vec!["c"])) as ArrayRef,
                ]),
                &mut none,
            )
            .expect("probe");
        assert!(none.is_empty(), "one differing column must not match");
    }

    /// The build columns are retained, not copied: the table holds the caller's
    /// `ArrayRef`, so the data exists once.
    #[test]
    fn build_columns_are_retained_not_copied() {
        let key: ArrayRef = ints(&[1, 2, 3]);
        let mut table = HashTable::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
        table
            .insert(DataChunk::from_vectors(vec![Arc::clone(&key)]))
            .expect("insert");
        let (retained, _) = table.build_chunk(0).resolve(0, 0).expect("resolve row").unwrap();
        assert!(Arc::ptr_eq(retained, &key));
    }

    /// The bucket table grows with the build side, so probing stays short-chained
    /// rather than degrading into a linear scan.
    #[test]
    fn the_table_grows_with_the_build_side() {
        let mut table = HashTable::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
        let values: Vec<i64> = (0..20_000).collect();
        table.insert(chunk(ints(&values))).expect("insert");
        assert!(
            table.buckets.len() > INITIAL_BUCKETS,
            "buckets must grow past the initial size"
        );
        assert!(
            table.buckets.len().is_power_of_two(),
            "the bucketing mask needs a power-of-two bucket count"
        );

        let mut pairs = Vec::new();
        table
            .probe(&chunk(ints(&[19_999, 0, 500])), &mut pairs)
            .expect("probe");
        assert_eq!(pairs.len(), 3, "every key must still be found after growth");
    }

    /// The key need not be at the same column position on both sides: the probe
    /// is resolved with the probe's positions, not the build's.
    #[test]
    fn the_two_sides_may_key_on_different_columns() {
        // Build columns are [payload, key]; the probe's only column is the key.
        let mut table = HashTable::new(JoinKeys::new(vec![1]), JoinKeys::new(vec![0]));
        table
            .insert(DataChunk::from_vectors(vec![
                ints(&[100, 200]),
                ints(&[7, 8]),
            ]))
            .expect("insert");

        let mut pairs = Vec::new();
        table.probe(&chunk(ints(&[8])), &mut pairs).expect("probe");

        assert_eq!(pairs.len(), 1, "key 8 must match the build's second column");
        assert_eq!(pairs[0].1, (0, 1));
    }

    /// Two tables built independently can be combined, which is what a parallel
    /// build produces: every row of both sides is still found, and the rebased
    /// chunk references resolve to the right data.
    #[test]
    fn merged_tables_find_every_row_from_both() {
        let mut left = HashTable::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
        left.insert(chunk(ints(&[10, 20]))).expect("insert");
        let mut right = HashTable::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
        right.insert(chunk(ints(&[30]))).expect("insert");
        // A null key on the merged-in side must survive the rebase too.
        let with_null: ArrayRef = Arc::new(Int64Array::from(vec![Some(40), None]));
        right
            .insert(DataChunk::from_vectors(vec![with_null]))
            .expect("insert");

        left.merge(right).expect("merge");
        assert!(
            left.buckets.len().is_power_of_two(),
            "a merge must leave the bucket count a power of two"
        );

        let mut pairs = Vec::new();
        left.probe(&chunk(ints(&[10, 30, 40, 99])), &mut pairs)
            .expect("probe");
        let mut matches: Vec<(u32, u32, u32)> = pairs
            .iter()
            .map(|(probe, (build_chunk, build_row))| (*probe, *build_chunk, *build_row))
            .collect();
        matches.sort();
        assert_eq!(
            matches,
            vec![
                // 10 is in `left`'s own chunk 0; the merged side contributes two
                // chunks, so 30 stays chunk 1 and 40 lands in chunk 2.
                (0, 0, 0),
                (1, 1, 0),
                (2, 2, 0),
            ],
            "every key must be found across both tables"
        );
        assert_eq!(
            left.all_build_rows(),
            vec![(0, 0), (0, 1), (1, 0), (2, 0), (2, 1)],
            "every row of both tables must survive the rebase, null-keyed included"
        );
    }

    #[test]
    fn composite_keys_match_only_when_every_key_column_matches() {
        let mut table = HashTable::new(JoinKeys::new(vec![0, 1]), JoinKeys::new(vec![0, 1]));
        table
            .insert(DataChunk::from_vectors(vec![
                ints(&[1, 1, 2]),
                ints(&[10, 20, 10]),
            ]))
            .expect("insert composite build keys");

        let probe = DataChunk::from_vectors(vec![ints(&[1, 1, 2, 2]), ints(&[20, 99, 10, 11])]);
        let mut pairs = Vec::new();
        table.probe(&probe, &mut pairs).expect("probe composite keys");

        let mut matches: Vec<(u32, u32)> = pairs
            .iter()
            .map(|(probe_row, (_, build_row))| (*probe_row, *build_row))
            .collect();
        matches.sort();
        assert_eq!(
            matches,
            vec![(0, 1), (2, 2)],
            "matching just one component must not produce a join match"
        );
    }

    /// Merging tables built on different keys is refused rather than comparing
    /// the wrong columns.
    #[test]
    fn merging_different_keys_is_refused() {
        let mut left = HashTable::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
        let right = HashTable::new(JoinKeys::new(vec![1]), JoinKeys::new(vec![1]));
        assert!(left.merge(right).is_err());
    }

    #[test]
    fn build_index_respects_budget_and_releases_reservation_on_error() {
        let cancel = common::cancellation::CancellationToken::new();
        let budget = QueryMemoryTracker::new(INITIAL_BUCKETS * size_of::<u32>(), cancel);
        let mut table = HashTable::with_memory_tracker(
            JoinKeys::new(vec![0]),
            JoinKeys::new(vec![0]),
            budget.clone(),
        )
            .expect("initial bucket table fits the configured budget");
        assert_eq!(budget.used(), INITIAL_BUCKETS * size_of::<u32>());

        let error = table
            .insert(chunk(ints(&[1])))
            .expect_err("the row index must not exceed the budget");
        assert!(matches!(error, VeinError::ResourceExhausted { .. }));
        drop(table);
        assert_eq!(budget.used(), 0, "failed build state releases all reservations");
    }

    #[test]
    fn retained_build_arrays_are_charged_once_across_views_and_live_through_outputs() {
        let source = ints(&[10, 20]);
        let source_bytes = source.get_array_memory_size();
        let tracker = QueryMemoryTracker::new(
            1 << 20,
            common::cancellation::CancellationToken::new(),
        );
        let mut table = HashTable::with_memory_tracker(
            JoinKeys::new(vec![0]),
            JoinKeys::new(vec![0]),
            tracker.clone(),
        )
            .expect("create budgeted table");
        let initial_bytes = tracker.used();

        for _ in 0..2 {
            let view = DataChunkView::from_chunk(chunk(Arc::clone(&source)))
                .expect("wrap retained source array");
            table.insert_view(view).expect("retain build view");
        }
        let after_build = tracker.used();
        assert!(
            after_build >= initial_bytes + source_bytes,
            "retained Arrow source array must be included in query memory"
        );

        let output = DataChunkView::compose_rows(&[table.build_chunk(0)], &[(0, 1)], 1)
            .expect("output keeps the source array reference");
        drop(table);
        assert!(
            tracker.used() >= source_bytes,
            "the result view keeps the source-array lease alive after the hash table drops"
        );
        assert_eq!(
            output.resolve(0, 0).expect("resolve output").unwrap().1,
            1,
            "the output maps to the requested source row"
        );
        drop(output);
        assert_eq!(tracker.used(), 0, "the last retaining view releases all state");
    }

    #[test]
    fn equal_sized_distinct_build_arrays_each_consume_their_own_budget() {
        let shared_tracker = QueryMemoryTracker::new(
            1 << 20,
            common::cancellation::CancellationToken::new(),
        );
        let mut shared_table = HashTable::with_memory_tracker(
            JoinKeys::new(vec![0]),
            JoinKeys::new(vec![0]),
            shared_tracker.clone(),
        )
            .expect("create shared-array table");
        let source = ints(&[10, 20]);
        let array_bytes = source.get_array_memory_size();
        let initial_shared_bytes = shared_tracker.used();

        for _ in 0..2 {
            shared_table
                .insert_view(
                    DataChunkView::from_chunk(chunk(Arc::clone(&source)))
                        .expect("shared array view"),
                )
                .expect("insert shared array");
        }
        let shared_bytes = shared_tracker.used() - initial_shared_bytes;

        let distinct_tracker = QueryMemoryTracker::new(
            1 << 20,
            common::cancellation::CancellationToken::new(),
        );
        let mut distinct_table = HashTable::with_memory_tracker(
            JoinKeys::new(vec![0]),
            JoinKeys::new(vec![0]),
            distinct_tracker.clone(),
        )
            .expect("create distinct-array table");
        let initial_distinct_bytes = distinct_tracker.used();
        for _ in 0..2 {
            distinct_table
                .insert_view(
                    DataChunkView::from_chunk(chunk(ints(&[10, 20])))
                        .expect("distinct array view"),
                )
                .expect("insert distinct array");
        }
        let distinct_bytes = distinct_tracker.used() - initial_distinct_bytes;

        assert_eq!(
            distinct_bytes - shared_bytes,
            array_bytes,
            "only the distinct source array adds another retained-array reservation"
        );
    }

    #[test]
    fn retained_build_array_budget_failure_happens_before_retaining_the_view() {
        let source = ints(&[10, 20]);
        let source_bytes = source.get_array_memory_size();
        let initial_buckets = INITIAL_BUCKETS * size_of::<u32>();
        let tracker = QueryMemoryTracker::new(
            initial_buckets + source_bytes - 1,
            common::cancellation::CancellationToken::new(),
        );
        let mut table = HashTable::with_memory_tracker(
            JoinKeys::new(vec![0]),
            JoinKeys::new(vec![0]),
            tracker.clone(),
        )
            .expect("initial buckets fit the budget");
        let view = DataChunkView::from_chunk(chunk(source)).expect("wrap build array");

        let error = table
            .insert_view(view)
            .expect_err("the retained source array must respect the query budget");
        assert!(matches!(error, VeinError::ResourceExhausted { .. }));
        assert_eq!(tracker.used(), initial_buckets, "failed insertion releases its lease");
        drop(table);
        assert_eq!(tracker.used(), 0);
    }

    #[test]
    fn merging_budgeted_tables_releases_the_replaced_bucket_allocation() {
        let tracker = QueryMemoryTracker::new(
            1 << 20,
            common::cancellation::CancellationToken::new(),
        );
        let mut left = HashTable::with_memory_tracker(
            JoinKeys::new(vec![0]),
            JoinKeys::new(vec![0]),
            tracker.clone(),
        )
            .expect("create left table");
        let mut right = HashTable::with_memory_tracker(
            JoinKeys::new(vec![0]),
            JoinKeys::new(vec![0]),
            tracker.clone(),
        )
            .expect("create right table");
        left.insert(chunk(ints(&[1]))).expect("insert left row");
        right.insert(chunk(ints(&[2]))).expect("insert right row");
        let before_merge = tracker.used();

        left.merge(right).expect("merge tables sharing one tracker");

        assert_eq!(
            tracker.used(),
            before_merge - INITIAL_BUCKETS * size_of::<u32>(),
            "merge replaces two bucket arrays with one"
        );
        drop(left);
        assert_eq!(tracker.used(), 0);
    }

    #[test]
    fn merging_tables_from_independent_budgets_transfers_accounting() {
        let left_tracker = QueryMemoryTracker::new(
            1 << 20,
            common::cancellation::CancellationToken::new(),
        );
        let right_tracker = QueryMemoryTracker::new(
            1 << 20,
            common::cancellation::CancellationToken::new(),
        );
        let mut left = HashTable::with_memory_tracker(
            JoinKeys::new(vec![0]),
            JoinKeys::new(vec![0]),
            left_tracker.clone(),
        )
            .expect("create left table");
        let mut right = HashTable::with_memory_tracker(
            JoinKeys::new(vec![0]),
            JoinKeys::new(vec![0]),
            right_tracker.clone(),
        )
            .expect("create right table");
        left.insert(chunk(ints(&[10]))).expect("insert left row");
        right.insert(chunk(ints(&[20]))).expect("insert right row");
        let left_before = left_tracker.used();
        let right_before = right_tracker.used();

        left.merge(right).expect("merge independently budgeted tables");

        assert_eq!(right_tracker.used(), 0, "source reservation was transferred");
        assert_eq!(
            left_tracker.used(),
            left_before + right_before - INITIAL_BUCKETS * size_of::<u32>(),
            "destination owns merged state and only one bucket array"
        );
        let mut pairs = Vec::new();
        left.probe(&chunk(ints(&[10, 20])), &mut pairs).expect("probe merged table");
        assert_eq!(pairs.len(), 2);
        drop(left);
        assert_eq!(left_tracker.used(), 0, "merged reservation releases with table");
    }

    #[test]
    fn merged_build_array_lease_moves_to_destination_and_outlives_the_table() {
        let left_tracker = QueryMemoryTracker::new(
            1 << 20,
            common::cancellation::CancellationToken::new(),
        );
        let right_tracker = QueryMemoryTracker::new(
            1 << 20,
            common::cancellation::CancellationToken::new(),
        );
        let mut left = HashTable::with_memory_tracker(
            JoinKeys::new(vec![0]),
            JoinKeys::new(vec![0]),
            left_tracker.clone(),
        )
            .expect("create destination table");
        let mut right = HashTable::with_memory_tracker(
            JoinKeys::new(vec![0]),
            JoinKeys::new(vec![0]),
            right_tracker.clone(),
        )
            .expect("create source table");
        let source = ints(&[20, 30]);
        let source_bytes = source.get_array_memory_size();
        right
            .insert_view(DataChunkView::from_chunk(chunk(Arc::clone(&source))).unwrap())
            .expect("retain source build array");

        left.merge(right).expect("transfer source table accounting");

        assert_eq!(right_tracker.used(), 0, "source budget releases after merge");
        assert!(
            left_tracker.used() >= source_bytes,
            "destination budget includes the merged build array"
        );
        let output = DataChunkView::compose_rows(&[left.build_chunk(0)], &[(0, 1)], 1)
            .expect("compose a view over the merged array");
        drop(left);

        assert!(
            left_tracker.used() >= source_bytes,
            "output view keeps the destination lease alive after table drop"
        );
        assert_eq!(output.resolve(0, 0).unwrap().unwrap().1, 1);
        drop(output);
        assert_eq!(left_tracker.used(), 0, "final output drop releases destination lease");
    }
}
