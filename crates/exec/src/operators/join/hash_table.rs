//! The build side of a hash join, as a table of references.
//!
//! Zero copy is the constraint this module exists to satisfy. A build row is
//! entered as a `(chunk, row)` reference — twelve bytes — pointing into chunks
//! that stay alive, and its key hash is computed once, straight from the Arrow
//! buffers. Comparing a probe key reads the build buffers directly, so probing
//! never materialises a build row and never copies a build column.
//!
//! What the table deliberately does not do: assemble output. Matching produces
//! `(probe row, build entry)` pairs, which the join operator turns into columns
//! with `take`. That is the only materialisation a join causes.

use arrow::array::{Array, Int64Array, StringArray};

use crate::pipeline::DataChunk;
use common::Result;
use error::VeinError;

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
    fn resolve(keys: &JoinKeys, chunk: &DataChunk) -> Result<Self> {
        let mut columns = Vec::with_capacity(keys.columns.len());
        for index in &keys.columns {
            let array = chunk.column(*index);
            let resolved = if let Some(ints) = array.as_any().downcast_ref::<Int64Array>() {
                KeyArray::Int64(ints.clone())
            } else if let Some(strings) = array.as_any().downcast_ref::<StringArray>() {
                KeyArray::Utf8(strings.clone())
            } else {
                return Err(VeinError::internal(format!(
                    "join key column {index} is {:?}, which hashing does not cover yet",
                    array.data_type()
                )));
            };
            columns.push(resolved);
        }
        Ok(Self { columns })
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
#[derive(Debug, Clone)]
pub struct HashTable {
    /// Key columns of a build chunk.
    build_keys: JoinKeys,
    /// Key columns of a probe chunk, resolved separately because the key need
    /// not sit at the same position on each side.
    probe_keys: JoinKeys,
    chunks: Vec<DataChunk>,
    keys_by_chunk: Vec<ChunkKeys>,
    entries: Vec<Entry>,
    hashes: Vec<u64>,
    buckets: Vec<u32>,
    matched: Vec<u64>,
    /// Build rows whose key is null: entered, but never bucketed, because no
    /// probe row may match them and an outer join still has to emit them.
    null_keyed: Vec<(u32, u32)>,
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
        Self {
            build_keys,
            probe_keys,
            chunks: Vec::new(),
            keys_by_chunk: Vec::new(),
            entries: Vec::new(),
            hashes: Vec::new(),
            buckets: vec![NONE; INITIAL_BUCKETS],
            matched: Vec::new(),
            null_keyed: Vec::new(),
        }
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
        let keys = ChunkKeys::resolve(&self.build_keys, &chunk)?;
        let chunk_index = self.chunks.len() as u32;
        // The keys stay a local, so the loop below can mutate the table's
        // vectors while still reading the chunk's resolved keys. Building a
        // `(row, hash, is_null)` scratch vector instead cost 24 bytes per build
        // row — three times what the table itself stores.
        self.chunks.push(chunk);

        for position in 0..self.chunks[chunk_index as usize].selection.size() {
            let row = self.chunks[chunk_index as usize].selection.get_index(position) as u32;
            if keys.any_null(row as usize) {
                self.null_keyed.push((chunk_index, row));
                continue;
            }
            // Grown per row rather than sized up front, so no count pass is
            // needed; the doubling makes it amortised constant.
            self.grow_if_needed(1);
            let hash = keys.hash(row as usize);
            let index = self.entries.len() as u32;
            let bucket = (hash as usize) % self.buckets.len();
            let next = self.buckets[bucket];
            self.buckets[bucket] = index;
            self.entries.push(Entry {
                chunk: chunk_index,
                row,
                next,
            });
            self.hashes.push(hash);
        }
        self.keys_by_chunk.push(keys);
        self.matched.resize(self.entries.len().div_ceil(64), 0);
        Ok(())
    }

    /// Grows the bucket table so chains stay short.
    ///
    /// Called once per chunk, sized for the entries about to arrive, so a large
    /// build side does not degrade into one long chain per bucket. Null-keyed
    /// rows are not in the table to rehash.
    fn grow_if_needed(&mut self, additional: usize) {
        let wanted = (self.entries.len() + additional) * 2;
        if wanted <= self.buckets.len() {
            return;
        }
        let mut size = self.buckets.len();
        while size < wanted {
            size *= 2;
        }
        self.buckets = vec![NONE; size];
        for index in 0..self.entries.len() {
            let bucket = (self.hashes[index] as usize) % size;
            self.entries[index].next = self.buckets[bucket];
            self.buckets[bucket] = index as u32;
        }
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
    pub fn probe(&mut self, probe: &DataChunk, matches: &mut Vec<(u32, (u32, u32))>) -> Result<()> {
        let probe_keys = ChunkKeys::resolve(&self.probe_keys, probe)?;
        for position in 0..probe.selection.size() {
            let row = probe.selection.get_index(position) as u32;
            if probe_keys.any_null(row as usize) {
                continue;
            }
            let hash = probe_keys.hash(row as usize);
            let mut head = self.buckets[(hash as usize) % self.buckets.len()];
            while head != NONE {
                let entry = self.entries[head as usize];
                if self.hashes[head as usize] == hash
                    && self.keys_by_chunk[entry.chunk as usize].equals(
                        entry.row as usize,
                        &probe_keys,
                        row as usize,
                    )
                {
                    matches.push((row, (entry.chunk, entry.row)));
                    self.mark_matched(head as usize);
                }
                head = entry.next;
            }
        }
        Ok(())
    }

    /// The build rows no probe row matched, as `(chunk, row)`.
    ///
    /// An outer join emits these with the probe columns null.
    pub fn unmatched_build_rows(&self) -> Vec<(u32, u32)> {
        let mut unmatched = Vec::new();
        for index in 0..self.entries.len() {
            if !self.is_matched(index) {
                let entry = self.entries[index];
                unmatched.push((entry.chunk, entry.row));
            }
        }
        unmatched.extend(self.null_keyed.iter().copied());
        unmatched
    }

    /// The retained chunk an entry belongs to, for gathering build columns.
    pub fn build_chunk(&self, chunk: u32) -> &DataChunk {
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

    fn mark_matched(&mut self, entry: usize) {
        self.matched[entry / 64] |= 1u64 << (entry % 64);
    }

    fn is_matched(&self, entry: usize) -> bool {
        self.matched
            .get(entry / 64)
            .is_some_and(|word| (word >> (entry % 64)) & 1 == 1)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array, StringArray};

    use super::*;

    fn ints(values: &[i64]) -> ArrayRef {
        Arc::new(Int64Array::from(values.to_vec()))
    }

    fn chunk(column: ArrayRef) -> DataChunk {
        DataChunk::from_vectors(vec![column])
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

    /// Build rows nothing matched are reported, so an outer join can emit them.
    #[test]
    fn unmatched_build_rows_are_reported() {
        let mut table = HashTable::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
        table.insert(chunk(ints(&[10, 20]))).expect("insert");

        let mut pairs = Vec::new();
        table.probe(&chunk(ints(&[10])), &mut pairs).expect("probe");

        assert_eq!(table.unmatched_build_rows(), vec![(0, 1)]);
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
        assert!(Arc::ptr_eq(table.build_chunk(0).column(0), &key));
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
}
