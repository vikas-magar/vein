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

use std::collections::HashSet;

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
    /// The build rows a probe has consumed, recorded only when the join flavour
    /// needs to know which rows went unmatched. A set rather than a bitset over
    /// entries because probing no longer has an entry index in hand — it reports
    /// `(chunk, row)`, and that is what lets `probe` borrow the table immutably
    /// and several probe tasks share one table.
    matched: HashSet<(u32, u32)>,
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
            matched: HashSet::new(),
            null_keyed: Vec::new(),
            keys: Vec::new(),
        }
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
            if let Some(value) = keys.single_i64(row as usize) {
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
    fn grow_if_needed(&mut self, additional: usize) {
        let wanted = (self.entries.len() + additional) * 2;
        if wanted <= self.buckets.len() {
            return;
        }
        let mut size = self.buckets.len();
        while size < wanted {
            size *= 2;
        }
        self.rebucket(size);
    }

    /// Rebuilds the buckets from the stored hashes.
    ///
    /// O(entries) and allocation-free apart from the bucket vector itself: the
    /// hashes are already computed, so nothing is re-hashed. Null-keyed rows are
    /// not in `entries`, so they are correctly left out.
    fn rebucket(&mut self, size: usize) {
        self.buckets = vec![NONE; size];
        for index in 0..self.entries.len() {
            let bucket = (self.hashes[index] as usize) % size;
            self.entries[index].next = self.buckets[bucket];
            self.buckets[bucket] = index as u32;
        }
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
    pub fn merge(&mut self, other: HashTable) -> Result<()> {
        if self.build_keys != other.build_keys || self.probe_keys != other.probe_keys {
            return Err(VeinError::internal(
                "cannot merge hash tables built on different keys",
            ));
        }

        let self_inline = self.inline_keys();
        let other_inline = other.inline_keys();
        let offset = self.chunks.len() as u32;

        self.chunks.extend(other.chunks);
        self.keys_by_chunk.extend(other.keys_by_chunk);
        self.entries.extend(other.entries.into_iter().map(|entry| Entry {
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

        self.matched.extend(
            other
                .matched
                .into_iter()
                .map(|(chunk, row)| (chunk + offset, row)),
        );
        self.rebucket(self.buckets.len().max(self.entries.len() * 2).max(INITIAL_BUCKETS));
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
        let probe_keys = ChunkKeys::resolve(&self.probe_keys, probe)?;
        let inline = self.inline_keys() && probe_keys.is_single_i64();
        for position in 0..probe.selection.size() {
            let row = probe.selection.get_index(position) as u32;
            if probe_keys.any_null(row as usize) {
                continue;
            }
            let hash = probe_keys.hash(row as usize);
            let value = inline.then(|| probe_keys.single_i64(row as usize).unwrap_or_default());
            let mut head = self.buckets[(hash as usize) % self.buckets.len()];
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
                if same
                {
                    matches.push((row, (entry.chunk, entry.row)));
                }
                head = entry.next;
            }
        }
        Ok(())
    }

    /// Records the build rows `matches` consumed.
    ///
    /// Deliberately separate from [`Self::probe`]: probing must borrow the table
    /// immutably so it can be shared by concurrent tasks, and only the right and
    /// full flavours need to know what went unmatched.
    pub fn record_matched(&mut self, matches: &[(u32, (u32, u32))]) {
        self.matched
            .extend(matches.iter().map(|(_, build)| *build));
    }

    /// The build rows no probe row matched, as `(chunk, row)`.
    ///
    /// An outer join emits these with the probe columns null.
    pub fn unmatched_build_rows(&self) -> Vec<(u32, u32)> {
        let mut unmatched = Vec::new();
        for entry in &self.entries {
            if !self.matched.contains(&(entry.chunk, entry.row)) {
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
        table.record_matched(&pairs);

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

        let mut pairs = Vec::new();
        left.probe(&chunk(ints(&[10, 30, 40, 99])), &mut pairs)
            .expect("probe");
        left.record_matched(&pairs);
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
            left.unmatched_build_rows(),
            vec![(0, 1), (2, 1)],
            "20 and the null-keyed row are the only unmatched build rows"
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
}
