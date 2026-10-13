//! The build side of a hash join, as a table of references.
//!
//! Build payloads stay in reference-backed views. Key resolution shares contiguous
//! Arrow buffers or gathers mapped key rows; the single-Int64 fast path also
//! copies keys into the hash index. These allocations are distinct from payload
//! copying. Probe keys are execution-local, keeping the shared table immutable.
//!
//! Matching produces row references. The join operator composes output views;
//! ordinary Arrow payload arrays are requested only at an explicit boundary.

use arrow::array::{Array, ArrayRef, BooleanArray, Int64Array, StringArray, builder::BooleanBuilder};
use arrow::record_batch::RecordBatch;
use std::cmp::Ordering;
use std::mem::size_of;
use std::sync::Arc;
use std::time::Instant;

use crate::memory::{MemoryReservation, QueryMemoryTracker};
use crate::pipeline::{DataChunk, DataChunkView, MaterializationReport};
use crate::runtime_filter::{RuntimeFilter, RuntimeFilterBuildInput, RuntimeFilterFactory};
use common::Result;
use error::VeinError;
use types::{ColumnId, HashJoinKeyType, LogicalLiteral, VeinDataType};

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

    #[inline]
    fn compare_values(&self, row: usize, other: &KeyArray, other_row: usize) -> Option<Ordering> {
        match (self, other) {
            (KeyArray::Int64(left), KeyArray::Int64(right)) => {
                Some(left.value(row).cmp(&right.value(other_row)))
            }
            (KeyArray::Utf8(left), KeyArray::Utf8(right)) => {
                Some(left.value(row).cmp(right.value(other_row)))
            }
            _ => None,
        }
    }

    fn logical_literal(&self, row: usize) -> Result<LogicalLiteral> {
        match self {
            KeyArray::Int64(array) => Ok(LogicalLiteral::Int64(array.value(row))),
            KeyArray::Utf8(array) => Ok(LogicalLiteral::String(array.value(row).to_owned())),
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
            columns.push(Self::resolve_array(*index, array)?);
        }
        Ok((Self { columns }, report, u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX)))
    }

    /// Resolve Arrow references without copying their value buffers.
    fn resolve_arrays(columns: &[ArrayRef]) -> Result<Self> {
        let columns = columns
            .iter()
            .enumerate()
            .map(|(index, array)| Self::resolve_array(index, Arc::clone(array)))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { columns })
    }

    fn resolve_array(index: usize, array: ArrayRef) -> Result<KeyArray> {
        let data_type = VeinDataType::try_from(array.data_type()).map_err(|error| {
            VeinError::internal(format!(
                "join key column {index} has an unsupported Arrow type {:?}: {error}",
                array.data_type()
            ))
        })?;
        match data_type.hash_join_key_type() {
            Some(HashJoinKeyType::Int64) => {
                let ints = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                    VeinError::internal(format!(
                        "join key column {index} does not match its declared Int64 type"
                    ))
                })?;
                Ok(KeyArray::Int64(ints.clone()))
            }
            Some(HashJoinKeyType::Utf8) => {
                let strings = array.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                    VeinError::internal(format!(
                        "join key column {index} does not match its declared Utf8 type"
                    ))
                })?;
                Ok(KeyArray::Utf8(strings.clone()))
            }
            None => Err(VeinError::internal(format!(
                "join key column {index} has type {:?}, which is not supported by hash join",
                array.data_type()
            ))),
        }
    }

    /// Whether any key column is null at `row`.
    ///
    /// Build insertion uses this lightweight pass to count null rows before it
    /// reserves index memory, keeping an allocation failure from leaving a
    /// partially inserted batch.
    fn any_null(&self, row: usize) -> bool {
        self.columns.iter().any(|column| column.is_null(row))
    }

    /// Encodes one row's key in one pass over its key columns.
    ///
    /// Null keys do not participate in equality joins. For a single Int64 key,
    /// return the inline value alongside its hash so callers do not read the
    /// same Arrow value again just to prepare the fast comparison path.
    #[inline]
    fn hash_and_single_i64(&self, row: usize) -> Option<(u64, Option<i64>)> {
        if let [KeyArray::Int64(array)] = self.columns.as_slice() {
            if array.is_null(row) {
                return None;
            }
            let value = array.value(row);
            let hash = fnv1a(&value.to_ne_bytes(), FNV_OFFSET);
            return Some((hash, Some(value)));
        }

        let mut hash = FNV_OFFSET;
        for column in &self.columns {
            if column.is_null(row) {
                return None;
            }
            hash = column.hash_into(row, hash);
        }
        Some((hash, None))
    }

    /// Whether this is a single `Int64` key, the shape that can be compared as a
    /// plain value instead of through the encoding.
    fn is_single_i64(&self) -> bool {
        matches!(self.columns.as_slice(), [KeyArray::Int64(_)])
    }

    fn equals(&self, row: usize, other: &Self, other_row: usize) -> bool {
        self.columns
            .iter()
            .zip(&other.columns)
            .all(|(left, right)| left.value_eq(row, right, other_row))
    }

    fn has_compatible_types(&self, other: &Self) -> bool {
        self.columns.len() == other.columns.len()
            && self
                .columns
                .iter()
                .zip(&other.columns)
                .all(|(left, right)| matches!(
                    (left, right),
                    (KeyArray::Int64(_), KeyArray::Int64(_))
                        | (KeyArray::Utf8(_), KeyArray::Utf8(_))
                ))
    }
}

/// Configuration for the default join-key Bloom runtime filter.
#[derive(Debug, Clone, Copy)]
pub struct JoinBloomFilterConfig {
    false_positive_rate: f64,
    max_bytes: Option<usize>,
}

impl JoinBloomFilterConfig {
    /// Configure the target false-positive rate and optional retained-byte cap.
    ///
    /// The rate must be finite and strictly between zero and one. The query
    /// memory tracker remains the authoritative limit even when `max_bytes` is
    /// unset.
    pub fn new(false_positive_rate: f64, max_bytes: Option<usize>) -> Result<Self> {
        if !false_positive_rate.is_finite() || !(0.0..1.0).contains(&false_positive_rate) {
            return Err(VeinError::internal(
                "join Bloom filter false-positive rate must be finite and between zero and one",
            ));
        }
        Ok(Self {
            false_positive_rate,
            max_bytes,
        })
    }

    /// Target false-positive rate used to size new filters.
    pub fn false_positive_rate(&self) -> f64 {
        self.false_positive_rate
    }

    /// Optional per-filter allocation cap.
    pub fn max_bytes(&self) -> Option<usize> {
        self.max_bytes
    }
}

impl Default for JoinBloomFilterConfig {
    fn default() -> Self {
        Self {
            false_positive_rate: 0.01,
            max_bytes: None,
        }
    }
}

/// Default runtime-filter factory for hash joins.
#[derive(Debug, Default)]
pub struct JoinBloomFilterFactory {
    config: JoinBloomFilterConfig,
}

impl JoinBloomFilterFactory {
    /// Create a factory with explicit sizing policy.
    pub fn with_config(config: JoinBloomFilterConfig) -> Self {
        Self { config }
    }
}

impl RuntimeFilterFactory for JoinBloomFilterFactory {
    fn build_filters(
        &self,
        input: RuntimeFilterBuildInput<'_>,
    ) -> Result<Vec<Arc<dyn RuntimeFilter>>> {
        let RuntimeFilterBuildInput {
            probe_columns,
            build_hashes,
            build_key_bounds,
            memory,
        } = input;
        if probe_columns.is_empty() {
            return Ok(Vec::new());
        }

        let mut filters: Vec<Arc<dyn RuntimeFilter>> = Vec::with_capacity(2);
        if let Some(filter) = JoinKeyRangeFilter::try_new(
            probe_columns,
            build_key_bounds,
            memory,
        )? {
            filters.push(Arc::new(filter));
        }

        let Some((bit_count, word_count, hash_functions)) =
            bloom_layout(build_hashes.len(), self.config.false_positive_rate)
        else {
            tracing::debug!(
                target: "vein.audit",
                event = "join_runtime_filter_skipped",
                reason = "size_overflow",
                build_keys = build_hashes.len(),
                "join runtime filter skipped"
            );
            return Ok(filters);
        };
        let Some(bytes) = word_count.checked_mul(size_of::<u64>()) else {
            return Ok(filters);
        };
        if self.config.max_bytes.is_some_and(|limit| bytes > limit) {
            tracing::debug!(
                target: "vein.audit",
                event = "join_runtime_filter_skipped",
                reason = "configured_memory_cap",
                required_bytes = bytes,
                "join runtime filter skipped"
            );
            return Ok(filters);
        }

        let mut reservation = match memory.reserve(bytes, "join runtime filter") {
            Ok(reservation) => reservation,
            Err(VeinError::ResourceExhausted { .. }) => {
                tracing::debug!(
                    target: "vein.audit",
                    event = "join_runtime_filter_skipped",
                    reason = "query_memory_budget",
                    required_bytes = bytes,
                    "join runtime filter skipped"
                );
                return Ok(filters);
            }
            Err(error) => return Err(error),
        };

        let mut words = Vec::new();
        if words.try_reserve_exact(word_count).is_err() {
            tracing::debug!(
                target: "vein.audit",
                event = "join_runtime_filter_skipped",
                reason = "allocation_failed",
                required_bytes = bytes,
                "join runtime filter skipped"
            );
            return Ok(filters);
        }
        words.resize(word_count, 0u64);
        let actual_bytes = words.capacity().saturating_mul(size_of::<u64>());
        if self.config.max_bytes.is_some_and(|limit| actual_bytes > limit) {
            return Ok(filters);
        }
        if actual_bytes > reservation.bytes() {
            if let Err(error) =
                reservation.grow(actual_bytes - reservation.bytes(), "join runtime filter")
            {
                if matches!(error, VeinError::ResourceExhausted { .. }) {
                    tracing::debug!(
                        target: "vein.audit",
                        event = "join_runtime_filter_skipped",
                        reason = "query_memory_budget",
                        required_bytes = actual_bytes,
                        "join runtime filter skipped"
                    );
                    return Ok(Vec::new());
                }
                return Err(error);
            }
        } else if reservation.bytes() > actual_bytes {
            reservation.shrink(reservation.bytes() - actual_bytes);
        }

        let mut filter = JoinBloomFilter {
            columns: Arc::from(probe_columns),
            words,
            bit_mask: bit_count.saturating_sub(1),
            hash_functions,
            memory: reservation,
        };
        for hash in build_hashes {
            filter.insert(*hash);
        }
        filters.push(Arc::new(filter));
        Ok(filters)
    }
}

fn bloom_layout(items: usize, false_positive_rate: f64) -> Option<(usize, usize, usize)> {
    if items == 0 {
        return Some((0, 0, 0));
    }
    let item_count = items as f64;
    let ln_two = std::f64::consts::LN_2;
    let desired_bits = (-(item_count * false_positive_rate.ln()) / (ln_two * ln_two)).ceil();
    if !desired_bits.is_finite() || desired_bits < 1.0 || desired_bits > (usize::MAX / 2) as f64 {
        return None;
    }
    let bit_count = (desired_bits as usize).checked_next_power_of_two()?;
    let word_count = bit_count.checked_add(u64::BITS as usize - 1)? / u64::BITS as usize;
    let actual_bits = word_count.checked_mul(u64::BITS as usize)?;
    let hash_functions = ((actual_bits as f64 / item_count) * ln_two).round();
    if !hash_functions.is_finite() || hash_functions < 1.0 || hash_functions > usize::MAX as f64 {
        return None;
    }
    Some((actual_bits, word_count, hash_functions as usize))
}

/// Bloom filter over the join's existing canonical row hashes.
#[derive(Debug)]
struct JoinBloomFilter {
    columns: Arc<[ColumnId]>,
    words: Vec<u64>,
    bit_mask: usize,
    hash_functions: usize,
    memory: MemoryReservation,
}

impl JoinBloomFilter {
    #[inline]
    fn bit_positions(&self, hash: u64, mut visit: impl FnMut(usize)) {
        if self.words.is_empty() {
            return;
        }
        let step = hash.rotate_left(32) | 1;
        for iteration in 0..self.hash_functions {
            let bit = hash
                .wrapping_add((iteration as u64).wrapping_mul(step)) as usize
                & self.bit_mask;
            visit(bit);
        }
    }

    fn insert(&mut self, hash: u64) {
        let step = hash.rotate_left(32) | 1;
        for iteration in 0..self.hash_functions {
            let bit = hash
                .wrapping_add((iteration as u64).wrapping_mul(step)) as usize
                & self.bit_mask;
            self.words[bit / u64::BITS as usize] |= 1u64 << (bit % u64::BITS as usize);
        }
    }

    fn may_contain(&self, hash: u64) -> bool {
        if self.words.is_empty() {
            return false;
        }
        let mut present = true;
        self.bit_positions(hash, |bit| {
            present &= self.words[bit / u64::BITS as usize]
                & (1u64 << (bit % u64::BITS as usize))
                != 0;
        });
        present
    }
}

impl RuntimeFilter for JoinBloomFilter {
    fn name(&self) -> &'static str {
        "join_hash_bloom"
    }

    fn required_columns(&self) -> &[ColumnId] {
        &self.columns
    }

    fn memory_bytes(&self) -> usize {
        self.memory.bytes()
    }

    fn evaluate(
        &self,
        batch: &RecordBatch,
        column_positions: &[usize],
    ) -> Result<BooleanArray> {
        if column_positions.len() != self.columns.len() {
            return Err(VeinError::internal(
                "runtime filter key positions do not match its declared columns",
            ));
        }
        let arrays = column_positions
            .iter()
            .map(|position| {
                batch
                    .columns()
                    .get(*position)
                    .cloned()
                    .ok_or_else(|| VeinError::internal("runtime filter key position is out of range"))
            })
            .collect::<Result<Vec<_>>>()?;
        let keys = ChunkKeys::resolve_arrays(&arrays)?;
        let mut mask = BooleanBuilder::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let may_match = keys
                .hash_and_single_i64(row)
                .is_some_and(|(hash, _)| self.may_contain(hash));
            mask.append_value(may_match);
        }
        Ok(mask.finish())
    }
}

/// Inclusive range bounds over the build-side join keys. The filter is
/// statistics-only: row correctness continues to be checked by the hash join,
/// and the Bloom filter remains the row-level early rejection path.
#[derive(Debug)]
struct JoinKeyRangeFilter {
    columns: Arc<[ColumnId]>,
    bounds: Vec<Option<(LogicalLiteral, LogicalLiteral)>>,
    memory: MemoryReservation,
}

impl JoinKeyRangeFilter {
    fn try_new(
        columns: &[ColumnId],
        bounds: Vec<Option<(LogicalLiteral, LogicalLiteral)>>,
        tracker: &QueryMemoryTracker,
    ) -> Result<Option<Self>> {
        if bounds.len() != columns.len() || !bounds.iter().any(Option::is_some) {
            return Ok(None);
        }

        let scalar_bytes = bounds
            .iter()
            .flatten()
            .flat_map(|(min, max)| [min, max])
            .map(logical_literal_heap_bytes)
            .fold(0usize, usize::saturating_add);
        let retained_bytes = size_of::<Self>()
            .saturating_add(columns.len().saturating_mul(size_of::<ColumnId>()))
            .saturating_add(
                bounds
                    .capacity()
                    .saturating_mul(size_of::<Option<(LogicalLiteral, LogicalLiteral)>>()),
            )
            .saturating_add(scalar_bytes);
        let memory = match tracker.reserve(retained_bytes, "join key range filter") {
            Ok(memory) => memory,
            Err(VeinError::ResourceExhausted { .. }) => {
                tracing::debug!(
                    target: "vein.audit",
                    event = "join_runtime_filter_skipped",
                    filter = "join_key_range",
                    reason = "query_memory_budget",
                    required_bytes = retained_bytes,
                    "join key range filter skipped"
                );
                return Ok(None);
            }
            Err(error) => return Err(error),
        };

        Ok(Some(Self {
            columns: Arc::from(columns),
            bounds,
            memory,
        }))
    }
}

fn logical_literal_heap_bytes(value: &LogicalLiteral) -> usize {
    match value {
        LogicalLiteral::String(value) => value.capacity(),
        _ => 0,
    }
}

impl RuntimeFilter for JoinKeyRangeFilter {
    fn name(&self) -> &'static str {
        "join_key_range"
    }

    fn required_columns(&self) -> &[ColumnId] {
        &self.columns
    }

    fn memory_bytes(&self) -> usize {
        self.memory.bytes()
    }

    fn evaluate(
        &self,
        _batch: &RecordBatch,
        _column_positions: &[usize],
    ) -> Result<BooleanArray> {
        Err(VeinError::internal(
            "statistics-only join key range filter cannot evaluate rows",
        ))
    }

    fn supports_row_evaluation(&self) -> bool {
        false
    }

    fn statistics_predicates(&self, schema: &types::VeinSchema) -> Vec<catalog::StatisticsPredicate> {
        let mut predicates = Vec::with_capacity(self.bounds.len().saturating_mul(2));
        for (column_id, bounds) in self.columns.iter().zip(&self.bounds) {
            let Some((min, max)) = bounds else {
                continue;
            };
            let Some(column) = schema.fields.iter().find(|column| column.id == *column_id) else {
                continue;
            };
            predicates.push(catalog::StatisticsPredicate::Comparison {
                column: column.name.clone(),
                comparison: catalog::StatisticsComparison::GreaterOrEqual,
                literal: min.clone(),
            });
            predicates.push(catalog::StatisticsPredicate::Comparison {
                column: column.name.clone(),
                comparison: catalog::StatisticsComparison::LessOrEqual,
                literal: max.clone(),
            });
        }
        predicates
    }
}

/// One build row: a reference into a retained chunk.
#[derive(Debug, Clone, Copy)]
struct Entry {
    chunk: u32,
    row: u32,
    next: u32,
}

/// A build key endpoint kept as a row reference into an already-retained key
/// array. Tracking endpoints this way avoids copying or retaining per-row key
/// values solely for runtime filtering.
#[derive(Debug, Clone, Copy)]
struct BuildRowReference {
    chunk: u32,
    row: u32,
}

impl BuildRowReference {
    fn rebase(self, offset: u32) -> Result<Self> {
        Ok(Self {
            chunk: self
                .chunk
                .checked_add(offset)
                .ok_or_else(|| VeinError::internal("merged join key bound chunk index overflow"))?,
            row: self.row,
        })
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct BuildKeyBounds {
    min: Option<BuildRowReference>,
    max: Option<BuildRowReference>,
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
    /// Per-key min/max endpoints stored as references into retained build arrays.
    key_bounds: Vec<BuildKeyBounds>,
    tracker: QueryMemoryTracker,
    memory: MemoryReservation,
}

impl HashTable {
    /// Computes the join's canonical row hashes for a spill batch.
    ///
    /// Null keys receive a deterministic hash only to choose a spill partition;
    /// they remain non-matching because [`Self::probe_view`] skips null keys.
    /// Keeping this beside the in-memory key encoder prevents spill partitioning
    /// from introducing a second, potentially incompatible key representation.
    pub fn hashes_for_view(view: &DataChunkView, keys: &JoinKeys) -> Result<Vec<u64>> {
        let (resolved, _, _) = ChunkKeys::resolve(keys, view)?;
        let mut hashes = Vec::new();
        hashes.try_reserve_exact(view.num_rows()).map_err(|error| {
            VeinError::internal(format!("cannot reserve join spill hashes: {error}"))
        })?;
        for row in 0..view.num_rows() {
            hashes.push(
                resolved
                    .hash_and_single_i64(row)
                    .map_or(FNV_OFFSET, |(hash, _)| hash),
            );
        }
        Ok(hashes)
    }

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
        let bounds_bytes = build_keys
            .columns
            .len()
            .checked_mul(size_of::<BuildKeyBounds>())
            .ok_or_else(|| VeinError::internal("initial join key bounds size overflow"))?;
        let initial_bytes = bucket_bytes
            .checked_add(bounds_bytes)
            .ok_or_else(|| VeinError::internal("initial hash join state size overflow"))?;
        let mut memory = tracker.reserve(initial_bytes, "hash join initial state")?;
        let mut buckets = Vec::new();
        buckets.try_reserve_exact(INITIAL_BUCKETS).map_err(|error| {
            VeinError::internal(format!("cannot allocate initial hash join buckets: {error}"))
        })?;
        buckets.resize(INITIAL_BUCKETS, NONE);
        let mut key_bounds = Vec::new();
        key_bounds
            .try_reserve_exact(build_keys.columns.len())
            .map_err(|error| {
                VeinError::internal(format!("cannot allocate initial join key bounds: {error}"))
            })?;
        key_bounds.resize(build_keys.columns.len(), BuildKeyBounds::default());
        let actual_bounds_bytes = key_bounds.capacity() * size_of::<BuildKeyBounds>();
        if actual_bounds_bytes > bounds_bytes {
            memory.grow(actual_bounds_bytes - bounds_bytes, "hash join key bounds")?;
        } else if bounds_bytes > actual_bounds_bytes {
            memory.shrink(bounds_bytes - actual_bounds_bytes);
        }
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
            key_bounds,
            tracker: tracker.clone(),
            memory,
        })
    }

    /// Whether the build key is a single `Int64`, so [`Self::keys`] is usable.
    fn inline_keys(&self) -> bool {
        self.keys.len() == self.entries.len()
    }

    fn key_array(&self, reference: BuildRowReference, key_index: usize) -> Result<&KeyArray> {
        self.keys_by_chunk
            .get(reference.chunk as usize)
            .and_then(|keys| keys.columns.get(key_index))
            .ok_or_else(|| VeinError::internal("join key bound points outside retained key arrays"))
    }

    fn compare_build_rows(
        &self,
        key_index: usize,
        left: BuildRowReference,
        right: BuildRowReference,
    ) -> Result<Ordering> {
        self.key_array(left, key_index)?
            .compare_values(
                left.row as usize,
                self.key_array(right, key_index)?,
                right.row as usize,
            )
            .ok_or_else(|| VeinError::internal("join key bound type changed during build"))
    }

    fn update_key_bounds(&mut self, current: BuildRowReference) -> Result<()> {
        for key_index in 0..self.key_bounds.len() {
            let Some(min) = self.key_bounds[key_index].min else {
                self.key_bounds[key_index] = BuildKeyBounds {
                    min: Some(current),
                    max: Some(current),
                };
                continue;
            };
            let max = self.key_bounds[key_index]
                .max
                .ok_or_else(|| VeinError::internal("join key bounds have an incomplete endpoint"))?;
            let replace_min = self.compare_build_rows(key_index, current, min)? == Ordering::Less;
            let replace_max = self.compare_build_rows(key_index, current, max)? == Ordering::Greater;
            if replace_min {
                self.key_bounds[key_index].min = Some(current);
            }
            if replace_max {
                self.key_bounds[key_index].max = Some(current);
            }
        }
        Ok(())
    }

    /// How many build rows are entered, null-keyed ones included.
    pub fn len(&self) -> usize {
        self.entries.len() + self.null_keyed.len()
    }

    /// Canonical hashes for the non-null build keys already indexed by this
    /// table. Runtime filters reuse these values instead of rescanning build
    /// payload columns or maintaining a second key encoder.
    pub fn runtime_filter_hashes(&self) -> &[u64] {
        &self.hashes
    }

    /// Inclusive bounds of each build key, derived incrementally while rows
    /// enter the hash table. Endpoints are copied only as typed scalar
    /// metadata; the table itself stores row references into retained arrays.
    pub fn runtime_key_bounds(
        &self,
    ) -> Result<Vec<Option<(LogicalLiteral, LogicalLiteral)>>> {
        let mut bounds = Vec::new();
        bounds
            .try_reserve_exact(self.key_bounds.len())
            .map_err(|error| VeinError::internal(format!("cannot allocate join key bounds snapshot: {error}")))?;
        for key_bounds in &self.key_bounds {
            let (Some(min), Some(max)) = (key_bounds.min, key_bounds.max) else {
                bounds.push(None);
                continue;
            };
            let min_column = self.key_array(min, bounds.len())?;
            let max_column = self.key_array(max, bounds.len())?;
            bounds.push(Some((
                min_column.logical_literal(min.row as usize)?,
                max_column.logical_literal(max.row as usize)?,
            )));
        }
        Ok(bounds)
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
        if self
            .keys_by_chunk
            .first()
            .is_some_and(|existing| !existing.has_compatible_types(&keys))
        {
            return Err(VeinError::internal(
                "hash join build key types changed between input batches",
            ));
        }
        let row_count = chunk.num_rows();
        if row_count > u32::MAX as usize || self.chunks.len() > u32::MAX as usize {
            return Err(VeinError::internal(
                "hash join build input exceeds the row-reference index range",
            ));
        }

        // Reserve the complete insertion before changing any index vectors.
        // This makes a budget failure retryable by the external hash join: the
        // caller can spill the retained input views without recovering a
        // partially indexed batch. Counting nulls is cheaper than retaining a
        // per-row scratch vector and does not copy payload data.
        let null_rows = (0..row_count)
            .filter(|row| keys.any_null(*row))
            .count();
        let indexed_rows = row_count - null_rows;
        let inline_key_bytes = if keys.is_single_i64() {
            size_of::<i64>()
        } else {
            0
        };
        let entry_bytes = size_of::<Entry>()
            .checked_add(size_of::<u64>())
            .and_then(|bytes| bytes.checked_add(inline_key_bytes))
            .ok_or_else(|| VeinError::internal("join index entry size overflow"))?;
        let index_bytes = size_of::<DataChunkView>()
            .checked_add(null_rows.checked_mul(size_of::<(u32, u32)>()).ok_or_else(|| {
                VeinError::internal("join null-key index size overflow")
            })?)
            .and_then(|bytes| bytes.checked_add(indexed_rows.checked_mul(entry_bytes)?))
            .ok_or_else(|| VeinError::internal("join index batch size overflow"))?;

        let wanted = self
            .entries
            .len()
            .checked_add(indexed_rows)
            .and_then(|entries| entries.checked_mul(2))
            .ok_or_else(|| VeinError::internal("hash join bucket count overflow"))?;
        let mut bucket_count = self.buckets.len();
        while bucket_count < wanted {
            bucket_count = bucket_count
                .checked_mul(2)
                .ok_or_else(|| VeinError::internal("hash join bucket count overflow"))?;
        }
        let bucket_bytes = if bucket_count > self.buckets.len() {
            bucket_count
                .checked_mul(size_of::<u32>())
                .ok_or_else(|| VeinError::internal("hash join bucket bytes overflow"))?
        } else {
            0
        };
        self.memory.grow(
            index_bytes
                .checked_add(bucket_bytes)
                .ok_or_else(|| VeinError::internal("hash join reservation size overflow"))?,
            "hash join build batch",
        )?;

        self.chunks.try_reserve(1).map_err(|error| {
            VeinError::internal(format!("cannot reserve hash join chunk references: {error}"))
        })?;
        self.keys_by_chunk.try_reserve(1).map_err(|error| {
            VeinError::internal(format!("cannot reserve hash join key references: {error}"))
        })?;
        self.null_keyed.try_reserve(null_rows).map_err(|error| {
            VeinError::internal(format!("cannot reserve null-key join rows: {error}"))
        })?;
        self.entries.try_reserve(indexed_rows).map_err(|error| {
            VeinError::internal(format!("cannot reserve hash join entries: {error}"))
        })?;
        self.hashes.try_reserve(indexed_rows).map_err(|error| {
            VeinError::internal(format!("cannot reserve hash join values: {error}"))
        })?;
        if keys.is_single_i64() && self.keys.len() == self.entries.len() {
            self.keys.try_reserve(indexed_rows).map_err(|error| {
                VeinError::internal(format!("cannot reserve inline hash join keys: {error}"))
            })?;
        }
        if bucket_count > self.buckets.len() {
            let old_bucket_count = self.buckets.len();
            self.rebucket(bucket_count)?;
            let old_bytes = old_bucket_count
                .checked_mul(size_of::<u32>())
                .ok_or_else(|| VeinError::internal("hash join bucket bytes overflow"))?;
            self.memory.shrink(old_bytes);
        }

        self.build_key_materialization.accumulate(report);
        self.build_key_materialize_us = self.build_key_materialize_us.saturating_add(materialize_us);
        let chunk_index = self.chunks.len() as u32;
        self.chunks.push(chunk);
        self.keys_by_chunk.push(keys);

        for position in 0..row_count {
            let row = position as u32;
            let Some((hash, inline_key)) = self.keys_by_chunk[chunk_index as usize]
                .hash_and_single_i64(position)
            else {
                self.null_keyed.push((chunk_index, row));
                continue;
            };
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
            self.update_key_bounds(BuildRowReference {
                chunk: chunk_index,
                row,
            })?;
        }
        // A chunk whose key is not a single Int64 invalidates the inline array:
        // drop it rather than leave a partially filled one behind.
        if self.keys.len() != self.entries.len() {
            self.keys.clear();
        }
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
        let offset = u32::try_from(self.chunks.len())
            .map_err(|_| VeinError::internal("hash join chunk index exceeds row-reference range"))?;
        let other_bounds_bytes = other
            .key_bounds
            .capacity()
            .checked_mul(size_of::<BuildKeyBounds>())
            .ok_or_else(|| VeinError::internal("merged join key bounds bytes overflow"))?;
        let mut merged_key_bounds = Vec::new();
        merged_key_bounds
            .try_reserve_exact(self.key_bounds.len())
            .map_err(|error| VeinError::internal(format!("cannot reserve merged join key bounds: {error}")))?;
        merged_key_bounds.extend_from_slice(&self.key_bounds);
        for (key_index, other_bounds) in other.key_bounds.iter().copied().enumerate() {
            let (Some(other_min), Some(other_max)) = (other_bounds.min, other_bounds.max) else {
                continue;
            };
            let rebased_min = other_min.rebase(offset)?;
            let rebased_max = other_max.rebase(offset)?;
            match merged_key_bounds[key_index].min {
                None => {
                    merged_key_bounds[key_index] = BuildKeyBounds {
                        min: Some(rebased_min),
                        max: Some(rebased_max),
                    };
                }
                Some(current_min) => {
                    let other_min_array = other.key_array(other_min, key_index)?;
                    let current_min_array = self.key_array(current_min, key_index)?;
                    if other_min_array
                        .compare_values(
                            other_min.row as usize,
                            current_min_array,
                            current_min.row as usize,
                        )
                        .ok_or_else(|| VeinError::internal("join key bound type changed during merge"))?
                        == Ordering::Less
                    {
                        merged_key_bounds[key_index].min = Some(rebased_min);
                    }

                    let current_max = merged_key_bounds[key_index]
                        .max
                        .ok_or_else(|| VeinError::internal("join key bounds have an incomplete endpoint"))?;
                    let other_max_array = other.key_array(other_max, key_index)?;
                    let current_max_array = self.key_array(current_max, key_index)?;
                    if other_max_array
                        .compare_values(
                            other_max.row as usize,
                            current_max_array,
                            current_max.row as usize,
                        )
                        .ok_or_else(|| VeinError::internal("join key bound type changed during merge"))?
                        == Ordering::Greater
                    {
                        merged_key_bounds[key_index].max = Some(rebased_max);
                    }
                }
            }
        }
        let replaced_bucket_bytes = self
            .buckets
            .len()
            .checked_add(other.buckets.len())
            .and_then(|count| count.checked_mul(size_of::<u32>()))
            .ok_or_else(|| VeinError::internal("merged join bucket bytes overflow"))?;
        self.memory.absorb(other.memory)?;
        self.build_key_materialization.accumulate(other.build_key_materialization);
        self.build_key_materialize_us = self.build_key_materialize_us.saturating_add(other.build_key_materialize_us);
        self.chunks.extend(other.chunks);
        self.keys_by_chunk.extend(other.keys_by_chunk);
        self.key_bounds.copy_from_slice(&merged_key_bounds);
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
        self.memory
            .shrink(replaced_bucket_bytes.saturating_add(other_bounds_bytes));
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
            let Some((hash, inline_key)) = probe_keys.hash_and_single_i64(position) else {
                continue;
            };
            let value = inline.then(|| inline_key.unwrap_or_default());
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
    use crate::runtime_filter::RuntimeFilterSet;

    use super::*;

    fn ints(values: &[i64]) -> ArrayRef {
        Arc::new(Int64Array::from(values.to_vec()))
    }

    fn chunk(column: ArrayRef) -> DataChunk {
        DataChunk::from_vectors(vec![column])
    }

    fn hashes_for_arrays(columns: &[ArrayRef]) -> Vec<u64> {
        let keys = ChunkKeys::resolve_arrays(columns).expect("resolve key arrays");
        let rows = columns.first().map_or(0, |column| column.len());
        (0..rows)
            .filter_map(|row| keys.hash_and_single_i64(row).map(|(hash, _)| hash))
            .collect()
    }

    fn query_memory(limit: usize) -> QueryMemoryTracker {
        QueryMemoryTracker::new(
            limit,
            common::cancellation::CancellationToken::new(),
        )
    }

    #[test]
    fn bloom_runtime_filter_preserves_composite_join_keys_and_null_semantics() {
        let build = vec![
            Arc::new(Int64Array::from(vec![Some(5), Some(8), Some(5)])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("a")])) as ArrayRef,
        ];
        let hashes = hashes_for_arrays(&build);
        let memory = query_memory(1 << 20);
        let columns = [ColumnId(11), ColumnId(12)];
        let filters = JoinBloomFilterFactory::default()
            .build_filters(RuntimeFilterBuildInput {
                probe_columns: &columns,
                build_hashes: &hashes,
                build_key_bounds: Vec::new(),
                memory: &memory,
            })
            .expect("build runtime filter");
        assert_eq!(filters.len(), 1);

        let probe = RecordBatch::try_from_iter(vec![
            (
                "id",
                Arc::new(Int64Array::from(vec![Some(5), Some(5), Some(8), None])) as ArrayRef,
            ),
            (
                "region",
                Arc::new(StringArray::from(vec![Some("a"), Some("missing"), Some("b"), Some("a")])) as ArrayRef,
            ),
        ])
            .expect("probe key batch");
        let selection = filters[0]
            .evaluate(&probe, &[0, 1])
            .expect("evaluate composite runtime filter");

        assert!(selection.value(0), "every build key must survive the filter");
        assert!(selection.value(2), "every build key must survive the filter");
        assert!(!selection.value(3), "null join keys never match");
        assert!(
            filters[0].memory_bytes() > 0,
            "filter storage is accounted while retained"
        );
        drop(filters);
        assert_eq!(memory.used(), 0, "filter reservation releases on drop");
    }

    #[test]
    fn bloom_runtime_filter_declines_optional_allocation_when_budget_is_exhausted() {
        let hashes = [fnv1a(&7_i64.to_ne_bytes(), FNV_OFFSET)];
        let memory = query_memory(0);
        let columns = [ColumnId(7)];

        let filters = JoinBloomFilterFactory::default()
            .build_filters(RuntimeFilterBuildInput {
                probe_columns: &columns,
                build_hashes: &hashes,
                build_key_bounds: Vec::new(),
                memory: &memory,
            })
            .expect("optional filter allocation failure must fall back");

        assert!(filters.is_empty());
        assert_eq!(memory.used(), 0);
    }

    #[test]
    fn row_key_encoding_hashes_composite_values_and_skips_null_keys() {
        let single_i64 = ChunkKeys {
            columns: vec![KeyArray::Int64(Int64Array::from(vec![Some(3), None]))],
        };
        let expected_i64_hash = fnv1a(&3_i64.to_ne_bytes(), FNV_OFFSET);
        assert_eq!(
            single_i64.hash_and_single_i64(0),
            Some((expected_i64_hash, Some(3))),
            "the inline Int64 comparison key shares the one-pass encoding result"
        );
        assert_eq!(single_i64.hash_and_single_i64(1), None);

        let composite = ChunkKeys {
            columns: vec![
                KeyArray::Int64(Int64Array::from(vec![Some(3), Some(3), None])),
                KeyArray::Utf8(StringArray::from(vec![Some("north"), None, Some("north")])),
            ],
        };
        let expected_composite_hash = fnv1a(
            b"north",
            fnv1a(&3_i64.to_ne_bytes(), FNV_OFFSET),
        );
        assert_eq!(
            composite.hash_and_single_i64(0),
            Some((expected_composite_hash, None)),
            "composite keys preserve column order in the canonical hash"
        );
        assert_eq!(composite.hash_and_single_i64(1), None, "a null in any key column skips the row");
        assert_eq!(composite.hash_and_single_i64(2), None, "null leading key columns skip the row");
    }

    #[test]
    fn spill_hashes_use_the_same_composite_key_encoding_as_join_indexing() {
        let keys = JoinKeys::new(vec![0, 1]);
        let build = DataChunkView::from_chunk(DataChunk::from_vectors(vec![
            Arc::new(Int64Array::from(vec![Some(3), Some(3), None])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("north"), Some("south"), Some("north")]))
                as ArrayRef,
        ]))
            .expect("build view");
        let probe = DataChunkView::from_chunk(DataChunk::from_vectors(vec![
            Arc::new(Int64Array::from(vec![Some(3)])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("north")])) as ArrayRef,
        ]))
            .expect("probe view");

        let build_hashes = HashTable::hashes_for_view(&build, &keys).expect("build hashes");
        let probe_hashes = HashTable::hashes_for_view(&probe, &keys).expect("probe hashes");

        assert_eq!(build_hashes[0], probe_hashes[0]);
        assert_ne!(build_hashes[0], build_hashes[1]);
        assert_eq!(build_hashes[2], FNV_OFFSET, "null keys still receive a partition hash");
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

    #[test]
    fn runtime_key_bounds_track_each_composite_component_and_ignore_null_rows() {
        let mut table = HashTable::new(JoinKeys::new(vec![0, 1]), JoinKeys::new(vec![0, 1]));
        table
            .insert(DataChunk::from_vectors(vec![
                Arc::new(Int64Array::from(vec![Some(5), None, Some(8)])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("m"), Some("z"), Some("a")])) as ArrayRef,
            ]))
            .expect("insert composite build rows");

        assert_eq!(
            table.runtime_key_bounds().expect("snapshot typed bounds"),
            vec![
                Some((LogicalLiteral::Int64(5), LogicalLiteral::Int64(8))),
                Some((
                    LogicalLiteral::String("a".into()),
                    LogicalLiteral::String("m".into()),
                )),
            ],
            "only fully non-null build keys can match an equijoin probe"
        );
    }

    #[test]
    fn range_filter_prunes_from_statistics_without_binding_a_row_mask() {
        let memory = query_memory(1 << 20);
        let filter = JoinKeyRangeFilter::try_new(
            &[ColumnId(7)],
            vec![Some((LogicalLiteral::Int64(2), LogicalLiteral::Int64(5)))],
            &memory,
        )
        .expect("create bounded filter")
        .expect("range bounds are available");
        let filters = RuntimeFilterSet::new(vec![Arc::new(filter)]);
        let schema = types::VeinSchema::new(vec![Arc::new(types::ResolvedColumn::new(
            ColumnId(7),
            "id",
            None,
            VeinDataType::Int64,
        ))]);

        assert!(!filters.has_row_filters());
        assert!(filters.bind(&schema).is_none());
        assert_eq!(
            filters.statistics_predicates(&schema),
            vec![
                catalog::StatisticsPredicate::Comparison {
                    column: "id".into(),
                    comparison: catalog::StatisticsComparison::GreaterOrEqual,
                    literal: LogicalLiteral::Int64(2),
                },
                catalog::StatisticsPredicate::Comparison {
                    column: "id".into(),
                    comparison: catalog::StatisticsComparison::LessOrEqual,
                    literal: LogicalLiteral::Int64(5),
                },
            ]
        );
        assert!(filters.memory_bytes() > 0);
        drop(filters);
        assert_eq!(memory.used(), 0, "range filter accounting releases on drop");
    }

    #[test]
    fn merging_hash_tables_combines_and_rebases_runtime_key_bounds() {
        let keys = JoinKeys::new(vec![0, 1]);
        let mut left = HashTable::new(keys.clone(), keys.clone());
        let mut right = HashTable::new(keys.clone(), keys);
        left.insert(DataChunk::from_vectors(vec![
            ints(&[5, 7]),
            Arc::new(StringArray::from(vec!["m", "z"])) as ArrayRef,
        ]))
        .expect("insert first worker rows");
        right
            .insert(DataChunk::from_vectors(vec![
                ints(&[1, 9]),
                Arc::new(StringArray::from(vec!["a", "y"])) as ArrayRef,
            ]))
            .expect("insert second worker rows");

        left.merge(right).expect("combine worker hash tables");

        assert_eq!(
            left.runtime_key_bounds().expect("snapshot merged bounds"),
            vec![
                Some((LogicalLiteral::Int64(1), LogicalLiteral::Int64(9))),
                Some((
                    LogicalLiteral::String("a".into()),
                    LogicalLiteral::String("z".into()),
                )),
            ],
            "merged endpoints must resolve into the destination's rebased chunks"
        );
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
        let initial_bytes = INITIAL_BUCKETS * size_of::<u32>() + size_of::<BuildKeyBounds>();
        let budget = QueryMemoryTracker::new(initial_bytes, cancel);
        let mut table = HashTable::with_memory_tracker(
            JoinKeys::new(vec![0]),
            JoinKeys::new(vec![0]),
            budget.clone(),
        )
            .expect("initial bucket table fits the configured budget");
        assert_eq!(budget.used(), initial_bytes);

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
        let initial_buckets = INITIAL_BUCKETS * size_of::<u32>() + size_of::<BuildKeyBounds>();
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
    fn row_index_budget_failure_does_not_partially_insert_a_build_batch() {
        let first = ints(&[10]);
        let second = ints(&[20]);
        let per_row_index_bytes = size_of::<DataChunkView>()
            + size_of::<Entry>()
            + size_of::<u64>()
            + size_of::<i64>();
        let per_batch_bytes = first.get_array_memory_size() + per_row_index_bytes;
        let tracker = QueryMemoryTracker::new(
            INITIAL_BUCKETS * size_of::<u32>()
                + size_of::<BuildKeyBounds>()
                + per_batch_bytes * 2
                - 1,
            common::cancellation::CancellationToken::new(),
        );
        let mut table = HashTable::with_memory_tracker(
            JoinKeys::new(vec![0]),
            JoinKeys::new(vec![0]),
            tracker.clone(),
        )
            .expect("initial buckets fit the budget");
        table
            .insert_view(DataChunkView::from_chunk(chunk(Arc::clone(&first))).unwrap())
            .expect("first build batch fits");
        let used_before = tracker.used();

        let error = table
            .insert_view(DataChunkView::from_chunk(chunk(Arc::clone(&second))).unwrap())
            .expect_err("second row index exceeds the remaining budget");

        assert!(matches!(error, VeinError::ResourceExhausted { .. }));
        assert_eq!(table.len(), 1, "failed insertion leaves the row count intact");
        assert_eq!(table.chunk_count(), 1, "failed insertion retains no partial chunk");
        assert_eq!(tracker.used(), used_before, "failed batch releases its array lease");
        let mut matches = Vec::new();
        table
            .probe(&chunk(ints(&[10, 20])), &mut matches)
            .expect("existing build rows remain probeable");
        assert_eq!(matches.len(), 1);
        assert_eq!(table.build_row(matches[0].1.0), (0, 0));
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
            before_merge
                - INITIAL_BUCKETS * size_of::<u32>()
                - size_of::<BuildKeyBounds>(),
            "merge replaces the second bucket array and key-bounds vector"
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
            left_before
                + right_before
                - INITIAL_BUCKETS * size_of::<u32>()
                - size_of::<BuildKeyBounds>(),
            "destination owns merged state with one bucket array and one key-bounds vector"
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
