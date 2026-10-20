//! The build side of a hash join, as a table of references.
//!
//! Build payloads and keys stay in reference-backed views. Hashing and equality
//! follow each key's row mapping into retained Arrow arrays without gathering key
//! columns or storing duplicate scalar key payloads. Probe access is local to the
//! execution, keeping the shared table immutable.
//!
//! Matching produces row references. The join operator composes output views;
//! ordinary Arrow payload arrays are requested only at an explicit boundary.

use arrow::array::{BooleanArray, builder::BooleanBuilder};
use arrow::record_batch::RecordBatch;
use std::cmp::Ordering;
use std::mem::size_of;
use std::sync::Arc;

use crate::memory::{MemoryReservation, QueryMemoryTracker};
use crate::pipeline::{DataChunkView, MaterializationReport};
use crate::runtime::filter::{RuntimeFilter, RuntimeFilterBuildInput, RuntimeFilterFactory};
use common::Result;
use error::VeinError;
use types::{ColumnId, LogicalLiteral};

mod key_access;
use key_access::ChunkKeys;

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
        if let Some(filter) = JoinKeyRangeFilter::try_new(probe_columns, build_key_bounds, memory)?
        {
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
        if self
            .config
            .max_bytes
            .is_some_and(|limit| actual_bytes > limit)
        {
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
            let bit =
                hash.wrapping_add((iteration as u64).wrapping_mul(step)) as usize & self.bit_mask;
            visit(bit);
        }
    }

    fn insert(&mut self, hash: u64) {
        let step = hash.rotate_left(32) | 1;
        for iteration in 0..self.hash_functions {
            let bit =
                hash.wrapping_add((iteration as u64).wrapping_mul(step)) as usize & self.bit_mask;
            self.words[bit / u64::BITS as usize] |= 1u64 << (bit % u64::BITS as usize);
        }
    }

    fn may_contain(&self, hash: u64) -> bool {
        if self.words.is_empty() {
            return false;
        }
        let mut present = true;
        self.bit_positions(hash, |bit| {
            present &=
                self.words[bit / u64::BITS as usize] & (1u64 << (bit % u64::BITS as usize)) != 0;
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

    fn reorder_safe(&self) -> bool {
        true
    }

    fn evaluate(&self, batch: &RecordBatch, column_positions: &[usize]) -> Result<BooleanArray> {
        if column_positions.len() != self.columns.len() {
            return Err(VeinError::internal(
                "runtime filter key positions do not match its declared columns",
            ));
        }
        let arrays = column_positions
            .iter()
            .map(|position| {
                batch.columns().get(*position).cloned().ok_or_else(|| {
                    VeinError::internal("runtime filter key position is out of range")
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let keys = ChunkKeys::resolve_arrays(&arrays)?;
        let mut mask = BooleanBuilder::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let may_match = keys
                .hash_and_single_i64(row, None)?
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

    fn evaluate(&self, _batch: &RecordBatch, _column_positions: &[usize]) -> Result<BooleanArray> {
        Err(VeinError::internal(
            "statistics-only join key range filter cannot evaluate rows",
        ))
    }

    fn supports_row_evaluation(&self) -> bool {
        false
    }

    fn statistics_predicates(
        &self,
        schema: &types::VeinSchema,
    ) -> Vec<catalog::StatisticsPredicate> {
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
    /// Key array handles and row mappings are retained without gathering values.
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
    /// they remain non-matching because [`Self::probe`] skips null keys.
    /// Keeping this beside the in-memory key encoder prevents spill partitioning
    /// from introducing a second, potentially incompatible key representation.
    pub fn hashes_for_view(view: &DataChunkView, keys: &JoinKeys) -> Result<Vec<u64>> {
        let (resolved, _) = ChunkKeys::resolve_view(keys, view)?;
        let mut hashes = Vec::new();
        hashes.try_reserve_exact(view.num_rows()).map_err(|error| {
            VeinError::internal(format!("cannot reserve join spill hashes: {error}"))
        })?;
        for row in 0..view.num_rows() {
            hashes.push(
                resolved
                    .hash_and_single_i64(row, Some(view))?
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
        buckets
            .try_reserve_exact(INITIAL_BUCKETS)
            .map_err(|error| {
                VeinError::internal(format!(
                    "cannot allocate initial hash join buckets: {error}"
                ))
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
            build_key_materialization: MaterializationReport::default(),
            build_key_materialize_us: 0,
            key_bounds,
            tracker: tracker.clone(),
            memory,
        })
    }

    /// Create an empty worker-local table with the same key layout and budget.
    pub(crate) fn fork_empty(&self) -> Result<Self> {
        Self::with_memory_tracker(
            self.build_keys.clone(),
            self.probe_keys.clone(),
            self.tracker.clone(),
        )
    }

    fn key_chunk(&self, reference: BuildRowReference) -> Result<(&ChunkKeys, &DataChunkView)> {
        let index = reference.chunk as usize;
        Ok((
            self.keys_by_chunk.get(index).ok_or_else(|| {
                VeinError::internal("join key reference points outside retained key arrays")
            })?,
            self.chunks.get(index).ok_or_else(|| {
                VeinError::internal("join key reference points outside retained chunks")
            })?,
        ))
    }

    fn compare_build_rows(
        &self,
        key_index: usize,
        left: BuildRowReference,
        right: BuildRowReference,
    ) -> Result<Ordering> {
        let (left_keys, left_view) = self.key_chunk(left)?;
        let (right_keys, right_view) = self.key_chunk(right)?;
        left_keys
            .compare_value(
                key_index,
                left.row as usize,
                left_view,
                right_keys,
                right.row as usize,
                right_view,
            )?
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
            let max = self.key_bounds[key_index].max.ok_or_else(|| {
                VeinError::internal("join key bounds have an incomplete endpoint")
            })?;
            let replace_min = self.compare_build_rows(key_index, current, min)? == Ordering::Less;
            let replace_max =
                self.compare_build_rows(key_index, current, max)? == Ordering::Greater;
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
    pub fn runtime_key_bounds(&self) -> Result<Vec<Option<(LogicalLiteral, LogicalLiteral)>>> {
        let mut bounds = Vec::new();
        bounds
            .try_reserve_exact(self.key_bounds.len())
            .map_err(|error| {
                VeinError::internal(format!("cannot allocate join key bounds snapshot: {error}"))
            })?;
        for (key_index, key_bounds) in self.key_bounds.iter().enumerate() {
            let (Some(min), Some(max)) = (key_bounds.min, key_bounds.max) else {
                bounds.push(None);
                continue;
            };
            let (min_keys, min_view) = self.key_chunk(min)?;
            let (max_keys, max_view) = self.key_chunk(max)?;
            bounds.push(Some((
                min_keys.logical_literal(key_index, min.row as usize, min_view)?,
                max_keys.logical_literal(key_index, max.row as usize, max_view)?,
            )));
        }
        Ok(bounds)
    }

    /// Whether nothing has been built.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty() && self.null_keyed.is_empty()
    }

    /// Retains a reference-backed build batch and indexes its active rows.
    pub fn insert(&mut self, mut chunk: DataChunkView) -> Result<()> {
        let tracker_id = self.tracker.identity();
        chunk.account_source_arrays(tracker_id, |identity, bytes| {
            let lease: std::sync::Arc<dyn crate::pipeline::view::ViewMemoryLease> = self
                .tracker
                .lease_retained_array(identity, bytes, "hash join retained arrays")?;
            Ok(lease)
        })?;
        let (keys, key_access_us) = ChunkKeys::resolve_view(&self.build_keys, &chunk)?;
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
        let mut null_rows = 0usize;
        for row in 0..row_count {
            if keys.any_null(&chunk, row)? {
                null_rows += 1;
            }
        }
        let indexed_rows = row_count - null_rows;
        let entry_bytes = size_of::<Entry>()
            .checked_add(size_of::<u64>())
            .ok_or_else(|| VeinError::internal("join index entry size overflow"))?;
        let index_bytes = size_of::<DataChunkView>()
            .checked_add(size_of::<ChunkKeys>())
            .and_then(|bytes| bytes.checked_add(keys.metadata_bytes()))
            .and_then(|bytes| {
                null_rows
                    .checked_mul(size_of::<(u32, u32)>())
                    .and_then(|null_bytes| bytes.checked_add(null_bytes))
            })
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
            VeinError::internal(format!(
                "cannot reserve hash join chunk references: {error}"
            ))
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
        if bucket_count > self.buckets.len() {
            let old_bucket_count = self.buckets.len();
            self.rebucket(bucket_count)?;
            let old_bytes = old_bucket_count
                .checked_mul(size_of::<u32>())
                .ok_or_else(|| VeinError::internal("hash join bucket bytes overflow"))?;
            self.memory.shrink(old_bytes);
        }

        self.build_key_materialize_us = self.build_key_materialize_us.saturating_add(key_access_us);
        let chunk_index = self.chunks.len() as u32;
        self.chunks.push(chunk);
        self.keys_by_chunk.push(keys);

        for position in 0..row_count {
            let row = position as u32;
            let Some((hash, _)) = self.keys_by_chunk[chunk_index as usize]
                .hash_and_single_i64(position as usize, Some(&self.chunks[chunk_index as usize]))?
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
            self.update_key_bounds(BuildRowReference {
                chunk: chunk_index,
                row,
            })?;
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
                    let lease: std::sync::Arc<dyn crate::pipeline::view::ViewMemoryLease> =
                        self.tracker.lease_retained_array(
                            identity,
                            bytes,
                            "merged hash join retained arrays",
                        )?;
                    Ok(lease)
                })?;
            }
        }
        let offset = u32::try_from(self.chunks.len()).map_err(|_| {
            VeinError::internal("hash join chunk index exceeds row-reference range")
        })?;
        let other_bounds_bytes = other
            .key_bounds
            .capacity()
            .checked_mul(size_of::<BuildKeyBounds>())
            .ok_or_else(|| VeinError::internal("merged join key bounds bytes overflow"))?;
        let mut merged_key_bounds = Vec::new();
        merged_key_bounds
            .try_reserve_exact(self.key_bounds.len())
            .map_err(|error| {
                VeinError::internal(format!("cannot reserve merged join key bounds: {error}"))
            })?;
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
                    let (other_min_keys, other_min_view) = other.key_chunk(other_min)?;
                    let (current_min_keys, current_min_view) = self.key_chunk(current_min)?;
                    if other_min_keys
                        .compare_value(
                            key_index,
                            other_min.row as usize,
                            other_min_view,
                            current_min_keys,
                            current_min.row as usize,
                            current_min_view,
                        )?
                        .ok_or_else(|| {
                            VeinError::internal("join key bound type changed during merge")
                        })?
                        == Ordering::Less
                    {
                        merged_key_bounds[key_index].min = Some(rebased_min);
                    }

                    let current_max = merged_key_bounds[key_index].max.ok_or_else(|| {
                        VeinError::internal("join key bounds have an incomplete endpoint")
                    })?;
                    let (other_max_keys, other_max_view) = other.key_chunk(other_max)?;
                    let (current_max_keys, current_max_view) = self.key_chunk(current_max)?;
                    if other_max_keys
                        .compare_value(
                            key_index,
                            other_max.row as usize,
                            other_max_view,
                            current_max_keys,
                            current_max.row as usize,
                            current_max_view,
                        )?
                        .ok_or_else(|| {
                            VeinError::internal("join key bound type changed during merge")
                        })?
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
        self.build_key_materialization
            .accumulate(other.build_key_materialization);
        self.build_key_materialize_us = self
            .build_key_materialize_us
            .saturating_add(other.build_key_materialize_us);
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
        let wanted_buckets = self
            .entries
            .len()
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

    /// Probes a reference-backed batch. Returned rows are logical positions in
    /// the selected input batches.
    /// Returns the probe key conversion report and resolution time in microseconds.
    /// The report excludes hash/index allocations and never mutates build metrics.
    pub fn probe(
        &self,
        probe: &DataChunkView,
        matches: &mut Vec<(u32, (u32, u32))>,
    ) -> Result<(MaterializationReport, u64)> {
        self.probe_view_inner(probe, matches, None)
    }

    /// Probes while recording NULL-keyed probe row positions for a
    /// null-aware anti join. Ordinary joins avoid allocating this side vector.
    pub fn probe_view_with_null_rows(
        &self,
        probe: &DataChunkView,
        matches: &mut Vec<(u32, (u32, u32))>,
        null_probe_rows: &mut Vec<u32>,
    ) -> Result<(MaterializationReport, u64)> {
        self.probe_view_inner(probe, matches, Some(null_probe_rows))
    }

    fn probe_view_inner(
        &self,
        probe: &DataChunkView,
        matches: &mut Vec<(u32, (u32, u32))>,
        mut null_probe_rows: Option<&mut Vec<u32>>,
    ) -> Result<(MaterializationReport, u64)> {
        let (probe_keys, key_access_us) = ChunkKeys::resolve_view(&self.probe_keys, probe)?;
        let report = MaterializationReport::default();
        let inline_i64 = probe_keys.is_single_i64()
            && self
                .keys_by_chunk
                .first()
                .is_some_and(ChunkKeys::is_single_i64);
        for position in 0..probe.num_rows() {
            let row = position as u32;
            let Some((hash, inline_key)) = probe_keys.hash_and_single_i64(position, Some(probe))?
            else {
                if let Some(rows) = &mut null_probe_rows {
                    rows.push(row);
                }
                continue;
            };
            let mut head = self.buckets[bucket_of(hash, self.buckets.len())];
            while head != NONE {
                let entry = self.entries[head as usize];
                let same = self.hashes[head as usize] == hash
                    && (if inline_i64 {
                        let value = inline_key.ok_or_else(|| {
                            VeinError::internal("single-Int64 probe key is missing its value")
                        })?;
                        self.keys_by_chunk[entry.chunk as usize].single_i64_value(
                            &self.chunks[entry.chunk as usize],
                            entry.row as usize,
                        )? == Some(value)
                    } else {
                        self.keys_by_chunk[entry.chunk as usize].equals(
                            entry.row as usize,
                            &self.chunks[entry.chunk as usize],
                            &probe_keys,
                            row as usize,
                            probe,
                        )?
                    });
                if same {
                    matches.push((row, (entry.chunk, entry.row)));
                }
                head = entry.next;
            }
        }
        Ok((report, key_access_us))
    }

    /// Whether a single-key membership build contains a NULL key.
    pub fn has_null_keys(&self) -> bool {
        !self.null_keyed.is_empty()
    }

    /// Accumulated build key conversion work, including merged build tables.
    pub(crate) fn build_key_materialization(&self) -> (MaterializationReport, u64) {
        (
            self.build_key_materialization,
            self.build_key_materialize_us,
        )
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
mod tests;

#[cfg(test)]
#[path = "hash_table/int32_tests.rs"]
mod int32_tests;
