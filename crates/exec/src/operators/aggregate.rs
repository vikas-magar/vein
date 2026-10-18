//! Hash aggregation: the pipeline breaker that groups rows and folds them.
//!
//! Like a join, an aggregate needs the whole input before it can emit anything,
//! so it is not a VM instruction: the runtime drains the program into
//! [`AggregateSink`] and then takes the groups. The columns it receives are the
//! program's output registers — the group keys first, then one argument column
//! per aggregate — so a key or an argument like `sum(price * quantity)` is an
//! ordinary register and this module never interprets an expression.
//!
//! A `DISTINCT` arrives here as the same shape with every column a key and no
//! aggregates, which is why it has no operator of its own.

use std::collections::HashSet;
use std::hash::{BuildHasher, Hash, Hasher};
use std::mem::size_of;
use std::num::NonZeroUsize;
use std::ops::Range;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use arrow::array::{
    Array, ArrayRef, Date32Array, Decimal128Array, Decimal256Array, DictionaryArray, Float16Array,
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, StringArray,
    UInt16Array, UInt32Array, UInt64Array, UInt8Array, new_null_array,
};
use arrow::datatypes::{Int32Type, i256};
use hashbrown::DefaultHashBuilder;

mod group_index;
mod group_key;
mod intermediate;
mod numeric;
mod state;
mod spill;
mod metrics;
mod init;
mod input;
mod distinct;
mod intermediate_merge;
mod output;
mod pipeline;
mod sink_spill;
mod spill_finish;
pub use pipeline::{shared, shared_with_memory_tracker};
use intermediate::{AggregateIntermediateGroup, AggregateIntermediateState, NumericPartial};
use numeric::{
    AggregateInput, GroupRows, NumericAccumulator, average_values, numeric_values,
    update_numeric_states,
};
use crate::memory::{MemoryReservation, QueryMemoryTracker};
use crate::spill::SpillRun;
use crate::pipeline::{DataChunk, PipelineNode};
use common::Result;
use common::arrow::SelectionVector;
use common::protocol::{AggOpCode, AggregatePlan};
use error::VeinError;
use group_index::{GroupIndex, GroupIndexEntry, ProbeResolution};
use group_key::{key_column, GroupKeyColumn};
use state::AggregateStateColumn;
use spill::AggregateSpillSchema;
pub(crate) use metrics::AggregateMetrics;
use types::VeinDataType;

type GroupHasher = <DefaultHashBuilder as BuildHasher>::Hasher;

/// Maximum group rows materialized into one aggregate output chunk.
pub(crate) const AGGREGATE_BATCH_ROWS: NonZeroUsize =
    NonZeroUsize::new(1024).expect("aggregate batch size must be non-zero");

/// A group key value borrowed from an Arrow array.
#[derive(Clone, Copy, Hash, PartialEq)]
enum KeyValueRef<'a> {
    Null,
    Date32(i32),
    Int64(i64),
    UInt64(u64),
    Decimal128(i128),
    Decimal256(i256),
    Utf8(&'a str),
}


/// One group's non-key state.
struct Group {
    hash: u64,
    grouping_set_id: usize,
    key_location: Option<GroupKeyLocation>,
}

/// Locates a group's original key row in retained Arrow-backed key batches.
#[derive(Clone, Copy)]
pub(super) struct GroupKeyLocation {
    batch: usize,
    row: usize,
}

/// Undo index entries and group-byte reservations when a batch cannot finish
/// retaining its keys or reserving the group output storage.
fn rollback_new_groups(
    index: &mut GroupIndex,
    hashes: &[u64],
    existing_group_count: usize,
    new_groups: &mut Vec<usize>,
    memory: &mut MemoryReservation,
    reserved_bytes_before_batch: usize,
) {
    for (new_group_index, &candidate) in new_groups.iter().enumerate() {
        let removed = index.remove(hashes[candidate], existing_group_count + new_group_index);
        debug_assert!(removed, "every uncommitted group must have an index entry");
    }
    let batch_bytes = memory.bytes().saturating_sub(reserved_bytes_before_batch);
    memory.shrink(batch_bytes);
    new_groups.clear();
}

/// Absorbs rows into groups, then emits one row per group.
pub struct AggregateSink {
    plan: AggregatePlan,
    /// Each grouping set's key positions in the projected key columns.
    grouping_sets: Vec<Vec<u32>>,
    /// Grouping-set membership by key column, prepared once per plan.
    grouping_sets_by_column: Vec<Vec<usize>>,
    /// Prehashed group slots; equality is checked against retained columnar keys.
    index: GroupIndex,
    hash_builder: DefaultHashBuilder,
    /// Reused hash and probe storage for the current input batch.
    group_hashers: Vec<GroupHasher>,
    group_hashes: Vec<u64>,
    batch_group_ids: Vec<usize>,
    /// Per-batch dictionary IDs used by the single-key Utf8 fast path.
    dictionary_ids: Vec<usize>,
    /// First physical row for each dictionary ID in the current batch.
    dictionary_first_rows: Vec<usize>,
    /// Dictionary ID to resolved group slot; the final entry represents a null key.
    dictionary_group_slots: Vec<usize>,
    /// Candidate indexes that will become new groups after this batch resolves.
    batch_new_groups: Vec<usize>,
    /// Arrow-backed grouping columns for batches that contributed new groups.
    group_key_batches: Vec<Vec<Option<GroupKeyColumn>>>,
    /// Memory leases for the distinct source arrays retained by each key batch.
    group_key_batch_leases: Vec<Vec<Arc<MemoryReservation>>>,
    /// Reusable group IDs for each grouping set in the current input batch.
    group_ids_by_set: Vec<Vec<usize>>,
    groups: Vec<Group>,
    aggregate_states: Vec<AggregateStateColumn>,
    memory: MemoryReservation,
    tracker: QueryMemoryTracker,
    spill_schema: AggregateSpillSchema,
    spill_runs: Vec<SpillRun>,
    metrics: AggregateMetrics,
    /// One spillable typed-key collector per distinct aggregate and grouping set.
    distinct_collectors: Vec<distinct::DistinctCollector>,
}


#[cfg(test)]
mod tests;
