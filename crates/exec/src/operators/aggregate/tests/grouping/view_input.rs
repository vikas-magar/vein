//! Reference-backed aggregate input coverage.

use std::collections::BTreeMap;
use std::hash::{BuildHasher, Hasher};
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use common::arrow::SelectionVector;
use hashbrown::DefaultHashBuilder;
use crate::pipeline::{DataChunkView, ViewColumnRef, ViewInputRows};

use super::*;

fn sum_plan() -> AggregatePlan {
    AggregatePlan {
        groups: 1,
        estimated_groups: None,
        grouping_sets: None,
        emit_empty_group_on_empty_input: false,
        aggs: vec![AggOpCode::Sum],
        aggregate_input_types: vec![VeinDataType::Int64],
        types: vec![VeinDataType::Int64, VeinDataType::Int64],
    }
}

fn mapped_input() -> (DataChunkView, ArrayRef) {
    let key_array: ArrayRef = Arc::new(Int64Array::from(vec![10, 20]));
    let key_view = DataChunkView::from_chunk(DataChunk::from_vectors(vec![Arc::clone(&key_array)]))
        .expect("key source view");
    let value_view = DataChunkView::from_chunk(DataChunk::from_vectors(vec![
        Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
    ]))
        .expect("value source view");
    let joined = DataChunkView::combine(
        &[
            ViewInputRows {
                view: &key_view,
                rows: &[Some(0), Some(1), None, Some(0), Some(1)],
            },
            ViewInputRows {
                view: &value_view,
                rows: &[Some(4), Some(2), Some(0), Some(3), Some(1)],
            },
        ],
        &[
            ViewColumnRef { input: 0, column: 0 },
            ViewColumnRef { input: 1, column: 0 },
        ],
    )
        .expect("mapped join output");
    (joined, key_array)
}

fn grouped_rows(sink: &mut AggregateSink) -> BTreeMap<Option<i64>, i64> {
    sink.finish()
        .expect("finish aggregate")
        .into_iter()
        .flat_map(|batch| {
            let keys = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let sums = batch.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
            (0..batch.num_rows())
                .map(|row| {
                    (
                        keys.is_null(row).then_some(None).unwrap_or(Some(keys.value(row))),
                        sums.value(row),
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

#[test]
fn aggregate_view_groups_mapped_duplicate_and_null_extended_keys_without_gathering_keys() {
    let (view, source_keys) = mapped_input();
    let tracker = QueryMemoryTracker::unlimited(common::cancellation::CancellationToken::new());
    let mut sink = AggregateSink::with_memory_tracker(sum_plan(), tracker.clone())
        .expect("aggregate sink");

    sink.accumulate_view(view).expect("aggregate mapped view");

    let Some(GroupKeyColumn::View(retained)) = &sink.group_key_batches[0][0] else {
        panic!("aggregate must retain the grouping key as a view");
    };
    let retained_sources = retained
        .source_arrays_for_column(0)
        .expect("retained group key sources");
    assert!(retained_sources.iter().any(|array| Arc::ptr_eq(array, &source_keys)));
    assert!(tracker.used() >= source_keys.get_array_memory_size());

    let rows = grouped_rows(&mut sink);
    assert_eq!(rows, BTreeMap::from([(None, 1), (Some(10), 9), (Some(20), 5)]));

    drop(sink);
    assert_eq!(tracker.used(), 0, "view-backed key leases release with the sink");
}

#[test]
fn combining_view_backed_local_groups_preserves_keys_and_merges_duplicates() {
    let (view, source_keys) = mapped_input();
    let tracker = QueryMemoryTracker::unlimited(common::cancellation::CancellationToken::new());
    let mut local = AggregateSink::with_memory_tracker(sum_plan(), tracker.clone())
        .expect("local aggregate sink");
    let mut target = AggregateSink::with_memory_tracker(sum_plan(), tracker.clone())
        .expect("target aggregate sink");
    local
        .inherit_hash_builders(&target)
        .expect("local worker inherits canonical hash seeds");
    local.accumulate_view(view).expect("accumulate local mapped view");

    target
        .accumulate(DataChunk::from_vectors(vec![
            Arc::new(Int64Array::from(vec![10])) as ArrayRef,
            Arc::new(Int64Array::from(vec![7])) as ArrayRef,
        ]))
        .expect("seed target with a duplicate key");
    let mut materialized_local =
        AggregateSink::with_memory_tracker(sum_plan(), tracker.clone()).expect("materialized local");
    materialized_local
        .inherit_hash_builders(&target)
        .expect("materialized worker inherits canonical hash seeds");
    materialized_local
        .accumulate(DataChunk::from_vectors(vec![
            Arc::new(Int64Array::from(vec![10])) as ArrayRef,
            Arc::new(Int64Array::from(vec![9])) as ArrayRef,
        ]))
        .expect("accumulate materialized local key");
    assert_eq!(
        target.groups[0].hash, materialized_local.groups[0].hash,
        "the same key must hash identically across worker-local sinks"
    );
    assert_eq!(
        materialized_local.groups[0].hash, local.groups[0].hash,
        "the same key must hash identically across materialized and view inputs"
    );
    target
        .combine_local_state(local)
        .expect("combine reference-backed local groups");
    assert_eq!(target.group_count(), 3, "equal materialized and view keys share a group");

    assert!(target.group_key_batches.iter().flatten().any(|column| {
        matches!(column, Some(GroupKeyColumn::View(view))
            if view.source_arrays_for_column(0).is_ok_and(|arrays| {
                arrays.iter().any(|array| Arc::ptr_eq(array, &source_keys))
            }))
    }), "combined groups retain the original key array");

    let rows = grouped_rows(&mut target);
    assert_eq!(rows, BTreeMap::from([(None, 1), (Some(10), 16), (Some(20), 5)]));
    drop(materialized_local);
    drop(target);
    assert_eq!(tracker.used(), 0, "combined source leases release with the sink");
}

#[test]
fn spilling_view_backed_groups_resolves_source_rows_and_null_extensions() {
    let (view, _) = mapped_input();
    let tracker = QueryMemoryTracker::unlimited(common::cancellation::CancellationToken::new());
    let mut sink = AggregateSink::with_memory_tracker(sum_plan(), tracker.clone())
        .expect("aggregate sink");
    sink.accumulate_view(view).expect("aggregate mapped view");
    assert!(matches!(
        &sink.group_key_batches[0][0],
        Some(GroupKeyColumn::View(_))
    ));

    sink.flush_current_run().expect("spill view-backed groups");
    let rows = grouped_rows(&mut sink);
    assert_eq!(rows, BTreeMap::from([(None, 1), (Some(10), 9), (Some(20), 5)]));

    drop(sink);
    assert_eq!(tracker.used(), 0, "spilled key source leases release with the sink");
}

#[test]
fn view_backed_group_hashes_match_materialized_hashes_for_null_and_mapped_rows() {
    let (view, _) = mapped_input();
    let key_view = view.project(&[0]).unwrap().compact_active_rows().unwrap();
    let materialized = key_view.materialize_column(0).unwrap();
    let mapped_key = GroupKeyColumn::from_view(key_view).unwrap();
    let materialized_key = GroupKeyColumn::new(&materialized).unwrap();
    let hash_builder = DefaultHashBuilder::default();

    let mut mapped_hashers = (0..view.num_rows())
        .map(|_| hash_builder.build_hasher())
        .collect::<Vec<_>>();
    let mut materialized_hashers = (0..view.num_rows())
        .map(|_| hash_builder.build_hasher())
        .collect::<Vec<_>>();
    let selection = SelectionVector::new_dense(view.num_rows());
    mapped_key.hash_batch(0, &selection, &[0], 1, &mut mapped_hashers);
    materialized_key.hash_batch(0, &selection, &[0], 1, &mut materialized_hashers);

    assert_eq!(
        mapped_hashers.iter().map(Hasher::finish).collect::<Vec<_>>(),
        materialized_hashers
            .iter()
            .map(Hasher::finish)
            .collect::<Vec<_>>()
    );
}
