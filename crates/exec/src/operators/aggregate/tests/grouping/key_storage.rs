//! Aggregate key storage regression coverage.

use super::*;

#[tokio::test]
async fn hash_collisions_keep_distinct_groups_and_update_matching_group() {
    let plan = AggregatePlan {
        groups: 1,
        estimated_groups: None,
        grouping_sets: None,
        emit_empty_group_on_empty_input: true,
        aggs: vec![AggOpCode::Sum],
        aggregate_input_types: vec![VeinDataType::Int64],
        types: vec![VeinDataType::Int64, VeinDataType::Int64],
    };
    let mut sink = AggregateSink::new(inferred_results(plan)).unwrap();
    sink.accumulate(crate::pipeline::test_support::dense_view(vec![
        Arc::new(Int64Array::from(vec![1])),
        Arc::new(Int64Array::from(vec![10])),
    ]))
        .unwrap();
    assert_eq!(sink.metrics().grouping_probes, 1);

    let colliding_chunk = crate::pipeline::test_support::dense_view(vec![
        Arc::new(Int64Array::from(vec![2, 2])),
        Arc::new(Int64Array::from(vec![20, 30])),
    ]);
    let colliding_sources = colliding_chunk.source_arrays_for_column(0).unwrap();
    let colliding_key_column = GroupKeyColumn::new(&colliding_sources[0]).unwrap();
    let mut colliding_hashers: Vec<_> = (0..colliding_chunk.num_rows())
        .map(|_| sink.hash_builder.build_hasher())
        .collect();
    for hasher in &mut colliding_hashers {
        0usize.hash(hasher);
    }
    colliding_key_column.hash_batch(
        0,
        colliding_chunk.selection(),
        &[0],
        1,
        &mut colliding_hashers,
    );
    let colliding_hash = colliding_hashers[0].finish();
    sink.index
        .insert_unique(GroupIndexEntry {
            hash: colliding_hash,
            slot: 0,
        })
        .unwrap();
    sink.accumulate(colliding_chunk).unwrap();
    let metrics = sink.metrics();
    assert_eq!(metrics.grouping_probes, 3);
    assert!(metrics.group_index_candidate_checks > 0);
    // HashTable may encounter the real group before the deliberately
    // injected colliding entry, so diagnostic counts vary with its
    // randomized bucket layout. Assert the collision path was exercised,
    // not a particular candidate traversal order.
    assert!(metrics.full_hash_matches >= 2);
    assert!(metrics.group_key_comparisons >= 2);
    assert!(metrics.group_collision_retries >= 1);
    assert_eq!(metrics.group_index_hits, 1);
    assert_eq!(metrics.group_index_misses, 2);

    let output = sink.finish().await.unwrap();
    let batch = &output[0];
    let keys = materialized_column::<Int64Array>(batch, 0);
    let sums = int128_column(batch, 1);

    assert_eq!(batch.num_rows(), 2);
    assert_eq!((keys.value(0), sums.value(0)), (1, 10));
    assert_eq!((keys.value(1), sums.value(1)), (2, 50));
}

#[test]
fn retained_group_keys_share_input_arrow_buffers() {
    let plan = AggregatePlan {
        groups: 1,
        estimated_groups: None,
        grouping_sets: None,
        emit_empty_group_on_empty_input: true,
        aggs: vec![AggOpCode::Count],
        aggregate_input_types: vec![VeinDataType::Int64],
        types: vec![VeinDataType::Int64, VeinDataType::Int64],
    };
    let tracker = QueryMemoryTracker::unlimited(common::cancellation::CancellationToken::new());
    let mut sink = AggregateSink::with_memory_tracker(inferred_results(plan), tracker.clone())
        .expect("create aggregate sink");
    let keys = Arc::new(Int64Array::from(vec![10, 20, 30]));
    let source_values = keys.values().as_ptr();
    let source_bytes = keys.get_array_memory_size();
    sink.accumulate(crate::pipeline::test_support::dense_view(vec![
        keys,
        Arc::new(Int64Array::from(vec![1, 1, 1])),
    ]))
        .expect("accumulate keys");

    let retained_sources = sink.group_key_batches[0][0]
        .as_ref()
        .expect("retained grouping key")
        .source_arrays()
        .expect("retained key sources are available");
    let retained = retained_sources
        .iter()
        .find_map(|array| array.as_any().downcast_ref::<Int64Array>())
        .expect("retained source is an int64 Arrow array");
    assert_eq!(retained.values().as_ptr(), source_values);
    assert_eq!(sink.metrics().string_key_bytes_copied, 0);
    assert!(
        tracker.used() >= source_bytes,
        "the retained group-key source array is charged to the query budget"
    );
    drop(sink);
    assert_eq!(
        tracker.used(),
        0,
        "retained key leases release with the sink"
    );
}

#[test]
fn duplicate_only_batch_does_not_retain_or_charge_its_key_array() {
    let plan = AggregatePlan {
        groups: 1,
        estimated_groups: None,
        grouping_sets: None,
        emit_empty_group_on_empty_input: false,
        aggs: vec![AggOpCode::Count],
        aggregate_input_types: vec![VeinDataType::Int64],
        types: vec![VeinDataType::Int64, VeinDataType::Int64],
    };
    let tracker = QueryMemoryTracker::unlimited(common::cancellation::CancellationToken::new());
    let mut sink = AggregateSink::with_memory_tracker(inferred_results(plan), tracker.clone())
        .expect("create aggregate sink");
    sink.accumulate(crate::pipeline::test_support::dense_view(vec![
        Arc::new(Int64Array::from(vec![10])),
        Arc::new(Int64Array::from(vec![1])),
    ]))
        .expect("create first group");
    let retained_bytes = tracker.used();
    let retained_batches = sink.group_key_batches.len();

    sink.accumulate(crate::pipeline::test_support::dense_view(vec![
        Arc::new(Int64Array::from(vec![10, 10, 10])),
        Arc::new(Int64Array::from(vec![1, 1, 1])),
    ]))
        .expect("update an existing group");

    assert_eq!(sink.group_key_batches.len(), retained_batches);
    assert_eq!(tracker.used(), retained_bytes);
    drop(sink);
    assert_eq!(tracker.used(), 0);
}
