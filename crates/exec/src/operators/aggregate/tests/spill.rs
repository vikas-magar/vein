//! spill regression tests for hash aggregation.

use super::*;

fn assert_spill_key_hash_matches_batch(array: ArrayRef) {
    let key = GroupKeyColumn::new(&array).expect("supported grouping key");
    let selection = SelectionVector::new_dense(array.len());
    let hash_builder = DefaultHashBuilder::default();
    let grouping_set_count = 2;
    let mut batch_hashers = (0..array.len() * grouping_set_count)
        .map(|_| hash_builder.build_hasher())
        .collect::<Vec<_>>();
    for (index, hasher) in batch_hashers.iter_mut().enumerate() {
        (index % grouping_set_count).hash(hasher);
    }
    key.hash_batch(
        0,
        &selection,
        &[0, 1],
        grouping_set_count,
        &mut batch_hashers,
    );

    for row in 0..array.len() {
        for set_id in 0..grouping_set_count {
            let mut spill_hasher = hash_builder.build_hasher();
            set_id.hash(&mut spill_hasher);
            key.hash_row(0, row, &mut spill_hasher);
            assert_eq!(
                spill_hasher.finish(),
                batch_hashers[row * grouping_set_count + set_id].finish(),
                "hash differs at row {row}, grouping set {set_id}"
            );
        }
    }
}

#[test]
fn spill_key_hash_matches_live_hash_for_typed_null_and_dictionary_keys() {
    assert_spill_key_hash_matches_batch(Arc::new(Int64Array::from(vec![
        Some(-7), None, Some(42), Some(-7),
    ])));
    assert_spill_key_hash_matches_batch(Arc::new(UInt32Array::from(vec![
        Some(7), None, Some(u32::MAX), Some(7),
    ])));
    assert_spill_key_hash_matches_batch(Arc::new(StringArray::from(vec![
        Some("alpha"), None, Some("beta"), Some("alpha"),
    ])));

    let dictionary = DictionaryArray::<Int32Type>::try_new(
        Int32Array::from(vec![Some(0), None, Some(1), Some(2)]),
        Arc::new(StringArray::from(vec![Some("alpha"), Some("beta"), None])),
    )
        .expect("valid dictionary keys");
    assert_spill_key_hash_matches_batch(Arc::new(dictionary));
}

#[test]
fn aggregate_spill_flushes_retries_and_merges_repeated_keys() {
    let plan = AggregatePlan {
        groups: 1,
        estimated_groups: None,
        grouping_sets: None,
        emit_empty_group_on_empty_input: false,
        aggs: vec![AggOpCode::Count],
        aggregate_input_types: vec![VeinDataType::Int64],
        types: vec![VeinDataType::Int64, VeinDataType::Int64],
    };
    let tracker = QueryMemoryTracker::new(
        32 * 1024,
        common::cancellation::CancellationToken::new(),
    );
    let mut sink = AggregateSink::with_memory_tracker(plan, tracker.clone())
        .expect("create budgeted aggregate");
    let input_rows = 2048_i64;
    let keys = (0..input_rows).map(|row| row % 257).collect::<Vec<_>>();
    let values = (0..input_rows).collect::<Vec<_>>();
    sink.accumulate(DataChunk::from_vectors(vec![
        Arc::new(Int64Array::from(keys.clone())) as ArrayRef,
        Arc::new(Int64Array::from(values)) as ArrayRef,
    ]))
        .expect("large input selection is split and retried after spill");
    assert!(
        !sink.spill_runs.is_empty(),
        "the small tracker budget must externalize groups"
    );
    assert!(tracker.spill_bytes_used() > 0, "aggregate runs consume query spill budget");

    let output = sink.finish().expect("merge spill runs");
    assert!(
        tracker.spill_bytes_used() == 0,
        "successfully consumed aggregate runs release the query spill budget"
    );
    let mut counts = std::collections::BTreeMap::new();
    for batch in output {
        let keys = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let values = batch.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        for row in 0..batch.num_rows() {
            counts.insert(keys.value(row), values.value(row));
        }
    }
    let expected = keys.into_iter().fold(
        std::collections::BTreeMap::new(),
        |mut counts, key| {
            *counts.entry(key).or_insert(0_i64) += 1;
            counts
        },
    );
    assert_eq!(counts, expected);
    assert!(tracker.peak() <= tracker.limit());
}

#[test]
fn worker_local_aggregate_states_combine_null_and_repeated_keys_without_copying_keys() {
    let plan = AggregatePlan {
        groups: 1,
        estimated_groups: None,
        grouping_sets: None,
        emit_empty_group_on_empty_input: false,
        aggs: vec![AggOpCode::Count],
        aggregate_input_types: vec![VeinDataType::Int64],
        types: vec![VeinDataType::Int64, VeinDataType::Int64],
    };
    let tracker = QueryMemoryTracker::unlimited(
        common::cancellation::CancellationToken::new(),
    );
    let (accumulator, mut canonical) =
        shared_with_memory_tracker(plan, tracker).expect("create canonical aggregate");
    let first_keys = Arc::new(Int64Array::from(vec![Some(7), None]));
    let second_keys = Arc::new(Int64Array::from(vec![Some(7), Some(8)]));
    let mut first = canonical
        .fork_local()
        .expect("fork aggregate state")
        .expect("aggregate supports worker-local state");
    let mut second = canonical
        .fork_local()
        .expect("fork second aggregate state")
        .expect("aggregate supports worker-local state");

    first
        .push(DataChunk::from_vectors(vec![
            Arc::clone(&first_keys) as ArrayRef,
            Arc::new(Int64Array::from(vec![10, 20])) as ArrayRef,
        ]))
        .expect("accumulate first worker batch");
    second
        .push(DataChunk::from_vectors(vec![
            Arc::clone(&second_keys) as ArrayRef,
            Arc::new(Int64Array::from(vec![30, 40])) as ArrayRef,
        ]))
        .expect("accumulate second worker batch");
    first
        .combine_local(canonical.as_mut())
        .expect("combine first aggregate state");
    second
        .combine_local(canonical.as_mut())
        .expect("combine second aggregate state");

    let mut aggregate = accumulator.lock().expect("aggregate mutex");
    let retained_key_buffers = aggregate
        .group_key_batches
        .iter()
        .flat_map(|batch| batch.iter().flatten())
        .filter_map(|column| match column {
            GroupKeyColumn::Int64(values) => Some(values.values().as_ptr()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(
        retained_key_buffers.contains(&first_keys.values().as_ptr())
            && retained_key_buffers.contains(&second_keys.values().as_ptr()),
        "combining group state retains the original Arrow key buffers"
    );

    let output = aggregate.finish().expect("finish combined aggregate");
    let mut counts = std::collections::BTreeMap::new();
    for batch in output {
        let keys = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let values = batch.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        for row in 0..batch.num_rows() {
            let key = keys.is_valid(row).then(|| keys.value(row));
            counts.insert(key, values.value(row));
        }
    }
    assert_eq!(
        counts,
        std::collections::BTreeMap::from([
            (None, 1),
            (Some(7), 2),
            (Some(8), 1),
        ])
    );
}

#[test]
fn aggregate_spill_preserves_null_keys_and_grouping_set_identity() {
    let plan = AggregatePlan {
        groups: 1,
        estimated_groups: None,
        grouping_sets: Some(vec![vec![0], vec![]]),
        emit_empty_group_on_empty_input: false,
        aggs: vec![AggOpCode::Count],
        aggregate_input_types: vec![VeinDataType::Int64],
        types: vec![VeinDataType::Int64, VeinDataType::Int64],
    };
    let tracker = QueryMemoryTracker::new(
        32 * 1024,
        common::cancellation::CancellationToken::new(),
    );
    let mut sink = AggregateSink::with_memory_tracker(plan, tracker)
        .expect("create budgeted grouping-set aggregate");
    let input_rows = 2048_i64;
    let keys = (0..input_rows)
        .map(|row| (row % 100 != 0).then_some(row % 257))
        .collect::<Vec<_>>();
    sink.accumulate(DataChunk::from_vectors(vec![
        Arc::new(Int64Array::from(keys.clone())) as ArrayRef,
        Arc::new(Int64Array::from_iter_values(0..input_rows)) as ArrayRef,
    ]))
        .expect("spill grouping-set state");
    assert!(
        !sink.spill_runs.is_empty(),
        "the constrained budget must create a spill run"
    );
    let output = sink.finish().expect("merge grouping-set spill runs");
    let mut null_key_counts = Vec::new();
    for batch in output {
        let keys = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let values = batch.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        for row in 0..batch.num_rows() {
            if keys.is_null(row) {
                null_key_counts.push(values.value(row));
            }
        }
    }
    null_key_counts.sort_unstable();
    let null_rows = keys.iter().filter(|key| key.is_none()).count() as i64;
    assert_eq!(null_key_counts, [null_rows, input_rows]);
}
