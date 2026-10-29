use super::*;

#[tokio::test]
async fn worker_local_aggregate_states_combine_null_and_repeated_keys_without_copying_keys() {
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
        .push(crate::pipeline::test_support::dense_view(vec![
            Arc::clone(&first_keys) as ArrayRef,
            Arc::new(Int64Array::from(vec![10, 20])) as ArrayRef,
        ]))
        .await.expect("accumulate first worker batch");
    second
        .push(crate::pipeline::test_support::dense_view(vec![
            Arc::clone(&second_keys) as ArrayRef,
            Arc::new(Int64Array::from(vec![30, 40])) as ArrayRef,
        ]))
        .await.expect("accumulate second worker batch");
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
        .flat_map(|column| column.source_arrays().expect("retained key sources are available"))
        .filter_map(|array| {
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .map(|values| values.values().as_ptr())
        })
        .collect::<Vec<_>>();
    assert!(
        retained_key_buffers.contains(&first_keys.values().as_ptr())
            && retained_key_buffers.contains(&second_keys.values().as_ptr()),
        "combining group state retains the original Arrow key buffers"
    );

    let output = aggregate.finish().await.expect("finish combined aggregate");
    let mut counts = std::collections::BTreeMap::new();
    for batch in output {
        let keys = crate::pipeline::test_support::materialized_column::<Int64Array>(&batch, 0);
        let values = crate::pipeline::test_support::materialized_column::<Int64Array>(&batch, 1);
        for row in 0..batch.num_rows() {
            counts.insert(keys.is_valid(row).then(|| keys.value(row)), values.value(row));
        }
    }
    assert_eq!(
        counts,
        std::collections::BTreeMap::from([(None, 1), (Some(7), 2), (Some(8), 1)])
    );
}
