//! Global cardinality hints must not be multiplied across worker-local states.
use super::*;
use arrow::array::{Array, ArrayRef, Int64Array};
use common::arrow::SelectionVector;

mod direct;

fn plan(estimate: Option<u64>) -> AggregatePlan {
    AggregatePlan {
        groups: 1,
        estimated_groups: estimate,
        grouping_sets: None,
        emit_empty_group_on_empty_input: false,
        aggs: vec![AggOpCode::Count],
        aggregate_input_types: vec![VeinDataType::Int64],
        types: vec![VeinDataType::Int64, VeinDataType::Int64],
    }
}

#[test]
fn worker_fork_does_not_reserve_the_global_cardinality_again() {
    let tracker = QueryMemoryTracker::unlimited(common::cancellation::CancellationToken::new());
    let (shared, mut canonical) =
        shared_with_memory_tracker(plan(Some(1_000_000)), tracker.clone()).unwrap();
    let before = tracker.used();
    assert!(before > 0, "canonical owner still uses its global estimate");
    let mut workers = (0..4)
        .map(|_| canonical.fork_local().unwrap().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        tracker.used(),
        before,
        "empty workers must not each reserve the global estimate"
    );
    for worker in &mut workers {
        let local = worker
            .as_any_mut()
            .downcast_mut::<LocalAggregateHandle>()
            .unwrap();
        assert_eq!(local.0.plan.estimated_groups, None);
        assert_eq!(local.0.group_count(), 0);
    }
    for worker in workers {
        worker.combine_local(canonical.as_mut()).unwrap();
    }
    assert_eq!(
        shared.lock().unwrap().plan.estimated_groups,
        Some(1_000_000)
    );
    drop(canonical);
    drop(shared);
    assert_eq!(tracker.used(), 0);
}

#[test]
fn local_tables_grow_and_combine_without_copying_group_keys() {
    let (shared, mut canonical) = shared(plan(Some(1_000_000))).unwrap();
    let source: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 10]));
    for _ in 0..2 {
        let mut worker = canonical.fork_local().unwrap().unwrap();
        worker
            .push(
                DataChunkView::from_arrays(
                    vec![
                        Arc::clone(&source),
                        Arc::new(Int64Array::from(vec![1, 1, 1])),
                    ],
                    3,
                    SelectionVector::new_dense(3),
                )
                .unwrap(),
            )
            .unwrap();
        let local = worker
            .as_any_mut()
            .downcast_mut::<LocalAggregateHandle>()
            .unwrap();
        assert_eq!(local.0.group_count(), 2);
        assert!(local.0.metrics().group_index_capacity < 100);
        worker.combine_local(canonical.as_mut()).unwrap();
    }
    let mut state = shared.lock().unwrap();
    let batches = state.finish().unwrap();
    let mut rows = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            let (key, index) = batch.resolve(0, row).unwrap().unwrap();
            assert_eq!(
                key.to_data().buffers()[0].as_ptr(),
                source.to_data().buffers()[0].as_ptr()
            );
            let (count, count_row) = batch.resolve(1, row).unwrap().unwrap();
            rows.push((
                key.as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(index),
                count
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(count_row),
            ));
        }
    }
    rows.sort();
    assert_eq!(rows, vec![(10, 4), (20, 2)]);
}
