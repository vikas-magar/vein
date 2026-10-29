use super::*;
use crate::pipeline::test_support::{dense_view, selected_view};
use arrow::array::Array;
use common::arrow::SelectionVector;

#[test]
fn permits_and_prepared_output_are_counted_before_publication() {
    let (mut consumer, mut producers, tracker, max) = setup(OutputOrder::Unordered, 1, 1);
    let baseline = tracker.used();
    let credit = permit(&mut producers[0]);
    assert_eq!(consumer.metrics().reserved_bytes, max);
    assert!(producers[0].try_acquire().is_err());
    assert!(producers[0].finish().is_err());
    let pending = credit.prepare(view(1)).unwrap();
    let metrics = consumer.metrics();
    assert_eq!((metrics.reserved_bytes, metrics.pending_batches, metrics.queued_batches), (0, 1, 0));
    assert!(metrics.retained_bytes <= max);
    assert!(tracker.used() > baseline);
    assert!(matches!(consumer.try_fetch().unwrap(), FetchResult::Pending(_)));
    drop(pending);
    assert_eq!(consumer.metrics().retained_bytes, 0);
    assert_eq!(tracker.used(), baseline);
    drop(permit(&mut producers[0]));
    assert_eq!(consumer.metrics().reserved_bytes, 0);
    producers[0].finish().unwrap();
}

#[test]
fn shared_arrays_are_charged_once_and_returned_views_retain_query_leases() {
    let (mut consumer, mut producers, tracker, _) = setup(OutputOrder::Unordered, 1, 4);
    let baseline = tracker.used();
    let array: ArrayRef = Arc::new(Int64Array::from(vec![7]));
    let bytes = array.get_array_memory_size();
    let first = dense_view(vec![Arc::clone(&array)]);
    let standalone = result_batch_footprint(&first).unwrap();
    send(&mut producers[0], first);
    send(&mut producers[0], dense_view(vec![Arc::clone(&array)]));
    assert_eq!(consumer.metrics().retained_arrays, 1);
    assert_eq!(consumer.metrics().retained_bytes, standalone * 2 - bytes);
    let first = fetch(&mut consumer);
    let second = fetch(&mut consumer);
    assert_eq!(consumer.metrics().retained_bytes, 0);
    assert!(tracker.used() > baseline, "client-held views still retain leases");
    assert!(Arc::ptr_eq(first.view().source_arrays_for_column(0).unwrap().first().unwrap(), &array));
    drop(first);
    assert!(tracker.used() > baseline);
    drop(second);
    assert_eq!(tracker.used(), baseline);
    producers[0].finish().unwrap();
}

#[test]
fn sparse_views_are_limited_by_the_full_array_they_pin() {
    let (_consumer, mut producers, tracker, _) = setup(OutputOrder::Unordered, 1, 1);
    let baseline = tracker.used();
    let array: ArrayRef = Arc::new(Int64Array::from(vec![0; 10_000]));
    let sparse = selected_view(vec![array], SelectionVector::new_sparse(vec![4]));
    assert!(matches!(permit(&mut producers[0]).prepare(sparse), Err(VeinError::ResourceExhausted { .. })));
    assert_eq!(tracker.used(), baseline);
    producers[0].finish().unwrap();
}

#[test]
fn query_memory_failure_refunds_credit_instead_of_waiting_forever() {
    let (consumer, producers, initial, max) = setup(OutputOrder::Unordered, 1, 1);
    let baseline = initial.used();
    drop(consumer);
    drop(producers);
    let tracker = QueryMemoryTracker::new(baseline + max - 1, CancellationToken::new());
    let (consumer, mut producers) = result_buffer(ResultBufferConfig {
        capacity_bytes: nz(max),
        max_batch_bytes: nz(max),
        order: OutputOrder::Unordered,
    }, nz(1), tracker.clone()).unwrap();
    assert!(matches!(permit(&mut producers[0]).prepare(view(1)), Err(VeinError::ResourceExhausted { .. })));
    assert_eq!(consumer.metrics().reserved_bytes, 0);
    assert_eq!(tracker.used(), baseline);
    producers[0].finish().unwrap();
}

#[test]
fn slow_consumers_bound_queued_and_reserved_output() {
    let (mut consumer, mut producers, _, max) = setup(OutputOrder::Unordered, 1, 2);
    let mut accepted = 0;
    for _ in 0..100 {
        match producers[0].try_acquire().unwrap() {
            Admission::Ready(permit) => {
                permit.prepare(view(1)).unwrap().publish().unwrap();
                accepted += 1;
            }
            Admission::Blocked(_) => break,
        }
        let metrics = consumer.metrics();
        assert!(metrics.reserved_bytes + metrics.retained_bytes <= max * 2);
    }
    assert!(accepted > 0 && accepted < 100);
    drop(fetch(&mut consumer));
    drop(permit(&mut producers[0]));
    producers[0].finish().unwrap();
}

#[test]
fn a_small_selection_still_accounts_for_a_large_retained_row_map() {
    use crate::pipeline::{RowMapping, SourceRow, ViewColumn};
    let (_consumer, mut producers, _, _) = setup(OutputOrder::Unordered, 1, 1);
    let mapping = Arc::new(RowMapping::new(Arc::from(vec![1]),
                                           vec![Some(SourceRow::new(0, 0).unwrap()); 10_000]).unwrap());
    let column = ViewColumn::new(vec![Arc::new(Int64Array::from(vec![1])) as ArrayRef],
                                 arrow::datatypes::DataType::Int64, mapping).unwrap();
    let view = DataChunkView::new(10_000, vec![column], SelectionVector::new_sparse(vec![0])).unwrap();
    assert_eq!(view.num_rows(), 1);
    assert!(view.retained_metadata_bytes().unwrap() >= 10_000 * std::mem::size_of::<SourceRow>());
    assert!(matches!(permit(&mut producers[0]).prepare(view), Err(VeinError::ResourceExhausted { .. })));
    producers[0].finish().unwrap();
}
