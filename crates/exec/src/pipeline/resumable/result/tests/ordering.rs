use super::*;

#[test]
fn unordered_results_follow_publication_order() {
    let (mut consumer, mut producers, _, _) = setup(OutputOrder::Unordered, 2, 3);
    send(&mut producers[1], view(2));
    send(&mut producers[0], view(1));
    assert_eq!(value(&fetch(&mut consumer)), 2);
    assert_eq!(value(&fetch(&mut consumer)), 1);
    for producer in &mut producers { producer.finish().unwrap(); }
    assert!(matches!(consumer.try_fetch().unwrap(), FetchResult::Finished));
}

#[test]
fn later_read_ahead_cannot_starve_the_earliest_required_producer() {
    let (mut consumer, mut producers, _, max) = setup(OutputOrder::ProducerOrder, 2, 2);
    send(&mut producers[1], view(2));
    assert!(matches!(producers[1].try_acquire().unwrap(), Admission::Blocked(_)));
    assert!(matches!(consumer.try_fetch().unwrap(), FetchResult::Pending(_)));
    let credit = permit(&mut producers[0]);
    assert!(consumer.metrics().retained_bytes + consumer.metrics().reserved_bytes <= max * 2);
    credit.prepare(view(1)).unwrap().publish().unwrap();
    assert_eq!(value(&fetch(&mut consumer)), 1);
    assert!(matches!(consumer.try_fetch().unwrap(), FetchResult::Pending(_)));
    producers[0].finish().unwrap();
    assert_eq!(value(&fetch(&mut consumer)), 2);
    producers[1].finish().unwrap();
    assert!(matches!(consumer.try_fetch().unwrap(), FetchResult::Finished));
}

#[test]
fn an_empty_ordinal_advances_order_without_being_confused_with_an_empty_batch() {
    let (mut consumer, mut producers, _, _) = setup(OutputOrder::ProducerOrder, 2, 2);
    let empty = crate::pipeline::test_support::dense_view(vec![Arc::new(Int64Array::from(Vec::<i64>::new()))]);
    send(&mut producers[1], empty);
    producers[1].finish().unwrap();
    assert!(matches!(consumer.try_fetch().unwrap(), FetchResult::Pending(_)));
    producers[0].finish().unwrap();
    assert_eq!(fetch(&mut consumer).view().num_rows(), 0);
    assert!(matches!(consumer.try_fetch().unwrap(), FetchResult::Finished));
}

#[test]
fn zero_column_rows_are_bounded_by_nonzero_metadata_credit() {
    let (mut consumer, mut producers, _, _) = setup(OutputOrder::Unordered, 1, 1);
    let view = DataChunkView::nulls(&[], 1_000_000).unwrap();
    assert!(result_batch_footprint(&view).unwrap() > 0);
    send(&mut producers[0], view);
    producers[0].finish().unwrap();
    let batch = fetch(&mut consumer);
    assert_eq!((batch.view().num_columns(), batch.view().num_rows()), (0, 1_000_000));
    assert!(matches!(consumer.try_fetch().unwrap(), FetchResult::Finished));
}
