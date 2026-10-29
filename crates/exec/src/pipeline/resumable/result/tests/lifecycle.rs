use super::*;

#[test]
fn close_releases_queued_output_and_returned_handles_remain_valid() {
    let (mut consumer, mut producers, tracker, _) = setup(OutputOrder::Unordered, 2, 5);
    let baseline = tracker.used();
    send(&mut producers[0], view(1));
    let returned = fetch(&mut consumer);
    send(&mut producers[0], view(2));
    let pending = permit(&mut producers[1]).prepare(view(3)).unwrap();
    consumer.close();
    consumer.close();
    assert_eq!(consumer.metrics().queued_batches, 0);
    assert_eq!(consumer.metrics().pending_batches, 1);
    assert_eq!(value(&returned), 1);
    assert!(pending.publish().is_err());
    assert_eq!(consumer.metrics().retained_bytes, 0);
    assert!(tracker.used() > baseline);
    drop(returned);
    assert_eq!(tracker.used(), baseline);
    assert!(producers[0].try_acquire().is_err());
    assert!(matches!(consumer.try_fetch().unwrap(), FetchResult::Finished));
}

#[test]
fn cancellation_and_failure_have_stable_terminal_results() {
    let (mut consumer, mut producers, _, _) = setup(OutputOrder::Unordered, 1, 1);
    send(&mut producers[0], view(1));
    consumer.cancel();
    assert_eq!(consumer.metrics().retained_bytes, 0);
    for _ in 0..2 { assert!(matches!(consumer.try_fetch(), Err(VeinError::Cancelled))); }
    let (mut consumer, mut producers, _, _) = setup(OutputOrder::Unordered, 1, 1);
    producers[0].fail(VeinError::internal("specific result failure"));
    assert!(consumer.try_fetch().err().unwrap().to_string().contains("specific result failure"));
    assert!(consumer.try_fetch().is_err());
}

#[test]
fn abandoned_producers_fail_instead_of_leaving_consumers_waiting() {
    let (mut consumer, producers, _, _) = setup(OutputOrder::ProducerOrder, 2, 2);
    drop(producers);
    assert!(consumer.try_fetch().err().unwrap().to_string().contains("dropped before completion"));
}

#[test]
fn dropping_the_consumer_releases_queued_array_and_metadata_leases() {
    let (consumer, mut producers, tracker, _) = setup(OutputOrder::Unordered, 1, 1);
    let baseline = tracker.used();
    send(&mut producers[0], view(1));
    assert!(tracker.used() > baseline);
    drop(consumer);
    assert_eq!(tracker.used(), baseline);
    assert!(producers[0].try_acquire().is_err());
    drop(producers);
    assert_eq!(tracker.used(), 0);
}

#[test]
fn invalid_capacity_and_batch_limits_are_rejected() {
    let result = result_buffer(ResultBufferConfig {
        capacity_bytes: nz(1),
        max_batch_bytes: nz(2),
        order: OutputOrder::Unordered,
    }, nz(1), tracker());
    assert!(result.is_err());
}
