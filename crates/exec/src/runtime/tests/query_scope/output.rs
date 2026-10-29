use super::*;
use crate::pipeline::resumable::result::{
    Admission, FetchResult, OutputOrder, ResultBufferConfig, result_batch_footprint,
};
use crate::pipeline::test_support::dense_view;
use arrow::array::{ArrayRef, Int64Array};
use std::num::NonZeroUsize;
use std::task::{Wake, Waker};

#[derive(Default)]
struct Wakes(AtomicUsize);
impl Wake for Wakes {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn query_close_wakes_data_and_capacity_waiters_but_keeps_returned_buffers_valid() {
    let (_, _, resources) = scoped_task();
    let guard = resources.guard();
    let tracker = resources.memory_tracker().clone();
    let array: ArrayRef = Arc::new(Int64Array::from(vec![17]));
    let make = || dense_view(vec![Arc::clone(&array)]);
    let bytes = NonZeroUsize::new(result_batch_footprint(&make()).unwrap()).unwrap();
    let config = ResultBufferConfig {
        capacity_bytes: bytes,
        max_batch_bytes: bytes,
        order: OutputOrder::Unordered,
    };
    let one = NonZeroUsize::new(1).unwrap();
    let (mut consumer, mut producers) = resources.result_buffer(config, one).unwrap();
    let (mut empty, other) = resources.result_buffer(config, one).unwrap();
    let publish = |producer: &mut crate::pipeline::resumable::result::ResultProducer| {
        let Admission::Ready(permit) = producer.try_acquire().unwrap() else {
            panic!("expected credit")
        };
        permit.prepare(make()).unwrap().publish().unwrap();
    };
    publish(&mut producers[0]);
    let FetchResult::Batch(returned) = consumer.try_fetch().unwrap() else {
        panic!()
    };
    publish(&mut producers[0]);
    let Admission::Blocked(capacity) = producers[0].try_acquire().unwrap() else {
        panic!()
    };
    let FetchResult::Pending(data) = empty.try_fetch().unwrap() else {
        panic!()
    };
    let wakes = Arc::new(Wakes::default());
    let _capacity_guard = capacity.register(Waker::from(Arc::clone(&wakes))).unwrap();
    let _data_guard = data.register(Waker::from(Arc::clone(&wakes))).unwrap();
    drop(guard);
    assert_eq!(wakes.0.load(Ordering::SeqCst), 2);
    assert!(matches!(consumer.try_fetch(), Err(VeinError::Cancelled)));
    assert!(matches!(empty.try_fetch(), Err(VeinError::Cancelled)));
    assert_eq!(consumer.metrics().queued_batches, 0);
    let output = returned.view().materialize_column(0).unwrap();
    assert_eq!(
        output.to_data().buffers()[0].as_ptr(),
        array.to_data().buffers()[0].as_ptr()
    );
    assert_eq!(
        output
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        17
    );
    drop(output);
    drop(_capacity_guard);
    drop(_data_guard);
    drop(producers);
    drop(other);
    drop(consumer);
    drop(empty);
    drop(resources);
    assert!(
        tracker.used() > 0,
        "fetched batch still owns its memory leases"
    );
    drop(returned);
    assert_eq!(tracker.used(), 0);
}

#[test]
fn producer_failure_preserves_original_error_and_cancels_the_query() {
    let (_, _, resources) = scoped_task();
    let bytes = NonZeroUsize::new(4096).unwrap();
    let (mut consumer, mut producers) = resources
        .result_buffer(
            ResultBufferConfig {
                capacity_bytes: bytes,
                max_batch_bytes: bytes,
                order: OutputOrder::Unordered,
            },
            NonZeroUsize::new(1).unwrap(),
        )
        .unwrap();
    producers[0].fail(VeinError::internal("original scope failure"));
    assert!(resources.cancellation().is_cancelled());
    assert!(
        consumer
            .try_fetch()
            .err()
            .unwrap()
            .to_string()
            .contains("original scope failure")
    );
}
