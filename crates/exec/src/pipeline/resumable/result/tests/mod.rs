//! Output admission, ordering, lifetime and scheduler-readiness regressions.
mod accounting;
mod ordering;
mod lifecycle;
mod readiness;

use super::*;
use arrow::array::{ArrayRef, Int64Array};
use common::cancellation::CancellationToken;
use std::num::NonZeroUsize;
use std::sync::Arc;

fn nz(value: usize) -> NonZeroUsize { NonZeroUsize::new(value).unwrap() }
fn tracker() -> QueryMemoryTracker { QueryMemoryTracker::unlimited(CancellationToken::new()) }
fn view(value: i64) -> DataChunkView {
    crate::pipeline::test_support::dense_view(vec![Arc::new(Int64Array::from(vec![value])) as ArrayRef])
}
fn setup(order: OutputOrder, producers: usize, slots: usize) -> (ResultConsumer, Vec<ResultProducer>, QueryMemoryTracker, usize) {
    let tracker = tracker();
    let size = result_batch_footprint(&view(1)).unwrap();
    let (consumer, producers) = result_buffer(ResultBufferConfig {
        capacity_bytes: nz(size * slots),
        max_batch_bytes: nz(size),
        order,
    }, nz(producers), tracker.clone()).unwrap();
    (consumer, producers, tracker, size)
}
fn permit(producer: &mut ResultProducer) -> ResultPermit {
    match producer.try_acquire().unwrap() {
        Admission::Ready(permit) => permit,
        Admission::Blocked(_) => panic!("expected output capacity"),
    }
}
fn send(producer: &mut ResultProducer, view: DataChunkView) { permit(producer).prepare(view).unwrap().publish().unwrap(); }
fn fetch(consumer: &mut ResultConsumer) -> ResultBatch {
    match consumer.try_fetch().unwrap() {
        FetchResult::Batch(batch) => batch,
        _ => panic!("expected a result batch"),
    }
}
fn value(batch: &ResultBatch) -> i64 {
    batch.view().materialize_column(0).unwrap().as_any().downcast_ref::<Int64Array>().unwrap().value(0)
}
