use super::super::execution::{fixture::prepared_result, stage::Action::*};
use super::{Notifications, budget, one};
use crate::memory::QueryMemoryTracker;
use crate::pipeline::resumable::result::{FetchResult, OutputOrder,
                                         ResultBufferConfig, ResultWaitRegistrar, result_batch_footprint, result_buffer};
use crate::pipeline::resumable::scheduler::{PipelineScheduler, SchedulerStep};
use crate::pipeline::test_support::dense_view;
use arrow::array::{ArrayRef, Int64Array};
use common::cancellation::CancellationToken;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::task::Waker;

#[test]
fn the_actual_result_sink_parks_and_resumes_without_replaying_source_output() {
    let view = |value| dense_view(vec![Arc::new(Int64Array::from(vec![value]))]);
    let bytes = NonZeroUsize::new(result_batch_footprint(&view(1)).unwrap()).unwrap();
    let tracker = QueryMemoryTracker::unlimited(CancellationToken::new());
    let (mut consumer, mut producers) = result_buffer(ResultBufferConfig {
        capacity_bytes: bytes,
        max_batch_bytes: bytes,
        order: OutputOrder::Unordered,
    }, one(), tracker).unwrap();
    let producer = producers.pop().unwrap();
    let (plan, records) = prepared_result(vec![vec![Output(view(1)), Output(view(2)), Output(view(3)), Finish], vec![]], Some(producer));
    let scheduler = PipelineScheduler::new(&plan, one(), ResultWaitRegistrar,
                                           Waker::from(Arc::new(Notifications::default())));
    for expected in 1..=3 {
        assert_eq!(scheduler.run_next(budget()).unwrap(), SchedulerStep::Progress);
        assert_eq!(scheduler.run_next(budget()).unwrap(), SchedulerStep::Idle);
        assert_eq!(records[0].lock().unwrap().steps, expected);
        let batch = match consumer.try_fetch().unwrap() {
            FetchResult::Batch(batch) => batch,
            _ => panic!()
        };
        assert_eq!(batch.view().materialize_column(0).unwrap().as_any().downcast_ref::<Int64Array>().unwrap().value(0), expected as i64);
    }
    assert_eq!(scheduler.run_next(budget()).unwrap(), SchedulerStep::Finished);
    assert!(matches!(consumer.try_fetch().unwrap(), FetchResult::Finished));
    assert_eq!(records[0].lock().unwrap().steps, 4);
}

#[test]
fn cancelling_a_parked_result_pipeline_releases_queued_and_future_arrays() {
    let array: ArrayRef = Arc::new(Int64Array::from(vec![1]));
    let make = || dense_view(vec![Arc::clone(&array)]);
    let bytes = NonZeroUsize::new(result_batch_footprint(&make()).unwrap()).unwrap();
    let tracker = QueryMemoryTracker::unlimited(CancellationToken::new());
    let (mut consumer, mut producers) = result_buffer(ResultBufferConfig {
        capacity_bytes: bytes,
        max_batch_bytes: bytes,
        order: OutputOrder::Unordered,
    }, one(), tracker.clone()).unwrap();
    let (plan, _) = prepared_result(vec![vec![Output(make()), Output(make()), Finish], vec![]], producers.pop());
    let scheduler = PipelineScheduler::new(&plan, one(), ResultWaitRegistrar,
                                           Waker::from(Arc::new(Notifications::default())));
    scheduler.run_next(budget()).unwrap();
    consumer.cancel();
    scheduler.cancel();
    assert_eq!(scheduler.run_next(budget()).unwrap(), SchedulerStep::Cancelled);
    assert_eq!(consumer.metrics().retained_bytes, 0);
    assert_eq!(Arc::strong_count(&array), 1);
    drop(scheduler);
    drop(plan);
    drop(consumer);
    drop(producers);
    assert_eq!(tracker.used(), 0);
}
