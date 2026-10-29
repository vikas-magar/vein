use super::*;
use super::super::execution::fixture::prepared_result;
use crate::memory::QueryMemoryTracker;
use crate::pipeline::resumable::result::{
    FetchResult, OutputOrder, ResultBufferConfig, ResultConsumer, ResultWaitRegistrar,
    result_batch_footprint, result_buffer,
};
use crate::pipeline::test_support::dense_view;
use arrow::array::{Array, ArrayRef, Int64Array};
use common::cancellation::CancellationToken;

async fn collect(mut consumer: ResultConsumer, original: &ArrayRef) -> usize {
    let mut rows = 0;
    loop {
        let mut subscription = None;
        let next = futures::future::poll_fn(|cx| {
            match consumer.try_fetch().unwrap() {
                FetchResult::Batch(batch) => Poll::Ready(Some(batch)),
                FetchResult::Finished => Poll::Ready(None),
                FetchResult::Pending(wait) => {
                    subscription = Some(wait.register(cx.waker().clone()).unwrap());
                    Poll::Pending
                }
            }
        }).await;
        drop(subscription);
        let Some(batch) = next else { return rows };
        let output = batch.view().materialize_column(0).unwrap();
        assert_eq!(output.to_data().buffers()[0].as_ptr(), original.to_data().buffers()[0].as_ptr());
        rows += batch.view().num_rows();
        EventHost::default().yield_control().await.unwrap();
    }
}

#[tokio::test]
async fn both_hosts_stream_through_one_batch_capacity_to_a_slow_consumer() {
    for cooperative in [true, false] {
        let array: ArrayRef = Arc::new(Int64Array::from(vec![7]));
        let view = || dense_view(vec![Arc::clone(&array)]);
        let bytes = n(result_batch_footprint(&view()).unwrap());
        let tracker = QueryMemoryTracker::unlimited(CancellationToken::new());
        let (consumer, mut producers) = result_buffer(ResultBufferConfig {
            capacity_bytes: bytes,
            max_batch_bytes: bytes,
            order: OutputOrder::Unordered,
        }, n(1), tracker.clone()).unwrap();
        let (plan, records) = prepared_result(vec![
            vec![Action::Output(view()), Action::Output(view()), Action::Output(view()), Action::Finish],
            vec![],
        ], producers.pop());
        let driver = HostDriver::new(&plan, n(1), n(2), ResultWaitRegistrar);
        if cooperative {
            let mut host = EventHost::default();
            let (execution, rows) = futures::executor::block_on(async {
                futures::join!(driver.run_cooperative(&mut host), collect(consumer, &array))
            });
            assert_eq!(execution.unwrap(), SchedulerStep::Finished);
            assert_eq!(rows, 3);
        } else {
            #[cfg(not(target_arch = "wasm32"))]
            {
                let (execution, rows) = tokio::join!(driver.run_native(n(2)), collect(consumer, &array));
                assert_eq!(rows, 3);
                assert_eq!(execution.unwrap(), SchedulerStep::Finished);
            }
            #[cfg(target_arch = "wasm32")]
            { continue; }
        }
        assert_eq!(records[0].lock().unwrap().steps, 4);
        drop(plan);
        drop(producers);
        assert_eq!(tracker.used(), 0);
    }
}
