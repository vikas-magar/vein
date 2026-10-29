use super::super::super::execution::fixture::prepared_result;
use super::*;
use crate::pipeline::resumable::result::{
    FetchResult, OutputOrder, ResultBufferConfig, ResultWaitRegistrar, result_batch_footprint,
};
use crate::pipeline::test_support::dense_view;
use arrow::array::{Array, ArrayRef, Int64Array};

#[test]
fn finished_computation_keeps_unread_output_until_the_consumer_closes() {
    let (template, _) = prepared(vec![
        vec![Action::Finish],
        vec![Action::Need, Action::AcceptEof, Action::Finish],
    ]);
    let resources = resources(&template);
    let array: ArrayRef = Arc::new(Int64Array::from(vec![9]));
    let make = || dense_view(vec![Arc::clone(&array)]);
    let bytes = result_batch_footprint(&make()).unwrap();
    let (mut consumer, mut producers) = resources
        .result_buffer(
            ResultBufferConfig {
                capacity_bytes: n(bytes * 2),
                max_batch_bytes: n(bytes),
                order: OutputOrder::Unordered,
            },
            n(1),
        )
        .unwrap();
    let (plan, _) = prepared_result(
        vec![vec![Action::Output(make()), Action::Finish], vec![]],
        producers.pop(),
    );
    let driver = resources
        .host_driver(&plan, n(1), n(10), ResultWaitRegistrar)
        .unwrap();
    assert_eq!(
        futures::executor::block_on(driver.run_cooperative(&mut EventHost::default())).unwrap(),
        SchedulerStep::Finished
    );
    assert!(
        !resources.cancellation().is_cancelled(),
        "compute completion cannot discard unread output"
    );
    let FetchResult::Batch(returned) = consumer.try_fetch().unwrap() else {
        panic!()
    };
    assert_eq!(
        returned
            .view()
            .materialize_column(0)
            .unwrap()
            .to_data()
            .buffers()[0]
            .as_ptr(),
        array.to_data().buffers()[0].as_ptr()
    );
    assert!(matches!(
        consumer.try_fetch().unwrap(),
        FetchResult::Finished
    ));
    consumer.close();
    assert!(resources.cancellation().is_cancelled());
    assert_eq!(returned.view().num_rows(), 1);
}

#[test]
fn closing_results_cancels_the_bound_scheduler_before_source_creation() {
    let (plan, records) = prepared(vec![
        vec![Action::Finish],
        vec![Action::Need, Action::AcceptEof, Action::Finish],
    ]);
    let resources = resources(&plan);
    let (mut consumer, _producers) = resources
        .result_buffer(
            ResultBufferConfig {
                capacity_bytes: n(4096),
                max_batch_bytes: n(4096),
                order: OutputOrder::Unordered,
            },
            n(1),
        )
        .unwrap();
    let driver = resources
        .host_driver(&plan, n(1), n(1), Io::default())
        .unwrap();
    consumer.close();
    assert_eq!(
        futures::executor::block_on(driver.run_cooperative(&mut EventHost::default())).unwrap(),
        SchedulerStep::Cancelled
    );
    assert_eq!(records[0].lock().unwrap().steps, 0);
}
