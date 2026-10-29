use super::{budget, drain, fixture::pipeline, stage::Action::*};
use crate::pipeline::resumable::executor::PipelineStep;
use crate::pipeline::test_support::dense_view;
use arrow::array::{Array, ArrayRef, Int64Array};
use std::sync::Arc;

#[test]
fn every_small_budget_preserves_retries_empty_batches_nulls_order_and_buffers() {
    for work in 1..=12 {
        let array: ArrayRef = Arc::new(Int64Array::from(vec![Some(9), None, Some(9), Some(2)]));
        let original = array.to_data().buffers()[0].as_ptr();
        let (mut executor, records) = pipeline(vec![
            vec![Work, Block(1), Output(dense_view(vec![Arc::new(Int64Array::from(Vec::<i64>::new()))])),
                 Block(2), Output(dense_view(vec![Arc::clone(&array)])), Finish],
            vec![Need, RetryBatch, Work, Block(3), Need, AcceptBatch, Forward,
                 Need, AcceptBatch, Work, Forward, Need, RetryEof, Block(4),
                 Need, AcceptEof, Work, Block(5), Finish],
            vec![Need, RetryBatch, Block(6), Need, AcceptBatch, Work,
                 Need, AcceptBatch, Block(7), Need, AcceptEof, Work, Finish],
        ]);
        assert_eq!(drain(&mut executor, work), 7);
        for record in &records { assert_eq!(record.lock().unwrap().combined, 1); }
        let sink = records[2].lock().unwrap();
        assert_eq!((sink.batches, sink.eof, sink.offers), (2, 1, 4));
        assert_eq!(sink.received[0].num_rows(), 0, "an empty batch is not EOF");
        let output = sink.received[1].materialize_column(0).unwrap();
        assert_eq!(output.as_any().downcast_ref::<Int64Array>().unwrap().iter().collect::<Vec<_>>(),
                   vec![Some(9), None, Some(9), Some(2)]);
        assert_eq!(output.to_data().buffers()[0].as_ptr(), original);
        assert_eq!(records[0].lock().unwrap().steps, 6, "source steps must not replay");
    }
}

#[test]
fn one_input_can_emit_multiple_batches_before_eof() {
    let view = |value| dense_view(vec![Arc::new(Int64Array::from(vec![value]))]);
    let (mut executor, records) = pipeline(vec![
        vec![Output(view(4)), Finish],
        vec![Need, AcceptBatch, Forward, Work, Output(view(5)), Need, AcceptEof, Finish],
        vec![Need, AcceptBatch, Need, AcceptBatch, Need, AcceptEof, Finish],
    ]);
    drain(&mut executor, 1);
    let sink = records[2].lock().unwrap();
    let values: Vec<_> = sink.received.iter().map(|view|
        view.materialize_column(0).unwrap().as_any().downcast_ref::<Int64Array>().unwrap().value(0)).collect();
    assert_eq!(values, vec![4, 5]);
    assert_eq!(records[1].lock().unwrap().batches, 1);
}

#[test]
fn finalization_can_yield_emit_and_block_before_finishing() {
    let (mut executor, records) = pipeline(vec![
        vec![Finish],
        vec![Need, AcceptEof, Work, Output(dense_view(vec![Arc::new(Int64Array::from(vec![42]))])), Block(1), Finish],
        vec![Need, AcceptBatch, Need, AcceptEof, Work, Block(2), Finish],
    ]);
    assert_eq!(drain(&mut executor, 1), 2);
    assert_eq!(records[2].lock().unwrap().received[0].num_rows(), 1);
    assert_eq!(records[1].lock().unwrap().eof, 1);
}

#[test]
fn work_budget_bounds_operator_calls_and_completed_turns_are_idempotent() {
    let (mut executor, records) = pipeline(vec![
        vec![Work, Work, Finish], vec![Need, AcceptEof, Work, Finish],
    ]);
    let calls = || records.iter().map(|record| {
        let r = record.lock().unwrap();
        r.steps + r.offers
    }).sum::<usize>();
    for _ in 0..100 {
        let before = calls();
        let finished = matches!(executor.execute(budget(1)).unwrap(), PipelineStep::Finished);
        assert!(calls() - before <= 1);
        if finished {
            let before = calls();
            assert!(matches!(executor.execute(budget(5)).unwrap(), PipelineStep::Finished));
            assert_eq!(calls(), before);
            return;
        }
    }
    panic!("budgeted pipeline failed to finish");
}
