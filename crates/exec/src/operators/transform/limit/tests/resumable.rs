use crate::operators::transform::limit::LimitState;
use crate::pipeline::resumable::{Handoff, InputEvent, OperatorInputPort, OperatorStep, ResumableOperator};
use crate::pipeline::test_support::dense_view;
use arrow::array::{ArrayRef, Int64Array};
use std::sync::Arc;

#[test]
fn completed_limit_stops_input_then_finishes_once() {
    let mut limit = LimitState::new(0, 0);
    assert!(matches!(limit.step().unwrap(), OperatorStep::StopInput));
    assert!(matches!(limit.step().unwrap(), OperatorStep::Finished));
    assert!(limit.push_input(OperatorInputPort::new(0), InputEvent::EndOfInput).is_err());
}

#[test]
fn zero_count_finishes_without_stopping_an_already_ended_input() {
    let mut limit = LimitState::new(0, 0);
    assert!(matches!(limit.push_input(OperatorInputPort::new(0), InputEvent::EndOfInput), Ok(Handoff::Accepted)));
    assert!(matches!(limit.step().unwrap(), OperatorStep::Finished));
}

#[test]
fn output_reuses_input_arrow_array_and_requests_upstream_stop() {
    let array: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30, 40]));
    let original = Arc::clone(&array);
    let view = dense_view(vec![array]);
    let mut limit = LimitState::new(1, 2);
    assert!(matches!(limit.push_input(OperatorInputPort::new(0), InputEvent::Batch(view)), Ok(Handoff::Accepted)));
    let OperatorStep::Output(output) = limit.step().unwrap() else { panic!("expected selected output") };
    assert_eq!(output.num_rows(), 2);
    assert!(Arc::ptr_eq(output.column_view(0).unwrap().source_arrays().first().unwrap(), &original));
    assert!(matches!(limit.step().unwrap(), OperatorStep::StopInput));
    assert!(matches!(limit.step().unwrap(), OperatorStep::Finished));
}
