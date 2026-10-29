use super::{budget, drain, fixture::pipeline, stage::Action::*};
use crate::pipeline::resumable::executor::{PipelineExecutor, PipelineStep, ResumeToken};
use crate::pipeline::test_support::dense_view;
use arrow::array::{ArrayRef, Int64Array};
use std::sync::Arc;

fn blocked(executor: &mut PipelineExecutor) -> ResumeToken {
    match executor.execute(budget(100)).unwrap() {
        PipelineStep::Blocked { token, reason } => {
            assert_eq!(reason.downcast::<u8>().unwrap(), 1);
            token
        }
        _ => panic!("expected a resource wait"),
    }
}

#[test]
fn only_the_current_owning_resume_token_can_wake_a_pipeline() {
    let script = || vec![vec![Block(1), Block(1), Finish], vec![Need, AcceptEof, Finish]];
    let (mut first, records) = pipeline(script());
    let (mut second, _) = pipeline(script());
    let old = blocked(&mut first);
    let foreign = blocked(&mut second);
    assert_eq!(old.stage(), 0);
    assert!(first.execute(budget(100)).is_err(), "waiting must not poll the stage");
    assert_eq!(records[0].lock().unwrap().steps, 1);
    assert!(first.resume(&foreign).is_err());
    first.resume(&old).unwrap();
    let current = blocked(&mut first);
    assert!(first.resume(&old).is_err(), "a previous suspension cannot wake the next one");
    first.resume(&current).unwrap();
    assert!(first.resume(&current).is_err(), "duplicate wakeups are rejected");
    drain(&mut first, 1);
    second.resume(&foreign).unwrap();
    drain(&mut second, 1);
}

#[test]
fn failure_releases_pending_views_and_prevents_reexecution() {
    let array: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
    let (mut executor, records) = pipeline(vec![
        vec![Output(dense_view(vec![Arc::clone(&array)])), Finish],
        vec![Need, RetryBatch, Fail],
    ]);
    assert!(executor.execute(budget(100)).is_err());
    assert_eq!(Arc::strong_count(&array), 1, "failed delivery must release its pending view");
    let steps = records[1].lock().unwrap().steps;
    assert!(executor.execute(budget(100)).is_err());
    assert_eq!(records[1].lock().unwrap().steps, steps);
    assert_eq!(records[0].lock().unwrap().combined, 0);
}

#[test]
fn dropping_a_suspended_pipeline_releases_undelivered_views() {
    let array: ArrayRef = Arc::new(Int64Array::from(vec![1]));
    let (mut executor, _) = pipeline(vec![
        vec![Output(dense_view(vec![Arc::clone(&array)])), Finish],
        vec![Need, RetryBatch, Block(1), Need, AcceptBatch, Need, AcceptEof, Finish],
    ]);
    let token = blocked(&mut executor);
    assert!(Arc::strong_count(&array) > 1);
    drop(executor);
    drop(token);
    assert_eq!(Arc::strong_count(&array), 1);
}

#[test]
fn invalid_stage_completion_is_terminal_instead_of_silently_losing_rows() {
    let (mut executor, _) = pipeline(vec![vec![Finish], vec![Finish]]);
    assert!(executor.execute(budget(100)).is_err(), "sink cannot finish before EOF");
    assert!(executor.execute(budget(1)).is_err());
}
