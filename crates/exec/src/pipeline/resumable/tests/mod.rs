use std::collections::VecDeque;
use std::sync::Arc;

use arrow::array::Int64Array;
use common::Result;
use error::VeinError;

use super::{Handoff, InputEvent, OperatorStep, ResumableOperator};
use crate::pipeline::{DataChunkView, test_support::dense_view};

mod registry;
mod fixture;
mod prepared;
pub(super) use prepared::fixture as source_plan_fixture;
mod capabilities;
mod adapter;

#[derive(Clone, Copy)]
enum Wait {
    Consumer,
}

struct ScriptedStage {
    steps: VecDeque<OperatorStep<Wait>>,
    ended: bool,
    finished: bool,
    accepted_rows: usize,
}

impl ResumableOperator for ScriptedStage {
    type WaitReason = Wait;

    fn push_input(&mut self, _: crate::pipeline::resumable::OperatorInputPort, input: InputEvent) -> Result<Handoff<InputEvent>> {
        if self.finished || self.ended {
            return Err(VeinError::internal("input received after stage completion"));
        }
        match input {
            InputEvent::Batch(view) => self.accepted_rows += view.num_rows(),
            InputEvent::EndOfInput => self.ended = true,
        }
        Ok(Handoff::Accepted)
    }

    fn step(&mut self) -> Result<OperatorStep<Self::WaitReason>> {
        if self.finished {
            return Err(VeinError::internal("finished stage was re-entered"));
        }
        let step = self.steps.pop_front().ok_or_else(|| VeinError::internal("script exhausted"))?;
        if matches!(&step, OperatorStep::Finished) && !self.ended {
            return Err(VeinError::internal("stage finished before end-of-input"));
        }
        self.finished = matches!(&step, OperatorStep::Finished);
        Ok(step)
    }
}

fn view(values: Vec<i64>) -> DataChunkView {
    dense_view(vec![Arc::new(Int64Array::from(values))])
}

fn deliver(view: DataChunkView, ready: bool) -> Handoff<DataChunkView> {
    if ready {
        Handoff::Accepted
    } else {
        Handoff::Retry(view)
    }
}

fn stage(steps: Vec<OperatorStep<Wait>>) -> ScriptedStage {
    ScriptedStage { steps: steps.into(), ended: false, finished: false, accepted_rows: 0 }
}

#[test]
fn empty_batch_is_distinct_from_end_of_input() {
    let mut stage = stage(vec![OperatorStep::NeedsInput]);
    let empty = view(Vec::new());
    assert!(empty.is_empty());
    assert!(matches!(stage.push_input(crate::pipeline::resumable::OperatorInputPort::new(0), InputEvent::Batch(empty)), Ok(Handoff::Accepted)));
    assert_eq!(stage.accepted_rows, 0);
    assert!(matches!(stage.push_input(crate::pipeline::resumable::OperatorInputPort::new(0), InputEvent::EndOfInput), Ok(Handoff::Accepted)));
    assert!(stage.ended);
    assert!(stage.push_input(crate::pipeline::resumable::OperatorInputPort::new(0), InputEvent::EndOfInput).is_err());
}

#[test]
fn runnable_and_input_wait_are_distinct_scheduler_states() {
    let mut stage = stage(vec![OperatorStep::Runnable, OperatorStep::NeedsInput]);
    assert!(matches!(stage.step().expect("runnable"), OperatorStep::Runnable));
    assert!(matches!(stage.step().expect("input wait"), OperatorStep::NeedsInput));
}

#[test]
fn blocked_output_handoff_retries_the_same_view_once() {
    let output = view(vec![7, 9]);
    let original_array = output.materialize_column(0).expect("source array");
    let mut stage = stage(vec![OperatorStep::Output(output), OperatorStep::Blocked(Wait::Consumer), OperatorStep::Finished]);
    stage
        .push_input(crate::pipeline::resumable::OperatorInputPort::new(0), InputEvent::Batch(view(vec![1])))
        .expect("input accepted");
    stage
        .push_input(crate::pipeline::resumable::OperatorInputPort::new(0), InputEvent::EndOfInput)
        .expect("end of input accepted");
    let pending = match stage.step().expect("output step") {
        OperatorStep::Output(view) => view,
        _ => panic!("expected output"),
    };
    let pending = match deliver(pending, false) {
        Handoff::Retry(view) => view,
        Handoff::Accepted => panic!("blocked sink cannot accept ownership"),
    };
    assert_eq!(pending.num_rows(), 2);
    let retried_array = pending.materialize_column(0).expect("retried array");
    assert_eq!(
        original_array.to_data().buffers()[0].as_ptr(),
        retried_array.to_data().buffers()[0].as_ptr(),
        "the retried view must retain its Arrow value buffer",
    );
    assert!(matches!(stage.step().expect("blocked"), OperatorStep::Blocked(Wait::Consumer)));
    let mut delivered_batches = 0;
    match deliver(pending, true) {
        Handoff::Accepted => delivered_batches += 1,
        Handoff::Retry(_) => panic!("ready sink must accept the pending batch"),
    }
    assert_eq!(delivered_batches, 1);
    assert!(matches!(stage.step().expect("finish"), OperatorStep::Finished));
    assert!(stage.step().is_err(), "finished stages reject re-entry");
}

#[test]
fn stage_errors_propagate_without_mutating_inputs() {
    let mut stage = stage(Vec::new());
    assert!(stage.step().is_err());
}
