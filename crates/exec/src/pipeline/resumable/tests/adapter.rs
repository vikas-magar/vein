use std::collections::VecDeque;
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use common::Result;

use crate::pipeline::resumable::operator::OperatorAdapter;
use crate::pipeline::resumable::{
    ErasedResumableOperator, Handoff, InputEvent, LocalStateOperator, OperatorChild,
    OperatorMetadata, OperatorNodeId, OperatorRole, OperatorStep, ResumableOperator,
};
use crate::pipeline::{DataChunkView, test_support::dense_view};

struct ScriptedOperator {
    steps: VecDeque<OperatorStep<&'static str>>,
}

impl ResumableOperator for ScriptedOperator {
    type WaitReason = &'static str;

    fn push_input(&mut self, _: crate::pipeline::resumable::OperatorInputPort, _: InputEvent) -> Result<Handoff<InputEvent>> {
        Ok(Handoff::Accepted)
    }

    fn step(&mut self) -> Result<OperatorStep<Self::WaitReason>> {
        self.steps.pop_front().ok_or_else(|| common::VeinError::internal("script exhausted"))
    }
}

impl LocalStateOperator for ScriptedOperator {
    type LocalState = ();

    fn take_local_state(&mut self) -> Result<()> { Ok(()) }
}

fn metadata(role: OperatorRole) -> OperatorMetadata {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    let children = if role == OperatorRole::Source {
        Vec::new()
    } else {
        vec![OperatorChild { node: OperatorNodeId::new(1), schema: Arc::clone(&schema) }]
    };
    let output = (role != OperatorRole::Sink).then_some(schema);
    OperatorMetadata::new(OperatorNodeId::new(2), role, children, output).expect("valid role metadata")
}

fn output() -> DataChunkView {
    dense_view(vec![Arc::new(Int64Array::from(vec![1]))])
}

fn stage(role: OperatorRole, steps: Vec<OperatorStep<&'static str>>) -> Box<dyn ErasedResumableOperator> {
    Box::new(OperatorAdapter::new(ScriptedOperator { steps: steps.into() }, &metadata(role)))
}

#[test]
fn source_rejects_upstream_input_and_input_waits() {
    let mut source = stage(OperatorRole::Source, vec![OperatorStep::NeedsInput]);
    assert!(source.push_input(crate::pipeline::resumable::OperatorInputPort::new(0), InputEvent::EndOfInput).is_err());
    assert!(source.step().is_err());
}

#[test]
fn transform_must_receive_eof_before_finalizing() {
    let mut transform = stage(OperatorRole::Transform, vec![OperatorStep::Finished]);
    assert!(transform.step().is_err());

    let mut transform = stage(
        OperatorRole::Transform,
        vec![OperatorStep::Runnable, OperatorStep::Finished],
    );
    assert!(matches!(
        transform.push_input(crate::pipeline::resumable::OperatorInputPort::new(0), InputEvent::EndOfInput).expect("accept EOF"),
        Handoff::Accepted
    ));
    assert!(matches!(transform.step().expect("drain"), OperatorStep::Runnable));
    assert!(matches!(transform.step().expect("finish"), OperatorStep::Finished));
    assert!(transform.push_input(crate::pipeline::resumable::OperatorInputPort::new(0), InputEvent::EndOfInput).is_err());
    transform.take_local_state().expect("take completed local state");
}

#[test]
fn sink_cannot_emit_batches() {
    let mut sink = stage(OperatorRole::Sink, vec![OperatorStep::Output(output())]);
    sink.push_input(crate::pipeline::resumable::OperatorInputPort::new(0), InputEvent::EndOfInput).expect("accept EOF");
    assert!(sink.step().is_err());
}

#[test]
fn input_ports_close_independently_and_invalid_ports_are_rejected() {
    use crate::pipeline::resumable::OperatorInputPort;
    let mut metadata = metadata(OperatorRole::Transform);
    let mut children = metadata.children.to_vec();
    children.push(children[0].clone());
    metadata.children = children.into();
    let operator = ScriptedOperator { steps: vec![OperatorStep::Finished].into() };
    let mut stage = OperatorAdapter::new(operator, &metadata);
    assert!(stage.push_input(OperatorInputPort::new(2), InputEvent::EndOfInput).is_err());
    stage.push_input(OperatorInputPort::new(0), InputEvent::EndOfInput).unwrap();
    assert!(stage.push_input(OperatorInputPort::new(0), InputEvent::EndOfInput).is_err());
    stage.push_input(OperatorInputPort::new(1), InputEvent::EndOfInput).unwrap();
    assert!(matches!(stage.step().unwrap(), OperatorStep::Finished));

    let operator = ScriptedOperator { steps: vec![OperatorStep::Finished].into() };
    let mut stage = OperatorAdapter::new(operator, &metadata);
    stage.push_input(OperatorInputPort::new(0), InputEvent::EndOfInput).unwrap();
    assert!(stage.step().is_err(), "one closed port cannot finalize another port");
}

#[test]
fn phase_adapter_waits_only_for_its_assigned_input_and_rejects_closed_input_waits() {
    use crate::pipeline::resumable::OperatorInputPort;
    use common::protocol::pipeline_graph::PipelinePhase;
    let mut definition = metadata(OperatorRole::Transform);
    let mut children = definition.children.to_vec();
    children.push(children[0].clone());
    definition.children = children.into();
    let operator = ScriptedOperator { steps: vec![OperatorStep::Finished].into() };
    let mut sink = OperatorAdapter::for_phase(operator, &definition,
                                              PipelinePhase::Sink(OperatorInputPort::new(0))).unwrap();
    assert!(sink.push_input(OperatorInputPort::new(1), InputEvent::EndOfInput).is_err());
    sink.push_input(OperatorInputPort::new(0), InputEvent::EndOfInput).unwrap();
    assert!(matches!(sink.step().unwrap(), OperatorStep::Finished));

    let operator = ScriptedOperator { steps: vec![OperatorStep::Finished].into() };
    let mut source = OperatorAdapter::for_phase(operator, &definition, PipelinePhase::Source).unwrap();
    assert!(matches!(source.step().unwrap(), OperatorStep::Finished));

    let operator = ScriptedOperator { steps: vec![OperatorStep::NeedsInput].into() };
    let mut finalizer = OperatorAdapter::for_phase(operator, &metadata(OperatorRole::Sink), PipelinePhase::Finalize).unwrap();
    assert!(finalizer.step().is_err());

    let operator = ScriptedOperator { steps: vec![OperatorStep::Finished].into() };
    assert!(OperatorAdapter::for_phase(operator, &definition,
                                       PipelinePhase::Transform(OperatorInputPort::new(4))).is_err());
}
