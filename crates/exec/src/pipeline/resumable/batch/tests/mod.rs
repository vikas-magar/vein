use super::stage::BatchProcessorStage;
use crate::pipeline::DataChunkView;
use crate::pipeline::resumable::{Handoff, InputEvent, OperatorInputPort, OperatorStep, ResumableOperator};
use crate::pipeline::test_support::dense_view;
use arrow::array::{ArrayRef, Int64Array};
use common::Result;
use std::sync::Arc;

struct IdentityProcessor;

impl super::BatchProcessor for IdentityProcessor {
    fn process(&mut self, input: DataChunkView) -> Result<Option<DataChunkView>> { Ok(Some(input)) }
    fn fork(&self) -> Result<Box<dyn super::BatchProcessor>> { Ok(Box::new(Self)) }
}

struct EmptyProcessor;

impl super::BatchProcessor for EmptyProcessor {
    fn process(&mut self, _input: DataChunkView) -> Result<Option<DataChunkView>> { Ok(None) }
    fn fork(&self) -> Result<Box<dyn super::BatchProcessor>> { Ok(Box::new(Self)) }
}

#[test]
fn transform_emits_the_same_arrow_array_owner() {
    let values: ArrayRef = Arc::new(Int64Array::from(vec![4, 8, 12]));
    let original = Arc::clone(&values);
    let view = dense_view(vec![values]);
    let mut stage = BatchProcessorStage::new(Box::new(IdentityProcessor));
    assert!(matches!(stage.push_input(OperatorInputPort::new(0), InputEvent::Batch(view)), Ok(Handoff::Accepted)));
    let OperatorStep::Output(output) = stage.step().expect("identity transform") else { panic!("expected output") };
    assert!(Arc::ptr_eq(output.column_view(0).unwrap().source_arrays().first().unwrap(), &original));
    assert!(matches!(stage.push_input(OperatorInputPort::new(0), InputEvent::EndOfInput), Ok(Handoff::Accepted)));
    assert!(matches!(stage.step().unwrap(), OperatorStep::Finished));
}

#[test]
fn transform_without_output_remains_runnable_until_end_of_input() {
    let mut stage = BatchProcessorStage::new(Box::new(EmptyProcessor));
    let input = dense_view(vec![Arc::new(Int64Array::from(vec![1])) as ArrayRef]);
    assert!(matches!(stage.push_input(OperatorInputPort::new(0), InputEvent::Batch(input)), Ok(Handoff::Accepted)));
    assert!(matches!(stage.step().unwrap(), OperatorStep::Runnable));
    assert!(matches!(stage.push_input(OperatorInputPort::new(0), InputEvent::EndOfInput), Ok(Handoff::Accepted)));
    assert!(matches!(stage.step().unwrap(), OperatorStep::Finished));
}
