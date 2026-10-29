use super::super::ExecutablePipelinePlan;
use super::fixtures::*;
use crate::pipeline::resumable::{InputEvent, OperatorFactoryRegistry, OperatorStep};
use common::protocol::pipeline_graph::PipelinePhase;

#[test]
fn worker_states_are_independent_and_combined_once_into_the_shared_owner() {
    let joined = join(source(empty()), source(empty()));
    let provider = Provider { combine: true, ..Default::default() };
    let executable = ExecutablePipelinePlan::prepare(&client(joined), &OperatorFactoryRegistry::new(), &provider).unwrap();
    for definition in executable.pipelines() {
        for _ in 0..2 {
            let mut worker = definition.create_worker().unwrap();
            for (index, binding) in definition.stages().iter().enumerate() {
                assert!(worker.finish_stage(index).is_err(), "unfinished local state is not publishable");
                let stage = worker.stage_mut(index).unwrap();
                if let Some(port) = binding.definition().phase().input_port() {
                    stage.push_input(port, InputEvent::EndOfInput).unwrap();
                }
                assert!(matches!(stage.step().unwrap(), OperatorStep::Finished));
                worker.finish_stage(index).unwrap();
                assert!(worker.finish_stage(index).is_err());
            }
            assert!(worker.stage_mut(definition.stages().len()).is_none());
        }
    }
    let records = provider.records.lock().unwrap();
    for record in records.values() {
        assert_eq!(record.created.lock().unwrap().len(), record.combined.lock().unwrap().len());
    }
}

#[test]
fn registered_extension_remains_bound_after_registry_drop_and_retains_arrow_buffers() {
    use crate::pipeline::resumable::tests::source_plan_fixture;
    let (registry, extension) = source_plan_fixture();
    let provider = Provider::default();
    let executable = ExecutablePipelinePlan::prepare(&client(extension), &registry, &provider).unwrap();
    drop(registry);
    assert_eq!(provider.requests.lock().unwrap().len(), 1, "extension uses its registered factory");
    let pipeline = &executable.pipelines()[0];
    let mut a = pipeline.create_worker().unwrap();
    let mut b = pipeline.create_worker().unwrap();
    fn array(worker: &mut super::super::PipelineInstance) -> arrow::array::ArrayRef {
        let stage = worker.stage_mut(0).unwrap();
        assert!(matches!(stage.step().unwrap(), OperatorStep::Blocked(_)));
        let output = match stage.step().unwrap() {
            OperatorStep::Output(view) => view,
            _ => panic!("extension must produce its source view"),
        };
        output.materialize_column(0).unwrap()
    }
    let first = array(&mut a);
    let second = array(&mut b);
    assert_eq!(first.to_data().buffers()[0].as_ptr(), second.to_data().buffers()[0].as_ptr());
    assert_eq!(pipeline.stages()[0].definition().phase(), PipelinePhase::Source);
}
