use super::super::ExecutablePipelinePlan;
use super::fixtures::*;
use crate::pipeline::resumable::{InputEvent, OperatorFactoryRegistry, OperatorStep, ParallelismCapability, TypedOperatorFactory};
use common::protocol::extension::{ExtensionInput, ExtensionInputMode, OperatorRole, PhysicalExtension};
use common::protocol::pipeline_graph::PipelinePhase;
use common::protocol::{PhysicalPlan, PhysicalPlanNode};
use std::sync::Arc;
use std::sync::atomic::Ordering;

pub(super) fn prepared_sink() -> (ExecutablePipelinePlan, Arc<Counts>) {
    let counts = Arc::new(Counts::default());
    let factory = SinkFactory(Arc::clone(&counts));
    let id = factory.operator_type_id();
    let mut registry = OperatorFactoryRegistry::new();
    registry.register(factory).unwrap();
    let inputs = vec![source(empty()), source(empty())];
    let descriptor = PhysicalExtension {
        factory_id: id,
        configuration_version: 1,
        configuration: 2u64.to_le_bytes().into(),
        role: OperatorRole::Sink,
        inputs: inputs.iter().map(|input| ExtensionInput {
            schema: Arc::clone(&input.output_schema),
            mode: ExtensionInputMode::Blocking,
        }).collect::<Vec<_>>().into(),
    };
    let root = PhysicalPlanNode::extension(Arc::new(descriptor), inputs, empty()).unwrap();
    let plan = PhysicalPlan { root };
    let executable = ExecutablePipelinePlan::prepare(&plan, &registry, &Provider::default()).unwrap();
    assert_eq!(counts.allocations.load(Ordering::Relaxed), 1);
    assert_eq!(executable.pipelines().len(), 3);
    (executable, counts)
}

#[test]
fn blocking_extension_inputs_and_terminal_finalization_share_one_state() {
    let (executable, counts) = prepared_sink();
    for id in executable.execution_order() {
        let pipeline = &executable.pipelines()[id.0];
        let mut worker = pipeline.create_worker().unwrap();
        for (index, binding) in pipeline.stages().iter().enumerate() {
            let phase = binding.definition().phase();
            if phase == PipelinePhase::Finalize {
                assert_eq!(pipeline.parallelism(), ParallelismCapability::Serial);
                assert_eq!(pipeline.stages().len(), 1);
            }
            let stage = worker.stage_mut(index).unwrap();
            if let Some(port) = phase.input_port() { stage.push_input(port, InputEvent::EndOfInput).unwrap(); }
            assert!(matches!(stage.step().unwrap(), OperatorStep::Finished));
            worker.finish_stage(index).unwrap();
        }
    }
    assert_eq!(counts.inputs.load(Ordering::Relaxed), 2);
    assert_eq!(counts.finalizations.load(Ordering::Relaxed), 1);
}

#[test]
fn resumable_driver_combines_blocking_inputs_before_the_single_stage_finalizer() {
    use crate::pipeline::resumable::executor::{PipelineExecutor, PipelineStep};
    use std::num::NonZeroUsize;
    let (executable, counts) = prepared_sink();
    for id in executable.execution_order() {
        let mut executor = PipelineExecutor::new(executable.pipelines()[id.0].create_worker().unwrap());
        let mut finished = false;
        for _ in 0..100 {
            match executor.execute(NonZeroUsize::new(1).unwrap()).unwrap() {
                PipelineStep::Runnable => {}
                PipelineStep::Finished => {
                    finished = true;
                    break;
                }
                PipelineStep::Blocked { .. } => panic!("test finalizer does not block"),
            }
        }
        assert!(finished);
        assert!(matches!(executor.execute(NonZeroUsize::new(1).unwrap()).unwrap(), PipelineStep::Finished));
    }
    assert_eq!(counts.inputs.load(Ordering::Relaxed), 2);
    assert_eq!(counts.finalizations.load(Ordering::Relaxed), 1);
}
