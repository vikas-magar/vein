use common::protocol::pipeline_graph::PipelinePhase;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::pipeline::resumable::{
    OperatorFactoryRegistry, OperatorStep, OperatorTypeId, ParallelismCapability,
};
use crate::pipeline::test_support::dense_view;

use super::fixture::{Shared, SourceFactory, make_definition};
#[test]
fn registered_extension_runs_through_erased_boundary_and_combines_state() {
    let id = OperatorTypeId::new("test.source").expect("type id");
    let definition = make_definition(id.clone());
    let expected = dense_view(vec![Arc::clone(&definition.configuration().array)]);
    let expected_array = expected.materialize_column(0).expect("expected array");
    let mut registry = OperatorFactoryRegistry::new();
    registry.register(SourceFactory(id.clone(), ParallelismCapability::CombineLocalState)).expect("register extension");
    let global = registry.create_global_state(&definition).expect("shared state");
    let mut stage = registry.create_operator(&definition, Arc::clone(&global), PipelinePhase::Source).expect("create stage");

    let wait = match stage.step().expect("source wait") {
        OperatorStep::Blocked(wait) => wait.downcast::<&'static str>().expect("typed wait reason"),
        _ => panic!("source should expose its wait reason"),
    };
    assert_eq!(wait, "source io");
    let output = match stage.step().expect("source output") {
        OperatorStep::Output(view) => view,
        _ => panic!("source should emit its view"),
    };
    assert!(stage.step().is_err(), "a stage cannot advance before downstream accepts output");
    let actual_array = output.materialize_column(0).expect("output array");
    assert_eq!(expected_array.to_data().buffers()[0].as_ptr(), actual_array.to_data().buffers()[0].as_ptr());
    stage.acknowledge_output().expect("downstream accepted output");
    assert!(stage.acknowledge_output().is_err(), "an output is acknowledged once");
    assert!(matches!(stage.step().expect("source finish"), OperatorStep::Finished));
    let local = stage.take_local_state().expect("finished local state");
    registry.combine_local_state(&id, &global, local, PipelinePhase::Source).expect("combine worker state");
    assert_eq!(global.downcast_ref::<Shared>().expect("shared type").0.load(Ordering::Relaxed), 12);
    assert!(stage.take_local_state().is_err(), "local state is consumed once");
}

#[test]
fn registry_rejects_duplicate_and_unknown_factories() {
    let id = OperatorTypeId::new("test.source").expect("type id");
    let mut registry = OperatorFactoryRegistry::new();
    registry.register(SourceFactory(id.clone(), ParallelismCapability::CombineLocalState)).expect("first registration");
    assert!(registry.register(SourceFactory(id.clone(), ParallelismCapability::CombineLocalState)).is_err());
    let unknown = OperatorTypeId::new("test.missing").expect("unknown id");
    let plan = make_definition(unknown);
    assert!(registry.create_global_state(&plan).is_err());
}

#[test]
fn registry_rejects_combination_without_declared_capability() {
    let id = OperatorTypeId::new("test.independent").expect("type id");
    let definition = make_definition(id.clone());
    let mut registry = OperatorFactoryRegistry::new();
    registry.register(SourceFactory(id.clone(), ParallelismCapability::Independent)).expect("register");
    let global = registry.create_global_state(&definition).expect("shared state");
    assert!(registry.combine_local_state(&id, &global, Box::new(1usize), PipelinePhase::Source).is_err());
}

#[test]
fn registered_source_cannot_be_constructed_as_a_sink_phase() {
    let id = OperatorTypeId::new("test.phase").unwrap();
    let definition = make_definition(id.clone());
    let mut registry = OperatorFactoryRegistry::new();
    registry.register(SourceFactory(id, ParallelismCapability::Independent)).unwrap();
    let global = registry.create_global_state(&definition).unwrap();
    assert!(registry.create_operator(&definition, global,
                                     PipelinePhase::Sink(crate::pipeline::resumable::OperatorInputPort::new(0))).is_err());
}
