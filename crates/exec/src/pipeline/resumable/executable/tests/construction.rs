use super::super::ExecutablePipelinePlan;
use super::fixtures::*;
use crate::pipeline::resumable::{OperatorFactoryRegistry, OperatorInputPort, ParallelismCapability};
use common::protocol::pipeline_graph::PipelinePhase;
use common::protocol::{PhysicalPlanOperator, PipelineId};
use std::sync::Arc;

#[test]
fn join_build_and_probe_use_one_binding_with_distinct_local_phases() {
    let inner = join(source(empty()), source(empty()));
    let outer = join(Arc::clone(&inner), source(empty()));
    let plan = client(Arc::clone(&outer));
    let provider = Provider::default();
    let executable = ExecutablePipelinePlan::prepare(&plan, &OperatorFactoryRegistry::new(), &provider).unwrap();
    assert_eq!(executable.pipelines().len(), 3);
    assert_eq!(executable.execution_order(), &[PipelineId(0), PipelineId(1), PipelineId(2)]);
    let requests = provider.requests.lock().unwrap();
    for joined in [&inner, &outer] {
        let matching = requests.iter().filter(|request| request.node == pointer(joined)).collect::<Vec<_>>();
        assert_eq!(matching.len(), 1);
        assert!(matching[0].phases.contains(&PipelinePhase::Sink(OperatorInputPort::new(1))));
        assert!(matching[0].phases.contains(&PipelinePhase::Transform(OperatorInputPort::new(0))));
    }
    for pipeline in executable.pipelines() { pipeline.create_worker().unwrap(); }
    let records = provider.records.lock().unwrap();
    assert_eq!(records[&pointer(&outer)].created.lock().unwrap().len(), 2);
    assert_eq!(records[&pointer(&inner)].created.lock().unwrap().len(), 2);
}

#[test]
fn nested_blockers_reuse_nodes_and_keep_dependency_order() {
    let aggregate = aggregate(source(empty()));
    let sorted = sort(Arc::clone(&aggregate));
    let provider = Provider::default();
    let executable = ExecutablePipelinePlan::prepare(&client(Arc::clone(&sorted)),
                                                     &OperatorFactoryRegistry::new(), &provider).unwrap();
    assert_eq!(executable.pipelines().len(), 3);
    assert_eq!(executable.graph().dependencies().len(), 2);
    let middle = executable.pipelines()[1].stages();
    assert!(Arc::ptr_eq(middle[0].definition().node(), &aggregate));
    assert!(Arc::ptr_eq(middle.last().unwrap().definition().node(), &sorted));
    assert!(Arc::ptr_eq(middle[0].definition().output_schema().unwrap(), &aggregate.output_schema));
}

#[test]
fn construction_is_deterministic_and_respects_serial_capabilities() {
    let input = source(schema());
    let limited = limit(Arc::clone(&input));
    let plan = client(limited);
    let mut provider = Provider::default();
    provider.serial.insert(pointer(&input));
    let registry = OperatorFactoryRegistry::new();
    let first = ExecutablePipelinePlan::prepare(&plan, &registry, &provider).unwrap();
    let first_calls = provider.requests.lock().unwrap().iter()
        .map(|request| (request.node, request.phases.clone())).collect::<Vec<_>>();
    provider.requests.lock().unwrap().clear();
    let second = ExecutablePipelinePlan::prepare(&plan, &registry, &provider).unwrap();
    let second_calls = provider.requests.lock().unwrap().iter()
        .map(|request| (request.node, request.phases.clone())).collect::<Vec<_>>();
    assert_eq!(first_calls, second_calls);
    assert_eq!(first.execution_order(), second.execution_order());
    assert_eq!(first.pipelines()[0].parallelism(), ParallelismCapability::Serial);
}

#[test]
fn reused_streaming_occurrences_require_explicit_replay_support() {
    let shared = source(empty());
    let plan = client(join(Arc::clone(&shared), shared));
    let provider = Provider::default();
    let result = ExecutablePipelinePlan::prepare(&plan, &OperatorFactoryRegistry::new(), &provider);
    assert!(result.err().unwrap().to_string().contains("replay or multicast"));
    assert!(provider.requests.lock().unwrap().is_empty());
}

#[test]
fn computed_sort_program_owner_is_not_duplicated_during_construction() {
    use common::protocol::ExpressionProgram;
    let program = Arc::new(ExpressionProgram::empty());
    let original = sort(source(empty()));
    let PhysicalPlanOperator::Sort { input, plan, output_columns, .. } = &original.operator else { panic!() };
    let sorted = node(empty(), PhysicalPlanOperator::Sort {
        input: Arc::clone(input),
        plan: plan.clone(),
        output_columns: *output_columns,
        key_program: Some(Arc::clone(&program)),
    });
    let executable = ExecutablePipelinePlan::prepare(&client(Arc::clone(&sorted)),
                                                     &OperatorFactoryRegistry::new(), &Provider::default()).unwrap();
    for pipeline in executable.pipelines() {
        for stage in pipeline.stages() {
            if Arc::ptr_eq(stage.definition().node(), &sorted) {
                let PhysicalPlanOperator::Sort { key_program: Some(keys), .. } =
                    &stage.definition().node().operator else { panic!() };
                assert!(Arc::ptr_eq(keys, &program));
            }
        }
    }
}
