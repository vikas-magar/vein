use super::fixture::{SourceFactory, make_definition};
use crate::pipeline::resumable::{
    ErasedResumableOperator, OperatorFactoryRegistry, OperatorStep, OperatorTypeId,
    ParallelismCapability, PreparedExtensionPlan,
};
use arrow::array::ArrayRef;
use common::protocol::pipeline_graph::PipelinePhase;
use common::protocol::{DataSink, PhysicalPlan, PhysicalPlanNode, PhysicalPlanOperator};
use std::sync::Arc;
use types::{ColumnId, ResolvedColumn, VeinDataType, VeinSchema};

fn client(input: Arc<PhysicalPlanNode>) -> PhysicalPlan {
    PhysicalPlan {
        root: Arc::new(PhysicalPlanNode {
            output_schema: Arc::clone(&input.output_schema),
            operator: PhysicalPlanOperator::Sink { input, sink: Arc::new(DataSink::Discard) },
        })
    }
}

pub(in crate::pipeline::resumable) fn fixture() -> (OperatorFactoryRegistry, Arc<PhysicalPlanNode>) {
    let id = OperatorTypeId::new("tests.prepared").unwrap();
    let definition = make_definition(id.clone());
    let mut registry = OperatorFactoryRegistry::new();
    registry.register(SourceFactory(id, ParallelismCapability::CombineLocalState)).unwrap();
    let descriptor = registry.describe(&definition, &[]).unwrap();
    let schema = Arc::new(VeinSchema::new(vec![Arc::new(ResolvedColumn::new(
        ColumnId(0), "v", None, VeinDataType::Int64,
    ))]));
    let source = PhysicalPlanNode::extension(Arc::new(descriptor), vec![], schema).unwrap();
    (registry, source)
}

fn emit(stage: &mut dyn ErasedResumableOperator) -> ArrayRef {
    assert!(matches!(stage.step().unwrap(), OperatorStep::Blocked(_)));
    let view = match stage.step().unwrap() {
        OperatorStep::Output(view) => view,
        _ => panic!("prepared test source should emit one view"),
    };
    view.materialize_column(0).unwrap()
}

#[test]
fn prepared_extension_keeps_its_factory_and_shares_payloads_across_worker_instances() {
    let (registry, source) = fixture();
    let prepared = PreparedExtensionPlan::prepare(&client(Arc::clone(&source)), &registry).unwrap();
    drop(registry);
    assert_eq!(prepared.len(), 1);
    let operator = prepared.operator(&source).unwrap();
    let mut first = operator.create_operator(PipelinePhase::Source).unwrap();
    let mut second = operator.create_operator(PipelinePhase::Source).unwrap();
    let a = emit(first.as_mut());
    let b = emit(second.as_mut());
    assert_eq!(a.to_data().buffers()[0].as_ptr(), b.to_data().buffers()[0].as_ptr());
    for stage in [&mut first, &mut second] {
        stage.acknowledge_output().unwrap();
        assert!(matches!(stage.step().unwrap(), OperatorStep::Finished));
        operator.combine_local_state(PipelinePhase::Source, stage.take_local_state().unwrap()).unwrap();
    }
}

#[test]
fn preparation_rejects_unknown_factories_versions_and_malformed_configuration() {
    let (registry, source) = fixture();
    assert!(PreparedExtensionPlan::prepare(&client(Arc::clone(&source)), &OperatorFactoryRegistry::new()).is_err());
    let PhysicalPlanOperator::Extension { descriptor, .. } = &source.operator else { panic!() };
    for (version, payload) in [(999, descriptor.configuration.clone()), (1, Arc::from([]))] {
        let mut descriptor = descriptor.as_ref().clone();
        descriptor.configuration_version = version;
        descriptor.configuration = payload;
        let node = PhysicalPlanNode::extension(Arc::new(descriptor), vec![], Arc::clone(&source.output_schema)).unwrap();
        assert!(PreparedExtensionPlan::prepare(&client(node), &registry).is_err());
    }
}

#[test]
fn factory_output_schema_is_validated_before_execution() {
    let (registry, source) = fixture();
    let PhysicalPlanOperator::Extension { descriptor, .. } = &source.operator else { panic!() };
    let invalid = PhysicalPlanNode::extension(Arc::clone(descriptor), vec![],
                                              Arc::new(VeinSchema::empty())).unwrap();
    assert!(PreparedExtensionPlan::prepare(&client(invalid), &registry).is_err());
}
