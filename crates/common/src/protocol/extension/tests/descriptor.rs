use super::*;

#[test]
fn canonical_extension_round_trips_and_plan_clones_share_configuration_and_children() {
    let child = source();
    let contract = Arc::new(descriptor(OperatorRole::Transform, &[ExtensionInputMode::Streaming]));
    let extension = PhysicalPlanNode::extension(Arc::clone(&contract), vec![Arc::clone(&child)], schema()).unwrap();
    let plan = client(Arc::clone(&extension));
    plan.validate().unwrap();
    let clone = plan.clone();
    assert!(Arc::ptr_eq(&clone.root, &plan.root));
    let PhysicalPlanOperator::Extension { descriptor, inputs } = &extension.operator else { panic!() };
    assert!(Arc::ptr_eq(&descriptor.configuration, &contract.configuration));
    assert!(Arc::ptr_eq(&inputs[0], &child));

    let encoded = serde_json::to_vec(&plan).unwrap();
    let decoded: PhysicalPlan = serde_json::from_slice(&encoded).unwrap();
    decoded.validate().unwrap();
    assert!(decoded.has_extensions());
    assert!(DerivedPipelineGraph::derive(&decoded).is_ok());
}

#[test]
fn child_schemas_and_invalid_factory_ids_are_rejected() {
    let mut contract = descriptor(OperatorRole::Transform, &[ExtensionInputMode::Streaming]);
    let mut input = contract.inputs[0].clone();
    input.schema = Arc::new(VeinSchema::empty());
    contract.inputs = vec![input].into();
    assert!(PhysicalPlanNode::extension(Arc::new(contract), vec![source()], schema()).is_err());
    assert!(serde_json::from_str::<OperatorTypeId>("\"   \"").is_err());
}

#[test]
fn terminal_extension_sinks_are_valid_but_nested_sinks_are_rejected() {
    let sink = PhysicalPlanNode::extension(Arc::new(descriptor(OperatorRole::Sink,
                                                               &[ExtensionInputMode::Streaming])), vec![source()], schema()).unwrap();
    let plan = PhysicalPlan { root: Arc::clone(&sink) };
    plan.validate().unwrap();
    assert!(DerivedPipelineGraph::derive(&plan).is_ok());
    assert!(client(sink).validate().is_err());
}
