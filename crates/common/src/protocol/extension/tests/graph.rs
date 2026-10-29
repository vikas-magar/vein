use super::*;

#[test]
fn blocking_ports_become_dependencies_of_the_streaming_pipeline() {
    let build = source();
    let probe = source();
    let extension = PhysicalPlanNode::extension(Arc::new(descriptor(OperatorRole::Transform,
                                                                    &[ExtensionInputMode::Blocking, ExtensionInputMode::Streaming])),
                                                vec![Arc::clone(&build), Arc::clone(&probe)], schema()).unwrap();
    let graph = DerivedPipelineGraph::derive(&client(Arc::clone(&extension))).unwrap();
    assert_eq!(graph.pipelines().len(), 2);
    assert_eq!(graph.dependencies().len(), 1);
    assert!(graph.execution_order().is_some());
    let dependency = graph.dependencies()[0];
    assert!(Arc::ptr_eq(&graph.pipelines()[dependency.prerequisite.0].source.node, &build));
    assert!(Arc::ptr_eq(&graph.pipelines()[dependency.dependent.0].source.node, &probe));
    assert!(graph.pipelines()[dependency.dependent.0].operators.iter()
        .any(|node| Arc::ptr_eq(node, &extension)));
}

#[test]
fn multiple_streaming_ports_fail_with_an_explicit_capability_error() {
    let extension = PhysicalPlanNode::extension(Arc::new(descriptor(OperatorRole::Transform,
                                                                    &[ExtensionInputMode::Streaming, ExtensionInputMode::Streaming])),
                                                vec![source(), source()], schema()).unwrap();
    let error = DerivedPipelineGraph::derive(&client(extension)).unwrap_err();
    assert!(error.to_string().contains("multiple streaming input ports"));
}

#[test]
fn extension_sources_and_all_blocking_inputs_form_source_boundaries() {
    let extension = PhysicalPlanNode::extension(Arc::new(descriptor(OperatorRole::Source, &[])),
                                                vec![], schema()).unwrap();
    assert_eq!(DerivedPipelineGraph::derive(&client(extension)).unwrap().pipelines().len(), 1);
    let blocker = PhysicalPlanNode::extension(Arc::new(descriptor(OperatorRole::Transform,
                                                                  &[ExtensionInputMode::Blocking])), vec![source()], schema()).unwrap();
    let graph = DerivedPipelineGraph::derive(&client(blocker)).unwrap();
    assert_eq!(graph.pipelines().len(), 2);
    assert_eq!(graph.dependencies().len(), 1);
}
