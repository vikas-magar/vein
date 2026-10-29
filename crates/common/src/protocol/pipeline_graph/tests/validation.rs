use super::*;

#[test]
fn cycles_unknown_dependencies_and_duplicate_edges_are_rejected() {
    let first = DerivedPipelineGraph::derive(&plan(source())).unwrap().pipelines[0].clone();
    let mut second = first.clone();
    second.id = PipelineId(1);
    let edge = PipelineDependency {
        prerequisite: PipelineId(0),
        dependent: PipelineId(1),
        kind: PipelineDependencyKind::BlockingInput,
    };
    let mut graph = DerivedPipelineGraph { pipelines: vec![first, second], dependencies: vec![edge] };
    graph.validate().unwrap();
    graph.dependencies.push(PipelineDependency {
        prerequisite: PipelineId(1),
        dependent: PipelineId(0),
        kind: PipelineDependencyKind::BlockingInput,
    });
    assert!(graph.validate().unwrap_err().to_string().contains("cycle"));
    graph.dependencies = vec![edge, edge];
    assert!(graph.validate().unwrap_err().to_string().contains("duplicated"));
    graph.dependencies = vec![PipelineDependency { prerequisite: PipelineId(8), ..edge }];
    assert!(graph.validate().unwrap_err().to_string().contains("unknown"));
    graph.dependencies = vec![];
    graph.pipelines[1].id = PipelineId(4);
    assert!(graph.validate().is_err());
}
