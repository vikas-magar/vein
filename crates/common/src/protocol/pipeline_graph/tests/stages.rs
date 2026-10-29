use super::*;
use crate::protocol::extension::OperatorInputPort;
use crate::protocol::pipeline_graph::PipelinePhase;

#[test]
fn join_build_and_probe_bind_distinct_ports_of_the_same_node() {
    let build = source();
    let probe = source();
    let joined = node(PhysicalPlanOperator::Join {
        probe,
        build: Arc::clone(&build),
        plan: JoinPlan {
            join_type: JoinKind::Inner,
            probe_keys: vec![],
            build_keys: vec![],
            residual: None,
            outputs: vec![],
            probe_types: vec![],
            build_types: vec![],
        },
    });
    let graph = DerivedPipelineGraph::derive(&plan(Arc::clone(&joined))).unwrap();
    let build_stages = graph.pipelines()[0].stages().unwrap();
    let probe_stages = graph.pipelines()[1].stages().unwrap();
    assert_eq!(build_stages.last().unwrap().phase(), PipelinePhase::Sink(OperatorInputPort::new(1)));
    assert_eq!(probe_stages[1].phase(), PipelinePhase::Transform(OperatorInputPort::new(0)));
    assert!(Arc::ptr_eq(build_stages.last().unwrap().node(), &joined));
    assert!(Arc::ptr_eq(probe_stages[1].node(), &joined));
    assert!(Arc::ptr_eq(build_stages.last().unwrap().input_schema().unwrap(), &build.output_schema));
}

#[test]
fn sort_input_and_source_keep_computed_key_program_in_the_canonical_node() {
    let program = Arc::new(ExpressionProgram::empty());
    let sorted = node(PhysicalPlanOperator::Sort {
        input: source(),
        plan: SortPlan { keys: vec![] },
        key_program: Some(Arc::clone(&program)),
        output_columns: 0,
    });
    let graph = DerivedPipelineGraph::derive(&plan(Arc::clone(&sorted))).unwrap();
    for pipeline in graph.pipelines() {
        for stage in pipeline.stages().unwrap() {
            if !Arc::ptr_eq(stage.node(), &sorted) { continue; }
            let PhysicalPlanOperator::Sort { key_program: Some(keys), .. } = &stage.node().operator else { panic!() };
            assert!(Arc::ptr_eq(keys, &program));
        }
    }
}

#[test]
fn incorrect_phase_boundaries_and_schema_discontinuities_are_rejected() {
    let mut graph = DerivedPipelineGraph::derive(&plan(source())).unwrap();
    graph.pipelines[0].sink.kind = PipelineBoundaryKind::JoinBuild;
    assert!(graph.validate().is_err());
    let mut graph = DerivedPipelineGraph::derive(&plan(source())).unwrap();
    graph.pipelines[0].source.kind = PipelineBoundaryKind::SortInput;
    assert!(graph.validate().is_err());
}
