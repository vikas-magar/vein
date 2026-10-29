use super::*;
use crate::protocol::{
    AggregatePlan,
    DataSink, DataSource, ExpressionProgram, JoinKind, JoinPlan, PhysicalPlanNode, SortPlan,
};
use std::sync::Arc;
use types::VeinSchema;

fn node(operator: PhysicalPlanOperator) -> Arc<PhysicalPlanNode> {
    Arc::new(PhysicalPlanNode {
        output_schema: Arc::new(VeinSchema::empty()),
        operator,
    })
}

fn source() -> Arc<PhysicalPlanNode> {
    node(PhysicalPlanOperator::Pipeline {
        source: Arc::new(DataSource::Memory),
        input_schema: Arc::new(VeinSchema::empty()),
        program: Arc::new(ExpressionProgram::empty()),
    })
}

fn plan(input: Arc<PhysicalPlanNode>) -> PhysicalPlan {
    PhysicalPlan {
        root: node(PhysicalPlanOperator::Sink {
            input,
            sink: Arc::new(DataSink::Discard),
        }),
    }
}

#[test]
fn aggregate_and_sort_create_blocking_boundaries_and_ordered_dependencies() {
    let aggregate = node(PhysicalPlanOperator::Aggregate {
        input: source(),
        plan: AggregatePlan {
            groups: 0,
            estimated_groups: None,
            grouping_sets: Some(vec![vec![]]),
            emit_empty_group_on_empty_input: true,
            aggs: Vec::new(),
            aggregate_input_types: Vec::new(),
            types: Vec::new(),
        },
    });
    let sorted = node(PhysicalPlanOperator::Sort {
        input: aggregate,
        plan: SortPlan { keys: Vec::new() },
        key_program: None,
        output_columns: 0,
    });
    let graph = DerivedPipelineGraph::derive(&plan(sorted)).expect("valid operator tree");

    assert_eq!(graph.pipelines().len(), 3);
    assert_eq!(graph.dependencies().len(), 2);
    assert_eq!(graph.pipelines()[0].sink.kind, PipelineBoundaryKind::AggregateInput);
    assert_eq!(graph.pipelines()[1].source.kind, PipelineBoundaryKind::Source);
    assert!(matches!(
            &graph.pipelines()[1].source.node.operator,
            PhysicalPlanOperator::Aggregate { .. }
        ));
    assert_eq!(graph.pipelines()[1].sink.kind, PipelineBoundaryKind::SortInput);
    assert_eq!(graph.pipelines()[2].source.kind, PipelineBoundaryKind::Source);
    assert_eq!(graph.pipelines()[2].sink.kind, PipelineBoundaryKind::ResultSink);
    let order = graph.execution_order().expect("blocking dependencies are acyclic");
    assert_eq!(order, vec![PipelineId(0), PipelineId(1), PipelineId(2)]);
}

#[test]
fn chained_join_builds_precede_probe_pipeline_without_copying_plan_nodes() {
    let inner_probe = source();
    let inner_build = source();
    let outer_build = source();
    let inner_join = node(PhysicalPlanOperator::Join {
        probe: Arc::clone(&inner_probe),
        build: Arc::clone(&inner_build),
        plan: JoinPlan {
            join_type: JoinKind::Inner,
            probe_keys: Vec::new(),
            build_keys: Vec::new(),
            residual: None,
            outputs: Vec::new(),
            probe_types: Vec::new(),
            build_types: Vec::new(),
        },
    });
    let outer_join = node(PhysicalPlanOperator::Join {
        probe: Arc::clone(&inner_join),
        build: Arc::clone(&outer_build),
        plan: JoinPlan {
            join_type: JoinKind::Inner,
            probe_keys: Vec::new(),
            build_keys: Vec::new(),
            residual: None,
            outputs: Vec::new(),
            probe_types: Vec::new(),
            build_types: Vec::new(),
        },
    });
    let graph = DerivedPipelineGraph::derive(&plan(Arc::clone(&outer_join)))
        .expect("valid chained join tree");

    assert_eq!(graph.pipelines().len(), 3);
    assert_eq!(graph.dependencies().len(), 2);
    assert!(graph
        .dependencies()
        .iter()
        .all(|dependency| dependency.kind == PipelineDependencyKind::JoinBuildBeforeProbe));
    assert!(graph
        .pipelines()
        .iter()
        .take(2)
        .all(|pipeline| pipeline.sink.kind == PipelineBoundaryKind::JoinBuild));
    assert!(graph.pipelines()[2]
        .operators
        .iter()
        .any(|operator| Arc::ptr_eq(operator, &inner_join)));
    assert!(graph.pipelines()[2]
        .operators
        .iter()
        .any(|operator| Arc::ptr_eq(operator, &outer_join)));
    assert!(Arc::ptr_eq(&graph.pipelines()[0].source.node, &outer_build));
    assert!(Arc::ptr_eq(&graph.pipelines()[1].source.node, &inner_build));
    assert!(Arc::ptr_eq(&graph.pipelines()[2].source.node, &inner_probe));
    assert_eq!(
        graph.execution_order(),
        Some(vec![PipelineId(0), PipelineId(1), PipelineId(2)])
    );
}

#[test]
fn rejects_missing_root_sink_and_nested_sink_nodes() {
    assert!(DerivedPipelineGraph::derive(&PhysicalPlan { root: source() }).is_err());

    let nested_sink = node(PhysicalPlanOperator::Sink {
        input: source(),
        sink: Arc::new(DataSink::Discard),
    });
    assert!(DerivedPipelineGraph::derive(&plan(nested_sink)).is_err());
}

mod stages;
mod validation;
