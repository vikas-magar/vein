use std::sync::Arc;

use super::find_first_plan_node;
use common::protocol::{
    DataSource, ExpressionProgram, JoinKind, JoinPlan, PhysicalPlanNode, PhysicalPlanOperator,
    SortPlan,
};

#[test]
fn plan_search_walks_unary_nodes_and_checks_join_probe_before_build() {
    let probe = pipeline();
    let build = pipeline();
    let join = node(PhysicalPlanOperator::Join {
        probe: Arc::clone(&probe),
        build,
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
    let sorted = node(PhysicalPlanOperator::Sort {
        input: join,
        plan: SortPlan { keys: Vec::new() },
        key_program: None,
        output_columns: 0,
    });
    let root = node(PhysicalPlanOperator::Transform {
        input: sorted,
        program: Arc::new(ExpressionProgram::empty()),
    });

    let first_pipeline = find_first_plan_node(&root, &|candidate| {
        matches!(
            &candidate.operator,
            PhysicalPlanOperator::Pipeline { .. }
        )
    })
        .expect("search must find a nested pipeline");

    assert!(
        std::ptr::eq(first_pipeline, probe.as_ref()),
        "pre-order must search the probe input before the build input"
    );
}

fn pipeline() -> Arc<PhysicalPlanNode> {
    let schema = Arc::new(plan::VeinSchema::empty());
    node(PhysicalPlanOperator::Pipeline {
        source: Arc::new(DataSource::Memory),
        input_schema: Arc::clone(&schema),
        program: Arc::new(ExpressionProgram::empty()),
    })
}

fn node(operator: PhysicalPlanOperator) -> Arc<PhysicalPlanNode> {
    Arc::new(PhysicalPlanNode {
        output_schema: Arc::new(plan::VeinSchema::empty()),
        operator,
    })
}
