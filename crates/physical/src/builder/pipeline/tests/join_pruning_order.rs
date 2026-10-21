use std::sync::Arc;

use super::support::{equality, inner_join, scan_with_statistics, test_column};
use super::super::{DefaultPipelineStageBuilder, PipelineStageBuilder};
use common::protocol::{JoinOutput, PhysicalPlanNode, PhysicalPlanOperator};
use optimizer::{JoinOrderOutcome, OptimizerPipeline};
use plan::{LogicalExpr, LogicalPlan, VeinSchema};

#[test]
fn cost_reordered_join_refreshes_pruned_scan_lineage_before_lowering() {
    let a_key = test_column(1, "a_key");
    let a_value = test_column(2, "a_value");
    let b_a_key = test_column(3, "b_a_key");
    let b_c_key = test_column(4, "b_c_key");
    let b_value = test_column(5, "b_value");
    let c_key = test_column(6, "c_key");
    let c_value = test_column(7, "c_value");

    let a = scan_with_statistics("a", 1_000_000, &[(a_key.clone(), 1_000_000), (a_value.clone(), 100)]);
    let b = scan_with_statistics(
        "b",
        100,
        &[(b_a_key.clone(), 100), (b_c_key.clone(), 100), (b_value.clone(), 100)],
    );
    let c = scan_with_statistics("c", 10, &[(c_key.clone(), 10), (c_value, 10)]);
    let ab = inner_join(a, b, equality(Arc::clone(&a_key), Arc::clone(&b_a_key)));
    let joined = inner_join(ab, c, equality(Arc::clone(&b_c_key), Arc::clone(&c_key)));
    let plan = LogicalPlan::Project {
        input: Box::new(joined),
        exprs: vec![LogicalExpr::Column(Arc::clone(&a_value))],
        schema: Box::new(VeinSchema::new(vec![a_value])),
    };

    let (optimized, diagnostics) = OptimizerPipeline::default()
        .optimize_with_diagnostics(plan)
        .expect("join optimization should retain required columns");
    assert!(diagnostics.join_order_decisions.iter().any(|decision| {
        decision.outcome == JoinOrderOutcome::Reordered
    }));

    let stage = DefaultPipelineStageBuilder::new()
        .build(&optimized)
        .expect("reordered, pruned joins must lower against emitted scan columns");
    let task = stage.tasks.first().expect("optimized join task");
    let PhysicalPlanOperator::Sink { input, .. } = &task.plan.root.operator else {
        panic!("physical plan must retain its sink root");
    };
    let join_count = assert_join_output_lineage(input);
    assert!(join_count >= 2, "expected the chained join tree, got {join_count} joins");
}

fn assert_join_output_lineage(node: &PhysicalPlanNode) -> usize {
    match &node.operator {
        PhysicalPlanOperator::Join { probe, build, plan } => {
            assert_eq!(node.output_schema.fields.len(), plan.outputs.len());
            for (output_field, source) in node.output_schema.fields.iter().zip(&plan.outputs) {
                let input_field = match source {
                    JoinOutput::Probe(index) => probe.output_schema.fields.get(*index as usize),
                    JoinOutput::Build(index) => build.output_schema.fields.get(*index as usize),
                }
                    .expect("join output reference must be in range for its physical input");
                assert_eq!(
                    input_field.id, output_field.id,
                    "join output {} must preserve its ColumnId through physical orientation",
                    output_field.name
                );
            }
            1 + assert_join_output_lineage(probe) + assert_join_output_lineage(build)
        }
        PhysicalPlanOperator::Transform { input, .. }
        | PhysicalPlanOperator::Aggregate { input, .. }
        | PhysicalPlanOperator::Distinct { input, .. }
        | PhysicalPlanOperator::Sort { input, .. }
        | PhysicalPlanOperator::Limit { input, .. }
        | PhysicalPlanOperator::Sink { input, .. } => assert_join_output_lineage(input),
        PhysicalPlanOperator::Pipeline { .. } => 0,
    }
}
