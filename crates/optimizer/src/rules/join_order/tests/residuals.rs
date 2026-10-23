use super::*;
use crate::rules::predicate_analysis::conjuncts;

fn terms(plan: &LogicalPlan) -> Vec<LogicalExpr> {
    let mut output = Vec::new();
    let mut pending = vec![plan];
    while let Some(node) = pending.pop() {
        if let LogicalPlan::Join { on: Some(on), .. } = node {
            output.extend(conjuncts(on).into_iter().cloned());
        }
        pending.extend(node.children());
    }
    output
}

#[test]
fn reordered_tree_keeps_each_residual_and_constant_once() {
    let mut plan = selective_chain();
    let LogicalPlan::Join { on: Some(on), .. } = &mut plan else { unreachable!() };
    let mut residual = equality(1, "a_key", 4, "c_key");
    let LogicalExpr::BinaryOp { op, .. } = &mut residual else { unreachable!() };
    *op = BinaryOp::Lt;
    *on = conjunction(on.clone(), conjunction(residual.clone(),
                                              LogicalExpr::Literal(plan::LogicalLiteral::Boolean(true))));
    let expected = terms(&plan);
    let schema = plan.schema().clone();
    let (optimized, diagnostics) = optimizer(JoinOrderConfig::default())
        .optimize_with_diagnostics(plan).unwrap();
    assert_eq!(optimized.schema(), &schema);
    assert_eq!(diagnostics.join_order_decisions[0].relation_count, 3);
    assert_eq!(diagnostics.join_order_decisions[0].outcome, JoinOrderOutcome::Reordered);
    let actual = terms(&optimized);
    assert_eq!(actual.len(), expected.len());
    for expression in expected {
        assert_eq!(actual.iter().filter(|candidate| **candidate == expression).count(), 1);
    }
    let graph = JoinGraph::build(&optimized).unwrap_or_else(|_| panic!("valid reordered graph"));
    assert_eq!(graph.predicates.len(), 4);
}

#[test]
fn hyperedge_placement_waits_for_all_dependencies() {
    assert!(!predicates::crosses(0b111, 0b001, 0b010));
    assert!(predicates::crosses(0b111, 0b011, 0b100));
    assert!(!predicates::crosses(0b011, 0b011, 0b100));
    assert!(!predicates::has_edge(0b001, 0b100, &[0b010, 0b101, 0b010]));
}

#[test]
fn volatile_residual_preserves_join_boundary() {
    let mut plan = selective_chain();
    let LogicalPlan::Join { on: Some(on), .. } = &mut plan else { unreachable!() };
    *on = conjunction(on.clone(), LogicalExpr::FunctionCall {
        name: "random".into(),
        args: vec![],
        is_aggregate: false,
        distinct: false,
        return_type: VeinDataType::Boolean,
    });
    assert!(!is_reorderable_join(&plan));
}
