use super::*;

#[path = "fixtures.rs"]
mod fixtures;
use fixtures::*;

#[test]
fn exact_inclusive_bounds_propagate_to_unread_input_once() {
    let context = context(10, Some(bounds(2, 9)));
    let plan = join(10, JoinType::Inner);
    let rewritten = CheckpointJoinFilterRule.apply(&plan, &context).unwrap().unwrap();
    assert!(contains_predicate(&rewritten, &predicate(BinaryOp::Geq, 2)));
    assert!(contains_predicate(&rewritten, &predicate(BinaryOp::Leq, 9)));
    assert!(CheckpointJoinFilterRule.apply(&rewritten, &context).unwrap().is_none());
    let LogicalPlan::Join { left, .. } = &rewritten else { panic!("expected join"); };
    let LogicalPlan::Join { left: original, .. } = &plan else { unreachable!(); };
    assert_eq!(left, original, "completed checkpoint must remain unchanged");
}

#[test]
fn production_pipeline_pushes_bounds_into_scan_and_converges() {
    let optimizer = crate::OptimizerPipeline::production().with_statistics_provider(provider(10, Some(bounds(2, 9))));
    let (rewritten, _) = optimizer.optimize_with_diagnostics(join(10, JoinType::Inner)).unwrap();
    let mut filters = Vec::new();
    fn visit(plan: &LogicalPlan, filters: &mut Vec<LogicalExpr>) {
        if let LogicalPlan::Scan { filters: found, .. } = plan { filters.extend(found.clone()); }
        for child in plan.children() { visit(child, filters); }
    }
    visit(&rewritten, &mut filters);
    assert!(filters.contains(&predicate(BinaryOp::Geq, 2)));
    assert!(filters.contains(&predicate(BinaryOp::Leq, 9)));
    assert_eq!(optimizer.optimize_with_diagnostics(rewritten.clone()).unwrap().0, rewritten);
}

#[test]
fn singleton_bounds_become_equality() {
    let rewritten = CheckpointJoinFilterRule.apply(&join(10, JoinType::Inner), &context(10, Some(bounds(4, 4)))).unwrap().unwrap();
    assert!(contains_predicate(&rewritten, &predicate(BinaryOp::Eq, 4)));
    assert!(!contains_predicate(&rewritten, &predicate(BinaryOp::Geq, 4)));
}

#[test]
fn no_propagation_for_non_inner_joins() {
    let context = context(10, Some(bounds(2, 9)));
    for kind in [JoinType::Left, JoinType::Right, JoinType::Full, JoinType::Semi, JoinType::Anti, JoinType::Single, JoinType::Cross] {
        assert!(CheckpointJoinFilterRule.apply(&join(10, kind), &context).unwrap().is_none());
    }
}

#[test]
fn missing_incomplete_nan_or_inconsistent_statistics_cannot_exclude_rows() {
    let mut incomplete = bounds(2, 9);
    incomplete.min_max_complete = Some(false);
    let mut nan = bounds(2, 9);
    nan.nan_count = Some(1);
    let mut unknown_nan = bounds(2, 9);
    unknown_nan.nan_count = None;
    let mut inverted = bounds(9, 2);
    inverted.distinct_count = Some(2);
    let mut invalid_population = bounds(2, 9);
    invalid_population.null_count = Some(11);
    for summary in [None, Some(incomplete), Some(nan), Some(unknown_nan), Some(inverted), Some(invalid_population)] {
        assert!(CheckpointJoinFilterRule.apply(&join(10, JoinType::Inner), &context(10, summary)).unwrap().is_none());
    }
    assert!(CheckpointJoinFilterRule.apply(&join(11, JoinType::Inner), &context(10, Some(bounds(2, 9)))).unwrap().is_none());
    assert!(CheckpointJoinFilterRule.apply(&join(10, JoinType::Inner), &OptimizerContext::default()).unwrap().is_none());
}

#[test]
fn checkpoint_on_right_and_reversed_equality_have_the_same_constraints() {
    let context = context(10, Some(bounds(2, 9)));
    let mut plan = join(10, JoinType::Inner);
    let LogicalPlan::Join { left, right, on, .. } = &mut plan else { unreachable!(); };
    std::mem::swap(left, right);
    let LogicalExpr::BinaryOp { left, right, .. } = on.as_mut().unwrap() else { unreachable!(); };
    std::mem::swap(left, right);
    let rewritten = CheckpointJoinFilterRule.apply(&plan, &context).unwrap().unwrap();
    assert!(contains_predicate(&rewritten, &predicate(BinaryOp::Geq, 2)));
    assert!(contains_predicate(&rewritten, &predicate(BinaryOp::Leq, 9)));
}

#[test]
fn non_equality_cannot_use_key_bounds_as_equality_constraints() {
    let mut plan = join(10, JoinType::Inner);
    let LogicalPlan::Join { on, .. } = &mut plan else { unreachable!(); };
    let LogicalExpr::BinaryOp { op, .. } = on.as_mut().unwrap() else { unreachable!(); };
    *op = BinaryOp::Lt;
    assert!(CheckpointJoinFilterRule.apply(&plan, &context(10, Some(bounds(2, 9)))).unwrap().is_none());
}

#[test]
fn empty_or_all_null_population_cannot_match_equality() {
    let all_null = plan::ColumnStatistics { null_count: Some(10), ..Default::default() };
    for (rows, summary) in [(0, None), (10, Some(all_null))] {
        let rewritten = CheckpointJoinFilterRule.apply(&join(rows, JoinType::Inner), &context(rows, summary)).unwrap().unwrap();
        assert!(contains_predicate(&rewritten, &LogicalExpr::Literal(LogicalLiteral::Boolean(false))));
    }
}
