use super::*;

#[test]
fn equality_and_negation_exclude_nulls() {
    let plan = scan(1, Some(100), Some(10), Some(20));
    let c = plan.schema().fields[0].clone();
    let eq = predicate(&c, BinaryOp::Eq, LogicalLiteral::Int64(4));
    assert_eq!(rows(&filter(plan.clone(), eq.clone())), Some(8.0));
    let actual = rows(&filter(plan, LogicalExpr::Not { expr: Box::new(eq) })).unwrap();
    assert!((actual - 72.0).abs() < 1e-10, "{actual}");
}

#[test]
fn null_checks_and_comparison_to_null_follow_sql_truth() {
    let plan = scan(1, Some(100), Some(10), Some(20));
    let c = LogicalExpr::Column(plan.schema().fields[0].clone());
    assert_eq!(rows(&filter(plan.clone(), LogicalExpr::IsNull { expr: Box::new(c.clone()) })), Some(20.0));
    assert_eq!(rows(&filter(plan.clone(), LogicalExpr::IsNotNull { expr: Box::new(c.clone()) })), Some(80.0));
    let comparison = binary(c, BinaryOp::Eq, LogicalExpr::Literal(LogicalLiteral::Null(VeinDataType::Int64)));
    assert_eq!(rows(&filter(plan.clone(), comparison.clone())), Some(0.0));
    assert_eq!(rows(&filter(plan, LogicalExpr::Not { expr: Box::new(comparison) })), Some(0.0));
}

#[test]
fn unsupported_expressions_and_absent_column_stats_use_explicit_fallbacks() {
    let mut plan = scan(1, Some(100), None, Some(0));
    let c = plan.schema().fields[0].clone();
    let eq = predicate(&c, BinaryOp::Eq, LogicalLiteral::Int64(4));
    let equality_estimate = root(&filter(plan.clone(), eq)).rows;
    assert_eq!(equality_estimate.value(), Some(100.0));
    assert_eq!(equality_estimate.source, EstimateSource::SelectivityFallback);
    assert_eq!(equality_estimate.confidence, EstimateConfidence::Model);
    with_stats(&mut plan, |s| s.column_statistics.clear());
    let like = LogicalExpr::Like {
        expr: Box::new(LogicalExpr::Column(c)),
        pattern: Box::new(LogicalExpr::Literal(LogicalLiteral::String("%".into()))),
        negated: false,
    };
    let like_estimate = root(&filter(plan, like)).rows;
    assert_eq!(like_estimate.value(), Some(100.0));
    assert_eq!(like_estimate.source, EstimateSource::SelectivityFallback);
}

#[test]
fn and_or_and_repeated_predicates_are_bounded() {
    let plan = scan(1, Some(100), Some(10), Some(0));
    let c = plan.schema().fields[0].clone();
    let a = predicate(&c, BinaryOp::Eq, LogicalLiteral::Int64(1));
    let b = predicate(&c, BinaryOp::Eq, LogicalLiteral::Int64(2));
    assert_eq!(rows(&filter(plan.clone(), binary(a.clone(), BinaryOp::And, a.clone()))), Some(10.0));
    let estimate = rows(&filter(plan.clone(), binary(a.clone(), BinaryOp::And, b.clone()))).unwrap();
    assert!((estimate - 1.0).abs() < 1e-12);
    let estimate = rows(&filter(plan, binary(a, BinaryOp::Or, b))).unwrap();
    assert!((estimate - 19.0).abs() < 1e-12);
}

#[test]
fn range_estimates_use_bounds_and_handle_reversed_comparisons() {
    let mut plan = scan(1, Some(100), Some(10), Some(0));
    bounds(&mut plan, LogicalLiteral::Int64(10), LogicalLiteral::Int64(20));
    let c = plan.schema().fields[0].clone();
    for (op, value, expected) in [(BinaryOp::Lt, 10, 0.0), (BinaryOp::Leq, 20, 100.0),
        (BinaryOp::Eq, 30, 0.0), (BinaryOp::Lt, 15, 50.0), (BinaryOp::Gt, 15, 50.0)]
    {
        assert_eq!(rows(&filter(plan.clone(), predicate(&c, op, LogicalLiteral::Int64(value)))), Some(expected));
    }
    let reversed = binary(LogicalExpr::Literal(LogicalLiteral::Int64(15)), BinaryOp::Gt,
                          LogicalExpr::Column(c));
    assert_eq!(rows(&filter(plan, reversed)), Some(50.0));
}

#[test]
fn missing_scan_statistics_still_allow_logical_false() {
    let mut plan = scan(1, None, None, None);
    let LogicalPlan::Scan { filters, .. } = &mut plan else { unreachable!(); };
    filters.push(LogicalExpr::Literal(LogicalLiteral::Boolean(false)));
    assert_eq!(rows(&plan), Some(0.0));
}

#[test]
fn pushed_filters_and_standalone_filters_agree() {
    let mut scan_plan = scan(1, Some(100), Some(10), Some(0));
    let c = scan_plan.schema().fields[0].clone();
    let eq = predicate(&c, BinaryOp::Eq, LogicalLiteral::Int64(4));
    let standalone = filter(scan_plan.clone(), eq.clone());
    let LogicalPlan::Scan { filters, .. } = &mut scan_plan else { unreachable!(); };
    filters.extend([eq.clone(), eq]);
    assert_eq!(rows(&scan_plan), rows(&standalone));
    assert_eq!(root(&scan_plan).selectivity.unwrap().value(), Some(0.1));
}

#[test]
fn range_distance_preserves_precision_for_large_integers() {
    let mut plan = scan(1, Some(100), Some(10), Some(0));
    bounds(&mut plan, LogicalLiteral::Int64(i64::MAX - 10), LogicalLiteral::Int64(i64::MAX));
    let c = plan.schema().fields[0].clone();
    assert_eq!(rows(&filter(plan, predicate(&c, BinaryOp::Lt,
                                            LogicalLiteral::Int64(i64::MAX - 5)))), Some(50.0));
}
