use super::*;

#[test]
fn missing_ndv_does_not_turn_equality_into_a_unique_lookup() {
    for population in [100, 2_400_000] {
        let plan = scan(1, Some(population), None, Some(0));
        let c = plan.schema().fields[0].clone();
        let filtered = filter(plan, predicate(&c, BinaryOp::Eq, LogicalLiteral::Int64(7)));
        let estimate = root(&filtered).rows;
        assert_eq!(estimate.value(), Some(population as f64));
        assert_eq!(estimate.source, EstimateSource::SelectivityFallback);
        assert_eq!(estimate.confidence, EstimateConfidence::Model);
    }
}

#[test]
fn discrete_bounds_supply_a_join_prior_without_claiming_measured_filter_ndv() {
    let mut left = scan(1, Some(16_000), None, Some(0));
    bounds(&mut left, LogicalLiteral::Int64(0), LogicalLiteral::Int64(24));
    let c = left.schema().fields[0].clone();
    let filtered = filter(left.clone(), predicate(&c, BinaryOp::Eq, LogicalLiteral::Int64(7)));
    assert_eq!(rows(&filtered), Some(640.0));
    assert_eq!(root(&filtered).rows.confidence, EstimateConfidence::Model);
    let right = scan(2, Some(1), Some(1), Some(0));
    let relation = join(left, right, JoinType::Inner, false);
    let estimate = root(&relation).rows;
    assert_eq!(estimate.value(), Some(640.0));
    assert_eq!(estimate.source, EstimateSource::JoinSelectivityFallback);
    assert_eq!(estimate.confidence, EstimateConfidence::Model);
}

#[test]
fn missing_ndv_keeps_bounds_and_known_empty_or_constant_populations() {
    let mut plan = scan(1, Some(100), None, Some(0));
    bounds(&mut plan, LogicalLiteral::Int64(10), LogicalLiteral::Int64(10));
    let c = plan.schema().fields[0].clone();
    assert_eq!(rows(&filter(plan.clone(), predicate(&c, BinaryOp::Eq, LogicalLiteral::Int64(10)))), Some(100.0));
    assert_eq!(rows(&filter(plan, predicate(&c, BinaryOp::Eq, LogicalLiteral::Int64(11)))), Some(0.0));
    let empty = scan(1, Some(0), None, Some(0));
    let c = empty.schema().fields[0].clone();
    assert_eq!(rows(&filter(empty, predicate(&c, BinaryOp::Eq, LogicalLiteral::Int64(1)))), Some(0.0));
}
