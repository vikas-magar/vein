use super::*;

#[test]
fn null_comparison_is_logically_unknown_even_without_statistics() {
    let plan = scan(1, None, None, None);
    let c = plan.schema().fields[0].clone();
    let expr = predicate(&c, BinaryOp::Eq, LogicalLiteral::Null(VeinDataType::Int64));
    let result = root(&filter(plan, expr));
    assert_eq!(result.rows.value(), Some(0.0));
    assert_eq!(result.rows.confidence, EstimateConfidence::Logical);
}

#[test]
fn missing_null_fraction_uses_a_visible_model_prior() {
    let plan = scan(1, Some(100), Some(10), None);
    let c = plan.schema().fields[0].clone();
    let estimate = root(&filter(plan, predicate(&c, BinaryOp::Eq, LogicalLiteral::Int64(1)))).rows;
    assert_eq!(estimate.value(), Some(10.0));
    assert_eq!(estimate.confidence, EstimateConfidence::Model);
}

#[test]
fn invalid_degenerate_bounds_fall_back_without_losing_rows() {
    let mut plan = scan(1, Some(100), Some(10), Some(0));
    bounds(&mut plan, LogicalLiteral::Int64(3), LogicalLiteral::Int64(3));
    assert_eq!(root(&plan).rows.value(), Some(100.0));
}

#[test]
fn disjoint_key_bounds_estimate_no_matches_without_claiming_a_pruning_proof() {
    let mut left = scan(1, Some(100), None, Some(0));
    let mut right = scan(2, Some(100), None, Some(0));
    bounds(&mut left, LogicalLiteral::Int64(1), LogicalLiteral::Int64(10));
    bounds(&mut right, LogicalLiteral::Int64(11), LogicalLiteral::Int64(20));
    let result = root(&join(left, right, JoinType::Inner, false));
    assert_eq!(result.rows.value(), Some(0.0));
    assert_eq!(result.rows.confidence, EstimateConfidence::Model);
}

#[test]
fn temporal_distances_use_physical_units_without_float_cancellation() {
    use crate::statistics::{compare, distance};
    for (a, b) in [(LogicalLiteral::Date(20000), LogicalLiteral::Date(19990)),
        (LogicalLiteral::Timestamp(i64::MAX), LogicalLiteral::Timestamp(i64::MAX - 10)),
        (LogicalLiteral::Time(20), LogicalLiteral::Time(10))]
    {
        assert_eq!(distance(&a, &b), Some(10.0));
        assert_eq!(compare(&a, &b), Some(std::cmp::Ordering::Greater));
    }
    assert_eq!(distance(&LogicalLiteral::Float64(f64::MAX),
                        &LogicalLiteral::Float64(-f64::MAX)), None);
}

#[test]
fn computed_projection_has_unknown_distribution_but_preserves_rows() {
    let plan = scan(1, Some(100), Some(10), Some(0));
    let output = column(2, "computed", false);
    let projected = LogicalPlan::Project {
        input: Box::new(plan),
        exprs: vec![LogicalExpr::Literal(LogicalLiteral::Int64(1))],
        schema: Box::new(VeinSchema { fields: vec![output.clone()] }),
    };
    assert_eq!(rows(&projected), Some(100.0));
    let estimate = root(&filter(projected, predicate(&output, BinaryOp::Eq, LogicalLiteral::Int64(1)))).rows;
    assert_eq!(estimate.value(), Some(100.0));
    assert_eq!(estimate.source, EstimateSource::SelectivityFallback);
}

#[test]
fn deeply_nested_predicates_fall_back_without_exhausting_the_stack() {
    let plan = scan(1, Some(100), Some(10), Some(0));
    let c = plan.schema().fields[0].clone();
    let mut expr = predicate(&c, BinaryOp::Eq, LogicalLiteral::Int64(1));
    for _ in 0..256 { expr = LogicalExpr::Not { expr: Box::new(expr) }; }
    let estimate = root(&filter(plan, expr)).rows;
    assert_eq!(estimate.value(), Some(100.0));
    assert_eq!(estimate.source, EstimateSource::SelectivityFallback);
}

struct InvalidSelectivity;
impl SelectivityEstimator for InvalidSelectivity {
    fn estimate(&self, _: &LogicalExpr, _: &dyn ColumnStatisticsLookup) -> PredicateEstimate {
        PredicateEstimate {
            true_fraction: Estimate::model(1.1, EstimateSource::Filter),
            false_fraction: Estimate::model(0.0, EstimateSource::Filter),
        }
    }
}

#[test]
fn an_extension_cannot_publish_a_selectivity_above_one() {
    let plan = filter(scan(1, Some(100), Some(10), Some(0)),
                      LogicalExpr::Literal(LogicalLiteral::Boolean(true)));
    let result = StatisticsEstimator::with_selectivity(&CatalogStatisticsProvider, InvalidSelectivity)
        .estimate_plan(&plan);
    let estimate = result.last().unwrap();
    assert_eq!(estimate.rows.value(), Some(100.0));
    assert_eq!(estimate.rows.source, EstimateSource::SelectivityFallback);
    assert_eq!(estimate.selectivity.unwrap().source, EstimateSource::SelectivityFallback);
}

#[test]
fn unsupported_operators_stay_unknown_and_children_remain_observable() {
    let plan = scan(1, Some(100), Some(10), Some(0));
    let schema = Box::new(plan.schema().clone());
    let plan = LogicalPlan::Call { input: Box::new(plan), name: "extension".into(), args: vec![], schema };
    let estimates = StatisticsEstimator::new(&CatalogStatisticsProvider).estimate_plan(&plan);
    assert_eq!(estimates[0].rows.value(), Some(100.0));
    assert_eq!(estimates[1].rows.source, EstimateSource::UnsupportedOperator);
}
