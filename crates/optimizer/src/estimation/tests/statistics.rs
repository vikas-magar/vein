use super::*;

#[test]
fn statistics_are_borrowed_not_cloned_and_confidence_is_explicit() {
    let plan = scan(42, Some(100), Some(10), Some(0));
    let LogicalPlan::Scan { table, .. } = &plan else { unreachable!(); };
    let snapshot = CatalogStatisticsProvider.table_statistics(table).unwrap();
    assert!(std::ptr::eq(snapshot.statistics, table.statistics.as_ref().unwrap()));
    assert_eq!(snapshot.freshness, StatisticsFreshness::Unverified);
    let estimate = root(&plan).rows;
    assert_eq!(estimate.value(), Some(100.0));
    assert_eq!(estimate.source, EstimateSource::Statistics);
    assert_eq!(estimate.confidence, EstimateConfidence::Statistics);
}

#[test]
fn missing_statistics_and_missing_rows_stay_unknown() {
    let mut plan = scan(1, None, None, None);
    assert_eq!(root(&plan).rows.source, EstimateSource::MissingStatistics);
    let LogicalPlan::Scan { table, .. } = &mut plan else { unreachable!(); };
    Arc::make_mut(table).statistics = None;
    assert_eq!(rows(&plan), None);
    assert_eq!(root(&plan).rows.source, EstimateSource::MissingStatistics);
}

#[test]
fn stale_statistics_are_rejected_and_current_statistics_are_used() {
    let plan = scan(1, Some(200), Some(10), Some(0));
    let stale = MarkedProvider(StatisticsFreshness::Stale);
    let current = MarkedProvider(StatisticsFreshness::Current);
    let result = StatisticsEstimator::new(&stale).estimate_plan(&plan);
    assert_eq!(result[0].rows.source, EstimateSource::StaleStatistics);
    assert_eq!(result[0].rows.value(), None);
    assert_eq!(StatisticsEstimator::new(&current).estimate_plan(&plan)[0].rows.value(), Some(200.0));
}

#[test]
fn contradictory_populations_and_bounds_are_rejected() {
    for (distinct, nulls) in [(Some(101), Some(0)), (Some(81), Some(20)),
        (Some(1), Some(101)), (Some(0), Some(0))]
    {
        let plan = scan(1, Some(100), distinct, nulls);
        assert_eq!(root(&plan).rows.source, EstimateSource::InvalidStatistics);
        assert_eq!(rows(&plan), None);
    }
    let mut plan = scan(1, Some(100), Some(10), Some(0));
    bounds(&mut plan, LogicalLiteral::Int64(20), LogicalLiteral::Int64(10));
    assert_eq!(root(&plan).rows.source, EstimateSource::InvalidStatistics);
    bounds(&mut plan, LogicalLiteral::Float64(f64::NAN), LogicalLiteral::Float64(10.0));
    assert_eq!(rows(&plan), None);
}

#[test]
fn empty_and_all_null_populations_are_valid() {
    assert_eq!(rows(&scan(1, Some(0), Some(0), Some(0))), Some(0.0));
    let plan = scan(1, Some(100), Some(0), Some(100));
    assert_eq!(rows(&plan), Some(100.0));
    let c = plan.schema().fields[0].clone();
    assert_eq!(rows(&filter(plan, predicate(&c, BinaryOp::Eq, LogicalLiteral::Int64(1)))), Some(0.0));
}

#[test]
fn nonfinite_or_negative_values_never_escape_the_estimate_type() {
    for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -1.0] {
        let e = Estimate::known(value, EstimateSource::Statistics, EstimateConfidence::Statistics);
        assert_eq!(e.value(), None);
    }
    let huge = Estimate::model(f64::MAX, EstimateSource::Join);
    assert_eq!(huge.combine(huge, EstimateSource::Join, |a, b| a * b).value(), None);
}
