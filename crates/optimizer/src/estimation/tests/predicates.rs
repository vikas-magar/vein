use super::*;

use plan::{HistogramBucket, HistogramSample, HistogramStatistics, MostCommonValue,
           MostCommonValuesStatistics};

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
fn unsupported_expressions_and_absent_column_stats_stay_unknown() {
    let mut plan = scan(1, Some(100), None, Some(0));
    let c = plan.schema().fields[0].clone();
    let eq = predicate(&c, BinaryOp::Eq, LogicalLiteral::Int64(4));
    assert_eq!(rows(&filter(plan.clone(), eq)), None);
    with_stats(&mut plan, |s| s.column_statistics.clear());
    let like = LogicalExpr::Like {
        expr: Box::new(LogicalExpr::Column(c)),
        pattern: Box::new(LogicalExpr::Literal(LogicalLiteral::String("%".into()))),
        negated: false,
    };
    assert_eq!(root(&filter(plan, like)).rows.source, EstimateSource::UnsupportedExpression);
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

#[test]
fn most_common_values_adjust_equality_and_residual_ndv_estimates() {
    let mut plan = scan(1, Some(100), Some(4), Some(0));
    with_stats(&mut plan, |statistics| {
        let key = statistics.column_statistics.get_mut("key").unwrap();
        key.nan_count = Some(0);
        key.most_common_values = Some(MostCommonValuesStatistics {
            capacity: 2,
            analyzed_count: 100,
            error_bound: 20,
            candidates: vec![
                MostCommonValue { value: LogicalLiteral::Int64(1), hash: 1, lower_count: 40 },
                MostCommonValue { value: LogicalLiteral::Int64(2), hash: 2, lower_count: 20 },
            ],
        });
    });
    let key = plan.schema().fields[0].clone();

    assert_eq!(rows(&filter(plan.clone(), predicate(&key, BinaryOp::Eq, LogicalLiteral::Int64(1)))), Some(50.0));
    assert_eq!(rows(&filter(plan, predicate(&key, BinaryOp::Eq, LogicalLiteral::Int64(3)))), Some(10.0));
}

#[test]
fn most_common_values_covering_all_distinct_values_estimate_other_values_as_absent() {
    let mut plan = scan(1, Some(100), Some(4), Some(0));
    with_stats(&mut plan, |statistics| {
        let key = statistics.column_statistics.get_mut("key").unwrap();
        key.nan_count = Some(0);
        key.most_common_values = Some(MostCommonValuesStatistics {
            capacity: 4,
            analyzed_count: 100,
            error_bound: 0,
            candidates: (1..=4)
                .rev()
                .map(|value| MostCommonValue {
                    value: LogicalLiteral::Int64(value),
                    hash: value as u64,
                    lower_count: 25,
                })
                .collect(),
        });
    });
    let key = plan.schema().fields[0].clone();

    assert_eq!(rows(&filter(plan, predicate(&key, BinaryOp::Eq, LogicalLiteral::Int64(5)))), Some(0.0));
}

#[test]
fn approximate_ndv_does_not_turn_mcv_absence_into_a_proven_zero() {
    let mut plan = scan(1, Some(100), Some(4), Some(0));
    with_stats(&mut plan, |statistics| {
        let key = statistics.column_statistics.get_mut("key").unwrap();
        key.nan_count = Some(0);
        key.distinct_count_relative_error = Some(0.1);
        key.most_common_values = Some(MostCommonValuesStatistics {
            capacity: 4,
            analyzed_count: 100,
            error_bound: 0,
            candidates: (1..=4)
                .rev()
                .map(|value| MostCommonValue {
                    value: LogicalLiteral::Int64(value),
                    hash: value as u64,
                    lower_count: 25,
                })
                .collect(),
        });
    });
    let key = plan.schema().fields[0].clone();

    assert_eq!(rows(&filter(plan, predicate(&key, BinaryOp::Eq, LogicalLiteral::Int64(5)))), Some(25.0));
}

#[test]
fn sampled_histograms_drive_range_estimates_and_invalid_histograms_fall_back() {
    let mut plan = scan(1, Some(100), Some(6), Some(0));
    with_stats(&mut plan, |statistics| {
        let key = statistics.column_statistics.get_mut("key").unwrap();
        key.nan_count = Some(0);
        key.min_value = Some(LogicalLiteral::Int64(1));
        key.max_value = Some(LogicalLiteral::Int64(6));
        key.min_max_complete = Some(true);
        let samples = (1..=6).map(|value| HistogramSample {
            priority: value as u64,
            tie_breaker: 0,
            value: LogicalLiteral::Int64(value),
        }).collect();
        let rank_error = ((2.0_f64 / (1.0 - 0.95)).ln() / (2.0 * 6.0)).sqrt().min(1.0);
        key.histogram = Some(HistogramStatistics {
            population_count: 100,
            sample_count: 6,
            sample_capacity: 6,
            bucket_count: 2,
            confidence_ppm: 950_000,
            samples,
            buckets: vec![
                HistogramBucket {
                    lower_bound: LogicalLiteral::Int64(1),
                    upper_bound: LogicalLiteral::Int64(3),
                    sample_count: 3,
                    estimated_count: 50,
                },
                HistogramBucket {
                    lower_bound: LogicalLiteral::Int64(4),
                    upper_bound: LogicalLiteral::Int64(6),
                    sample_count: 3,
                    estimated_count: 50,
                },
            ],
            rank_error_bound: Some(rank_error),
        });
    });
    let key = plan.schema().fields[0].clone();

    let estimate = rows(&filter(plan.clone(), predicate(&key, BinaryOp::Lt, LogicalLiteral::Int64(3))))
        .expect("histogram range estimate");
    assert!((estimate - 100.0 / 3.0).abs() < 1e-10, "{estimate}");
    assert_eq!(rows(&filter(plan.clone(), predicate(&key, BinaryOp::Leq, LogicalLiteral::Int64(3)))), Some(50.0));

    with_stats(&mut plan, |statistics| {
        statistics.column_statistics.get_mut("key").unwrap()
            .histogram.as_mut().unwrap().buckets[0].estimated_count += 1;
    });
    assert_eq!(rows(&filter(plan, predicate(&key, BinaryOp::Lt, LogicalLiteral::Int64(3)))), Some(40.0));
}

#[test]
fn decimal_bounds_are_compared_without_float_rounding() {
    use crate::statistics::compare;
    assert_eq!(compare(&LogicalLiteral::Decimal(123, 3, 2),
                       &LogicalLiteral::Decimal(1230, 4, 3)), Some(std::cmp::Ordering::Equal));
    assert_eq!(compare(&LogicalLiteral::Decimal(i128::MAX, 38, 0),
                       &LogicalLiteral::Decimal(1, 1, 1)), None);
}
