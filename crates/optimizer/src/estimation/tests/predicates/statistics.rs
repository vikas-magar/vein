use super::*;
use plan::{
    HistogramBucket, HistogramSample, HistogramStatistics, MostCommonValue,
    MostCommonValuesStatistics,
};

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
