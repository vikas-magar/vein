use catalog::ColumnStatistics;

use super::{RUNTIME_STATISTIC_METRICS, runtime_metric_availability};
use types::LogicalLiteral;

#[test]
fn runtime_statistics_availability_reports_present_and_missing_synopses() {
    let statistics = ColumnStatistics {
        null_count: Some(0),
        distinct_count: Some(4),
        min_value: Some(LogicalLiteral::Int64(1)),
        max_value: Some(LogicalLiteral::Int64(9)),
        ..ColumnStatistics::default()
    };

    let availability = runtime_metric_availability(&statistics);
    let metric_is_available = |name: &str| {
        let index = RUNTIME_STATISTIC_METRICS
            .iter()
            .position(|metric| *metric == name)
            .expect("metric is part of the runtime visibility contract");
        availability[index]
    };
    assert_eq!(availability.len(), RUNTIME_STATISTIC_METRICS.len());
    assert!(metric_is_available("null_count"));
    assert!(metric_is_available("distinct_count"));
    assert!(metric_is_available("min_max"));
    assert!(!metric_is_available("distinct_sketch"));
    assert!(!metric_is_available("histogram"));
}
