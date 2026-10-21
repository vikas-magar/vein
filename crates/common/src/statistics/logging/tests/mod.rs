use catalog::{
    ColumnStatistics, HistogramBucket, HistogramSample, HistogramStatistics, MostCommonValue,
    MostCommonValuesStatistics, SerializedSketch, TableStatistics,
};
use types::LogicalLiteral;

use super::TableStatisticsLog;

#[test]
fn synopsis_summary_keeps_metric_coverage_and_omits_data_values() {
    let sensitive_value = 987_654_321;
    let statistics = TableStatistics {
        num_rows: Some(4),
        column_statistics: [(
            "key".to_owned(),
            ColumnStatistics {
                min_value: Some(LogicalLiteral::Int64(sensitive_value)),
                max_value: Some(LogicalLiteral::Int64(sensitive_value)),
                min_max_complete: Some(true),
                distinct_count: Some(3),
                distinct_sketch: Some(SerializedSketch {
                    algorithm: "hll".to_owned(),
                    version: 1,
                    parameters: vec![1, 2],
                    payload: vec![9; 128],
                }),
                most_common_values: Some(MostCommonValuesStatistics {
                    capacity: 2,
                    analyzed_count: 4,
                    error_bound: 1,
                    candidates: vec![MostCommonValue {
                        value: LogicalLiteral::Int64(sensitive_value),
                        hash: 42,
                        lower_count: 2,
                    }],
                }),
                histogram: Some(HistogramStatistics {
                    population_count: 4,
                    sample_count: 1,
                    sample_capacity: 8,
                    bucket_count: 1,
                    confidence_ppm: 950_000,
                    samples: vec![HistogramSample {
                        priority: 123456,
                        tie_breaker: 654321,
                        value: LogicalLiteral::Int64(sensitive_value),
                    }],
                    buckets: vec![HistogramBucket {
                        lower_bound: LogicalLiteral::Int64(sensitive_value),
                        upper_bound: LogicalLiteral::Int64(sensitive_value),
                        sample_count: 1,
                        estimated_count: 4,
                    }],
                    rank_error_bound: Some(0.1),
                }),
                ..ColumnStatistics::default()
            },
        )]
            .into_iter()
            .collect(),
        ..TableStatistics::default()
    };

    let summary = TableStatisticsLog::new(&statistics).to_string();
    assert!(summary.contains("ndv=3"));
    assert!(summary.contains("bounds_available=true"));
    assert!(summary.contains("hll@1(parameter_bytes=2,state_bytes=128)"));
    assert!(summary.contains("mcv=1/2, analyzed=4, error=1"));
    assert!(summary.contains("hist=population:4,sample:1/8,buckets:1/1"));
    assert!(!summary.contains(&sensitive_value.to_string()));
    assert!(!summary.contains("123456"));
    assert!(!summary.contains("654321"));
    assert!(!summary.contains("range="));
}
