use catalog::{ByteLengthStatistics, ColumnStatistics, NumericStatistics};
use plan::LogicalLiteral;

#[test]
fn analyzed_column_statistics_reject_contradictory_synopses() {
    let valid = ColumnStatistics {
        null_count: Some(1),
        distinct_count: Some(4),
        nan_count: Some(0),
        min_value: Some(LogicalLiteral::Int64(1)),
        max_value: Some(LogicalLiteral::Int64(9)),
        min_max_complete: Some(true),
        numeric: Some(NumericStatistics {
            finite_count: 9,
            non_finite_count: 0,
            sum: Some(45.0),
            mean: Some(5.0),
            m2: Some(60.0),
        }),
        ..ColumnStatistics::default()
    };
    assert!(valid.is_consistent_with_rows(10));

    let mut invalid = valid.clone();
    invalid.numeric.as_mut().unwrap().finite_count = 8;
    assert!(!invalid.is_consistent_with_rows(10));

    let mut invalid = valid.clone();
    invalid.numeric.as_mut().unwrap().m2 = Some(-1.0);
    assert!(!invalid.is_consistent_with_rows(10));

    let mut invalid = valid.clone();
    invalid.min_value = Some(LogicalLiteral::Int64(10));
    assert!(!invalid.is_consistent_with_rows(10));

    let mut invalid = valid.clone();
    invalid.min_value = None;
    invalid.max_value = None;
    assert!(!invalid.is_consistent_with_rows(10));

    let mut invalid = valid.clone();
    invalid.min_max_complete = Some(false);
    assert!(!invalid.is_consistent_with_rows(10));

    let all_nan = ColumnStatistics {
        null_count: Some(0),
        distinct_count: Some(1),
        nan_count: Some(2),
        min_max_complete: Some(true),
        numeric: Some(NumericStatistics {
            finite_count: 0,
            non_finite_count: 2,
            sum: Some(0.0),
            mean: None,
            m2: None,
        }),
        ..ColumnStatistics::default()
    };
    assert!(all_nan.is_consistent_with_rows(2));

    let empty = ColumnStatistics {
        null_count: Some(0),
        nan_count: Some(0),
        min_max_complete: Some(true),
        ..ColumnStatistics::default()
    };
    assert!(empty.is_consistent_with_rows(0));

    let mut invalid = valid.clone();
    invalid.distinct_count = None;
    invalid.distinct_count_relative_error = Some(0.1);
    assert!(!invalid.is_consistent_with_rows(10));

    let invalid_lengths = ColumnStatistics {
        null_count: Some(1),
        byte_length: Some(ByteLengthStatistics {
            value_count: 9,
            total_bytes: Some(8),
            min_bytes: Some(1),
            max_bytes: Some(2),
            contains_non_ascii: None,
        }),
        ..ColumnStatistics::default()
    };
    assert!(!invalid_lengths.is_consistent_with_rows(10));
}
