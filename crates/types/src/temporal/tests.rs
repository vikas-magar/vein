use super::*;

#[test]
fn converts_calendar_and_subday_units() {
    let cases = [
        (2, "YEAR", (24, 0, 0)),
        (-3, "months", (-3, 0, 0)),
        (4, "DAY", (0, 4, 0)),
        (-5, "hours", (0, 0, -18_000_000_000_000)),
        (6, "minute", (0, 0, 360_000_000_000)),
        (7, "seconds", (0, 0, 7_000_000_000)),
        (8, "milliseconds", (0, 0, 8_000_000)),
        (9, "microsecond", (0, 0, 9_000)),
        (10, "nanoseconds", (0, 0, 10)),
    ];

    for (amount, unit, expected) in cases {
        assert_eq!(
            interval_from_unit(amount, unit).unwrap(),
            LogicalLiteral::Interval {
                months: expected.0,
                days: expected.1,
                nanos: expected.2,
            }
        );
    }
}

#[test]
fn rejects_unknown_and_out_of_range_units() {
    assert!(interval_from_unit(1, "fortnight").is_err());
    assert!(interval_from_unit(i64::MAX, "year").is_err());
    assert!(interval_from_unit(i64::MAX, "day").is_err());
    assert!(interval_from_unit(i64::MAX, "second").is_err());
}

#[test]
fn truncates_calendar_units_with_epoch_and_leap_year_edges() {
    let march_31 = days_from_civil(2024, 3, 31);
    assert_eq!(
        truncate_epoch_value(march_31, TemporalUnit::Quarter, None, 1, true, false).unwrap(),
        days_from_civil(2024, 1, 1)
    );
    assert_eq!(
        truncate_epoch_value(march_31, TemporalUnit::Month, None, 1, true, false).unwrap(),
        days_from_civil(2024, 3, 1)
    );
    assert_eq!(
        truncate_epoch_value(0, TemporalUnit::Week, None, 1, true, false).unwrap(),
        -3
    );
    assert_eq!(
        truncate_epoch_value(-1, TemporalUnit::Week, None, 1, true, false).unwrap(),
        -3
    );
    assert_eq!(
        truncate_epoch_value(march_31, TemporalUnit::Year, None, 1, true, false).unwrap(),
        days_from_civil(2024, 1, 1)
    );
}

#[test]
fn truncates_subday_values_using_physical_scale_and_euclidean_floor() {
    assert_eq!(
        truncate_epoch_value(
            -1_000_000_001,
            TemporalUnit::Second,
            Some(1_000_000_000),
            86_400_000_000_000,
            false,
            false,
        )
            .unwrap(),
        -2_000_000_000
    );
    assert_eq!(
        truncate_epoch_value(45_123_456, TemporalUnit::Minute, Some(1_000_000), 86_400_000_000, false, true)
            .unwrap(),
        0
    );
    assert!(truncate_epoch_value(1, TemporalUnit::Hour, None, 1, true, false).is_err());
    assert!(truncate_epoch_value(1, TemporalUnit::Month, None, 86_400_000_000, false, true).is_err());
}
