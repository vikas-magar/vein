use super::*;
use error::VeinError;
use plan::LogicalLiteral;

mod multiplication;

fn lit(t: VeinDataType) -> LogicalExpr {
    LogicalExpr::Literal(LogicalLiteral::Null(t))
}

#[test]
fn identical_types_pass_through() {
    let a = lit(VeinDataType::Int64);
    let b = lit(VeinDataType::Int64);
    let (l, r, t) = coerce_types(a.clone(), b.clone()).unwrap();
    assert_eq!(l, a);
    assert_eq!(r, b);
    assert_eq!(t, VeinDataType::Int64);
}

#[test]
fn widens_int32_to_int64() {
    let (_, _, t) = coerce_types(lit(VeinDataType::Int32), lit(VeinDataType::Int64)).unwrap();
    assert_eq!(t, VeinDataType::Int64);
    let (l, _, _) = coerce_types(lit(VeinDataType::Int32), lit(VeinDataType::Int64)).unwrap();
    assert!(matches!(
            l,
            LogicalExpr::Cast {
                target_type: VeinDataType::Int64,
                ..
            }
        ));
}

#[test]
fn null_unifies_without_cast() {
    let (l, r, t) = coerce_types(lit(VeinDataType::Null), lit(VeinDataType::Utf8)).unwrap();
    assert!(matches!(l, LogicalExpr::Literal(_)));
    assert!(matches!(r, LogicalExpr::Literal(_)));
    assert_eq!(t, VeinDataType::Utf8);
}

#[test]
fn date_promotes_to_timestamp() {
    let (_, _, t) = coerce_types(
        lit(VeinDataType::Date32),
        lit(VeinDataType::Timestamp(types::TimeUnit::Microsecond, None)),
    )
        .unwrap();
    assert_eq!(t, VeinDataType::Timestamp(types::TimeUnit::Microsecond, None));
}

#[test]
fn date_interval_arithmetic_preserves_temporal_type_and_normalizes_addition() {
    let date = lit(VeinDataType::Date32);
    let interval = lit(VeinDataType::Interval(types::IntervalUnit::MonthDayNano));

    let (left, right, result) =
        coerce_arithmetic(date.clone(), interval.clone(), BinaryOp::Minus).unwrap();
    assert_eq!((left.data_type(), right.data_type(), result), (
        VeinDataType::Date32,
        VeinDataType::Interval(types::IntervalUnit::MonthDayNano),
        VeinDataType::Date32,
    ));

    let (left, right, result) =
        coerce_arithmetic(interval, date, BinaryOp::Plus).unwrap();
    assert_eq!((left.data_type(), right.data_type(), result), (
        VeinDataType::Date32,
        VeinDataType::Interval(types::IntervalUnit::MonthDayNano),
        VeinDataType::Date32,
    ));
}

#[test]
fn interval_minus_date_is_not_supported() {
    assert!(coerce_arithmetic(
        lit(VeinDataType::Interval(types::IntervalUnit::MonthDayNano)),
        lit(VeinDataType::Date32),
        BinaryOp::Minus,
    )
        .is_err());
}

#[test]
fn date_promotes_preserving_timezone() {
    let (l, _, t) = coerce_types(
        lit(VeinDataType::Date32),
        lit(VeinDataType::Timestamp(types::TimeUnit::Nanosecond, Some("UTC".to_string()))),
    )
        .unwrap();
    assert_eq!(t, VeinDataType::Timestamp(types::TimeUnit::Nanosecond, Some("UTC".to_string())));
    assert!(matches!(
            l,
            LogicalExpr::Cast {
                target_type: VeinDataType::Timestamp(types::TimeUnit::Nanosecond, Some(ref tz)),
                ..
            } if tz == "UTC"
        ));
}

#[test]
fn timestamp_promotes_date_to_its_timezone() {
    let (_, r, t) = coerce_types(
        lit(VeinDataType::Timestamp(
            types::TimeUnit::Nanosecond,
            Some("America/New_York".to_string()),
        )),
        lit(VeinDataType::Date32),
    )
        .unwrap();
    assert_eq!(
        t,
        VeinDataType::Timestamp(
            types::TimeUnit::Nanosecond,
            Some("America/New_York".to_string()),
        )
    );
    assert!(matches!(
            r,
            LogicalExpr::Cast {
                target_type: VeinDataType::Timestamp(
                    types::TimeUnit::Nanosecond,
                    Some(ref tz),
                ),
                ..
            } if tz == "America/New_York"
        ));
}

#[test]
fn string_family_is_order_independent() {
    let (l, r, t) =
        coerce_types(lit(VeinDataType::Utf8), lit(VeinDataType::StringView)).unwrap();
    assert_eq!(t, VeinDataType::StringView);
    assert!(matches!(
            l,
            LogicalExpr::Cast {
                target_type: VeinDataType::StringView,
                ..
            }
        ));
    assert!(matches!(r, LogicalExpr::Literal(_)));

    // Same pair, reversed arguments, must yield the same common type.
    let (_, _, t) =
        coerce_types(lit(VeinDataType::StringView), lit(VeinDataType::Utf8)).unwrap();
    assert_eq!(t, VeinDataType::StringView);
}

#[test]
fn large_utf8_is_widest_string() {
    let (l, _, t) =
        coerce_types(lit(VeinDataType::Utf8), lit(VeinDataType::LargeUtf8)).unwrap();
    assert_eq!(t, VeinDataType::LargeUtf8);
    assert!(matches!(
            l,
            LogicalExpr::Cast {
                target_type: VeinDataType::LargeUtf8,
                ..
            }
        ));
}

#[test]
fn conflicting_types_are_semantic_errors() {
    let err = coerce_types(lit(VeinDataType::Boolean), lit(VeinDataType::Int64)).unwrap_err();
    assert!(matches!(err, VeinError::Semantic { .. }), "got {err}");
}
