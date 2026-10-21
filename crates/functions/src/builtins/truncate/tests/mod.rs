use super::*;
use arrow::array::{Date32Array, Date64Array, Int64Array, Time64MicrosecondArray, TimestampNanosecondArray};

#[test]
fn truncates_calendar_values_and_preserves_nulls() {
    let input = Arc::new(Date32Array::from(vec![
        Some(types::days_from_civil(2024, 3, 31) as i32),
        Some(-1),
        None,
    ])) as ArrayRef;
    let function = TruncateFunction {
        unit: TemporalUnit::Quarter,
    };
    let output = crate::test_support::invoke_dense(&function, vec![input]).unwrap();
    let output = output.as_any().downcast_ref::<Date32Array>().unwrap();
    assert_eq!(output.value(0), types::days_from_civil(2024, 1, 1) as i32);
    assert_eq!(output.value(1), types::days_from_civil(1969, 10, 1) as i32);
    assert!(output.is_null(2));

    let date64 = Arc::new(Date64Array::from(vec![Some(1_709_251_200_000)])) as ArrayRef;
    let output = crate::test_support::invoke_dense(
        &TruncateFunction { unit: TemporalUnit::Month },
        vec![date64],
    )
        .unwrap();
    assert_eq!(
            output.as_any().downcast_ref::<Date64Array>().unwrap().value(0),
            types::days_from_civil(2024, 3, 1) * 86_400_000,
        );
}

#[test]
fn truncates_negative_timestamps_and_time_values_at_their_physical_scale() {
    let input = Arc::new(TimestampNanosecondArray::from(vec![Some(-1_000_000_001)])) as ArrayRef;
    let output = crate::test_support::invoke_dense(
        &TruncateFunction { unit: TemporalUnit::Second },
        vec![input],
    )
        .unwrap();
    assert_eq!(output.as_any().downcast_ref::<TimestampNanosecondArray>().unwrap().value(0), -2_000_000_000);

    let input = Arc::new(Time64MicrosecondArray::from(vec![Some(1_923_456_789)])) as ArrayRef;
    let output = crate::test_support::invoke_dense(
        &TruncateFunction { unit: TemporalUnit::Minute },
        vec![input],
    )
        .unwrap();
    assert_eq!(output.as_any().downcast_ref::<Time64MicrosecondArray>().unwrap().value(0), 1_920_000_000);
}

#[test]
fn validates_temporal_types_and_rejects_timezone_aware_timestamps() {
    let function = TruncateFunction { unit: TemporalUnit::Month };
    assert!(function.return_type(&[DataType::Int64]).is_err());
    assert!(function.return_type(&[DataType::Time64(TimeUnit::Microsecond)]).is_err());
    assert!(function.return_type(&[DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))]).is_err());
    let input = Arc::new(Int64Array::from(vec![1])) as ArrayRef;
    assert!(crate::test_support::invoke_dense(&function, vec![input]).is_err());
}
