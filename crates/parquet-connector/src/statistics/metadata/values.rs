//! Conversions from Arrow statistic arrays to conservative logical literals.

use arrow::array::{
    Array, BooleanArray, Date32Array, Date64Array, Decimal128Array, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    Int8Array, LargeStringArray, StringArray, StringViewArray,
    Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::{DataType, TimeUnit};
use types::LogicalLiteral;

pub(in crate::parquet_statistics) fn exact_scalar_at(
    values: Option<&dyn Array>,
    exact: Option<&BooleanArray>,
    index: usize,
    data_type: &DataType,
) -> Option<LogicalLiteral> {
    let values = values?;
    let exact = exact?;
    if values.is_null(index) || exact.is_null(index) || !exact.value(index) {
        return None;
    }
    let value = logical_literal_at(values, index, data_type)?;
    // A stored NaN is not an ordered bound. The independent NaN count remains
    // available to estimators that can use it.
    if matches!(value, LogicalLiteral::Float64(number) if !number.is_finite()) {
        return None;
    }
    Some(value)
}

pub(in crate::parquet_statistics) fn logical_literal_at(
    values: &dyn Array,
    index: usize,
    data_type: &DataType,
) -> Option<LogicalLiteral> {
    use LogicalLiteral as Literal;

    macro_rules! signed_integer {
        ($array_type:ty) => {
            if let Some(array) = values.as_any().downcast_ref::<$array_type>() {
                return Some(Literal::Int64(i64::from(array.value(index))));
            }
        };
    }
    match data_type {
        DataType::Boolean => values
            .as_any()
            .downcast_ref::<BooleanArray>()
            .map(|array| Literal::Boolean(array.value(index))),
        DataType::Int8 => {
            signed_integer!(Int8Array);
            None
        }
        DataType::Int16 => {
            signed_integer!(Int16Array);
            None
        }
        DataType::Int32 => {
            signed_integer!(Int32Array);
            None
        }
        DataType::Int64 => values
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|array| Literal::Int64(array.value(index))),
        DataType::UInt8 => values
            .as_any()
            .downcast_ref::<UInt8Array>()
            .map(|array| Literal::UInt64(u64::from(array.value(index)))),
        DataType::UInt16 => values
            .as_any()
            .downcast_ref::<UInt16Array>()
            .map(|array| Literal::UInt64(u64::from(array.value(index)))),
        DataType::UInt32 => values
            .as_any()
            .downcast_ref::<UInt32Array>()
            .map(|array| Literal::UInt64(u64::from(array.value(index)))),
        DataType::UInt64 => values
            .as_any()
            .downcast_ref::<UInt64Array>()
            .map(|array| Literal::UInt64(array.value(index))),
        DataType::Float32 => values
            .as_any()
            .downcast_ref::<Float32Array>()
            .map(|array| Literal::Float64(f64::from(array.value(index)))),
        DataType::Float64 => values
            .as_any()
            .downcast_ref::<Float64Array>()
            .map(|array| Literal::Float64(array.value(index))),
        DataType::Utf8 => values
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|array| Literal::String(array.value(index).to_owned())),
        DataType::LargeUtf8 => values
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .map(|array| Literal::String(array.value(index).to_owned())),
        DataType::Utf8View => values
            .as_any()
            .downcast_ref::<StringViewArray>()
            .map(|array| Literal::String(array.value(index).to_owned())),
        DataType::Date32 => values
            .as_any()
            .downcast_ref::<Date32Array>()
            .map(|array| Literal::Date(array.value(index))),
        DataType::Date64 => values
            .as_any()
            .downcast_ref::<Date64Array>()
            .and_then(|array| date64_as_days(array.value(index)))
            .map(Literal::Date),
        DataType::Time32(TimeUnit::Second) => values
            .as_any()
            .downcast_ref::<Time32SecondArray>()
            .and_then(|array| to_micros(i64::from(array.value(index)), TimeUnit::Second))
            .map(Literal::Time),
        DataType::Time32(TimeUnit::Millisecond) => values
            .as_any()
            .downcast_ref::<Time32MillisecondArray>()
            .and_then(|array| to_micros(i64::from(array.value(index)), TimeUnit::Millisecond))
            .map(Literal::Time),
        DataType::Time64(TimeUnit::Microsecond) => values
            .as_any()
            .downcast_ref::<Time64MicrosecondArray>()
            .map(|array| Literal::Time(array.value(index))),
        DataType::Time64(TimeUnit::Nanosecond) => values
            .as_any()
            .downcast_ref::<Time64NanosecondArray>()
            .and_then(|array| to_micros(array.value(index), TimeUnit::Nanosecond))
            .map(Literal::Time),
        DataType::Timestamp(unit, _) => timestamp_array_value(values, index, unit)
            .and_then(|value| to_micros(value, *unit))
            .map(Literal::Timestamp),
        DataType::Decimal128(precision, scale) => values
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .map(|array| Literal::Decimal(array.value(index), *precision, *scale)),
        _ => None,
    }
}

fn timestamp_array_value(values: &dyn Array, index: usize, unit: &TimeUnit) -> Option<i64> {
    match unit {
        TimeUnit::Second => values
            .as_any()
            .downcast_ref::<TimestampSecondArray>()
            .map(|array| array.value(index)),
        TimeUnit::Millisecond => values
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .map(|array| array.value(index)),
        TimeUnit::Microsecond => values
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .map(|array| array.value(index)),
        TimeUnit::Nanosecond => values
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .map(|array| array.value(index)),
    }
}

pub(in crate::parquet_statistics) fn to_micros(value: i64, unit: TimeUnit) -> Option<i64> {
    match unit {
        TimeUnit::Second => value.checked_mul(1_000_000),
        TimeUnit::Millisecond => value.checked_mul(1_000),
        TimeUnit::Microsecond => Some(value),
        TimeUnit::Nanosecond if value % 1_000 == 0 => Some(value / 1_000),
        TimeUnit::Nanosecond => None,
    }
}

pub(in crate::parquet_statistics) fn date64_as_days(milliseconds: i64) -> Option<i32> {
    const MILLIS_PER_DAY: i64 = 86_400_000;
    (milliseconds % MILLIS_PER_DAY == 0)
        .then(|| i32::try_from(milliseconds / MILLIS_PER_DAY).ok())
        .flatten()
}

pub(in crate::parquet_statistics) fn array_u64_at(
    values: Option<&arrow::array::UInt64Array>,
    index: usize,
) -> Option<u64> {
    let values = values?;
    (!values.is_null(index)).then(|| values.value(index))
}
