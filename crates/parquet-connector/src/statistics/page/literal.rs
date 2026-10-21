//! Decode page-index and page-header bounds into logical values.

use arrow::datatypes::{DataType, TimeUnit};
use parquet::basic::Type as ParquetType;
use types::LogicalLiteral;

use super::super::metadata::{date64_as_days, to_micros};

/// Decodes a Parquet page bound as a format-neutral literal when its physical
/// representation is compatible with the Arrow data type.
pub(in crate::parquet_statistics) fn page_statistic_literal(
    bytes: &[u8],
    data_type: &DataType,
    physical_type: ParquetType,
) -> Option<LogicalLiteral> {
    use LogicalLiteral as Literal;

    fn int32(bytes: &[u8]) -> Option<i32> {
        Some(i32::from_le_bytes(bytes.try_into().ok()?))
    }
    fn int64(bytes: &[u8]) -> Option<i64> {
        Some(i64::from_le_bytes(bytes.try_into().ok()?))
    }
    fn decimal128(bytes: &[u8], physical_type: ParquetType) -> Option<i128> {
        match physical_type {
            ParquetType::INT32 => int32(bytes).map(i128::from),
            ParquetType::INT64 => int64(bytes).map(i128::from),
            ParquetType::BYTE_ARRAY | ParquetType::FIXED_LEN_BYTE_ARRAY
            if !bytes.is_empty() && bytes.len() <= 16 =>
                {
                    let fill = if bytes[0] & 0x80 == 0 { 0 } else { u8::MAX };
                    let mut value = [fill; 16];
                    value[16 - bytes.len()..].copy_from_slice(bytes);
                    Some(i128::from_be_bytes(value))
                }
            _ => None,
        }
    }

    match data_type {
        DataType::Boolean if bytes.len() == 1 => match bytes[0] {
            0 => Some(Literal::Boolean(false)),
            1 => Some(Literal::Boolean(true)),
            _ => None,
        },
        DataType::Int8 => i8::try_from(int32(bytes)?).ok().map(|value| Literal::Int64(i64::from(value))),
        DataType::Int16 => i16::try_from(int32(bytes)?).ok().map(|value| Literal::Int64(i64::from(value))),
        DataType::Int32 => int32(bytes).map(|value| Literal::Int64(i64::from(value))),
        DataType::Int64 => int64(bytes).map(Literal::Int64),
        DataType::UInt8 => u8::try_from(u32::from_le_bytes(bytes.try_into().ok()?))
            .ok()
            .map(|value| Literal::UInt64(u64::from(value))),
        DataType::UInt16 => u16::try_from(u32::from_le_bytes(bytes.try_into().ok()?))
            .ok()
            .map(|value| Literal::UInt64(u64::from(value))),
        DataType::UInt32 => Some(Literal::UInt64(u64::from(u32::from_le_bytes(
            bytes.try_into().ok()?,
        )))),
        DataType::UInt64 => Some(Literal::UInt64(u64::from_le_bytes(bytes.try_into().ok()?))),
        DataType::Float32 => {
            let value = f32::from_le_bytes(bytes.try_into().ok()?);
            value.is_finite().then_some(Literal::Float64(f64::from(value)))
        }
        DataType::Float64 => {
            let value = f64::from_le_bytes(bytes.try_into().ok()?);
            value.is_finite().then_some(Literal::Float64(value))
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            std::str::from_utf8(bytes).ok().map(|value| Literal::String(value.to_owned()))
        }
        DataType::Date32 => int32(bytes).map(Literal::Date),
        DataType::Date64 => int64(bytes).and_then(date64_as_days).map(Literal::Date),
        DataType::Time32(TimeUnit::Second) => int32(bytes)
            .and_then(|value| to_micros(i64::from(value), TimeUnit::Second))
            .map(Literal::Time),
        DataType::Time32(TimeUnit::Millisecond) => int32(bytes)
            .and_then(|value| to_micros(i64::from(value), TimeUnit::Millisecond))
            .map(Literal::Time),
        DataType::Time64(TimeUnit::Microsecond) => int64(bytes).map(Literal::Time),
        DataType::Time64(TimeUnit::Nanosecond) => int64(bytes)
            .and_then(|value| to_micros(value, TimeUnit::Nanosecond))
            .map(Literal::Time),
        DataType::Timestamp(unit, _) => int64(bytes)
            .and_then(|value| to_micros(value, *unit))
            .map(Literal::Timestamp),
        DataType::Decimal128(precision, scale) => decimal128(bytes, physical_type)
            .map(|value| Literal::Decimal(value, *precision, *scale)),
        _ => None,
    }
}
