//! Physical-type-aware comparisons for PLAIN Parquet dictionary values.

use arrow::datatypes::DataType;
use bytes::Bytes;
use parquet::basic::Type as ParquetType;
use types::LogicalLiteral;

use super::super::bloom::{
    dictionary_value_type, time32_physical_value, time64_physical_value,
    timestamp_physical_value, BloomFilterColumn,
};
use super::decoder::{
    decode_dictionary, BoolType, ByteArrayType, DoubleType, FixedLenByteArrayType, FloatType,
    Int32Type, Int64Type,
};
use super::DictionaryPredicate;

pub(super) fn decode_values(
    column: &BloomFilterColumn,
    bytes: Bytes,
    value_count: usize,
    predicates: &[DictionaryPredicate],
) -> Vec<Option<bool>> {
    let data_type = dictionary_value_type(&column.arrow_type);
    let physical_type = column.parquet_type;
    let type_length = column.type_length;
    match (data_type, physical_type) {
        (DataType::Boolean, ParquetType::BOOLEAN) => decode_dictionary::<BoolType>(
            bytes, value_count, type_length, predicates,
            |value, literal| match literal {
                LogicalLiteral::Boolean(candidate) => Some(value == candidate),
                _ => None,
            },
        ),
        (DataType::Int8 | DataType::Int16 | DataType::Int32, ParquetType::INT32) => {
            decode_dictionary::<Int32Type>(bytes, value_count, type_length, predicates,
                                           |value, literal| match literal {
                                               LogicalLiteral::Int64(candidate) => Some(i64::from(*value) == *candidate),
                                               _ => None,
                                           })
        }
        (DataType::UInt8 | DataType::UInt16 | DataType::UInt32, ParquetType::INT32) => {
            decode_dictionary::<Int32Type>(bytes, value_count, type_length, predicates,
                                           |value, literal| match literal {
                                               LogicalLiteral::UInt64(candidate) => Some(u64::from(*value as u32) == *candidate),
                                               _ => None,
                                           })
        }
        (DataType::Date32, ParquetType::INT32) => decode_dictionary::<Int32Type>(
            bytes, value_count, type_length, predicates,
            |value, literal| match literal {
                LogicalLiteral::Date(candidate) => Some(*value == *candidate),
                _ => None,
            },
        ),
        (DataType::Int64, ParquetType::INT64) => decode_dictionary::<Int64Type>(
            bytes, value_count, type_length, predicates,
            |value, literal| match literal {
                LogicalLiteral::Int64(candidate) => Some(*value == *candidate),
                _ => None,
            },
        ),
        (DataType::UInt64, ParquetType::INT64) => decode_dictionary::<Int64Type>(
            bytes, value_count, type_length, predicates,
            |value, literal| match literal {
                LogicalLiteral::UInt64(candidate) => Some(*value as u64 == *candidate),
                _ => None,
            },
        ),
        (DataType::Timestamp(unit, _), ParquetType::INT64) => decode_dictionary::<Int64Type>(
            bytes, value_count, type_length, predicates,
            |value, literal| match literal {
                LogicalLiteral::Timestamp(candidate) =>
                    timestamp_physical_value(*candidate, unit).map(|physical| *value == physical),
                _ => None,
            },
        ),
        (DataType::Time32(unit), ParquetType::INT32) => decode_dictionary::<Int32Type>(
            bytes, value_count, type_length, predicates,
            |value, literal| match literal {
                LogicalLiteral::Time(candidate) =>
                    time32_physical_value(*candidate, unit).map(|physical| *value == physical),
                _ => None,
            },
        ),
        (DataType::Time64(unit), ParquetType::INT64) => decode_dictionary::<Int64Type>(
            bytes, value_count, type_length, predicates,
            |value, literal| match literal {
                LogicalLiteral::Time(candidate) =>
                    time64_physical_value(*candidate, unit).map(|physical| *value == physical),
                _ => None,
            },
        ),
        (DataType::Decimal128(precision, scale), ParquetType::INT32) if *precision <= 9 => {
            decode_dictionary::<Int32Type>(bytes, value_count, type_length, predicates,
                                           |value, literal| decimal_candidate(i128::from(*value), *precision, *scale, literal))
        }
        (DataType::Decimal128(precision, scale), ParquetType::INT64) if *precision <= 18 => {
            decode_dictionary::<Int64Type>(bytes, value_count, type_length, predicates,
                                           |value, literal| decimal_candidate(i128::from(*value), *precision, *scale, literal))
        }
        (DataType::Decimal128(precision, scale), ParquetType::FIXED_LEN_BYTE_ARRAY) => {
            decode_dictionary::<FixedLenByteArrayType>(bytes, value_count, type_length, predicates,
                                                       |value, literal| signed_decimal(value).and_then(|raw| {
                                                           decimal_candidate(raw, *precision, *scale, literal)
                                                       }))
        }
        (DataType::Float32, ParquetType::FLOAT) => decode_dictionary::<FloatType>(
            bytes, value_count, type_length, predicates,
            |value, literal| match literal {
                LogicalLiteral::Float64(candidate) => Some(*value == *candidate as f32),
                _ => None,
            },
        ),
        (DataType::Float64, ParquetType::DOUBLE) => decode_dictionary::<DoubleType>(
            bytes, value_count, type_length, predicates,
            |value, literal| match literal {
                LogicalLiteral::Float64(candidate) => Some(*value == *candidate),
                _ => None,
            },
        ),
        (DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View, ParquetType::BYTE_ARRAY) => {
            decode_dictionary::<ByteArrayType>(bytes, value_count, type_length, predicates,
                                               |value, literal| match literal {
                                                   LogicalLiteral::String(candidate) => {
                                                       std::str::from_utf8(value.as_ref()).ok()?;
                                                       Some(value.as_ref() == candidate.as_bytes())
                                                   }
                                                   _ => None,
                                               })
        }
        _ => vec![None; predicates.len()],
    }
}

fn decimal_candidate(
    value: i128,
    precision: u8,
    scale: i8,
    literal: &LogicalLiteral,
) -> Option<bool> {
    match literal {
        LogicalLiteral::Decimal(candidate, literal_precision, literal_scale)
        if precision == *literal_precision && scale == *literal_scale =>
            Some(value == *candidate),
        LogicalLiteral::Decimal(..) => None,
        _ => None,
    }
}

fn signed_decimal(bytes: &Bytes) -> Option<i128> {
    let width = bytes.len();
    if width == 0 || width > 16 {
        return None;
    }
    let sign = if bytes[0] & 0x80 == 0 { 0 } else { u8::MAX };
    let mut encoded = [sign; 16];
    encoded[16 - width..].copy_from_slice(bytes);
    Some(i128::from_be_bytes(encoded))
}

#[cfg(test)]
#[path = "values_tests.rs"]
mod tests;
