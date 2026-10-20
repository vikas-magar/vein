use arrow::datatypes::{DataType, TimeUnit};
use bytes::Bytes;
use parquet::basic::Type as ParquetType;
use types::LogicalLiteral;

use super::decode_values;
use super::super::super::{bloom::BloomFilterColumn, dictionary::DictionaryPredicate};

fn matches(
    data_type: DataType,
    physical_type: ParquetType,
    type_length: i32,
    encoded: Vec<u8>,
    literal: LogicalLiteral,
) -> Option<bool> {
    let column = BloomFilterColumn {
        name: "value".into(),
        parquet_column_index: 0,
        arrow_type: data_type,
        parquet_type: physical_type,
        type_length,
    };
    let predicates = [DictionaryPredicate {
        index: 0,
        column: "value".into(),
        candidates: vec![literal],
    }];
    decode_values(&column, Bytes::from(encoded), 1, &predicates)[0]
}

#[test]
fn date32_dictionary_values_compare_as_epoch_days() {
    assert_eq!(
        matches(
            DataType::Date32,
            ParquetType::INT32,
            -1,
            19_i32.to_le_bytes().to_vec(),
            LogicalLiteral::Date(19),
        ),
        Some(true)
    );
}

#[test]
fn decimal_int32_values_require_matching_precision_and_scale() {
    let encoded = 12_345_i32.to_le_bytes().to_vec();
    assert_eq!(
        matches(
            DataType::Decimal128(9, 2),
            ParquetType::INT32,
            -1,
            encoded.clone(),
            LogicalLiteral::Decimal(12_345, 9, 2),
        ),
        Some(true)
    );
    assert_eq!(
        matches(
            DataType::Decimal128(9, 2),
            ParquetType::INT32,
            -1,
            encoded,
            LogicalLiteral::Decimal(12_345, 9, 1),
        ),
        None
    );
}

#[test]
fn fixed_length_decimal_dictionary_is_sign_extended_without_materializing_values() {
    assert_eq!(
        matches(
            DataType::Decimal128(12, 2),
            ParquetType::FIXED_LEN_BYTE_ARRAY,
            8,
            (-12_345_i64).to_be_bytes().to_vec(),
            LogicalLiteral::Decimal(-12_345, 12, 2),
        ),
        Some(true)
    );
}

#[test]
fn timestamp_and_time_literals_are_converted_to_parquet_units() {
    assert_eq!(
        matches(
            DataType::Timestamp(TimeUnit::Millisecond, None),
            ParquetType::INT64,
            -1,
            1_234_i64.to_le_bytes().to_vec(),
            LogicalLiteral::Timestamp(1_234_000),
        ),
        Some(true)
    );
    assert_eq!(
        matches(
            DataType::Time32(TimeUnit::Second),
            ParquetType::INT32,
            -1,
            3_i32.to_le_bytes().to_vec(),
            LogicalLiteral::Time(3_000_000),
        ),
        Some(true)
    );
}

#[test]
fn truncated_byte_array_dictionary_values_remain_unknown() {
    assert_eq!(
        matches(
            DataType::Utf8,
            ParquetType::BYTE_ARRAY,
            -1,
            [5_u32.to_le_bytes().as_slice(), b"x"].concat(),
            LogicalLiteral::String("x".into()),
        ),
        None
    );
}

#[test]
fn invalid_fixed_byte_array_width_remains_unknown() {
    assert_eq!(
        matches(
            DataType::Decimal128(4, 0),
            ParquetType::FIXED_LEN_BYTE_ARRAY,
            0,
            vec![1],
            LogicalLiteral::Decimal(1, 4, 0),
        ),
        None
    );
}
