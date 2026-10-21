use std::collections::HashMap;

use arrow::datatypes::{DataType, TimeUnit};
use catalog::StatisticsPredicate;
use parquet::basic::Type as ParquetType;
use parquet::bloom_filter::Sbbf;
use parquet::data_type::Decimal as ParquetDecimal;

use super::ParquetBloomFilter;

pub(in crate::parquet_statistics) fn predicate_may_match_bloom(
    predicate: &StatisticsPredicate,
    filters: &HashMap<String, ParquetBloomFilter>,
    probes: &mut u64,
) -> bool {
    match predicate {
        StatisticsPredicate::Comparison {
            column,
            comparison: catalog::StatisticsComparison::Equal,
            literal,
        } => filters
            .get(column)
            .and_then(|filter| filter.may_contain(literal))
            .is_none_or(|may_contain| {
                *probes = probes.saturating_add(1);
                may_contain
            }),
        StatisticsPredicate::And(children) => children
            .iter()
            .all(|child| predicate_may_match_bloom(child, filters, probes)),
        StatisticsPredicate::Or(children) => children
            .iter()
            .any(|child| predicate_may_match_bloom(child, filters, probes)),
        _ => true,
    }
}

pub(in crate::parquet_statistics) fn dictionary_value_type(data_type: &DataType) -> &DataType {
    match data_type {
        DataType::Dictionary(_, value_type) => dictionary_value_type(value_type),
        other => other,
    }
}

pub(in crate::parquet_statistics) fn timestamp_physical_value(value: i64, unit: &TimeUnit) -> Option<i64> {
    match unit {
        TimeUnit::Second => (value % 1_000_000 == 0).then_some(value / 1_000_000),
        TimeUnit::Millisecond => (value % 1_000 == 0).then_some(value / 1_000),
        TimeUnit::Microsecond => Some(value),
        TimeUnit::Nanosecond => value.checked_mul(1_000),
    }
}

pub(in crate::parquet_statistics) fn time32_physical_value(value: i64, unit: &TimeUnit) -> Option<i32> {
    let subsecond_value = match unit {
        TimeUnit::Second => (value % 1_000_000 == 0).then_some(value / 1_000_000)?,
        TimeUnit::Millisecond => (value % 1_000 == 0).then_some(value / 1_000)?,
        _ => return None,
    };
    i32::try_from(subsecond_value).ok()
}

pub(in crate::parquet_statistics) fn time64_physical_value(value: i64, unit: &TimeUnit) -> Option<i64> {
    match unit {
        TimeUnit::Microsecond => Some(value),
        TimeUnit::Nanosecond => value.checked_mul(1_000),
        _ => None,
    }
}

pub(super) fn decimal_may_contain(
    filter: &Sbbf,
    value: i128,
    precision: u8,
    scale: i8,
    physical_type: ParquetType,
    type_length: i32,
) -> Option<bool> {
    match physical_type {
        ParquetType::INT32 if precision <= 9 => {
            let value = i32::try_from(value).ok()?;
            Some(filter.check(&ParquetDecimal::Int32 {
                value: value.to_le_bytes(),
                precision: i32::from(precision),
                scale: i32::from(scale),
            }))
        }
        ParquetType::INT64 if precision <= 18 => {
            let value = i64::try_from(value).ok()?;
            Some(filter.check(&ParquetDecimal::Int64 {
                value: value.to_le_bytes(),
                precision: i32::from(precision),
                scale: i32::from(scale),
            }))
        }
        ParquetType::FIXED_LEN_BYTE_ARRAY => {
            let length = usize::try_from(type_length).ok()?;
            if length == 0 || length > 16 {
                return None;
            }
            let encoded = value.to_be_bytes();
            let prefix = &encoded[..encoded.len() - length];
            let sign_extension = if value < 0 { u8::MAX } else { 0 };
            if prefix.iter().any(|byte| *byte != sign_extension) {
                return None;
            }
            let decimal = ParquetDecimal::Bytes {
                value: encoded[encoded.len() - length..].to_vec().into(),
                precision: i32::from(precision),
                scale: i32::from(scale),
            };
            Some(filter.check(&decimal))
        }
        _ => None,
    }
}
