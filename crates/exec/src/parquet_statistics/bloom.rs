//! Parquet Bloom filter binding and conservative row-group pruning.

use std::collections::{HashMap, HashSet};

use arrow::datatypes::{DataType, Schema, TimeUnit};
use catalog::StatisticsPredicate;
use parquet::arrow::arrow_reader::statistics::StatisticsConverter;
use parquet::arrow::arrow_reader::RowGroupSelection;
use parquet::basic::Type as ParquetType;
use parquet::bloom_filter::Sbbf;
use parquet::data_type::Decimal as ParquetDecimal;
use parquet::file::metadata::ParquetMetaData;
use types::LogicalLiteral;

use super::metadata::{nonnegative, row_group_unit};

/// A predicate-referenced Parquet leaf column that can have a Bloom filter.
///
/// The column index and physical encoding are derived from the file schema. No
/// table or column names are special-cased by the reader.
#[derive(Debug, Clone)]
pub struct BloomFilterColumn {
    /// Logical Arrow field name used by the bound statistics predicate.
    pub name: String,
    /// Parquet leaf-column ordinal used to read the column-chunk filter.
    pub parquet_column_index: usize,
    pub(super) arrow_type: DataType,
    pub(super) parquet_type: ParquetType,
    pub(super) type_length: i32,
}

/// A loaded column-chunk Bloom filter and the schema needed to hash literals
/// using the Parquet physical representation.
#[derive(Debug)]
pub struct ParquetBloomFilter {
    column: BloomFilterColumn,
    filter: Sbbf,
}

impl ParquetBloomFilter {
    /// Binds a decoded Parquet split-block Bloom filter to its physical column.
    pub fn new(column: BloomFilterColumn, filter: Sbbf) -> Self {
        Self { column, filter }
    }

    fn may_contain(&self, literal: &LogicalLiteral) -> Option<bool> {
        let data_type = dictionary_value_type(&self.column.arrow_type);
        let parquet_type = self.column.parquet_type;
        let filter = &self.filter;

        match (data_type, parquet_type, literal) {
            (DataType::Boolean, ParquetType::BOOLEAN, LogicalLiteral::Boolean(value)) => {
                Some(filter.check(value))
            }
            (
                DataType::Int8 | DataType::Int16 | DataType::Int32,
                ParquetType::INT32,
                LogicalLiteral::Int64(value),
            ) => i32::try_from(*value).ok().map(|value| filter.check(&value)),
            (
                DataType::UInt8 | DataType::UInt16 | DataType::UInt32,
                ParquetType::INT32,
                LogicalLiteral::UInt64(value),
            ) => u32::try_from(*value).ok().map(|value| filter.check(&value)),
            (DataType::Date32, ParquetType::INT32, LogicalLiteral::Date(value)) => {
                Some(filter.check(value))
            }
            (DataType::Int64, ParquetType::INT64, LogicalLiteral::Int64(value)) => {
                Some(filter.check(value))
            }
            (DataType::UInt64, ParquetType::INT64, LogicalLiteral::UInt64(value)) => {
                Some(filter.check(value))
            }
            (DataType::Float32, ParquetType::FLOAT, LogicalLiteral::Float64(value)) => {
                Some(filter.check(&(*value as f32)))
            }
            (DataType::Float64, ParquetType::DOUBLE, LogicalLiteral::Float64(value)) => {
                Some(filter.check(value))
            }
            (
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
                ParquetType::BYTE_ARRAY,
                LogicalLiteral::String(value),
            ) => Some(filter.check(value.as_str())),
            (
                DataType::Timestamp(unit, _),
                ParquetType::INT64,
                LogicalLiteral::Timestamp(value),
            ) => timestamp_physical_value(*value, unit).map(|value| filter.check(&value)),
            (
                DataType::Time32(unit),
                ParquetType::INT32,
                LogicalLiteral::Time(value),
            ) => time32_physical_value(*value, unit).map(|value| filter.check(&value)),
            (
                DataType::Time64(unit),
                ParquetType::INT64,
                LogicalLiteral::Time(value),
            ) => time64_physical_value(*value, unit).map(|value| filter.check(&value)),
            (
                DataType::Decimal128(precision, scale),
                physical,
                LogicalLiteral::Decimal(value, literal_precision, literal_scale),
            ) if u8::try_from(*precision).ok() == Some(*literal_precision)
                && i8::try_from(*scale).ok() == Some(*literal_scale) =>
                {
                    decimal_may_contain(
                        filter,
                        *value,
                        *precision,
                        *scale,
                        physical,
                        self.column.type_length,
                    )
                }
            _ => None,
        }
    }
}

/// Bloom pruning result. Row-group selections keep their existing page-index
/// row selections, except for groups the filter proves cannot match.
#[derive(Debug, Default)]
pub struct BloomPruning {
    /// Row groups still eligible for decoding.
    pub selections: Vec<RowGroupSelection>,
    /// Whole row groups excluded by Bloom evidence.
    pub row_groups_skipped: u64,
    /// Rows in excluded groups.
    pub rows_skipped: u64,
    /// Compressed column bytes estimated to be avoided.
    pub estimated_compressed_bytes_skipped: u64,
    /// Equality probes answered by a loaded Bloom filter.
    pub probes: u64,
}

/// Resolves equality predicate columns to Parquet leaf ordinals.
///
/// Unsupported nested/schema mappings are omitted and therefore remain
/// conservative: they cannot cause a row group to be skipped.
pub fn bloom_filter_columns(
    arrow_schema: &Schema,
    metadata: &ParquetMetaData,
    predicates: &[StatisticsPredicate],
) -> Vec<BloomFilterColumn> {
    let names = equality_predicate_columns(predicates);

    let parquet_schema = metadata.file_metadata().schema_descr();
    names
        .into_iter()
        .filter_map(|name| {
            let (_, field) = arrow_schema.column_with_name(&name)?;
            let converter = StatisticsConverter::try_new(&name, arrow_schema, parquet_schema).ok()?;
            let parquet_column_index = converter.parquet_column_index()?;
            let descriptor = parquet_schema.column(parquet_column_index);
            Some(BloomFilterColumn {
                name,
                parquet_column_index,
                arrow_type: field.data_type().clone(),
                parquet_type: descriptor.physical_type(),
                type_length: descriptor.type_length(),
            })
        })
        .collect()
}

/// Returns the distinct columns with equality predicates in stable order.
/// Parquet column-chunk Bloom filters answer point-membership queries; range
/// predicates and row-level runtime Bloom filters are not equality candidates.
pub fn equality_predicate_columns(predicates: &[StatisticsPredicate]) -> Vec<String> {
    let mut names = HashSet::new();
    for predicate in predicates {
        collect_equality_columns(predicate, &mut names);
    }
    let mut names = names.into_iter().collect::<Vec<_>>();
    names.sort_unstable();
    names
}

/// Applies row-group column-chunk Bloom filters after exact footer and page
/// index pruning. A missing or unsupported filter is treated as “may match.”
pub fn prune_row_groups_with_bloom(
    metadata: &ParquetMetaData,
    predicates: &[StatisticsPredicate],
    selections: Vec<RowGroupSelection>,
    filters: &HashMap<usize, HashMap<String, ParquetBloomFilter>>,
) -> BloomPruning {
    let mut pruning = BloomPruning::default();
    for selection in selections {
        let row_group_index = selection.row_group_index();
        let Some(row_group_filters) = filters.get(&row_group_index) else {
            pruning.selections.push(selection);
            continue;
        };
        let mut probes = 0;
        let may_match = predicates
            .iter()
            .all(|predicate| predicate_may_match_bloom(predicate, row_group_filters, &mut probes));
        pruning.probes = pruning.probes.saturating_add(probes);
        if may_match {
            pruning.selections.push(selection);
            continue;
        }

        let row_group = metadata.row_group(row_group_index);
        pruning.row_groups_skipped = pruning.row_groups_skipped.saturating_add(1);
        pruning.rows_skipped = pruning
            .rows_skipped
            .saturating_add(nonnegative(row_group.num_rows()).unwrap_or(0));
        pruning.estimated_compressed_bytes_skipped = pruning
            .estimated_compressed_bytes_skipped
            .saturating_add(row_group_unit(row_group_index, row_group).compressed_bytes.unwrap_or(0));
    }
    pruning
}

fn collect_equality_columns(predicate: &StatisticsPredicate, names: &mut HashSet<String>) {
    match predicate {
        StatisticsPredicate::Comparison {
            column,
            comparison: catalog::StatisticsComparison::Equal,
            ..
        } => {
            names.insert(column.clone());
        }
        StatisticsPredicate::And(children) | StatisticsPredicate::Or(children) => {
            for child in children {
                collect_equality_columns(child, names);
            }
        }
        _ => {}
    }
}

pub(super) fn predicate_may_match_bloom(
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
            .map_or(true, |may_contain| {
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

pub(super) fn dictionary_value_type(data_type: &DataType) -> &DataType {
    match data_type {
        DataType::Dictionary(_, value_type) => dictionary_value_type(value_type),
        other => other,
    }
}

fn timestamp_physical_value(value: i64, unit: &TimeUnit) -> Option<i64> {
    match unit {
        TimeUnit::Second => (value % 1_000_000 == 0).then_some(value / 1_000_000),
        TimeUnit::Millisecond => (value % 1_000 == 0).then_some(value / 1_000),
        TimeUnit::Microsecond => Some(value),
        TimeUnit::Nanosecond => value.checked_mul(1_000),
    }
}

fn time32_physical_value(value: i64, unit: &TimeUnit) -> Option<i32> {
    let subsecond_value = match unit {
        TimeUnit::Second => (value % 1_000_000 == 0).then_some(value / 1_000_000)?,
        TimeUnit::Millisecond => (value % 1_000 == 0).then_some(value / 1_000)?,
        _ => return None,
    };
    i32::try_from(subsecond_value).ok()
}

fn time64_physical_value(value: i64, unit: &TimeUnit) -> Option<i64> {
    match unit {
        TimeUnit::Microsecond => Some(value),
        TimeUnit::Nanosecond => value.checked_mul(1_000),
        _ => None,
    }
}

fn decimal_may_contain(
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
