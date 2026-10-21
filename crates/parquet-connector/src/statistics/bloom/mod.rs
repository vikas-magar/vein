//! Parquet Bloom filter binding and conservative row-group pruning.

use arrow::datatypes::DataType;
use parquet::arrow::arrow_reader::RowGroupSelection;
use parquet::basic::Type as ParquetType;
use parquet::bloom_filter::Sbbf;
use types::LogicalLiteral;

use values::decimal_may_contain;

mod planning;
mod values;


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
            ) if *precision == *literal_precision && *scale == *literal_scale =>
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

pub use planning::{bloom_filter_columns, equality_predicate_columns, prune_row_groups_with_bloom};
#[cfg(test)]
pub(super) use values::predicate_may_match_bloom;
pub(super) use values::{
    dictionary_value_type, time32_physical_value, time64_physical_value,
    timestamp_physical_value,
};
