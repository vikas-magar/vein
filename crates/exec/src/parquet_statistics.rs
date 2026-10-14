//! Footer-only Parquet statistics extraction and conservative rollups.
//!
//! The extraction path reads the already parsed footer. It never scans column
//! values. Parquet's typed statistics converter handles physical-to-Arrow
//! mappings; unsupported, malformed, or inexact values remain unknown.

use std::collections::{HashMap, HashSet};

use arrow::array::{
    Array, BooleanArray, Date32Array, Date64Array, Decimal128Array, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, LargeStringArray, StringArray, StringViewArray,
    Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Schema, TimeUnit};
use bytes::Bytes;
use catalog::{
    ColumnStatistics, ColumnStorageStatistics, DataUnitStatistics, StatisticsUnitKind,
    StatisticsPredicate, TableStatistics, may_match_data_unit,
};
use parquet::arrow::arrow_reader::statistics::StatisticsConverter;
use parquet::arrow::arrow_reader::{RowGroupSelection, RowSelection, RowSelector};
use parquet::basic::{Encoding, Type as ParquetType};
use parquet::bloom_filter::Sbbf;
use parquet::column::page::{Page, PageReader};
use parquet::data_type::Decimal as ParquetDecimal;
use parquet::file::metadata::{ColumnChunkMetaData, ParquetMetaData, RowGroupMetaData};
use parquet::file::reader::{ChunkReader, Length, SerializedPageReader};
use parquet::file::statistics::Statistics as ParquetStatistics;
use types::LogicalLiteral;
use std::io::Cursor;
use std::sync::Arc;

/// Extracts one file node and all its row-group children from a cached footer.
///
/// The returned child indices are local to this vector; callers that append
/// several file trees must offset their `parent` indices by the destination
/// vector's starting length.
pub fn file_units(
    file_ordinal: usize,
    file_bytes: u64,
    arrow_schema: &Schema,
    metadata: &ParquetMetaData,
) -> Vec<DataUnitStatistics> {
    let row_groups = metadata.row_groups();
    let mut units = Vec::with_capacity(row_groups.len().saturating_add(1));
    let row_group_units = row_groups
        .iter()
        .enumerate()
        .map(|(ordinal, row_group)| row_group_unit(ordinal, row_group))
        .collect::<Vec<_>>();

    let row_group_rows = if row_group_units.is_empty() {
        Some(0)
    } else {
        complete_sum(row_group_units.iter().map(|unit| unit.num_rows))
    };
    let footer_rows = nonnegative(metadata.file_metadata().num_rows());
    let file_rows = footer_rows.filter(|rows| row_group_rows == Some(*rows));
    let mut file_unit = DataUnitStatistics::new(StatisticsUnitKind::File, file_ordinal);
    file_unit.num_rows = file_rows;
    file_unit.physical_bytes = Some(file_bytes);
    file_unit.compressed_bytes = complete_sum(
        row_group_units
            .iter()
            .map(|unit| unit.compressed_bytes),
    );
    file_unit.uncompressed_bytes = complete_sum(
        row_group_units
            .iter()
            .map(|unit| unit.uncompressed_bytes),
    );
    units.push(file_unit);

    for mut unit in row_group_units {
        unit.parent = Some(0);
        units.push(unit);
    }

    populate_column_statistics(arrow_schema, metadata, &mut units[1..], None);
    units
}

/// Exact row groups that may satisfy all pushed scan predicates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowGroupPruning {
    /// Original row-group indexes to pass to the Parquet reader.
    pub selected: Vec<usize>,
    /// Rows in row groups skipped from their footer row counts.
    pub skipped_rows: u64,
    /// Compressed bytes in skipped column chunks, when the footer provides them.
    pub estimated_compressed_bytes_skipped: u64,
    /// Per-predicate evidence explaining why candidate row groups were retained
    /// or pruned. This is populated from footer statistics only.
    pub predicate_diagnostics: PredicatePruningDiagnostics,
}

/// Bounded per-predicate counters emitted with scan diagnostics.
///
/// This records counts rather than one event per row group/page, so DEBUG logs
/// stay useful on large files. Predicate labels are truncated before logging.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PredicatePruningDiagnostics {
    /// One bounded counter set for each input statistics predicate.
    pub predicates: Vec<PredicatePruningDiagnostic>,
}

/// Decision counts for one statistics predicate at one pruning stage.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PredicatePruningDiagnostic {
    /// Bounded debug representation of the predicate.
    pub predicate: String,
    /// Candidate data units for which this predicate had enough context to decide.
    pub units_considered: u64,
    /// Units excluded by complete, compatible metadata bounds.
    pub units_pruned: u64,
    /// Units retained because required column or null-count statistics were absent.
    pub retained_missing_statistics: u64,
    /// Units retained because at least one required min/max bound was incomplete.
    pub retained_incomplete_bounds: u64,
    /// Units retained because the predicate form is not understood by statistics pruning.
    pub retained_unsupported: u64,
    /// Units retained because bounds may overlap or are type-incompatible.
    pub retained_bounds_may_match: u64,
}

impl PredicatePruningDiagnostics {
    fn for_predicates(predicates: &[StatisticsPredicate]) -> Self {
        Self {
            predicates: predicates
                .iter()
                .map(|predicate| PredicatePruningDiagnostic {
                    predicate: bounded_predicate_label(predicate),
                    ..PredicatePruningDiagnostic::default()
                })
                .collect(),
        }
    }

    fn record(
        &mut self,
        index: usize,
        predicate: &StatisticsPredicate,
        unit: &DataUnitStatistics,
        may_match: bool,
    ) {
        let Some(diagnostic) = self.predicates.get_mut(index) else {
            return;
        };
        diagnostic.units_considered = diagnostic.units_considered.saturating_add(1);
        if !may_match {
            diagnostic.units_pruned = diagnostic.units_pruned.saturating_add(1);
            return;
        }
        match retained_statistics_reason(predicate, unit) {
            RetainedStatisticsReason::Missing => {
                diagnostic.retained_missing_statistics =
                    diagnostic.retained_missing_statistics.saturating_add(1);
            }
            RetainedStatisticsReason::Incomplete => {
                diagnostic.retained_incomplete_bounds =
                    diagnostic.retained_incomplete_bounds.saturating_add(1);
            }
            RetainedStatisticsReason::Unsupported => {
                diagnostic.retained_unsupported =
                    diagnostic.retained_unsupported.saturating_add(1);
            }
            RetainedStatisticsReason::BoundsMayMatch => {
                diagnostic.retained_bounds_may_match =
                    diagnostic.retained_bounds_may_match.saturating_add(1);
            }
        }
    }

    fn record_missing_metadata(&mut self, index: usize) {
        if let Some(diagnostic) = self.predicates.get_mut(index) {
            diagnostic.units_considered = diagnostic.units_considered.saturating_add(1);
            diagnostic.retained_missing_statistics =
                diagnostic.retained_missing_statistics.saturating_add(1);
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum RetainedStatisticsReason {
    Missing,
    Incomplete,
    Unsupported,
    BoundsMayMatch,
}

fn bounded_predicate_label(predicate: &StatisticsPredicate) -> String {
    const MAX_LABEL_CHARS: usize = 160;
    let label = format!("{predicate:?}");
    if label.chars().count() <= MAX_LABEL_CHARS {
        return label;
    }
    let mut bounded = label.chars().take(MAX_LABEL_CHARS).collect::<String>();
    bounded.push_str("...");
    bounded
}

fn retained_statistics_reason(
    predicate: &StatisticsPredicate,
    unit: &DataUnitStatistics,
) -> RetainedStatisticsReason {
    match predicate {
        StatisticsPredicate::Unknown => RetainedStatisticsReason::Unsupported,
        StatisticsPredicate::Comparison { column, .. } => {
            let Some(statistics) = unit.column_statistics.get(column) else {
                return RetainedStatisticsReason::Missing;
            };
            if statistics.min_max_complete != Some(true)
                && !unit
                .num_rows
                .is_some_and(|rows| statistics.null_count == Some(rows))
            {
                return RetainedStatisticsReason::Incomplete;
            }
            RetainedStatisticsReason::BoundsMayMatch
        }
        StatisticsPredicate::ColumnComparison {
            left_column,
            right_column,
            ..
        } => {
            let (Some(left), Some(right)) = (
                unit.column_statistics.get(left_column),
                unit.column_statistics.get(right_column),
            ) else {
                return RetainedStatisticsReason::Missing;
            };
            if left.min_max_complete != Some(true) || right.min_max_complete != Some(true) {
                return RetainedStatisticsReason::Incomplete;
            }
            RetainedStatisticsReason::BoundsMayMatch
        }
        StatisticsPredicate::IsNull { column } | StatisticsPredicate::IsNotNull { column } => {
            if unit
                .column_statistics
                .get(column)
                .and_then(|statistics| statistics.null_count)
                .is_none()
            {
                RetainedStatisticsReason::Missing
            } else {
                RetainedStatisticsReason::BoundsMayMatch
            }
        }
        StatisticsPredicate::And(children) | StatisticsPredicate::Or(children) => {
            let reasons = children
                .iter()
                .map(|child| retained_statistics_reason(child, unit))
                .collect::<Vec<_>>();
            if reasons
                .iter()
                .any(|reason| matches!(reason, RetainedStatisticsReason::Unsupported))
            {
                RetainedStatisticsReason::Unsupported
            } else if reasons
                .iter()
                .any(|reason| matches!(reason, RetainedStatisticsReason::Missing))
            {
                RetainedStatisticsReason::Missing
            } else if reasons
                .iter()
                .any(|reason| matches!(reason, RetainedStatisticsReason::Incomplete))
            {
                RetainedStatisticsReason::Incomplete
            } else {
                RetainedStatisticsReason::BoundsMayMatch
            }
        }
    }
}

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
    arrow_type: DataType,
    parquet_type: ParquetType,
    type_length: i32,
}

/// One equality or same-column `IN` condition eligible for dictionary checks.
///
/// `index` preserves the position in the extracted predicate list so a scan
/// can combine results from multiple dictionary columns without reordering
/// conjuncts.
#[derive(Debug, Clone, PartialEq)]
pub struct DictionaryPredicate {
    /// Position in the dictionary predicate list.
    pub index: usize,
    /// Source schema column containing the dictionary values.
    pub column: String,
    /// Equality alternatives. Any one value may satisfy this condition.
    pub candidates: Vec<LogicalLiteral>,
}

/// Dictionary evidence used to select row groups before data pages are decoded.
#[derive(Debug, Default)]
pub struct DictionaryPruning {
    /// Row groups retained, including their existing page-index selections.
    pub selections: Vec<RowGroupSelection>,
    /// Whole row groups excluded by dictionary evidence.
    pub row_groups_skipped: u64,
    /// Rows in excluded row groups.
    pub rows_skipped: u64,
    /// Compressed bytes in excluded row groups when known.
    pub estimated_compressed_bytes_skipped: u64,
    /// Dictionary predicate checks with a supported decoded result.
    pub probes: u64,
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
    let mut names = HashSet::new();
    for predicate in predicates {
        collect_equality_columns(predicate, &mut names);
    }
    let mut names = names.into_iter().collect::<Vec<_>>();
    names.sort_unstable();

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

/// Extracts equality and same-column `IN` conditions from a conjunction.
///
/// Other predicate shapes are intentionally ignored. In particular, a mixed
/// `OR` cannot be used as a dictionary proof unless every branch is an equality
/// against the same column.
pub fn dictionary_predicates(predicates: &[StatisticsPredicate]) -> Vec<DictionaryPredicate> {
    fn collect(predicate: &StatisticsPredicate, output: &mut Vec<DictionaryPredicate>) {
        match predicate {
            StatisticsPredicate::And(children) => {
                for child in children {
                    collect(child, output);
                }
            }
            StatisticsPredicate::Comparison {
                column,
                comparison: catalog::StatisticsComparison::Equal,
                literal,
            } => output.push(DictionaryPredicate {
                index: output.len(),
                column: column.clone(),
                candidates: vec![literal.clone()],
            }),
            StatisticsPredicate::Or(children) => {
                let mut column: Option<&str> = None;
                let mut candidates = Vec::with_capacity(children.len());
                for child in children {
                    let StatisticsPredicate::Comparison {
                        column: child_column,
                        comparison: catalog::StatisticsComparison::Equal,
                        literal,
                    } = child
                    else {
                        return;
                    };
                    if column.is_some_and(|column| column != child_column) {
                        return;
                    }
                    column = Some(child_column);
                    candidates.push(literal.clone());
                }
                if let Some(column) = column {
                    output.push(DictionaryPredicate {
                        index: output.len(),
                        column: column.to_string(),
                        candidates,
                    });
                }
            }
            _ => {}
        }
    }

    let mut output = Vec::new();
    for predicate in predicates {
        collect(predicate, &mut output);
    }
    output
}

/// Returns whether every data page in this chunk is dictionary encoded.
///
/// Missing encoding statistics and mixed encodings are not sufficient proof;
/// callers must retain those row groups.
pub fn is_fully_dictionary_encoded(column: &ColumnChunkMetaData) -> bool {
    column.dictionary_page_offset().is_some()
        && column.page_encoding_stats_mask().is_some_and(|mask| {
        mask.is_only(Encoding::PLAIN_DICTIONARY) || mask.is_only(Encoding::RLE_DICTIONARY)
    })
}

/// Gets the serialized dictionary page range when its offsets are complete and
/// contained by the known file length.
pub fn dictionary_page_range(
    column: &ColumnChunkMetaData,
    file_length: u64,
) -> Option<std::ops::Range<u64>> {
    if !is_fully_dictionary_encoded(column) {
        return None;
    }
    let start = u64::try_from(column.dictionary_page_offset()?).ok()?;
    let end = u64::try_from(column.data_page_offset()).ok()?;
    (start < end && end <= file_length).then_some(start..end)
}

/// Checks candidate literals against a serialized dictionary page.
///
/// Each result is `Some(true)` when the dictionary contains a candidate,
/// `Some(false)` when the supported dictionary proves none are present, and
/// `None` when the page, physical type, or literal conversion is unsupported.
/// `Bytes` slices are retained through Parquet page decoding; this examines only
/// the small dictionary and does not materialize row batches or column arrays.
pub fn dictionary_page_predicate_matches(
    page_bytes: Bytes,
    file_length: u64,
    row_count: usize,
    chunk: &ColumnChunkMetaData,
    column: &BloomFilterColumn,
    predicates: &[DictionaryPredicate],
) -> Vec<Option<bool>> {
    if predicates.is_empty() || !is_fully_dictionary_encoded(chunk) {
        return vec![None; predicates.len()];
    }
    let Some(range) = dictionary_page_range(chunk, file_length) else {
        return vec![None; predicates.len()];
    };
    let base_offset = range.start;
    let reader = DictionaryPageChunkReader {
        bytes: page_bytes,
        base_offset,
        file_length,
    };
    let Ok(mut pages) = SerializedPageReader::new(Arc::new(reader), chunk, row_count, None) else {
        return vec![None; predicates.len()];
    };
    let Ok(Some(Page::DictionaryPage {
                    buf,
                    num_values,
                    encoding: Encoding::PLAIN,
                    ..
                })) = pages.get_next_page()
    else {
        return vec![None; predicates.len()];
    };
    let Ok(num_values) = usize::try_from(num_values) else {
        return vec![None; predicates.len()];
    };

    let data_type = dictionary_value_type(&column.arrow_type);
    match (data_type, column.parquet_type) {
        (DataType::Boolean, ParquetType::BOOLEAN) => {
            decode_dictionary::<BoolType>(buf, num_values, column.type_length, predicates, |value, literal| {
                match literal {
                    LogicalLiteral::Boolean(candidate) => Some(value == candidate),
                    _ => None,
                }
            })
        }
        (
            DataType::Int8 | DataType::Int16 | DataType::Int32,
            ParquetType::INT32,
        ) => decode_dictionary::<Int32Type>(
            buf,
            num_values,
            column.type_length,
            predicates,
            |value, literal| match literal {
                LogicalLiteral::Int64(candidate) => Some(i64::from(*value) == *candidate),
                _ => None,
            },
        ),
        (
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32,
            ParquetType::INT32,
        ) => decode_dictionary::<Int32Type>(
            buf,
            num_values,
            column.type_length,
            predicates,
            |value, literal| match literal {
                LogicalLiteral::UInt64(candidate) => Some(u64::from(*value as u32) == *candidate),
                _ => None,
            },
        ),
        (DataType::Date32, ParquetType::INT32) => decode_dictionary::<Int32Type>(
            buf,
            num_values,
            column.type_length,
            predicates,
            |value, literal| match literal {
                LogicalLiteral::Date(candidate) => Some(*value == *candidate),
                _ => None,
            },
        ),
        (DataType::Int64, ParquetType::INT64) => decode_dictionary::<Int64Type>(
            buf,
            num_values,
            column.type_length,
            predicates,
            |value, literal| match literal {
                LogicalLiteral::Int64(candidate) => Some(*value == *candidate),
                _ => None,
            },
        ),
        (DataType::UInt64, ParquetType::INT64) => decode_dictionary::<Int64Type>(
            buf,
            num_values,
            column.type_length,
            predicates,
            |value, literal| match literal {
                LogicalLiteral::UInt64(candidate) => Some(*value as u64 == *candidate),
                _ => None,
            },
        ),
        (DataType::Float32, ParquetType::FLOAT) => decode_dictionary::<FloatType>(
            buf,
            num_values,
            column.type_length,
            predicates,
            |value, literal| match literal {
                LogicalLiteral::Float64(candidate) => Some(*value == *candidate as f32),
                _ => None,
            },
        ),
        (DataType::Float64, ParquetType::DOUBLE) => decode_dictionary::<DoubleType>(
            buf,
            num_values,
            column.type_length,
            predicates,
            |value, literal| match literal {
                LogicalLiteral::Float64(candidate) => Some(*value == *candidate),
                _ => None,
            },
        ),
        (
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
            ParquetType::BYTE_ARRAY,
        ) => decode_dictionary::<ByteArrayType>(
            buf,
            num_values,
            column.type_length,
            predicates,
            |value, literal| match literal {
                LogicalLiteral::String(candidate) => {
                    std::str::from_utf8(value.as_ref()).ok()?;
                    Some(value.as_ref() == candidate.as_bytes())
                }
                _ => None,
            },
        ),
        _ => vec![None; predicates.len()],
    }
}

trait PlainDictionaryValue {
    type Value;

    fn decode(bytes: &Bytes, index: usize, offset: &mut usize) -> Option<Self::Value>;
}

struct BoolType;
struct Int32Type;
struct Int64Type;
struct FloatType;
struct DoubleType;
struct ByteArrayType;

impl PlainDictionaryValue for BoolType {
    type Value = bool;

    fn decode(bytes: &Bytes, index: usize, _offset: &mut usize) -> Option<Self::Value> {
        let byte = *bytes.get(index / 8)?;
        Some(byte & (1 << (index % 8)) != 0)
    }
}

macro_rules! impl_fixed_dictionary_value {
    ($marker:ty, $value:ty, $width:literal, $decode:path) => {
        impl PlainDictionaryValue for $marker {
            type Value = $value;

            fn decode(bytes: &Bytes, index: usize, _offset: &mut usize) -> Option<Self::Value> {
                let start = index.checked_mul($width)?;
                let end = start.checked_add($width)?;
                let encoded: [u8; $width] = bytes.get(start..end)?.try_into().ok()?;
                Some($decode(encoded))
            }
        }
    };
}

impl_fixed_dictionary_value!(Int32Type, i32, 4, i32::from_le_bytes);
impl_fixed_dictionary_value!(Int64Type, i64, 8, i64::from_le_bytes);
impl_fixed_dictionary_value!(FloatType, f32, 4, f32::from_le_bytes);
impl_fixed_dictionary_value!(DoubleType, f64, 8, f64::from_le_bytes);

impl PlainDictionaryValue for ByteArrayType {
    type Value = Bytes;

    fn decode(bytes: &Bytes, _index: usize, offset: &mut usize) -> Option<Self::Value> {
        let length_end = (*offset).checked_add(4)?;
        let encoded_length: [u8; 4] = bytes.get(*offset..length_end)?.try_into().ok()?;
        let value_length = usize::try_from(u32::from_le_bytes(encoded_length)).ok()?;
        let value_end = length_end.checked_add(value_length)?;
        let value = bytes.slice(length_end..value_end);
        *offset = value_end;
        Some(value)
    }
}

fn decode_dictionary<T>(
    bytes: Bytes,
    value_count: usize,
    _type_length: i32,
    predicates: &[DictionaryPredicate],
    mut candidate_matches: impl FnMut(&T::Value, &LogicalLiteral) -> Option<bool>,
) -> Vec<Option<bool>>
where
    T: PlainDictionaryValue,
{
    let mut found = vec![false; predicates.len()];
    let mut supported = vec![true; predicates.len()];
    let mut offset = 0;
    for value_index in 0..value_count {
        if found.iter().zip(&supported).all(|(found, supported)| *found || !*supported) {
            break;
        }
        let Some(value) = T::decode(&bytes, value_index, &mut offset) else {
            return vec![None; predicates.len()];
        };
        for (index, predicate) in predicates.iter().enumerate() {
            if found[index] || !supported[index] {
                continue;
            }
            for candidate in &predicate.candidates {
                match candidate_matches(&value, candidate) {
                    Some(true) => {
                        found[index] = true;
                        break;
                    }
                    Some(false) => {}
                    None => supported[index] = false,
                }
                if !supported[index] {
                    break;
                }
            }
        }
    }
    found
        .into_iter()
        .zip(supported)
        .map(|(found, supported)| {
            if found {
                Some(true)
            } else if supported {
                Some(false)
            } else {
                None
            }
        })
        .collect()
}

struct DictionaryPageChunkReader {
    bytes: Bytes,
    base_offset: u64,
    file_length: u64,
}

impl Length for DictionaryPageChunkReader {
    fn len(&self) -> u64 {
        self.file_length
    }
}

impl ChunkReader for DictionaryPageChunkReader {
    type T = Cursor<Bytes>;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        let position = start
            .checked_sub(self.base_offset)
            .filter(|position| *position <= self.bytes.len() as u64)
            .ok_or_else(|| parquet::errors::ParquetError::General("read outside dictionary page range".into()))?;
        let mut cursor = Cursor::new(self.bytes.clone());
        cursor.set_position(position);
        Ok(cursor)
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        let start = start
            .checked_sub(self.base_offset)
            .and_then(|offset| usize::try_from(offset).ok())
            .ok_or_else(|| parquet::errors::ParquetError::General("read outside dictionary page range".into()))?;
        let end = start
            .checked_add(length)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| parquet::errors::ParquetError::General("read outside dictionary page range".into()))?;
        Ok(self.bytes.slice(start..end))
    }
}

/// Applies dictionary predicate evidence to existing row-group selections.
/// Unknown/missing outcomes retain their selection and its page-index ranges.
pub fn prune_row_groups_with_dictionary(
    metadata: &ParquetMetaData,
    selections: Vec<RowGroupSelection>,
    membership: &HashMap<usize, Vec<Option<bool>>>,
) -> DictionaryPruning {
    let mut pruning = DictionaryPruning::default();
    for selection in selections {
        let row_group_index = selection.row_group_index();
        let outcomes = membership.get(&row_group_index);
        let mut proven_absent = false;
        if let Some(outcomes) = outcomes {
            for outcome in outcomes {
                if outcome.is_some() {
                    pruning.probes = pruning.probes.saturating_add(1);
                }
                proven_absent |= *outcome == Some(false);
            }
        }
        if !proven_absent {
            pruning.selections.push(selection);
            continue;
        }
        let Some(row_group) = metadata.row_groups().get(row_group_index) else {
            pruning.selections.push(selection);
            continue;
        };
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

fn predicate_may_match_bloom(
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

fn dictionary_value_type(data_type: &DataType) -> &DataType {
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

/// Conservative page-index selection for the row groups that survived footer
/// pruning. Row selections are local to each row group, so pages with different
/// boundaries across columns can be combined without converting them into
/// file-global row offsets.
#[derive(Debug, Default)]
pub struct PagePruning {
    /// Row groups to pass to the Parquet reader, with optional local selections.
    pub selections: Vec<RowGroupSelection>,
    /// Number of row-group/column pairs with usable column and offset indexes.
    pub indexed_columns: u64,
    /// Indexed page entries whose min/max bounds are complete and safe to use.
    pub page_bounds_complete: u64,
    /// Indexed page entries whose min/max bounds are absent, inexact, or unsafe.
    pub page_bounds_incomplete: u64,
    /// Number of row ranges considered after taking the union of indexed page boundaries.
    pub row_ranges_considered: u64,
    /// Number of ranges proven unable to satisfy the predicates.
    pub row_ranges_skipped: u64,
    /// Rows covered by skipped ranges.
    pub rows_skipped: u64,
    /// Whole row groups omitted by page-index evidence.
    pub row_groups_skipped: u64,
    /// Per-predicate evidence explaining page-range decisions.
    pub predicate_diagnostics: PredicatePruningDiagnostics,
}

#[derive(Debug, Clone)]
struct PageColumnStatistics {
    starts: Vec<usize>,
    ends: Vec<usize>,
    pages: Vec<ColumnStatistics>,
}

/// Exact row range and conservative statistics read from one data-page header.
#[derive(Debug, Clone)]
pub struct PageHeaderRowStatistics {
    /// First row covered by the page, local to its row group.
    pub first_row: usize,
    /// Number of rows covered by the page.
    pub row_count: usize,
    /// Statistics decoded from the page header. Unsupported or absent values
    /// are represented as incomplete statistics and never prune the page.
    pub statistics: ColumnStatistics,
}

/// Header-derived page statistics keyed by row-group ordinal and source column.
pub type PageHeaderStatistics = HashMap<usize, HashMap<String, Vec<PageHeaderRowStatistics>>>;

impl PageColumnStatistics {
    fn page_covering(&self, start: usize, end: usize) -> Option<(usize, &ColumnStatistics)> {
        let page = self.starts.partition_point(|page_start| *page_start <= start).checked_sub(1)?;
        (self.ends.get(page).is_some_and(|page_end| *page_end >= end))
            .then(|| (page, &self.pages[page]))
    }
}

/// Applies format-neutral statistics predicates to exact statistics from the
/// footer currently used by the Parquet reader.
///
/// This reads only already-loaded footer values. It neither decodes row data nor
/// retains/copies any Arrow column payload. Unknown or incomplete bounds keep the
/// affected row group.
pub fn prune_row_groups(
    arrow_schema: &Schema,
    metadata: &ParquetMetaData,
    predicates: &[StatisticsPredicate],
) -> RowGroupPruning {
    prune_row_groups_in_range(
        arrow_schema,
        metadata,
        predicates,
        0..metadata.num_row_groups(),
    )
}

/// Applies the same conservative pruning proof to a bounded range of the
/// reader's data-unit ordinals. The scan scheduler uses this to avoid creating
/// work for units it can already prove cannot match; each worker repeats the
/// proof against its assigned range before asking the reader to decode it.
pub fn prune_row_groups_in_range(
    arrow_schema: &Schema,
    metadata: &ParquetMetaData,
    predicates: &[StatisticsPredicate],
    range: std::ops::Range<usize>,
) -> RowGroupPruning {
    let row_groups = metadata.row_groups();
    let start = range.start.min(row_groups.len());
    let end = range.end.min(row_groups.len()).max(start);
    if predicates.is_empty() {
        return RowGroupPruning {
            selected: (start..end).collect(),
            skipped_rows: 0,
            estimated_compressed_bytes_skipped: 0,
            predicate_diagnostics: PredicatePruningDiagnostics::default(),
        };
    }

    let mut columns = HashSet::<&str>::new();
    for predicate in predicates {
        predicate.collect_columns(&mut columns);
    }
    if columns.is_empty() {
        return RowGroupPruning {
            selected: (start..end).collect(),
            skipped_rows: 0,
            estimated_compressed_bytes_skipped: 0,
            predicate_diagnostics: PredicatePruningDiagnostics::for_predicates(predicates),
        };
    }

    let statistics = row_group_column_statistics(arrow_schema, metadata, &columns);
    let mut selected = Vec::with_capacity(end - start);
    let mut skipped_rows = 0_u64;
    let mut estimated_compressed_bytes_skipped = 0_u64;
    let mut predicate_diagnostics = PredicatePruningDiagnostics::for_predicates(predicates);

    for (ordinal, row_group) in row_groups.iter().enumerate().take(end).skip(start) {
        let mut unit = row_group_unit(ordinal, row_group);
        unit.column_statistics = statistics.get(ordinal).cloned().unwrap_or_default();
        let mut may_match = true;
        for (predicate_index, predicate) in predicates.iter().enumerate() {
            let predicate_may_match = may_match_data_unit(predicate, &unit);
            predicate_diagnostics.record(
                predicate_index,
                predicate,
                &unit,
                predicate_may_match,
            );
            may_match &= predicate_may_match;
        }
        if may_match {
            selected.push(ordinal);
        } else {
            skipped_rows = skipped_rows.saturating_add(unit.num_rows.unwrap_or(0));
            estimated_compressed_bytes_skipped = estimated_compressed_bytes_skipped
                .saturating_add(unit.compressed_bytes.unwrap_or(0));
        }
    }

    RowGroupPruning {
        selected,
        skipped_rows,
        estimated_compressed_bytes_skipped,
        predicate_diagnostics,
    }
}

/// Uses Parquet column and offset indexes to skip page row ranges that cannot
/// satisfy the supplied statistics predicates. Unsupported, absent, malformed,
/// or type-incompatible page metadata is ignored conservatively.
pub fn prune_data_pages(
    arrow_schema: &Schema,
    metadata: &ParquetMetaData,
    predicates: &[StatisticsPredicate],
    row_groups: &[usize],
) -> PagePruning {
    prune_data_pages_with_headers(
        arrow_schema,
        metadata,
        predicates,
        row_groups,
        &PageHeaderStatistics::new(),
    )
}

/// Applies the same conservative page-range proof using page-header statistics
/// where available and page-index statistics as a fallback. Header ranges are
/// supplied only after their row mapping has been validated by the scan reader.
pub fn prune_data_pages_with_headers(
    arrow_schema: &Schema,
    metadata: &ParquetMetaData,
    predicates: &[StatisticsPredicate],
    row_groups: &[usize],
    header_pages: &PageHeaderStatistics,
) -> PagePruning {
    let page_index = metadata.page_index().map(|page_index| page_index.as_ref());
    if predicates.is_empty() {
        return full_row_group_selections(row_groups);
    }

    let mut referenced_columns = HashSet::<&str>::new();
    for predicate in predicates {
        predicate.collect_columns(&mut referenced_columns);
    }
    if referenced_columns.is_empty() {
        return full_row_group_selections(row_groups);
    }

    let mut pruning = PagePruning::default();
    pruning.predicate_diagnostics = PredicatePruningDiagnostics::for_predicates(predicates);
    let row_group_metadata = metadata.row_groups();
    for row_group_index in row_groups {
        let Some(row_group) = row_group_metadata.get(*row_group_index) else {
            pruning
                .selections
                .push(RowGroupSelection::new(*row_group_index, None));
            continue;
        };
        let Some(row_count) = nonnegative(row_group.num_rows()).and_then(|rows| usize::try_from(rows).ok()) else {
            pruning
                .selections
                .push(RowGroupSelection::new(*row_group_index, None));
            continue;
        };

        let mut columns = HashMap::<String, PageColumnStatistics>::new();
        for column in &referenced_columns {
            let index_statistics = page_index.and_then(|page_index| {
                page_column_statistics(
                    arrow_schema,
                    metadata,
                    page_index,
                    row_group_index,
                    column,
                )
            });
            let header_statistics = header_pages
                .get(row_group_index)
                .and_then(|columns| columns.get(*column))
                .and_then(|pages| header_page_column_statistics(pages, row_count));
            if let Some(statistics) = merge_page_column_statistics(
                index_statistics,
                header_statistics,
            ) {
                pruning.indexed_columns = pruning.indexed_columns.saturating_add(1);
                let complete_bounds = statistics
                    .pages
                    .iter()
                    .filter(|page| page.min_max_complete == Some(true))
                    .count() as u64;
                pruning.page_bounds_complete = pruning
                    .page_bounds_complete
                    .saturating_add(complete_bounds);
                pruning.page_bounds_incomplete = pruning
                    .page_bounds_incomplete
                    .saturating_add((statistics.pages.len() as u64).saturating_sub(complete_bounds));
                columns.insert((*column).to_string(), statistics);
            }
        }

        if columns.is_empty() || row_count == 0 {
            pruning
                .selections
                .push(RowGroupSelection::new(*row_group_index, None));
            continue;
        }

        let mut boundaries = Vec::with_capacity(columns.values().map(|column| column.starts.len()).sum::<usize>() + 2);
        boundaries.extend([0, row_count]);
        for column in columns.values() {
            boundaries.extend(column.starts.iter().copied());
            boundaries.extend(column.ends.iter().copied());
        }
        boundaries.sort_unstable();
        boundaries.dedup();

        let mut selectors = Vec::with_capacity(boundaries.len().saturating_sub(1));
        let mut row_ranges_considered = 0_u64;
        let mut row_ranges_skipped = 0_u64;
        let mut rows_skipped = 0_u64;
        for range in boundaries.windows(2) {
            let (start, end) = (range[0], range[1]);
            let Some(range_rows) = end.checked_sub(start).filter(|rows| *rows > 0) else {
                continue;
            };
            let Some(mut unit) = page_range_unit(
                *row_group_index,
                start,
                end,
                &columns,
            ) else {
                for predicate_index in 0..predicates.len() {
                    pruning
                        .predicate_diagnostics
                        .record_missing_metadata(predicate_index);
                }
                append_selector(&mut selectors, RowSelector::select(range_rows));
                continue;
            };
            row_ranges_considered = row_ranges_considered.saturating_add(1);
            unit.num_rows = u64::try_from(range_rows).ok();
            let mut may_match = true;
            for (predicate_index, predicate) in predicates.iter().enumerate() {
                let predicate_may_match = may_match_data_unit(predicate, &unit);
                pruning.predicate_diagnostics.record(
                    predicate_index,
                    predicate,
                    &unit,
                    predicate_may_match,
                );
                may_match &= predicate_may_match;
            }
            if may_match {
                append_selector(&mut selectors, RowSelector::select(range_rows));
            } else {
                append_selector(&mut selectors, RowSelector::skip(range_rows));
                row_ranges_skipped = row_ranges_skipped.saturating_add(1);
                rows_skipped = rows_skipped.saturating_add(range_rows as u64);
            }
        }

        pruning.row_ranges_considered = pruning
            .row_ranges_considered
            .saturating_add(row_ranges_considered);
        pruning.row_ranges_skipped = pruning
            .row_ranges_skipped
            .saturating_add(row_ranges_skipped);
        pruning.rows_skipped = pruning.rows_skipped.saturating_add(rows_skipped);

        if rows_skipped == 0 {
            pruning
                .selections
                .push(RowGroupSelection::new(*row_group_index, None));
        } else if rows_skipped >= row_count as u64 {
            pruning.row_groups_skipped = pruning.row_groups_skipped.saturating_add(1);
        } else {
            pruning.selections.push(RowGroupSelection::new(
                *row_group_index,
                Some(RowSelection::from(selectors)),
            ));
        }
    }

    pruning
}

/// Returns predicate columns for a header pre-read when the page index does not
/// cover every candidate row-group/column pair with complete bounds.
pub fn page_header_columns_needed(
    predicates: &[StatisticsPredicate],
    row_groups_to_probe: &[usize],
    index_candidate_row_groups: &[usize],
    page_index_available: bool,
    page_index_pruning: &PagePruning,
) -> Vec<String> {
    page_header_columns_needed_with_audit(
        predicates,
        row_groups_to_probe,
        index_candidate_row_groups,
        page_index_available,
        page_index_pruning,
        false,
    )
}

/// Returns predicate columns for a header pre-read, optionally auditing even
/// when a complete page index is available. Audit mode enables comparing page
/// header statistics with page-index statistics on real files; it adds an
/// extra header read before the regular scan and should remain opt-in.
pub fn page_header_columns_needed_with_audit(
    predicates: &[StatisticsPredicate],
    row_groups_to_probe: &[usize],
    index_candidate_row_groups: &[usize],
    page_index_available: bool,
    page_index_pruning: &PagePruning,
    audit_complete_page_index: bool,
) -> Vec<String> {
    if predicates.is_empty() || row_groups_to_probe.is_empty() {
        return Vec::new();
    }

    let mut referenced = HashSet::<&str>::new();
    for predicate in predicates {
        predicate.collect_columns(&mut referenced);
    }
    if referenced.is_empty() {
        return Vec::new();
    }

    let expected_indexed_columns = u64::try_from(referenced.len())
        .unwrap_or(u64::MAX)
        .saturating_mul(u64::try_from(index_candidate_row_groups.len()).unwrap_or(u64::MAX));
    let page_index_covers_predicates = page_index_available
        && page_index_pruning.indexed_columns == expected_indexed_columns
        && page_index_pruning.page_bounds_incomplete == 0;
    if page_index_covers_predicates && !audit_complete_page_index {
        return Vec::new();
    }

    let mut needed = referenced.into_iter().map(str::to_owned).collect::<Vec<_>>();
    needed.sort_unstable();
    needed
}

fn full_row_group_selections(row_groups: &[usize]) -> PagePruning {
    PagePruning {
        selections: row_groups
            .iter()
            .map(|row_group| RowGroupSelection::new(*row_group, None))
            .collect(),
        ..PagePruning::default()
    }
}

fn page_column_statistics<'a>(
    arrow_schema: &'a Schema,
    metadata: &'a ParquetMetaData,
    page_index: &dyn parquet::file::metadata::page_index::PageIndexProvider,
    row_group_index: &'a usize,
    column_name: &str,
) -> Option<PageColumnStatistics> {
    let field = arrow_schema.column_with_name(column_name)?.1;
    let parquet_schema = metadata.file_metadata().schema_descr();
    let converter = StatisticsConverter::try_new(column_name, arrow_schema, parquet_schema).ok()?;
    let parquet_column = converter.parquet_column_index()?;
    let column_index = page_index.column_index(*row_group_index, parquet_column)?;
    let offset_index = page_index.offset_index(*row_group_index, parquet_column)?;
    let locations = offset_index.page_locations();
    if locations.is_empty() {
        return None;
    }

    let row_count = nonnegative(metadata.row_groups().get(*row_group_index)?.num_rows())
        .and_then(|rows| usize::try_from(rows).ok())?;
    let starts = locations
        .iter()
        .map(|location| usize::try_from(location.first_row_index).ok())
        .collect::<Option<Vec<_>>>()?;
    if starts.first() != Some(&0)
        || starts.iter().any(|start| *start >= row_count)
        || starts.windows(2).any(|pair| pair[0] >= pair[1])
    {
        return None;
    }
    let ends = starts
        .iter()
        .skip(1)
        .copied()
        .chain(std::iter::once(row_count))
        .collect::<Vec<_>>();

    let minima = converter
        .data_page_mins(page_index, std::iter::once(row_group_index))
        .ok()?;
    let maxima = converter
        .data_page_maxes(page_index, std::iter::once(row_group_index))
        .ok()?;
    if minima.len() != starts.len() || maxima.len() != starts.len() {
        return None;
    }
    let nan_counts = converter
        .data_page_nan_counts(page_index, std::iter::once(row_group_index))
        .ok();
    let floating = matches!(field.data_type(), DataType::Float32 | DataType::Float64);

    let pages = (0..starts.len())
        .map(|page| {
            let min_value = logical_literal_at(minima.as_ref(), page, field.data_type());
            let max_value = logical_literal_at(maxima.as_ref(), page, field.data_type());
            let nan_count = array_u64_at(nan_counts.as_ref(), page);
            let usable_bounds = min_value.is_some()
                && max_value.is_some()
                && (!floating || nan_count == Some(0));
            let null_count = column_index
                .null_count(page)
                .and_then(|count| u64::try_from(count).ok())
                .or_else(|| column_index.is_null_page(page).then_some((ends[page] - starts[page]) as u64));
            ColumnStatistics {
                null_count,
                nan_count,
                min_value: usable_bounds.then_some(min_value).flatten(),
                max_value: usable_bounds.then_some(max_value).flatten(),
                min_max_complete: Some(usable_bounds),
                ..ColumnStatistics::default()
            }
        })
        .collect();

    Some(PageColumnStatistics {
        starts,
        ends,
        pages,
    })
}

fn header_page_column_statistics(
    pages: &[PageHeaderRowStatistics],
    row_count: usize,
) -> Option<PageColumnStatistics> {
    if pages.is_empty() {
        return None;
    }
    let mut starts = Vec::with_capacity(pages.len());
    let mut ends = Vec::with_capacity(pages.len());
    let mut statistics = Vec::with_capacity(pages.len());
    let mut next_row = 0usize;
    for page in pages {
        if page.first_row != next_row || page.row_count == 0 {
            return None;
        }
        next_row = next_row.checked_add(page.row_count)?;
        if next_row > row_count {
            return None;
        }
        starts.push(page.first_row);
        ends.push(next_row);
        statistics.push(page.statistics.clone());
    }
    (next_row == row_count).then_some(PageColumnStatistics {
        starts,
        ends,
        pages: statistics,
    })
}

fn merge_page_column_statistics(
    index: Option<PageColumnStatistics>,
    headers: Option<PageColumnStatistics>,
) -> Option<PageColumnStatistics> {
    let (index, headers) = match (index, headers) {
        (Some(index), Some(headers)) => (index, headers),
        (Some(index), None) => return Some(index),
        (None, Some(headers)) => return Some(headers),
        (None, None) => return None,
    };
    let mut boundaries = Vec::with_capacity(
        index
            .starts
            .len()
            .saturating_add(headers.starts.len())
            .saturating_mul(2),
    );
    boundaries.extend(index.starts.iter().copied());
    boundaries.extend(index.ends.iter().copied());
    boundaries.extend(headers.starts.iter().copied());
    boundaries.extend(headers.ends.iter().copied());
    boundaries.sort_unstable();
    boundaries.dedup();

    let mut merged = PageColumnStatistics {
        starts: Vec::with_capacity(boundaries.len().saturating_sub(1)),
        ends: Vec::with_capacity(boundaries.len().saturating_sub(1)),
        pages: Vec::with_capacity(boundaries.len().saturating_sub(1)),
    };
    for range in boundaries.windows(2) {
        let (start, end) = (range[0], range[1]);
        if start == end {
            continue;
        }
        let header_stats = headers.page_covering(start, end).map(|(_, stats)| stats);
        let index_stats = index.page_covering(start, end).map(|(_, stats)| stats);
        let Some(statistics) = header_stats
            .filter(|statistics| statistics.min_max_complete == Some(true))
            .or(index_stats)
            .or(header_stats)
        else {
            continue;
        };
        merged.starts.push(start);
        merged.ends.push(end);
        merged.pages.push(statistics.clone());
    }
    (!merged.pages.is_empty()).then_some(merged)
}

/// Converts typed Parquet page-header statistics to Vein's format-neutral
/// statistics representation. Unsupported logical/physical combinations are
/// left incomplete so the common pruning proof fails open.
pub fn column_statistics_from_page_header(
    statistics: Option<&ParquetStatistics>,
    data_type: &DataType,
    physical_type: ParquetType,
) -> ColumnStatistics {
    let Some(statistics) = statistics else {
        return ColumnStatistics::default();
    };
    let nan_count = statistics.nan_count_opt();
    let ordered_bounds_are_safe = !matches!(data_type, DataType::Float32 | DataType::Float64)
        || nan_count == Some(0);
    // The deprecated byte-array ordering is not safe for logical unsigned or
    // binary/string values. Numeric signed and date encodings retain the same
    // order and may still use those legacy fields.
    let legacy_order_is_safe = !matches!(
        data_type,
        DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
    ) || !statistics.is_min_max_deprecated();
    let min_exact = statistics.min_is_exact() && ordered_bounds_are_safe && legacy_order_is_safe;
    let max_exact = statistics.max_is_exact() && ordered_bounds_are_safe && legacy_order_is_safe;
    let min_value = min_exact
        .then(|| statistics.min_bytes_opt())
        .flatten()
        .and_then(|bytes| page_statistic_literal(bytes, data_type, physical_type));
    let max_value = max_exact
        .then(|| statistics.max_bytes_opt())
        .flatten()
        .and_then(|bytes| page_statistic_literal(bytes, data_type, physical_type));
    let complete = min_value.is_some() && max_value.is_some();

    ColumnStatistics {
        null_count: statistics.null_count_opt(),
        nan_count,
        min_value,
        max_value,
        min_max_complete: Some(complete),
        ..ColumnStatistics::default()
    }
}

fn page_statistic_literal(
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

fn page_range_unit(
    ordinal: usize,
    start: usize,
    end: usize,
    columns: &HashMap<String, PageColumnStatistics>,
) -> Option<DataUnitStatistics> {
    let mut unit = DataUnitStatistics::new(StatisticsUnitKind::RowGroup, ordinal);
    for (name, column) in columns {
        let (page, source_statistics) = column.page_covering(start, end)?;
        let page_rows = column.ends[page].checked_sub(column.starts[page])? as u64;
        let range_rows = end.checked_sub(start)? as u64;
        let mut statistics = source_statistics.clone();
        statistics.null_count = match statistics.null_count {
            Some(0) => Some(0),
            Some(nulls) if nulls == page_rows => Some(range_rows),
            _ => None,
        };
        unit.column_statistics.insert(name.clone(), statistics);
    }
    Some(unit)
}

fn append_selector(selectors: &mut Vec<RowSelector>, selector: RowSelector) {
    if let Some(last) = selectors.last_mut()
        && last.skip == selector.skip
    {
        last.row_count = last.row_count.saturating_add(selector.row_count);
    } else {
        selectors.push(selector);
    }
}

/// Extracts only named columns' exact statistics for every row group.
pub fn row_group_column_statistics(
    arrow_schema: &Schema,
    metadata: &ParquetMetaData,
    columns: &HashSet<&str>,
) -> Vec<HashMap<String, ColumnStatistics>> {
    let mut units = metadata
        .row_groups()
        .iter()
        .enumerate()
        .map(|(ordinal, row_group)| row_group_unit(ordinal, row_group))
        .collect::<Vec<_>>();
    populate_column_statistics(arrow_schema, metadata, &mut units, Some(columns));
    units
        .into_iter()
        .map(|unit| unit.column_statistics)
        .collect()
}

/// Builds table-wide facts from file and row-group scopes.
///
/// Additive counts and byte sizes are published only when every contributing
/// unit has a known value and the checked sum succeeds. NDV is intentionally
/// omitted at table scope: summing row-group NDVs double-counts values shared
/// by groups, and Parquet does not provide the sketches needed to merge them.
pub fn table_statistics(
    data_units: Vec<DataUnitStatistics>,
    schema: &Schema,
) -> TableStatistics {
    let files = data_units
        .iter()
        .filter(|unit| unit.kind == StatisticsUnitKind::File && unit.parent.is_none())
        .collect::<Vec<_>>();
    let row_groups = data_units
        .iter()
        .filter(|unit| unit.kind == StatisticsUnitKind::RowGroup)
        .collect::<Vec<_>>();

    let mut column_statistics = HashMap::new();
    for field in schema.fields() {
        let summary = summarize_column(&row_groups, field.name(), field.is_nullable());
        if has_values(&summary) {
            column_statistics.insert(field.name().clone(), summary);
        }
    }

    TableStatistics {
        num_rows: complete_sum(files.iter().map(|unit| unit.num_rows)),
        total_bytes: complete_sum(files.iter().map(|unit| unit.physical_bytes)),
        file_count: u64::try_from(files.len()).ok(),
        row_group_count: u64::try_from(row_groups.len()).ok(),
        compressed_bytes: complete_sum(row_groups.iter().map(|unit| unit.compressed_bytes)),
        uncompressed_bytes: complete_sum(row_groups.iter().map(|unit| unit.uncompressed_bytes)),
        column_statistics,
        column_groups: Vec::new(),
        data_units,
    }
}

fn row_group_unit(ordinal: usize, row_group: &RowGroupMetaData) -> DataUnitStatistics {
    let mut unit = DataUnitStatistics::new(StatisticsUnitKind::RowGroup, ordinal);
    unit.num_rows = nonnegative(row_group.num_rows());
    unit.compressed_bytes = sum_column_bytes(row_group, ColumnChunkMetaData::compressed_size);
    unit.uncompressed_bytes = sum_column_bytes(row_group, ColumnChunkMetaData::uncompressed_size);
    unit
}

fn sum_column_bytes(
    row_group: &RowGroupMetaData,
    size: fn(&ColumnChunkMetaData) -> i64,
) -> Option<u64> {
    let mut total = 0_u64;
    for column in row_group.columns() {
        total = total.checked_add(nonnegative(size(column))?)?;
    }
    Some(total)
}

fn populate_column_statistics(
    schema: &Schema,
    metadata: &ParquetMetaData,
    row_group_units: &mut [DataUnitStatistics],
    selected_columns: Option<&HashSet<&str>>,
) {
    let row_groups = metadata.row_groups();
    let parquet_schema = metadata.file_metadata().schema_descr();

    for field in schema.fields() {
        if selected_columns.is_some_and(|columns| !columns.contains(field.name().as_str())) {
            continue;
        }
        let Ok(converter) = StatisticsConverter::try_new(
            field.name(),
            schema,
            parquet_schema,
        ) else {
            // Top-level statistics for nested or otherwise unsupported Arrow
            // fields are optional metadata. Their absence must not fail scans.
            continue;
        };
        let converter = converter.with_missing_null_counts_as_zero(false);
        let Some(column_index) = converter.parquet_column_index() else {
            continue;
        };
        if row_groups
            .iter()
            .any(|row_group| column_index >= row_group.columns().len())
        {
            continue;
        }

        let min_values = converter.row_group_mins(row_groups.iter()).ok();
        let max_values = converter.row_group_maxes(row_groups.iter()).ok();
        let min_exact = converter
            .row_group_is_min_value_exact(row_groups.iter())
            .ok();
        let max_exact = converter
            .row_group_is_max_value_exact(row_groups.iter())
            .ok();
        let null_counts = converter.row_group_null_counts(row_groups.iter()).ok();
        let distinct_counts = converter.row_group_distinct_counts(row_groups.iter()).ok();
        let nan_counts = converter.row_group_nan_counts(row_groups.iter()).ok();

        for (ordinal, row_group) in row_groups.iter().enumerate() {
            let Some(unit) = row_group_units.get_mut(ordinal) else {
                continue;
            };
            let chunk = row_group.column(column_index);
            let null_count = array_u64_at(null_counts.as_ref(), ordinal).or_else(|| {
                (!field.is_nullable()).then_some(0)
            });
            let min_value = exact_scalar_at(
                min_values.as_deref(),
                min_exact.as_ref(),
                ordinal,
                field.data_type(),
            );
            let max_value = exact_scalar_at(
                max_values.as_deref(),
                max_exact.as_ref(),
                ordinal,
                field.data_type(),
            );
            let min_max_complete = Some(min_value.is_some() && max_value.is_some());
            let statistics = ColumnStatistics {
                null_count,
                distinct_count: array_u64_at(distinct_counts.as_ref(), ordinal),
                nan_count: array_u64_at(nan_counts.as_ref(), ordinal),
                min_value,
                max_value,
                min_max_complete,
                ..ColumnStatistics::default()
            };
            if has_values(&statistics) {
                unit.column_statistics
                    .insert(field.name().clone(), statistics);
            }
            unit.column_storage
                .insert(field.name().clone(), column_storage_statistics(chunk));
        }
    }
}

fn column_storage_statistics(chunk: &ColumnChunkMetaData) -> ColumnStorageStatistics {
    ColumnStorageStatistics {
        num_values: nonnegative(chunk.num_values()),
        compressed_bytes: nonnegative(chunk.compressed_size()),
        uncompressed_bytes: nonnegative(chunk.uncompressed_size()),
        data_page_offset: nonnegative(chunk.data_page_offset()),
        dictionary_page_offset: chunk.dictionary_page_offset().and_then(nonnegative),
        index_page_offset: chunk.index_page_offset().and_then(nonnegative),
        bloom_filter_offset: chunk.bloom_filter_offset().and_then(nonnegative),
        column_index_offset: chunk.column_index_offset().and_then(nonnegative),
        offset_index_offset: chunk.offset_index_offset().and_then(nonnegative),
        unencoded_byte_array_bytes: chunk
            .unencoded_byte_array_data_bytes()
            .and_then(nonnegative),
        compression: Some(format!("{:?}", chunk.compression_codec())),
        encodings: chunk.encodings().map(|encoding| format!("{encoding:?}")).collect(),
        has_statistics: Some(chunk.statistics().is_some()),
        has_dictionary_page: Some(chunk.dictionary_page_offset().is_some()),
        has_bloom_filter: Some(chunk.bloom_filter_offset().is_some()),
        has_column_index: Some(chunk.column_index_offset().is_some()),
        has_offset_index: Some(chunk.offset_index_offset().is_some()),
        repetition_level_histogram: chunk
            .repetition_level_histogram()
            .map(|histogram| histogram.values().to_vec()),
        definition_level_histogram: chunk
            .definition_level_histogram()
            .map(|histogram| histogram.values().to_vec()),
    }
}

fn exact_scalar_at(
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

fn logical_literal_at(
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
            .map(|array| Literal::Int64(i64::from(array.value(index)))),
        DataType::UInt16 => values
            .as_any()
            .downcast_ref::<UInt16Array>()
            .map(|array| Literal::Int64(i64::from(array.value(index)))),
        DataType::UInt32 => values
            .as_any()
            .downcast_ref::<UInt32Array>()
            .map(|array| Literal::Int64(i64::from(array.value(index)))),
        DataType::UInt64 => values
            .as_any()
            .downcast_ref::<UInt64Array>()
            .and_then(|array| i64::try_from(array.value(index)).ok())
            .map(Literal::Int64),
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

fn to_micros(value: i64, unit: TimeUnit) -> Option<i64> {
    match unit {
        TimeUnit::Second => value.checked_mul(1_000_000),
        TimeUnit::Millisecond => value.checked_mul(1_000),
        TimeUnit::Microsecond => Some(value),
        TimeUnit::Nanosecond if value % 1_000 == 0 => Some(value / 1_000),
        TimeUnit::Nanosecond => None,
    }
}

fn date64_as_days(milliseconds: i64) -> Option<i32> {
    const MILLIS_PER_DAY: i64 = 86_400_000;
    (milliseconds % MILLIS_PER_DAY == 0)
        .then(|| i32::try_from(milliseconds / MILLIS_PER_DAY).ok())
        .flatten()
}

fn array_u64_at(values: Option<&arrow::array::UInt64Array>, index: usize) -> Option<u64> {
    let values = values?;
    (!values.is_null(index)).then(|| values.value(index))
}

fn has_values(statistics: &ColumnStatistics) -> bool {
    statistics.null_count.is_some()
        || statistics.distinct_count.is_some()
        || statistics.nan_count.is_some()
        || statistics.min_value.is_some()
        || statistics.max_value.is_some()
}

fn summarize_column(
    row_groups: &[&DataUnitStatistics],
    column_name: &str,
    nullable: bool,
) -> ColumnStatistics {
    if row_groups.is_empty() {
        return ColumnStatistics::default();
    }
    let mut null_count = Some(0_u64);
    let mut nan_count = Some(0_u64);
    let mut min_value: Option<LogicalLiteral> = None;
    let mut max_value: Option<LogicalLiteral> = None;
    let mut min_complete = true;
    let mut max_complete = true;
    let mut saw_non_null_values = false;
    let mut saw_nan_statistics = false;

    for unit in row_groups {
        let Some(rows) = unit.num_rows else {
            null_count = None;
            nan_count = None;
            min_complete = false;
            max_complete = false;
            continue;
        };
        if rows == 0 {
            continue;
        }
        let statistics = unit.column_statistics.get(column_name);
        let Some(statistics) = statistics else {
            null_count = None;
            nan_count = None;
            min_complete = false;
            max_complete = false;
            continue;
        };

        match statistics.null_count {
            Some(count) if count <= rows => {
                null_count = null_count.and_then(|total| total.checked_add(count));
            }
            _ => null_count = None,
        }
        if let Some(count) = statistics.nan_count {
            saw_nan_statistics = true;
            nan_count = nan_count.and_then(|total| total.checked_add(count));
        } else {
            nan_count = None;
        }
        let all_null = statistics.null_count == Some(rows);
        if all_null {
            continue;
        }
        saw_non_null_values = true;
        if let (Some(min), Some(max)) = (&statistics.min_value, &statistics.max_value) {
            if min.try_cmp(max).is_none_or(|order| order.is_gt()) {
                min_complete = false;
                max_complete = false;
                continue;
            }
        }
        match &statistics.min_value {
            Some(value) => match &min_value {
                Some(current) => match value.try_cmp(current) {
                    Some(std::cmp::Ordering::Less) => min_value = Some(value.clone()),
                    Some(_) => {}
                    None => min_complete = false,
                },
                None => min_value = Some(value.clone()),
            },
            None => min_complete = false,
        }
        match &statistics.max_value {
            Some(value) => match &max_value {
                Some(current) => match value.try_cmp(current) {
                    Some(std::cmp::Ordering::Greater) => max_value = Some(value.clone()),
                    Some(_) => {}
                    None => max_complete = false,
                },
                None => max_value = Some(value.clone()),
            },
            None => max_complete = false,
        }
    }

    ColumnStatistics {
        null_count: if nullable { null_count } else { Some(0) },
        // Distinct counts are unit-local. A mergeable sketch is required to
        // produce a table NDV when values overlap across units.
        distinct_count: None,
        nan_count: saw_nan_statistics.then_some(nan_count).flatten(),
        min_value: (min_complete && saw_non_null_values)
            .then_some(min_value)
            .flatten(),
        max_value: (max_complete && saw_non_null_values)
            .then_some(max_value)
            .flatten(),
        ..ColumnStatistics::default()
    }
}

fn complete_sum(values: impl IntoIterator<Item=Option<u64>>) -> Option<u64> {
    let mut total = 0_u64;
    let mut saw_value = false;
    for value in values {
        total = total.checked_add(value?)?;
        saw_value = true;
    }
    saw_value.then_some(total)
}

fn nonnegative(value: i64) -> Option<u64> {
    u64::try_from(value).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::Field;
    use arrow::record_batch::RecordBatch;
    use catalog::{StatisticsComparison, StatisticsPredicate};
    use std::sync::Arc;
    use parquet::arrow::{
        ArrowWriter,
        arrow_reader::ParquetRecordBatchReaderBuilder,
    };
    use parquet::file::metadata::{PageIndexPolicy, ParquetMetaDataReader};
    use parquet::file::properties::WriterProperties;

    fn file_unit(rows: u64, bytes: u64) -> DataUnitStatistics {
        let mut unit = DataUnitStatistics::new(StatisticsUnitKind::File, 0);
        unit.num_rows = Some(rows);
        unit.physical_bytes = Some(bytes);
        unit
    }

    fn row_group(
        ordinal: usize,
        rows: u64,
        column: ColumnStatistics,
    ) -> DataUnitStatistics {
        let mut unit = DataUnitStatistics::new(StatisticsUnitKind::RowGroup, ordinal);
        unit.parent = Some(0);
        unit.num_rows = Some(rows);
        unit.compressed_bytes = Some(10);
        unit.uncompressed_bytes = Some(20);
        unit.column_statistics.insert("value".into(), column);
        unit
    }

    #[test]
    fn pruning_diagnostics_separate_proven_pruning_from_retained_reasons() {
        let predicate = equality("value", 5);
        let mut diagnostics = PredicatePruningDiagnostics::for_predicates(&[
            predicate.clone(),
            StatisticsPredicate::Unknown,
        ]);

        let complete_overlap = row_group(
            0,
            4,
            ColumnStatistics {
                null_count: Some(0),
                min_value: Some(LogicalLiteral::Int64(1)),
                max_value: Some(LogicalLiteral::Int64(9)),
                min_max_complete: Some(true),
                ..ColumnStatistics::default()
            },
        );
        diagnostics.record(0, &predicate, &complete_overlap, true);

        let missing = DataUnitStatistics::new(StatisticsUnitKind::RowGroup, 1);
        diagnostics.record(0, &predicate, &missing, true);
        diagnostics.record_missing_metadata(0);

        let incomplete = row_group(
            2,
            4,
            ColumnStatistics {
                min_value: Some(LogicalLiteral::Int64(1)),
                max_value: Some(LogicalLiteral::Int64(9)),
                min_max_complete: Some(false),
                ..ColumnStatistics::default()
            },
        );
        diagnostics.record(0, &predicate, &incomplete, true);

        let disjoint = row_group(
            3,
            4,
            ColumnStatistics {
                null_count: Some(0),
                min_value: Some(LogicalLiteral::Int64(10)),
                max_value: Some(LogicalLiteral::Int64(20)),
                min_max_complete: Some(true),
                ..ColumnStatistics::default()
            },
        );
        diagnostics.record(0, &predicate, &disjoint, false);
        diagnostics.record(1, &StatisticsPredicate::Unknown, &missing, true);

        let predicate_diagnostic = &diagnostics.predicates[0];
        assert_eq!(predicate_diagnostic.units_considered, 5);
        assert_eq!(predicate_diagnostic.units_pruned, 1);
        assert_eq!(predicate_diagnostic.retained_missing_statistics, 2);
        assert_eq!(predicate_diagnostic.retained_incomplete_bounds, 1);
        assert_eq!(predicate_diagnostic.retained_bounds_may_match, 1);
        assert_eq!(diagnostics.predicates[1].retained_unsupported, 1);
    }

    #[test]
    fn pruning_diagnostic_labels_are_bounded() {
        let predicate = StatisticsPredicate::Comparison {
            column: "value".into(),
            comparison: StatisticsComparison::Equal,
            literal: LogicalLiteral::String("x".repeat(512)),
        };

        let diagnostics = PredicatePruningDiagnostics::for_predicates(&[predicate]);

        assert!(diagnostics.predicates[0].predicate.chars().count() <= 163);
        assert!(diagnostics.predicates[0].predicate.ends_with("..."));
    }

    #[test]
    fn table_rollup_preserves_scope_and_only_sums_mergeable_statistics() {
        let first = ColumnStatistics {
            null_count: Some(0),
            distinct_count: Some(2),
            nan_count: Some(0),
            min_value: Some(LogicalLiteral::Int64(1)),
            max_value: Some(LogicalLiteral::Int64(4)),
            ..ColumnStatistics::default()
        };
        let second = ColumnStatistics {
            null_count: Some(1),
            distinct_count: Some(2),
            nan_count: Some(0),
            min_value: Some(LogicalLiteral::Int64(6)),
            max_value: Some(LogicalLiteral::Int64(9)),
            ..ColumnStatistics::default()
        };
        let stats = table_statistics(
            vec![file_unit(4, 100), row_group(0, 2, first), row_group(1, 2, second)],
            &Schema::new(vec![Field::new("value", DataType::Int64, true)]),
        );

        assert_eq!(stats.num_rows, Some(4));
        assert_eq!(stats.total_bytes, Some(100));
        assert_eq!(stats.file_count, Some(1));
        assert_eq!(stats.row_group_count, Some(2));
        assert_eq!(stats.compressed_bytes, Some(20));
        assert_eq!(stats.uncompressed_bytes, Some(40));
        let column = &stats.column_statistics["value"];
        assert_eq!(column.null_count, Some(1));
        assert_eq!(column.distinct_count, None, "local NDVs must never be added");
        assert_eq!(column.min_value, Some(LogicalLiteral::Int64(1)));
        assert_eq!(column.max_value, Some(LogicalLiteral::Int64(9)));
        assert_eq!(stats.data_units.len(), 3);
        assert_eq!(stats.data_units[1].parent, Some(0));
    }

    #[test]
    fn table_bounds_and_null_counts_stay_unknown_when_group_stats_are_partial() {
        let first = ColumnStatistics {
            null_count: Some(0),
            min_value: Some(LogicalLiteral::Int64(1)),
            max_value: Some(LogicalLiteral::Int64(4)),
            ..ColumnStatistics::default()
        };
        let second = ColumnStatistics {
            null_count: None,
            min_value: None,
            max_value: Some(LogicalLiteral::Int64(9)),
            ..ColumnStatistics::default()
        };
        let stats = table_statistics(
            vec![file_unit(4, 100), row_group(0, 2, first), row_group(1, 2, second)],
            &Schema::new(vec![Field::new("value", DataType::Int64, true)]),
        );
        let column = &stats.column_statistics["value"];
        assert_eq!(column.null_count, None);
        assert_eq!(
            column.min_value,
            None,
            "an unknown group minimum prevents a table minimum"
        );
        assert_eq!(column.max_value, Some(LogicalLiteral::Int64(9)));
    }

    #[test]
    fn checked_rollups_reject_overflow_and_empty_inputs() {
        assert_eq!(complete_sum([Some(u64::MAX), Some(1)]), None);
        assert_eq!(complete_sum([Some(1), None]), None);
        assert_eq!(complete_sum([]), None);
    }

    #[test]
    fn inexact_row_group_bounds_are_not_used_for_planning() {
        let values = Int64Array::from(vec![Some(3)]);
        let exact = BooleanArray::from(vec![Some(false)]);
        assert_eq!(
            exact_scalar_at(Some(&values), Some(&exact), 0, &DataType::Int64),
            None
        );
    }

    #[test]
    fn exact_footer_bounds_skip_row_groups_and_reader_returns_same_matching_rows() {
        let root = tempfile::tempdir().expect("temporary root");
        let path = root.path().join("ordered.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Int64, false)]));
        let values = Int64Array::from(vec![1, 2, 10, 11, 20, 21]);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(values)])
            .expect("valid input batch");
        let file = std::fs::File::create(&path).expect("create parquet file");
        let mut writer = ArrowWriter::try_new(file, schema, None).expect("create writer");
        for offset in (0..batch.num_rows()).step_by(2) {
            writer
                .write(&batch.slice(offset, 2))
                .expect("write row group");
            writer.flush().expect("finish row group");
        }
        writer.close().expect("finish parquet file");

        let file = std::fs::File::open(&path).expect("open parquet file");
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).expect("read footer");
        let predicate = [
            StatisticsPredicate::Comparison {
                column: "value".into(),
                comparison: StatisticsComparison::GreaterOrEqual,
                literal: LogicalLiteral::Int64(10),
            },
            StatisticsPredicate::Comparison {
                column: "value".into(),
                comparison: StatisticsComparison::Less,
                literal: LogicalLiteral::Int64(20),
            },
        ];
        let pruning = prune_row_groups(builder.schema(), builder.metadata(), &predicate);

        assert_eq!(pruning.selected, vec![1]);
        assert_eq!(pruning.skipped_rows, 4);
        assert!(pruning.estimated_compressed_bytes_skipped > 0);

        let reader = builder
            .with_row_groups(pruning.selected)
            .build()
            .expect("build reader over selected groups");
        let selected_rows = reader
            .map(|batch| {
                let batch = batch.expect("read selected row group");
                let values = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("int64 output");
                (0..values.len()).map(|row| values.value(row)).collect::<Vec<_>>()
            })
            .flatten()
            .collect::<Vec<_>>();
        assert_eq!(selected_rows, vec![10, 11]);
    }

    #[test]
    fn page_indexes_select_only_possible_rows_and_missing_indexes_fail_open() {
        let root = tempfile::tempdir().expect("temporary root");
        let path = root.path().join("page_index.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Int64, false)]));
        let values = Int64Array::from_iter_values(0..1024);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(values)])
            .expect("valid input batch");
        let file = std::fs::File::create(&path).expect("create Parquet file");
        let properties = WriterProperties::builder()
            .set_write_batch_size(64)
            .set_data_page_row_count_limit(64)
            .build();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(properties))
            .expect("create indexed writer");
        writer.write(&batch).expect("write test rows");
        writer.close().expect("finish indexed file");

        let file = std::fs::File::open(&path).expect("open Parquet file");
        let metadata = ParquetMetaDataReader::new()
            .with_page_index_policy(PageIndexPolicy::Optional)
            .parse_and_finish(&file)
            .expect("read page indexes");
        assert!(metadata.page_index().is_some(), "writer should emit page indexes");

        let predicates = [
            StatisticsPredicate::Comparison {
                column: "value".into(),
                comparison: StatisticsComparison::GreaterOrEqual,
                literal: LogicalLiteral::Int64(900),
            },
            StatisticsPredicate::Comparison {
                column: "value".into(),
                comparison: StatisticsComparison::Less,
                literal: LogicalLiteral::Int64(1000),
            },
        ];
        let pruning = prune_data_pages(&schema, &metadata, &predicates, &[0]);
        assert!(pruning.indexed_columns > 0);
        assert!(pruning.page_bounds_complete > 0);
        assert!(
            pruning.page_bounds_complete + pruning.page_bounds_incomplete > 0,
            "available indexes must account for each page's bound quality"
        );
        assert!(pruning.row_ranges_considered > 1);
        assert!(pruning.row_ranges_skipped > 0);
        assert!(pruning.rows_skipped > 0);
        assert_eq!(pruning.row_groups_skipped, 0);
        let selection = pruning.selections[0]
            .selection()
            .cloned()
            .expect("at least some pages should be skipped");

        // Applying the generated selection before the row predicate must retain
        // every true row even when page boundaries do not align with constants.
        let file = std::fs::File::open(&path).expect("reopen Parquet file");
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .expect("build ordinary reader")
            .with_row_selection(selection)
            .build()
            .expect("apply page-derived row selection");
        let selected_rows = reader
            .map(|batch| {
                let batch = batch.expect("read selected page ranges");
                let values = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("int64 value output");
                (0..values.len())
                    .map(|row| values.value(row))
                    .filter(|value| (900..1000).contains(value))
                    .collect::<Vec<_>>()
            })
            .flatten()
            .collect::<Vec<_>>();
        assert_eq!(selected_rows, (900..1000).collect::<Vec<_>>());

        let file = std::fs::File::open(&path).expect("reopen Parquet file");
        let metadata_without_indexes = ParquetMetaDataReader::new()
            .with_page_index_policy(PageIndexPolicy::Skip)
            .parse_and_finish(&file)
            .expect("read footer without page indexes");
        let fallback = prune_data_pages(&schema, &metadata_without_indexes, &predicates, &[0]);
        assert_eq!(fallback.rows_skipped, 0);
        assert_eq!(fallback.page_bounds_complete, 0);
        assert_eq!(fallback.page_bounds_incomplete, 0);
        assert_eq!(fallback.selections.len(), 1);
        assert!(fallback.selections[0].selection().is_none());
    }

    fn int64_bloom_filter(name: &str, values: impl IntoIterator<Item=i64>) -> ParquetBloomFilter {
        let mut filter = Sbbf::new_with_num_of_bytes(4096);
        for value in values {
            filter.insert(&value);
        }
        ParquetBloomFilter::new(
            BloomFilterColumn {
                name: name.to_string(),
                parquet_column_index: 0,
                arrow_type: DataType::Int64,
                parquet_type: ParquetType::INT64,
                type_length: -1,
            },
            filter,
        )
    }

    fn dictionary_equality(column: &str, literal: LogicalLiteral) -> StatisticsPredicate {
        StatisticsPredicate::Comparison {
            column: column.to_string(),
            comparison: StatisticsComparison::Equal,
            literal,
        }
    }

    fn write_dictionary_row_groups(path: &std::path::Path) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("shipmode", DataType::Utf8, false),
            Field::new("id", DataType::Int64, false),
        ]));
        let properties = WriterProperties::builder()
            .set_dictionary_enabled(true)
            .set_max_row_group_row_count(Some(2))
            .build();
        let file = std::fs::File::create(path).expect("create dictionary Parquet file");
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(properties))
            .expect("create dictionary writer");
        for (modes, ids) in [
            (vec!["MAIL", "RAIL"], vec![10, 20]),
            (vec!["SHIP", "AIR"], vec![30, 40]),
        ] {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(modes)),
                    Arc::new(Int64Array::from(ids)),
                ],
            )
                .expect("valid dictionary test batch");
            writer.write(&batch).expect("write dictionary test rows");
            writer.flush().expect("finish dictionary row group");
        }
        writer.close().expect("finish dictionary Parquet file");
    }

    fn dictionary_page_bytes(
        file_bytes: &Bytes,
        file_length: u64,
        column: &ColumnChunkMetaData,
    ) -> Bytes {
        let range = dictionary_page_range(column, file_length)
            .expect("dictionary page range must be available");
        let start = usize::try_from(range.start).expect("range start fits usize");
        let end = usize::try_from(range.end).expect("range end fits usize");
        file_bytes.slice(start..end)
    }

    #[test]
    fn dictionary_predicates_extract_equality_and_safe_in_lists_only() {
        let equality = dictionary_equality("shipmode", LogicalLiteral::String("MAIL".into()));
        let in_list = StatisticsPredicate::Or(vec![
            dictionary_equality("shipmode", LogicalLiteral::String("MAIL".into())),
            dictionary_equality("shipmode", LogicalLiteral::String("SHIP".into())),
        ]);
        let mixed_or = StatisticsPredicate::Or(vec![
            equality.clone(),
            StatisticsPredicate::Unknown,
        ]);
        let other_column = StatisticsPredicate::Or(vec![
            equality.clone(),
            dictionary_equality("other", LogicalLiteral::String("SHIP".into())),
        ]);
        let predicates = dictionary_predicates(&[
            StatisticsPredicate::And(vec![equality, in_list]),
            mixed_or,
            other_column,
        ]);

        assert_eq!(predicates.len(), 2);
        assert_eq!(predicates[0].index, 0);
        assert_eq!(predicates[0].candidates, vec![LogicalLiteral::String("MAIL".into())]);
        assert_eq!(predicates[1].index, 1);
        assert_eq!(predicates[1].candidates.len(), 2);
    }

    #[test]
    fn dictionary_pages_prune_absent_values_and_keep_present_values() {
        let root = tempfile::tempdir().expect("temporary root");
        let path = root.path().join("dictionary_filter.parquet");
        write_dictionary_row_groups(&path);

        let bytes = Bytes::from(std::fs::read(&path).expect("read test Parquet bytes"));
        let file_length = bytes.len() as u64;
        let file = std::fs::File::open(&path).expect("open dictionary Parquet file");
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).expect("read Parquet footer");
        assert_eq!(builder.metadata().num_row_groups(), 2);

        let predicate = dictionary_equality("shipmode", LogicalLiteral::String("SHIP".into()));
        let probes = dictionary_predicates(std::slice::from_ref(&predicate));
        let columns = bloom_filter_columns(
            builder.schema().as_ref(),
            builder.metadata(),
            std::slice::from_ref(&predicate),
        );
        let column = columns
            .iter()
            .find(|column| column.name == "shipmode")
            .expect("resolve dictionary column");

        let first_chunk = builder.metadata().row_group(0).column(0);
        let second_chunk = builder.metadata().row_group(1).column(0);
        assert!(is_fully_dictionary_encoded(first_chunk));
        assert!(is_fully_dictionary_encoded(second_chunk));
        let first_matches = dictionary_page_predicate_matches(
            dictionary_page_bytes(&bytes, file_length, first_chunk),
            file_length,
            2,
            first_chunk,
            column,
            &probes,
        );
        let second_matches = dictionary_page_predicate_matches(
            dictionary_page_bytes(&bytes, file_length, second_chunk),
            file_length,
            2,
            second_chunk,
            column,
            &probes,
        );
        assert_eq!(first_matches, vec![Some(false)]);
        assert_eq!(second_matches, vec![Some(true)]);

        let id_predicate = dictionary_equality("id", LogicalLiteral::Int64(30));
        let id_probes = dictionary_predicates(std::slice::from_ref(&id_predicate));
        let id_column = bloom_filter_columns(
            builder.schema().as_ref(),
            builder.metadata(),
            std::slice::from_ref(&id_predicate),
        )
            .into_iter()
            .next()
            .expect("resolve Int64 dictionary column");
        let first_id_chunk = builder.metadata().row_group(0).column(1);
        let second_id_chunk = builder.metadata().row_group(1).column(1);
        assert_eq!(
            dictionary_page_predicate_matches(
                dictionary_page_bytes(&bytes, file_length, first_id_chunk),
                file_length,
                2,
                first_id_chunk,
                &id_column,
                &id_probes,
            ),
            vec![Some(false)]
        );
        assert_eq!(
            dictionary_page_predicate_matches(
                dictionary_page_bytes(&bytes, file_length, second_id_chunk),
                file_length,
                2,
                second_id_chunk,
                &id_column,
                &id_probes,
            ),
            vec![Some(true)]
        );

        let incompatible_predicate =
            dictionary_equality("shipmode", LogicalLiteral::Int64(30));
        let incompatible_probes =
            dictionary_predicates(std::slice::from_ref(&incompatible_predicate));
        assert_eq!(
            dictionary_page_predicate_matches(
                dictionary_page_bytes(&bytes, file_length, first_chunk),
                file_length,
                2,
                first_chunk,
                column,
                &incompatible_probes,
            ),
            vec![None],
            "a type mismatch must retain the row group"
        );

        let membership = HashMap::from([
            (0, first_matches),
            (1, second_matches),
        ]);
        let pruning = prune_row_groups_with_dictionary(
            builder.metadata(),
            vec![
                RowGroupSelection::new(0, None),
                RowGroupSelection::new(1, None),
            ],
            &membership,
        );
        assert_eq!(pruning.row_groups_skipped, 1);
        assert_eq!(pruning.rows_skipped, 2);
        assert_eq!(pruning.probes, 2);
        assert_eq!(pruning.selections.len(), 1);
        assert_eq!(pruning.selections[0].row_group_index(), 1);
    }

    #[test]
    fn dictionary_page_pruning_fails_open_for_chunks_without_dictionary_encoding() {
        let root = tempfile::tempdir().expect("temporary root");
        let path = root.path().join("plain_column.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new("shipmode", DataType::Utf8, false)]));
        let properties = WriterProperties::builder()
            .set_dictionary_enabled(false)
            .build();
        let file = std::fs::File::create(&path).expect("create plain Parquet file");
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(properties))
            .expect("create plain writer");
        writer
            .write(
                &RecordBatch::try_new(
                    schema,
                    vec![Arc::new(StringArray::from(vec!["MAIL", "RAIL"]))],
                )
                    .expect("valid plain batch"),
            )
            .expect("write plain values");
        writer.close().expect("finish plain Parquet file");
        let file = std::fs::File::open(&path).expect("open plain Parquet file");
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).expect("read plain footer");
        let chunk = builder.metadata().row_group(0).column(0);

        assert!(!is_fully_dictionary_encoded(chunk));
        assert!(dictionary_page_range(chunk, 1024).is_none());
    }

    fn equality(column: &str, value: i64) -> StatisticsPredicate {
        StatisticsPredicate::Comparison {
            column: column.to_string(),
            comparison: StatisticsComparison::Equal,
            literal: LogicalLiteral::Int64(value),
        }
    }

    #[test]
    fn complete_page_index_coverage_skips_header_pre_read() {
        let predicates = [
            equality("shipmode", 1),
            StatisticsPredicate::And(vec![
                equality("receiptdate", 2),
                equality("shipmode", 3),
            ]),
        ];
        let page_index = PagePruning {
            indexed_columns: 4,
            page_bounds_complete: 8,
            ..PagePruning::default()
        };

        assert_eq!(
            page_header_columns_needed(&predicates, &[0, 1], &[0, 1], true, &page_index),
            Vec::<String>::new()
        );
    }

    #[test]
    fn audit_mode_requests_predicate_headers_with_complete_page_index_coverage() {
        let predicates = [
            equality("shipmode", 1),
            StatisticsPredicate::And(vec![
                equality("receiptdate", 2),
                equality("shipmode", 3),
            ]),
        ];
        let complete_index = PagePruning {
            indexed_columns: 4,
            page_bounds_complete: 8,
            ..PagePruning::default()
        };

        assert_eq!(
            page_header_columns_needed_with_audit(
                &predicates,
                &[0, 1],
                &[0, 1],
                true,
                &complete_index,
                true,
            ),
            vec!["receiptdate", "shipmode"]
        );
    }

    #[test]
    fn incomplete_or_missing_page_index_coverage_requests_predicate_headers() {
        let predicates = [
            equality("shipmode", 1),
            StatisticsPredicate::And(vec![
                equality("receiptdate", 2),
                equality("shipmode", 3),
            ]),
        ];
        let expected = vec!["receiptdate", "shipmode"];
        let incomplete_index = PagePruning {
            indexed_columns: 4,
            page_bounds_incomplete: 1,
            ..PagePruning::default()
        };
        let missing_column_index = PagePruning {
            indexed_columns: 3,
            ..PagePruning::default()
        };

        assert_eq!(
            page_header_columns_needed(&predicates, &[1], &[0, 1], true, &incomplete_index),
            expected
        );
        assert_eq!(
            page_header_columns_needed(
                &predicates,
                &[1],
                &[0, 1],
                true,
                &missing_column_index,
            ),
            expected
        );
        assert_eq!(
            page_header_columns_needed(&predicates, &[1], &[0, 1], false, &PagePruning::default()),
            expected
        );
    }

    #[test]
    fn page_header_checks_are_omitted_without_predicates_or_probe_row_groups() {
        let page_index = PagePruning::default();
        assert!(page_header_columns_needed(&[], &[0], &[0], false, &page_index).is_empty());
        assert!(page_header_columns_needed(
            &[equality("shipmode", 1)],
            &[],
            &[0],
            false,
            &page_index,
        )
            .is_empty());
    }

    #[test]
    fn bloom_boolean_logic_is_conservative_for_and_or_and_unknown_terms() {
        let filters = HashMap::from([("value".to_string(), int64_bloom_filter("value", [7]))]);

        let mut probes = 0;
        assert!(!predicate_may_match_bloom(
            &StatisticsPredicate::And(vec![equality("value", 100), StatisticsPredicate::Unknown]),
            &filters,
            &mut probes,
        ));
        assert_eq!(probes, 1);

        let mut probes = 0;
        assert!(predicate_may_match_bloom(
            &StatisticsPredicate::Or(vec![equality("value", 100), StatisticsPredicate::Unknown]),
            &filters,
            &mut probes,
        ));
        assert_eq!(probes, 1);

        let mut probes = 0;
        assert!(predicate_may_match_bloom(
            &equality("missing_filter", 100),
            &filters,
            &mut probes,
        ));
        assert_eq!(probes, 0, "a missing filter must fail open");
    }

    #[test]
    fn parquet_bloom_filter_prunes_row_group_without_losing_matching_rows() {
        let root = tempfile::tempdir().expect("temporary root");
        let path = root.path().join("bloom_filter.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Int64, false)]));
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(4096))
            .set_statistics_enabled(parquet::file::properties::EnabledStatistics::None)
            .set_bloom_filter_enabled(true)
            .build();
        let file = std::fs::File::create(&path).expect("create parquet file");
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(properties))
            .expect("create Bloom-enabled writer");

        let first_group = Int64Array::from_iter_values(0..4096);
        let first_batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(first_group)])
            .expect("valid first group");
        writer.write(&first_batch).expect("write first row group");
        writer.flush().expect("finish first row group");

        let second_group = Int64Array::from_iter_values(1_000_000..1_000_128);
        let second_batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(second_group)])
            .expect("valid second group");
        writer.write(&second_batch).expect("write second row group");
        writer.flush().expect("finish second row group");
        writer.close().expect("finish parquet file");

        let file = std::fs::File::open(&path).expect("open parquet file");
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).expect("read parquet footer");
        assert_eq!(builder.metadata().num_row_groups(), 2);
        let columns = bloom_filter_columns(
            builder.schema().as_ref(),
            builder.metadata(),
            &[equality("value", 1_000_000)],
        );
        assert_eq!(columns.len(), 1);
        let column = &columns[0];
        let first_filter = builder
            .get_row_group_column_bloom_filter(0, column.parquet_column_index)
            .expect("read first Bloom filter")
            .expect("first row group has a Bloom filter");
        let second_filter = builder
            .get_row_group_column_bloom_filter(1, column.parquet_column_index)
            .expect("read second Bloom filter")
            .expect("second row group has a Bloom filter");

        // Use an actual value from row group 1 that its filter confirms and
        // row group 0's filter rejects. This avoids relying on one guessed
        // non-member not colliding with a Bloom false positive.
        let matching_value = (1_000_000..1_000_128)
            .find(|value| second_filter.check(value) && !first_filter.check(value))
            .expect("the disjoint row groups should yield a Bloom-negative probe");
        let filters = HashMap::from([
            (
                0,
                HashMap::from([(
                    column.name.clone(),
                    ParquetBloomFilter::new(column.clone(), first_filter),
                )]),
            ),
            (
                1,
                HashMap::from([(
                    column.name.clone(),
                    ParquetBloomFilter::new(column.clone(), second_filter),
                )]),
            ),
        ]);
        let predicate = equality("value", matching_value);
        let pruning = prune_row_groups_with_bloom(
            builder.metadata(),
            std::slice::from_ref(&predicate),
            vec![
                RowGroupSelection::new(0, None),
                RowGroupSelection::new(1, None),
            ],
            &filters,
        );

        assert_eq!(pruning.selections.len(), 1);
        assert_eq!(pruning.selections[0].row_group_index(), 1);
        assert_eq!(pruning.row_groups_skipped, 1);
        assert_eq!(pruning.rows_skipped, 4096);
        assert!(pruning.probes >= 2);

        let reader = builder
            .with_row_groups(
                pruning
                    .selections
                    .iter()
                    .map(RowGroupSelection::row_group_index)
                    .collect(),
            )
            .build()
            .expect("build reader for Bloom-selected row groups");
        let values = reader
            .map(|batch| {
                let batch = batch.expect("read selected row group");
                let values = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("int64 output");
                (0..values.len()).map(|row| values.value(row)).collect::<Vec<_>>()
            })
            .flatten()
            .filter(|value| *value == matching_value)
            .collect::<Vec<_>>();
        assert_eq!(values, vec![matching_value]);
    }
}
