//! Footer statistics extraction, aggregation, and conservative table rollups.

use std::collections::{HashMap, HashSet};

use arrow::array::{
    Array, BooleanArray, Date32Array, Date64Array, Decimal128Array, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, LargeStringArray, StringArray, StringViewArray,
    Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Schema, TimeUnit};
use catalog::{
    ColumnStatistics, ColumnStorageStatistics, DataUnitStatistics, StatisticsUnitKind,
    TableStatistics,
};
use parquet::arrow::arrow_reader::statistics::StatisticsConverter;
use parquet::file::metadata::{ColumnChunkMetaData, ParquetMetaData, RowGroupMetaData};
use types::LogicalLiteral;

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

pub(super) fn row_group_unit(ordinal: usize, row_group: &RowGroupMetaData) -> DataUnitStatistics {
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

pub(super) fn populate_column_statistics(
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

pub(super) fn exact_scalar_at(
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

pub(super) fn logical_literal_at(
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

pub(super) fn to_micros(value: i64, unit: TimeUnit) -> Option<i64> {
    match unit {
        TimeUnit::Second => value.checked_mul(1_000_000),
        TimeUnit::Millisecond => value.checked_mul(1_000),
        TimeUnit::Microsecond => Some(value),
        TimeUnit::Nanosecond if value % 1_000 == 0 => Some(value / 1_000),
        TimeUnit::Nanosecond => None,
    }
}

pub(super) fn date64_as_days(milliseconds: i64) -> Option<i32> {
    const MILLIS_PER_DAY: i64 = 86_400_000;
    (milliseconds % MILLIS_PER_DAY == 0)
        .then(|| i32::try_from(milliseconds / MILLIS_PER_DAY).ok())
        .flatten()
}

pub(super) fn array_u64_at(values: Option<&arrow::array::UInt64Array>, index: usize) -> Option<u64> {
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

pub(super) fn complete_sum(values: impl IntoIterator<Item=Option<u64>>) -> Option<u64> {
    let mut total = 0_u64;
    let mut saw_value = false;
    for value in values {
        total = total.checked_add(value?)?;
        saw_value = true;
    }
    saw_value.then_some(total)
}

pub(super) fn nonnegative(value: i64) -> Option<u64> {
    u64::try_from(value).ok()
}
