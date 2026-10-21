//! Row-group and column-chunk metadata extraction.

use std::collections::{HashMap, HashSet};

use arrow::datatypes::Schema;
use catalog::{
    ColumnStatistics, ColumnStorageStatistics, DataUnitStatistics, StatisticsUnitKind,
};
use parquet::arrow::arrow_reader::statistics::StatisticsConverter;
use parquet::file::metadata::{ColumnChunkMetaData, ParquetMetaData, RowGroupMetaData};

use super::summary::{has_values, nonnegative};
use super::values::{array_u64_at, exact_scalar_at};

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

pub(in crate::parquet_statistics) fn row_group_unit(
    ordinal: usize,
    row_group: &RowGroupMetaData,
) -> DataUnitStatistics {
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

pub(in crate::parquet_statistics) fn populate_column_statistics(
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
