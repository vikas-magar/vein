//! Page-index statistics mapped to validated row ranges.

use arrow::datatypes::{DataType, Schema};
use catalog::ColumnStatistics;
use parquet::arrow::arrow_reader::statistics::StatisticsConverter;
use parquet::file::metadata::ParquetMetaData;

use super::super::metadata::{array_u64_at, logical_literal_at, nonnegative};
use super::PageColumnStatistics;

pub(super) fn page_column_statistics<'a>(
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
