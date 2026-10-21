//! Page-header statistics conversion and index/header reconciliation.

use arrow::datatypes::DataType;
use catalog::ColumnStatistics;
use parquet::basic::Type as ParquetType;
use parquet::file::statistics::Statistics as ParquetStatistics;

use super::{PageColumnStatistics, PageHeaderRowStatistics, page_statistic_literal};

pub(super) fn header_page_column_statistics(
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

pub(super) fn merge_page_column_statistics(
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
