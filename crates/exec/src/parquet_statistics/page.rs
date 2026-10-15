//! Conservative row-group and page pruning from Parquet statistics.

use std::collections::{HashMap, HashSet};

use arrow::datatypes::{DataType, Schema, TimeUnit};
use catalog::{
    may_match_data_unit, ColumnStatistics, DataUnitStatistics, StatisticsPredicate,
    StatisticsUnitKind,
};
use parquet::arrow::arrow_reader::statistics::StatisticsConverter;
use parquet::arrow::arrow_reader::{RowGroupSelection, RowSelection, RowSelector};
use parquet::basic::Type as ParquetType;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::statistics::Statistics as ParquetStatistics;
use types::LogicalLiteral;

use super::metadata::{
    array_u64_at, date64_as_days, logical_literal_at, nonnegative, row_group_unit, to_micros,
};
use super::{row_group_column_statistics, PredicatePruningDiagnostics, RowGroupPruning};

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

pub(super) fn page_statistic_literal(
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
