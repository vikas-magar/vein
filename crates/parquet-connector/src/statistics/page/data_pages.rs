//! Page-index and page-header based row-range pruning.

use std::collections::{HashMap, HashSet};

use arrow::datatypes::Schema;
use catalog::StatisticsPredicate;
use parquet::arrow::arrow_reader::RowGroupSelection;
use parquet::file::metadata::ParquetMetaData;

use super::super::PredicatePruningDiagnostics;
use super::super::metadata::nonnegative;
use super::{
    PageColumnStatistics, PageHeaderStatistics,
    PagePruning,
    full_row_group_selections, header_page_column_statistics, merge_page_column_statistics,
    page_column_statistics, select_row_group_ranges,
};

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

    let mut pruning = PagePruning {
        predicate_diagnostics: PredicatePruningDiagnostics::for_predicates(predicates),
        ..PagePruning::default()
    };
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
            if index_statistics.as_ref().is_some_and(|statistics| {
                !statistics.pages.is_empty()
                    && statistics
                    .pages
                    .iter()
                    .all(|page| page.min_max_complete == Some(true))
            }) {
                pruning
                    .page_index_complete_columns
                    .entry(*row_group_index)
                    .or_default()
                    .insert((*column).to_string());
            }
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
        select_row_group_ranges(*row_group_index, row_count, &columns, predicates, &mut pruning);
    }

    pruning
}
