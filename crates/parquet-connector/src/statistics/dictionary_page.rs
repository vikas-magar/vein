//! Conservative data-page pruning from dictionary membership and page encoding.

use std::collections::HashMap;

use parquet::arrow::arrow_reader::{RowGroupSelection, RowSelection, RowSelector};
use parquet::basic::Encoding;
use parquet::file::metadata::ParquetMetaData;

use super::bloom::BloomFilterColumn;
use super::dictionary::{is_fully_dictionary_encoded, DictionaryPredicate};
use super::page::{append_selector, PageHeaderStatistics};
use super::page_header_plan::PageHeaderReadPlan;

#[derive(Debug, Default)]
/// Row selections and bounded metrics from dictionary-based data-page pruning.
pub struct DictionaryPagePruning {
    /// Row groups retained with existing filters intersected with page proofs.
    pub selections: Vec<RowGroupSelection>,
    /// Dictionary membership checks evaluated for dictionary-encoded pages.
    pub predicate_page_checks: u64,
    /// Page/predicate pairs proven unable to match the filter.
    pub dictionary_pages_proven_absent: u64,
    /// Newly excluded rows after intersecting with prior row selections.
    pub rows_skipped: u64,
    /// Row groups with no rows remaining after page pruning.
    pub row_groups_emptied: u64,
}

/// Returns whether a row group has a dictionary-encoded data page for a column.
pub fn has_dictionary_encoded_page(
    page_headers: &PageHeaderStatistics,
    row_group: usize,
    column: &str,
) -> bool {
    page_headers
        .get(&row_group)
        .and_then(|columns| columns.get(column))
        .is_some_and(|pages| pages.iter().any(|page| is_dictionary_encoding(page.encoding)))
}

/// Requests page headers for mixed or unknown encodings; fully dictionary
/// encoded chunks can use the cheaper row-group dictionary decision.
pub fn dictionary_page_header_requests<'a>(
    metadata: &ParquetMetaData,
    row_groups: &[usize],
    columns: impl IntoIterator<Item=&'a BloomFilterColumn>,
) -> PageHeaderReadPlan {
    let columns = columns.into_iter().collect::<Vec<_>>();
    let mut plan = PageHeaderReadPlan::default();
    for row_group_index in row_groups {
        for column in &columns {
            let needs_headers = metadata
                .row_groups()
                .get(*row_group_index)
                .and_then(|row_group| row_group.columns().get(column.parquet_column_index))
                .is_some_and(|chunk| {
                    chunk.dictionary_page_offset().is_some()
                        && !is_fully_dictionary_encoded(chunk)
                });
            if needs_headers {
                plan.request(*row_group_index, &column.name);
            }
        }
    }
    plan
}

/// Excludes rows covered by dictionary-encoded pages whose dictionary cannot
/// satisfy a pushed equality predicate. Missing or malformed evidence fails open.
pub fn prune_dictionary_encoded_pages(
    selections: Vec<RowGroupSelection>,
    page_headers: &PageHeaderStatistics,
    predicates: &[DictionaryPredicate],
    membership: &HashMap<usize, Vec<Option<bool>>>,
) -> DictionaryPagePruning {
    let mut pruning = DictionaryPagePruning::default();
    for selection in selections {
        let row_group = selection.row_group_index();
        let mut total_rows = None;
        let mut excluded = Vec::new();
        if let (Some(columns), Some(outcomes)) = (page_headers.get(&row_group), membership.get(&row_group)) {
            for predicate in predicates {
                if outcomes.get(predicate.index) != Some(&Some(false)) {
                    continue;
                }
                let Some(pages) = columns.get(&predicate.column) else { continue };
                let Some(page_rows) = complete_page_coverage(pages) else { continue };
                if total_rows.is_some_and(|rows| rows != page_rows) {
                    continue;
                }
                total_rows = Some(page_rows);
                for page in pages {
                    if !is_dictionary_encoding(page.encoding) {
                        continue;
                    }
                    pruning.predicate_page_checks = pruning.predicate_page_checks.saturating_add(1);
                    pruning.dictionary_pages_proven_absent =
                        pruning.dictionary_pages_proven_absent.saturating_add(1);
                    excluded.push((page.first_row, page.first_row + page.row_count));
                }
            }
        }
        let Some(total_rows) = total_rows.filter(|_| !excluded.is_empty()) else {
            pruning.selections.push(selection);
            continue;
        };
        let page_selection = selection_from_excluded_ranges(total_rows, &mut excluded);
        let previously_selected = selection.selection().map_or(total_rows, RowSelection::row_count);
        let combined = selection
            .selection()
            .map_or(page_selection.clone(), |rows| rows.intersection(&page_selection));
        let selected_rows = combined.row_count();
        let newly_skipped = previously_selected.saturating_sub(selected_rows);
        if newly_skipped == 0 {
            pruning.selections.push(selection);
        } else if selected_rows == 0 {
            pruning.rows_skipped = pruning.rows_skipped.saturating_add(newly_skipped as u64);
            pruning.row_groups_emptied = pruning.row_groups_emptied.saturating_add(1);
        } else {
            pruning.rows_skipped = pruning.rows_skipped.saturating_add(newly_skipped as u64);
            pruning.selections.push(RowGroupSelection::new(row_group, Some(combined)));
        }
    }
    pruning
}

fn complete_page_coverage(pages: &[super::page::PageHeaderRowStatistics]) -> Option<usize> {
    let mut next_row = 0usize;
    for page in pages {
        if page.row_count == 0 || page.first_row != next_row {
            return None;
        }
        next_row = next_row.checked_add(page.row_count)?;
    }
    (!pages.is_empty()).then_some(next_row)
}

fn is_dictionary_encoding(encoding: Encoding) -> bool {
    matches!(encoding, Encoding::PLAIN_DICTIONARY | Encoding::RLE_DICTIONARY)
}

fn selection_from_excluded_ranges(
    total_rows: usize,
    excluded: &mut Vec<(usize, usize)>,
) -> RowSelection {
    excluded.sort_unstable();
    let mut selectors = Vec::with_capacity(excluded.len().saturating_mul(2).saturating_add(1));
    let mut cursor = 0usize;
    for (start, end) in merge_ranges(excluded) {
        if start > cursor {
            append_selector(&mut selectors, RowSelector::select(start - cursor));
        }
        append_selector(&mut selectors, RowSelector::skip(end - start));
        cursor = end;
    }
    if cursor < total_rows {
        append_selector(&mut selectors, RowSelector::select(total_rows - cursor));
    }
    RowSelection::from(selectors)
}

fn merge_ranges(ranges: &[(usize, usize)]) -> Vec<(usize, usize)> {
    let mut merged: Vec<(usize, usize)> = Vec::with_capacity(ranges.len());
    for &(start, end) in ranges {
        if let Some(last) = merged.last_mut().filter(|last| start <= last.1) {
            last.1 = last.1.max(end);
        } else {
            merged.push((start, end));
        }
    }
    merged
}

#[cfg(test)]
#[path = "dictionary_page_tests.rs"]
mod tests;
