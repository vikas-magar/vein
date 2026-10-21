//! Conservative row-group and page pruning from Parquet statistics.

use std::collections::{HashMap, HashSet};

use catalog::ColumnStatistics;
use parquet::arrow::arrow_reader::RowGroupSelection;

use super::PredicatePruningDiagnostics;

mod columns;
mod data_pages;
mod headers;
mod literal;
mod row_groups;
mod row_ranges;

use columns::page_column_statistics;
use headers::{header_page_column_statistics, merge_page_column_statistics};
pub(super) use literal::page_statistic_literal;
use row_ranges::{full_row_group_selections, select_row_group_ranges};

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
    /// Row-group/column pairs whose page index provides complete bounds for all pages.
    pub page_index_complete_columns: HashMap<usize, HashSet<String>>,
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
    /// Encoding used for values in this data page.
    pub encoding: parquet::basic::Encoding,
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

pub use data_pages::{prune_data_pages, prune_data_pages_with_headers};
pub use headers::column_statistics_from_page_header;
pub use row_groups::{prune_row_groups, prune_row_groups_in_range};
pub(super) use row_ranges::append_selector;
