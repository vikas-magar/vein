//! Selection construction for retained and skipped page row ranges.

use std::collections::HashMap;

use catalog::{DataUnitStatistics, StatisticsPredicate, StatisticsUnitKind, may_match_data_unit};
use parquet::arrow::arrow_reader::{RowGroupSelection, RowSelection, RowSelector};

use super::PageColumnStatistics;
use super::PagePruning;

pub(super) fn full_row_group_selections(row_groups: &[usize]) -> PagePruning {
    PagePruning {
        selections: row_groups
            .iter()
            .map(|row_group| RowGroupSelection::new(*row_group, None))
            .collect(),
        ..PagePruning::default()
    }
}

pub(super) fn select_row_group_ranges(
    row_group_index: usize,
    row_count: usize,
    columns: &HashMap<String, PageColumnStatistics>,
    predicates: &[StatisticsPredicate],
    pruning: &mut PagePruning,
) {
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
        let Some(mut unit) = page_range_unit(row_group_index, start, end, columns) else {
            for predicate_index in 0..predicates.len() {
                pruning.predicate_diagnostics.record_missing_metadata(predicate_index);
            }
            append_selector(&mut selectors, RowSelector::select(range_rows));
            continue;
        };
        row_ranges_considered = row_ranges_considered.saturating_add(1);
        unit.num_rows = u64::try_from(range_rows).ok();
        let mut may_match = true;
        for (predicate_index, predicate) in predicates.iter().enumerate() {
            let predicate_may_match = may_match_data_unit(predicate, &unit);
            pruning.predicate_diagnostics.record(predicate_index, predicate, &unit, predicate_may_match);
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

    pruning.row_ranges_considered = pruning.row_ranges_considered.saturating_add(row_ranges_considered);
    pruning.row_ranges_skipped = pruning.row_ranges_skipped.saturating_add(row_ranges_skipped);
    pruning.rows_skipped = pruning.rows_skipped.saturating_add(rows_skipped);
    if rows_skipped == 0 {
        pruning.selections.push(RowGroupSelection::new(row_group_index, None));
    } else if rows_skipped >= row_count as u64 {
        pruning.row_groups_skipped = pruning.row_groups_skipped.saturating_add(1);
    } else {
        pruning.selections.push(RowGroupSelection::new(row_group_index, Some(RowSelection::from(selectors))));
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

pub(in crate::parquet_statistics) fn append_selector(
    selectors: &mut Vec<RowSelector>,
    selector: RowSelector,
) {
    if let Some(last) = selectors.last_mut()
        && last.skip == selector.skip
    {
        last.row_count = last.row_count.saturating_add(selector.row_count);
    } else {
        selectors.push(selector);
    }
}
