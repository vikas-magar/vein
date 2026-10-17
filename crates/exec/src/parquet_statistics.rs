//! Footer-only Parquet statistics extraction and conservative rollups.
//!
//! The extraction path reads the already parsed footer. It never scans column
//! values. Parquet's typed statistics converter handles physical-to-Arrow
//! mappings; unsupported, malformed, or inexact values remain unknown.

use arrow::datatypes::Schema;
use catalog::{DataUnitStatistics, StatisticsPredicate, StatisticsUnitKind};
use parquet::file::metadata::ParquetMetaData;

mod metadata;
pub use metadata::{row_group_column_statistics, table_statistics};
use metadata::{complete_sum, nonnegative, populate_column_statistics, row_group_unit};
#[cfg(test)]
use metadata::exact_scalar_at;
mod page;
pub use page::{
    PageHeaderRowStatistics, PageHeaderStatistics, PagePruning,
    column_statistics_from_page_header, page_header_columns_needed,
    page_header_columns_needed_with_audit, prune_data_pages,
    prune_data_pages_with_headers, prune_row_groups, prune_row_groups_in_range,
};
#[cfg(test)]
use page::page_statistic_literal;
mod bloom;
mod dictionary;

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

/// Predicate diagnostics for files/row groups and Parquet metadata pruning paths.
pub use bloom::{
    bloom_filter_columns, equality_predicate_columns, prune_row_groups_with_bloom,
    BloomFilterColumn, BloomPruning, ParquetBloomFilter,
};
pub use dictionary::{
    dictionary_page_predicate_matches, dictionary_page_range, dictionary_predicates,
    is_fully_dictionary_encoded, prune_row_groups_with_dictionary, DictionaryPredicate,
    DictionaryPruning,
};


#[cfg(test)]
mod tests;
