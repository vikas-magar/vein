//! Footer-statistics pruning for Parquet row groups.

use std::collections::HashSet;

use arrow::datatypes::Schema;
use catalog::{StatisticsPredicate, may_match_data_unit};
use parquet::file::metadata::ParquetMetaData;

use super::super::metadata::row_group_unit;
use super::super::{PredicatePruningDiagnostics, RowGroupPruning, row_group_column_statistics};

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
