//! Collects static and runtime predicates for metadata pruning.

use catalog::StatisticsPredicate;
use common::protocol::ScanPredicate;

use crate::parquet_statistics::{BloomFilterColumn, DictionaryPredicate};

use super::{ParquetScanBuilder, PruningContext};

pub(super) struct PredicateInputs {
    pub(super) statistics: Vec<StatisticsPredicate>,
    pub(super) static_count: usize,
    pub(super) runtime_count: usize,
    pub(super) bloom_columns: Vec<BloomFilterColumn>,
    pub(super) dictionary_predicates: Vec<DictionaryPredicate>,
    pub(super) dictionary_columns: Vec<BloomFilterColumn>,
}

pub(super) fn build(
    builder: &ParquetScanBuilder,
    context: &PruningContext<'_>,
) -> PredicateInputs {
    let static_predicates = context
        .pushdown
        .map_or(&[][..], |predicate: &ScanPredicate| {
            predicate.statistics.as_slice()
        });
    let (runtime_predicates, diagnostics) = context
        .runtime_filters
        .statistics_predicates_with_diagnostics(context.projected);
    if !diagnostics.is_empty() {
        tracing::info!(
            target: "vein.parquet_runtime_filter",
            event = "parquet_runtime_filter_pruning_path",
            path = %context.path,
            filters = ?diagnostics,
            metadata_predicate_count = runtime_predicates.len(),
            row_filter_names = ?context.runtime_filters.row_filter_names(),
            metadata_stage = "footer_and_page_index",
            row_filter_stage = "arrow_row_filter_after_metadata_pruning",
            "runtime filter capabilities at the Parquet scan boundary"
        );
    }

    let static_count = static_predicates.len();
    let runtime_count = runtime_predicates.len();
    let mut statistics = static_predicates.to_vec();
    statistics.extend(runtime_predicates);
    let bloom_columns = crate::parquet_statistics::bloom_filter_columns(
        builder.schema().as_ref(),
        builder.metadata(),
        &statistics,
    );
    let dictionary_predicates =
        crate::parquet_statistics::dictionary_predicates(&statistics);
    let dictionary_columns = bloom_columns
        .iter()
        .filter(|column| {
            dictionary_predicates
                .iter()
                .any(|predicate| predicate.column == column.name)
        })
        .cloned()
        .collect();

    PredicateInputs {
        statistics,
        static_count,
        runtime_count,
        bloom_columns,
        dictionary_predicates,
        dictionary_columns,
    }
}
