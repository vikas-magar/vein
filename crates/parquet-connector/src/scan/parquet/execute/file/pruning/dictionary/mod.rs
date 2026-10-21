//! Loads dictionary metadata and applies conservative dictionary pruning.

mod load;
mod prune;

use parquet::arrow::arrow_reader::RowGroupSelection;
use parquet::file::metadata::ParquetMetaData;

use super::metadata::MetadataOutcome;
use super::predicates::PredicateInputs;
use super::{PruningContext, PruningSummary};

pub(super) struct DictionaryOutcome {
    pub(super) selections: Vec<RowGroupSelection>,
    pub(super) rows_skipped: u64,
}

pub(super) async fn apply(
    parquet_metadata: &ParquetMetaData,
    context: &PruningContext<'_>,
    inputs: &PredicateInputs,
    metadata: &MetadataOutcome,
    selections: Vec<RowGroupSelection>,
    summary: &mut PruningSummary,
) -> DictionaryOutcome {
    let loaded = load::load(
        parquet_metadata,
        context,
        inputs,
        metadata,
        &selections,
        summary,
    )
        .await;
    prune::apply(
        parquet_metadata,
        context,
        inputs,
        metadata,
        loaded,
        selections,
        summary,
    )
}
