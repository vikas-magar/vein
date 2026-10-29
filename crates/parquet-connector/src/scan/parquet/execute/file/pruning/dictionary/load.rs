//! Reads each eligible dictionary page once and records conservative matches.

use std::collections::HashMap;
use std::sync::Arc;

use parquet::arrow::arrow_reader::RowGroupSelection;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::file::metadata::ParquetMetaData;

use crate::parquet_statistics::{DictionaryPredicate, dictionary_page_predicate_matches};

use super::super::super::super::super::reader::ParquetFileReader;
use super::super::metadata::MetadataOutcome;
use super::super::predicates::PredicateInputs;
use super::super::{PruningContext, PruningSummary};

#[derive(Default)]
pub(super) struct DictionaryLoadMetrics {
    pub(super) chunks_considered: u64,
    pub(super) chunks_eligible: u64,
    pub(super) pages_loaded: u64,
    pub(super) read_errors: u64,
    pub(super) unsupported_chunks: u64,
    pub(super) page_bytes: u64,
    pub(super) fully_encoded_chunks: u64,
    pub(super) page_filtered_chunks: u64,
    pub(super) elapsed_us: u64,
}

pub(super) struct DictionaryLoad {
    pub(super) row_group_membership: HashMap<usize, Vec<Option<bool>>>,
    pub(super) page_membership: HashMap<usize, Vec<Option<bool>>>,
    pub(super) metrics: DictionaryLoadMetrics,
}

pub(super) async fn load(
    parquet_metadata: &ParquetMetaData,
    context: &PruningContext<'_>,
    inputs: &PredicateInputs,
    metadata: &MetadataOutcome,
    selections: &[RowGroupSelection],
    summary: &mut PruningSummary,
) -> DictionaryLoad {
    let mut metrics = DictionaryLoadMetrics::default();
    let load_started = common::time::Instant::now();
    let mut reader = if inputs.dictionary_predicates.is_empty() || selections.is_empty() {
        None
    } else {
        match ParquetFileReader::new(
            context.storage,
            context.path,
            &context.file.absolute,
            context.io_backend,
            context.file_size_bytes,
            Arc::clone(context.bytes_read),
            Arc::clone(context.io),
        )
            .await
        {
            Ok(reader) => Some(reader),
            Err(error) => {
                metrics.read_errors = metrics.read_errors.saturating_add(1);
                tracing::debug!(
                    target: "vein.audit",
                    event = "parquet_dictionary_reader_unavailable",
                    source_id = %common::logging::LogLocation::from(context.path),
                    backend = context.io_backend.name(),
                    error = %error,
                    "dictionary pruning unavailable; retaining candidate rows"
                );
                None
            }
        }
    };
    let mut row_group_membership = HashMap::new();
    let mut page_membership = HashMap::new();
    for selection in selections {
        let row_group_index = selection.row_group_index();
        let row_group = parquet_metadata.row_group(row_group_index);
        let mut group_outcomes = vec![None; inputs.dictionary_predicates.len()];
        let mut page_outcomes = vec![None; inputs.dictionary_predicates.len()];
        for column in &inputs.dictionary_columns {
            let column_predicates = inputs
                .dictionary_predicates
                .iter()
                .filter(|predicate| predicate.column == column.name)
                .cloned()
                .collect::<Vec<_>>();
            if column_predicates.is_empty() {
                continue;
            }
            metrics.chunks_considered = metrics.chunks_considered.saturating_add(1);
            let Some(chunk) = row_group.columns().get(column.parquet_column_index) else {
                metrics.unsupported_chunks = metrics.unsupported_chunks.saturating_add(1);
                continue;
            };
            let fully_encoded = crate::parquet_statistics::is_fully_dictionary_encoded(chunk);
            if !fully_encoded
                && !crate::parquet_statistics::has_dictionary_encoded_page(
                &metadata.page_header_result.pages,
                row_group_index,
                &column.name,
            )
            {
                continue;
            }
            let Some(range) =
                crate::parquet_statistics::dictionary_page_range(chunk, context.file_size_bytes)
            else {
                metrics.unsupported_chunks = metrics.unsupported_chunks.saturating_add(1);
                continue;
            };
            metrics.chunks_eligible = metrics.chunks_eligible.saturating_add(1);
            if fully_encoded {
                metrics.fully_encoded_chunks =
                    metrics.fully_encoded_chunks.saturating_add(1);
            } else {
                metrics.page_filtered_chunks =
                    metrics.page_filtered_chunks.saturating_add(1);
            }
            let Some(reader) = reader.as_mut() else {
                continue;
            };
            let page_bytes = match reader.get_bytes(range.clone()).await {
                Ok(bytes) => bytes,
                Err(error) => {
                    metrics.read_errors = metrics.read_errors.saturating_add(1);
                    tracing::debug!(
                        target: "vein.audit",
                        event = "parquet_dictionary_page_unavailable",
                        source_id = %common::logging::LogLocation::from(context.path),
                        row_group = row_group_index,
                        column = %column.name,
                        range_start = range.start,
                        range_end = range.end,
                        error = %error,
                        "retaining row group because dictionary page could not be read"
                    );
                    continue;
                }
            };
            metrics.pages_loaded = metrics.pages_loaded.saturating_add(1);
            metrics.page_bytes = metrics
                .page_bytes
                .saturating_add(u64::try_from(page_bytes.len()).unwrap_or(u64::MAX));
            let row_count = usize::try_from(row_group.num_rows()).unwrap_or(0);
            let matches = dictionary_page_predicate_matches(
                page_bytes,
                context.file_size_bytes,
                row_count,
                chunk,
                column,
                &column_predicates,
            );
            record_membership(
                &column_predicates,
                matches,
                fully_encoded,
                &mut group_outcomes,
                &mut page_outcomes,
                &mut metrics,
            );
        }
        row_group_membership.insert(row_group_index, group_outcomes);
        page_membership.insert(row_group_index, page_outcomes);
    }
    metrics.elapsed_us = load_started.elapsed().as_micros() as u64;
    summary.dictionary_chunks_considered = metrics.chunks_considered;
    summary.dictionary_chunks_eligible = metrics.chunks_eligible;
    summary.dictionary_pages_loaded = metrics.pages_loaded;
    summary.dictionary_page_bytes = metrics.page_bytes;
    summary.dictionary_page_read_errors = metrics.read_errors;
    summary.dictionary_unsupported_chunks = metrics.unsupported_chunks;
    summary.dictionary_load_us = metrics.elapsed_us;

    DictionaryLoad {
        row_group_membership,
        page_membership,
        metrics,
    }
}

fn record_membership(
    predicates: &[DictionaryPredicate],
    matches: Vec<Option<bool>>,
    fully_encoded: bool,
    row_group_outcomes: &mut [Option<bool>],
    page_outcomes: &mut [Option<bool>],
    metrics: &mut DictionaryLoadMetrics,
) {
    for (predicate, outcome) in predicates.iter().zip(matches) {
        page_outcomes[predicate.index] = outcome;
        if fully_encoded {
            row_group_outcomes[predicate.index] = outcome;
        }
        if outcome.is_none() {
            metrics.unsupported_chunks = metrics.unsupported_chunks.saturating_add(1);
        }
    }
}
