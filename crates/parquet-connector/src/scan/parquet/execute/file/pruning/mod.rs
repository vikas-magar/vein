//! Conservative Parquet pruning stages, composed in reader order.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use common::Result;
use common::protocol::ScanPredicate;
use exec::operators::source::scan::ScanFile;
use exec::runtime::filter::RuntimeFilterSet;
use parquet::arrow::arrow_reader::RowGroupSelection;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use storage::ReadStorage;
use types::VeinSchema;

use super::super::super::reader::{IoStats, ParquetFileReader, ParquetIoBackend};

mod bloom;
mod dictionary;
mod metadata;
mod predicates;
mod summary;

pub(super) use summary::PruningSummary;

pub(super) type ParquetScanBuilder = ParquetRecordBatchStreamBuilder<ParquetFileReader>;

pub(super) struct PruningContext<'a> {
    pub(super) path: &'a str,
    pub(super) file: &'a ScanFile,
    pub(super) storage: &'a Arc<dyn ReadStorage>,
    pub(super) projected: &'a VeinSchema,
    pub(super) pushdown: Option<&'a ScanPredicate>,
    pub(super) runtime_filters: &'a RuntimeFilterSet,
    pub(super) bytes_read: &'a Arc<AtomicU64>,
    pub(super) io: &'a Arc<IoStats>,
    pub(super) io_backend: ParquetIoBackend,
    pub(super) file_size_bytes: u64,
}

pub(super) struct PruningOutcome {
    pub(super) summary: PruningSummary,
    pub(super) selections: Option<Vec<RowGroupSelection>>,
}

pub(super) async fn apply(
    builder: &mut ParquetScanBuilder,
    context: PruningContext<'_>,
) -> Result<PruningOutcome> {
    let mut summary = PruningSummary::default();
    let inputs = predicates::build(builder, &context);
    summary.static_statistics_predicate_count = inputs.static_count;
    summary.runtime_statistics_predicates = inputs.runtime_count;
    summary.bloom_filter_columns = inputs.bloom_columns.len();
    summary.dictionary_predicates = inputs.dictionary_predicates.len();
    summary.dictionary_columns = inputs.dictionary_columns.len();

    let parquet_metadata = Arc::clone(builder.metadata());
    let arrow_schema = Arc::clone(builder.schema());
    let mut metadata = metadata::apply(
        &parquet_metadata,
        arrow_schema.as_ref(),
        &context,
        &inputs.statistics,
        &inputs.dictionary_columns,
        &mut summary,
    )
        .await;
    let bloom = bloom::apply(
        builder,
        &context,
        &inputs,
        std::mem::take(&mut metadata.page_selections),
        &mut summary,
    )
        .await;
    let dictionary = dictionary::apply(
        &parquet_metadata,
        &context,
        &inputs,
        &metadata,
        bloom.selections,
        &mut summary,
    )
        .await;

    let has_selection = context.file.scan_unit_range.is_some()
        || summary.row_groups_selected != summary.row_groups_total
        || summary.page_rows_skipped != 0
        || summary.row_groups_skipped_by_bloom != 0
        || summary.row_groups_skipped_by_dictionary != 0
        || dictionary.rows_skipped != 0
        || !metadata.page_header_result.page_locations.is_empty();
    Ok(PruningOutcome {
        summary,
        selections: has_selection.then_some(dictionary.selections),
    })
}
