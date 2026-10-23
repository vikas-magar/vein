use super::*;

use common::cancellation::CancellationToken;
use exec::memory::QueryMemoryTracker;
use exec::operators::join::JoinBloomFilterFactory;
use exec::runtime::filter::{RuntimeFilterBuildInput, RuntimeFilterFactory, RuntimeFilterSet};
use types::{ColumnId, LogicalLiteral, ResolvedColumn, VeinDataType, VeinSchema};

#[tokio::test]
async fn runtime_join_bounds_skip_page_payload_reads_before_row_filtering() {
    let (builder, requested_ranges, locations, row_count) = indexed_file().await;
    let schema = VeinSchema::new(vec![Arc::new(ResolvedColumn::new(
        ColumnId(70), "value", None, VeinDataType::Int32,
    ))]);
    let memory = QueryMemoryTracker::new(1 << 20, CancellationToken::new());
    let filters = JoinBloomFilterFactory::default().build_filters(RuntimeFilterBuildInput {
        probe_columns: &[ColumnId(70)],
        build_hashes: &[123],
        build_key_bounds: vec![Some((LogicalLiteral::Int64(6), LogicalLiteral::Int64(7)))],
        memory: &memory,
    }).unwrap();
    let predicates = RuntimeFilterSet::new(filters).statistics_predicates(&schema);
    assert!(!predicates.is_empty());
    let pruning = crate::parquet_statistics::prune_data_pages(
        builder.schema().as_ref(), builder.metadata(), &predicates, &[0],
    );
    assert!(pruning.rows_skipped > 0);
    assert!(pruning.rows_skipped < row_count as u64);
    let skipped_page = &locations[0];
    let skipped_start = skipped_page.offset as usize;
    let skipped_end = skipped_start + skipped_page.compressed_page_size as usize;
    requested_ranges.lock().unwrap().clear();
    let batches = builder.with_row_group_selections(pruning.selections)
        .build().unwrap().try_collect::<Vec<_>>().await.unwrap();
    let values = batches.iter().flat_map(|batch| {
        batch.column(0).as_primitive::<arrow::datatypes::Int32Type>().values()
    }).copied().collect::<Vec<_>>();
    assert!(values.contains(&6) && values.contains(&7), "matching keys must survive: {values:?}");
    assert!(!values.contains(&0), "a rejected page must not reach the decoder");
    assert!(requested_ranges.lock().unwrap().iter().all(|range| {
        range.end <= skipped_start || range.start >= skipped_end
    }), "runtime metadata pruning requested rejected page payload");
}
