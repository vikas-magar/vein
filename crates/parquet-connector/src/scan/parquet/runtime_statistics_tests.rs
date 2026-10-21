use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use catalog::ColumnStatistics;
use common::arrow::SelectionVector;
use common::statistics::runtime::{
    RuntimeStatisticsEnqueue, RuntimeStatisticsPartitionCompletion, RuntimeStatisticsStore,
};
use common::statistics::QueryStatisticsContext;

use super::{RUNTIME_STATISTIC_METRICS, runtime_metric_availability};
use exec::pipeline::DataChunkView;
use types::LogicalLiteral;

#[test]
fn runtime_statistics_availability_reports_present_and_missing_synopses() {
    let statistics = ColumnStatistics {
        null_count: Some(0),
        distinct_count: Some(4),
        min_value: Some(LogicalLiteral::Int64(1)),
        max_value: Some(LogicalLiteral::Int64(9)),
        ..ColumnStatistics::default()
    };

    let availability = runtime_metric_availability(&statistics);
    let metric_is_available = |name: &str| {
        let index = RUNTIME_STATISTIC_METRICS
            .iter()
            .position(|metric| *metric == name)
            .expect("metric is part of the runtime visibility contract");
        availability[index]
    };
    assert_eq!(availability.len(), RUNTIME_STATISTIC_METRICS.len());
    assert!(metric_is_available("null_count"));
    assert!(metric_is_available("distinct_count"));
    assert!(metric_is_available("min_max"));
    assert!(!metric_is_available("distinct_sketch"));
    assert!(!metric_is_available("histogram"));
}

#[test]
fn background_statistics_queue_consumes_selected_view_without_compaction() {
    let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, false)]));
    let source: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3, 4]));
    let view = DataChunkView::from_arrays(
        vec![Arc::clone(&source)],
        4,
        SelectionVector::new_sparse(vec![1, 3]),
    )
    .expect("view over source array")
    .with_schema(Arc::clone(&schema))
    .expect("view schema");
    assert!(Arc::ptr_eq(
        &source,
        &view.source_arrays_for_column(0).expect("view column")[0],
    ));
    let retained_bytes = view
        .source_array_memory_sizes()
        .expect("view array footprint")
        .into_iter()
        .map(|(_, bytes)| bytes)
        .sum();
    let query_context = Arc::new(QueryStatisticsContext::new([]));
    let store = RuntimeStatisticsStore::default();

    assert_eq!(
        store.enqueue_input(41, "table@revision", Some(4), 1, false, schema, view, retained_bytes),
        RuntimeStatisticsEnqueue::Queued,
    );
    assert!(store.enqueue_partition_completion(RuntimeStatisticsPartitionCompletion {
        scan_id: 41,
        table_key: "table@revision",
        expected_rows: Some(4),
        expected_partitions: 1,
        table_scan_candidate: false,
        partition_index: 0,
        query_context: Some(Arc::clone(&query_context)),
    }));

    let _ = store.complete_table_statistics("table@revision");
    let observations = query_context.scan_statistics("table@revision");
    let [observation] = observations.as_slice() else {
        panic!("queued view statistics should be published after scan completion");
    };
    assert_eq!(observation.statistics.num_rows, Some(2));
    assert_eq!(observation.statistics.column_statistics["key"].distinct_count, Some(2));
    assert_eq!(observation.statistics.column_statistics["key"].min_value, Some(LogicalLiteral::Int64(2)));
    assert_eq!(observation.statistics.column_statistics["key"].max_value, Some(LogicalLiteral::Int64(4)));
}
