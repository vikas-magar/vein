use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::record_batch::RecordBatch;
use common::cancellation::CancellationToken;
use crate::memory::QueryMemoryTracker;
use crate::pipeline::{DataChunkView, PipelineNode};
use crate::pipeline::view::{ViewColumnRef, ViewInputRows};
use super::IntermediateRelation;

fn schema() -> Arc<types::VeinSchema> {
    Arc::new(types::VeinSchema::new([11, 22].into_iter().map(|id| {
        let mut column = types::ResolvedColumn::new(types::ColumnId(id), "same_name", None,
                                                    types::VeinDataType::Int64);
        column.nullable = true;
        Arc::new(column)
    }).collect()))
}

#[tokio::test]
async fn checkpoint_synopses_follow_mapped_rows_null_extensions_and_column_identity() {
    let logical = schema();
    let arrow = Arc::new(logical.to_arrow());
    let arrays: Vec<arrow::array::ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![1, 2, 3])),
        Arc::new(Int64Array::from(vec![10, 20, 30])),
    ];
    let view = crate::pipeline::arrow_adapter::from_record_batch(RecordBatch::try_new(Arc::clone(&arrow), arrays.clone()).unwrap()).unwrap();
    let mapped = DataChunkView::combine(&[ViewInputRows {
        view: &view,
        rows: &[Some(2), None, Some(2), Some(0)],
    }], &[
        ViewColumnRef { input: 0, column: 0 }, ViewColumnRef { input: 0, column: 1 },
    ]).unwrap();
    let tracker = QueryMemoryTracker::new(1 << 20, CancellationToken::new());
    let relation = IntermediateRelation::new(arrow, tracker.clone());
    let mut sink = relation.sink();
    sink.push(mapped).unwrap();
    sink.flush().unwrap();
    let retained = tracker.used();
    let stats = relation.collect_statistics(logical).await.unwrap();
    assert_eq!(stats.num_rows, Some(4));
    for (key, min, max) in [("11", 1, 3), ("22", 10, 30)] {
        let column = &stats.column_statistics[key];
        assert_eq!(column.null_count, Some(1));
        assert_eq!(column.distinct_count, Some(2));
        assert_eq!(column.min_value, Some(types::LogicalLiteral::Int64(min)));
        assert_eq!(column.max_value, Some(types::LogicalLiteral::Int64(max)));
        assert!(column.most_common_values.is_some());
        assert!(column.histogram.is_some());
    }
    assert_eq!(tracker.used(), retained);
    let state = relation.state.lock().unwrap();
    let super::CheckpointState::SealedMemory(batches) = &*state else { panic!("memory checkpoint"); };
    for (index, original) in arrays.iter().enumerate() {
        assert!(Arc::ptr_eq(batches[0].resolve(index, 0).unwrap().unwrap().0, original));
    }
}

#[tokio::test]
async fn parallel_checkpoint_statistics_merge_batches_without_copying_arrays() {
    let logical = schema();
    let arrow = Arc::new(logical.to_arrow());
    let tracker = QueryMemoryTracker::new(1 << 20, CancellationToken::new());
    let relation = IntermediateRelation::new(Arc::clone(&arrow), tracker.clone());
    let mut sink = relation.sink();
    for _ in 0..8 {
        sink.push(crate::pipeline::arrow_adapter::from_record_batch(RecordBatch::try_new(
            Arc::clone(&arrow),
            vec![
                Arc::new(Int64Array::from(vec![Some(1), Some(2), None, Some(2)])),
                Arc::new(Int64Array::from(vec![Some(10), Some(20), None, Some(20)])),
            ],
        ).unwrap()).unwrap()).unwrap();
    }
    sink.flush().unwrap();
    let retained_bytes = tracker.used();
    let mut source = relation.source().unwrap();
    let statistics_schema = super::statistics::statistics_schema(&relation, &logical).unwrap();
    let (stats, batches) = super::statistics::collect_from_source(
        &mut source,
        statistics_schema,
        32,
        &tracker,
        2,
    ).unwrap();

    assert_eq!(batches, 8);
    assert_eq!(stats.num_rows, Some(32));
    for (key, min, max) in [("11", 1, 2), ("22", 10, 20)] {
        let column = &stats.column_statistics[key];
        assert_eq!(column.null_count, Some(8));
        assert_eq!(column.distinct_count, Some(2));
        assert_eq!(column.min_value, Some(types::LogicalLiteral::Int64(min)));
        assert_eq!(column.max_value, Some(types::LogicalLiteral::Int64(max)));
        assert!(column.most_common_values.is_some());
        assert_eq!(column.histogram.as_ref().map(|h| h.population_count), Some(24));
    }
    assert_eq!(tracker.used(), retained_bytes);
}

#[tokio::test]
async fn empty_checkpoint_produces_typed_zero_population_and_rejects_wrong_schema() {
    let logical = schema();
    let relation = IntermediateRelation::new(Arc::new(logical.to_arrow()),
                                             QueryMemoryTracker::new(1 << 20, CancellationToken::new()));
    assert!(relation.collect_statistics(Arc::clone(&logical)).await.is_err());
    relation.sink().flush().unwrap();
    let stats = relation.collect_statistics(logical).await.unwrap();
    assert_eq!(stats.num_rows, Some(0));
    assert_eq!(stats.column_statistics.len(), 2);
    assert_eq!(stats.column_statistics["11"].null_count, Some(0));
    assert!(relation.collect_statistics(Arc::new(types::VeinSchema::empty())).await.is_err());
}

#[tokio::test]
async fn spilled_checkpoint_synopses_cover_the_complete_population() {
    let logical = schema();
    let arrow = Arc::new(logical.to_arrow());
    let relation = IntermediateRelation::new(Arc::clone(&arrow),
                                             QueryMemoryTracker::new(0, CancellationToken::new()));
    let mut sink = relation.sink();
    sink.push(crate::pipeline::arrow_adapter::from_record_batch(RecordBatch::try_new(arrow, vec![
        Arc::new(Int64Array::from(vec![Some(5), None, Some(8)])),
        Arc::new(Int64Array::from(vec![Some(1), Some(1), Some(2)])),
    ]).unwrap()).unwrap()).unwrap();
    sink.flush().unwrap();
    let stats = relation.collect_statistics(logical).await.unwrap();
    assert_eq!(stats.num_rows, Some(3));
    assert_eq!(stats.column_statistics["11"].null_count, Some(1));
    assert_eq!(stats.column_statistics["22"].distinct_count, Some(2));
}

#[tokio::test]
async fn cancelled_checkpoint_statistics_stop_without_publishing_partial_results() {
    let logical = schema();
    let arrow = Arc::new(logical.to_arrow());
    let cancel = CancellationToken::new();
    let relation = IntermediateRelation::new(Arc::clone(&arrow),
                                             QueryMemoryTracker::new(1 << 20, cancel.clone()));
    let mut sink = relation.sink();
    sink.push(crate::pipeline::arrow_adapter::from_record_batch(RecordBatch::try_new(arrow, vec![
        Arc::new(Int64Array::from(vec![1])), Arc::new(Int64Array::from(vec![2])),
    ]).unwrap()).unwrap()).unwrap();
    sink.flush().unwrap();
    cancel.cancel();
    assert!(relation.collect_statistics(logical).await.is_err());
}
