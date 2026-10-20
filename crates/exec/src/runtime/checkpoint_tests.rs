use std::sync::Arc;

use arrow::array::{Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use common::cancellation::CancellationToken;
use common::identifiers::{IntermediateSourceId, QueryId};

use crate::memory::{ExecutionResourceLimits, QueryMemoryTracker};
use crate::pipeline::{DataChunkView, PipelineNode, Sink, Source};

use super::{IntermediateRelation, IntermediateSourceRegistry};

#[derive(Default)]
struct ViewCollector {
    batches: Vec<DataChunkView>,
}

impl PipelineNode for ViewCollector {
    fn push(&mut self, batch: DataChunkView) -> common::Result<()> {
        self.batches.push(batch);
        Ok(())
    }

    fn flush(&mut self) -> common::Result<()> {
        Ok(())
    }
}

fn input() -> (Arc<Schema>, RecordBatch) {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![3, 5, 8]))],
    )
        .expect("valid input batch");
    (schema, batch)
}

async fn read_values(relation: &IntermediateRelation) -> Vec<i64> {
    let mut source = relation.source().expect("sealed relation source");
    let mut output = ViewCollector::default();
    source.execute(&mut output).await.expect("replay relation");
    output
        .batches
        .into_iter()
        .flat_map(|view| {
            let (batch, report) = crate::pipeline::arrow_adapter::to_record_batch_with_report(
                &view,
                Arc::new(Schema::new(vec![Field::new(
                    "id",
                    DataType::Int64,
                    false,
                )])),
            )
                .expect("materialize test output");
            assert_eq!(report.gathered_columns, 0);
            assert_eq!(report.shared_columns, 1);
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Int64 output");
            values.values().to_vec()
        })
        .collect()
}

#[tokio::test]
async fn intermediate_source_registry_is_query_scoped_and_registration_owned() {
    let (schema, batch) = input();
    let tracker = QueryMemoryTracker::new(1 << 20, CancellationToken::new());
    let relation = IntermediateRelation::new(Arc::clone(&schema), tracker);
    let mut sink = relation.sink();
    sink.push(crate::pipeline::arrow_adapter::from_record_batch(batch).unwrap())
        .expect("retain checkpoint arrays");
    sink.flush().expect("seal relation before registering source");

    let registry = IntermediateSourceRegistry::default();
    let query_id = QueryId::new();
    let source_id = IntermediateSourceId::new();
    let registration = registry
        .register(query_id, source_id, relation)
        .expect("register query-local checkpoint");

    assert!(registry.source(QueryId::new(), source_id).is_err());
    let source = registry
        .source(query_id, source_id)
        .expect("resolve checkpoint in owning query");
    let mut output = ViewCollector::default();
    let mut source = source;
    source.execute(&mut output).await.expect("replay registered source");
    assert_eq!(output.batches.len(), 1);

    assert!(registry
                .register(query_id, source_id, IntermediateRelation::new(
                    Arc::clone(&schema),
                    QueryMemoryTracker::new(1 << 20, CancellationToken::new()),
                ))
                .is_err(), "a live source identifier cannot be replaced");
    drop(registration);
    assert!(registry.source(query_id, source_id).is_err());
}

#[tokio::test]
async fn in_memory_checkpoint_retains_shared_arrays_and_replays_views() {
    let (schema, batch) = input();
    let tracker = QueryMemoryTracker::new(1 << 20, CancellationToken::new());
    let relation = IntermediateRelation::new(Arc::clone(&schema), tracker.clone());
    let mut sink = relation.sink();

    sink.push(crate::pipeline::arrow_adapter::from_record_batch(batch.clone()).unwrap())
        .expect("retain reference-backed batch");
    assert_eq!(
        tracker.used(),
        batch.column(0).get_array_memory_size(),
        "the shared source array is charged once while the checkpoint retains it"
    );
    sink.flush().expect("seal in-memory relation");

    assert_eq!(relation.row_count().unwrap(), 3);
    assert_eq!(read_values(&relation).await, vec![3, 5, 8]);
    assert_eq!(tracker.used(), batch.column(0).get_array_memory_size());
}

#[test]
fn checkpoint_sink_reports_rows_and_seals_as_a_task_sink() {
    let (schema, batch) = input();
    let tracker = QueryMemoryTracker::new(1 << 20, CancellationToken::new());
    let relation = IntermediateRelation::new(schema, tracker);
    let mut sink = relation.sink();
    let sink: &mut dyn Sink = &mut sink;

    sink.push(crate::pipeline::arrow_adapter::from_record_batch(batch).unwrap())
        .expect("retain reference-backed batch");
    assert_eq!(sink.row_count(), 3);
    assert!(sink.collect().is_empty(), "checkpoints are not client batches");
    sink.flush().expect("seal checkpoint sink");
    assert_eq!(relation.row_count().unwrap(), 3);
}

#[tokio::test]
async fn checkpoint_spills_when_array_retention_exceeds_budget_and_replays_rows() {
    let (schema, batch) = input();
    let tracker = QueryMemoryTracker::with_resource_limits(
        ExecutionResourceLimits::new(0, 1 << 20),
        CancellationToken::new(),
    );
    let relation = IntermediateRelation::new(Arc::clone(&schema), tracker.clone());
    let mut sink = relation.sink();

    sink.push(crate::pipeline::arrow_adapter::from_record_batch(batch).unwrap())
        .expect("spill batch when memory budget is exhausted");
    sink.flush().expect("finish spill run");

    assert_eq!(relation.row_count().unwrap(), 3);
    assert_eq!(tracker.used(), 0, "spilled input arrays are no longer retained");
    assert!(tracker.spill_bytes_used() > 0);
    assert_eq!(read_values(&relation).await, vec![3, 5, 8]);

    drop(sink);
    drop(relation);
    assert_eq!(tracker.spill_bytes_used(), 0, "the last relation handle removes its run");
}

#[test]
fn unsealed_checkpoint_cannot_be_read_and_drop_discards_partial_state() {
    let (schema, batch) = input();
    let tracker = QueryMemoryTracker::new(1 << 20, CancellationToken::new());
    let relation = IntermediateRelation::new(schema, tracker.clone());
    let mut sink = relation.sink();
    sink.push(crate::pipeline::arrow_adapter::from_record_batch(batch).unwrap())
        .expect("retain batch");

    assert!(relation.source().is_err());
    drop(sink);

    assert!(relation.source().is_err());
    drop(relation);
    assert_eq!(tracker.used(), 0, "dropping a failed producer releases retained arrays");
}
