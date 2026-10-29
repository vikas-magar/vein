//! Bounded parallel synopsis collection for replayable adaptive checkpoints.
use super::*;
use std::sync::{mpsc, Mutex};
use std::thread;

impl IntermediateRelation {
    /// Collect complete statistics without gathering the relation's column payloads.
    pub async fn collect_statistics(
        &self,
        schema: types::VeinSchemaRef,
    ) -> Result<Arc<catalog::TableStatistics>> {
        let mut source = self.source()?;
        let expected_rows = self.row_count()?;
        let statistics_schema = statistics_schema(self, &schema)?;
        let workers = worker_count(&source);
        let tracker = self.tracker.clone();
        let span = tracing::Span::current();

        common::runtime::run_blocking(move || {
            let _entered = span.enter();
            let started = common::time::Instant::now();
            let (statistics, batches) = collect_from_source(
                &mut source, statistics_schema, expected_rows, &tracker, workers,
            )?;
            tracing::info!(target: "vein.adaptive", event = "adaptive_checkpoint_statistics",
                rows = expected_rows, batches, workers,
                columns = statistics.column_statistics.len(),
                ndv_columns = statistics.column_statistics.values().filter(|c| c.distinct_count.is_some()).count(),
                mcv_columns = statistics.column_statistics.values().filter(|c| c.most_common_values.is_some()).count(),
                histogram_columns = statistics.column_statistics.values().filter(|c| c.histogram.is_some()).count(),
                elapsed_us = started.elapsed().as_micros() as u64,
                payload_bytes_gathered = 0u64,
                collector_mode = if workers > 1 { "parallel_local_merge" } else { "single_worker" },
                "checkpoint synopsis computed from shared Arrow arrays");
            Ok(Arc::new(statistics))
        })
            .await
    }
}

pub(super) fn statistics_schema(
    relation: &IntermediateRelation,
    schema: &types::VeinSchema,
) -> Result<SchemaRef> {
    if relation.schema.as_ref() != &schema.to_arrow() {
        return Err(VeinError::internal(
            "checkpoint statistics schema does not match its relation",
        ));
    }
    let fields = relation.schema.fields().iter().zip(&schema.fields)
        .map(|(field, column)| field.as_ref().clone()
            .with_name(common::statistics::intermediate_column_key(column.id)))
        .collect::<Vec<_>>();
    Ok(Arc::new(arrow::datatypes::Schema::new(fields)))
}

fn worker_count(source: &IntermediateRelationSource) -> usize {
    if cfg!(target_arch = "wasm32") { return 1; }
    let available = thread::available_parallelism().map_or(1, |count| count.get());
    match &source.input {
        CheckpointInput::Memory(batches) => available.min(batches.len()).max(1),
        CheckpointInput::Spill(_) => available.max(1),
    }
}

pub(super) fn collect_from_source(
    source: &mut IntermediateRelationSource,
    schema: SchemaRef,
    expected_rows: u64,
    tracker: &QueryMemoryTracker,
    workers: usize,
) -> Result<(catalog::TableStatistics, usize)> {
    let workers = if cfg!(target_arch = "wasm32") { 1 } else { workers };
    if workers <= 1 {
        let mut collector = common::statistics::StatisticsCollector::new(
            schema, common::statistics::StatisticsConfig::default(),
        )?;
        let mut batches = 0;
        source.for_each_batch(|view| {
            tracker.check_cancelled()?;
            batches += 1;
            collector.update_input(&view)
        })?;
        return finish_collection(collector, expected_rows, batches);
    }

    collect_parallel(source, schema, expected_rows, tracker, workers)
}

fn collect_parallel(
    source: &mut IntermediateRelationSource,
    schema: SchemaRef,
    expected_rows: u64,
    tracker: &QueryMemoryTracker,
    workers: usize,
) -> Result<(catalog::TableStatistics, usize)> {
    let (sender, receiver) = mpsc::sync_channel::<DataChunkView>(workers);
    let receiver = Arc::new(Mutex::new(receiver));

    thread::scope(|scope| {
        let mut handles = Vec::with_capacity(workers);
        for _ in 0..workers {
            let receiver = Arc::clone(&receiver);
            let schema = Arc::clone(&schema);
            let tracker = tracker.clone();
            let config = common::statistics::StatisticsConfig::default();
            match thread::Builder::new().spawn_scoped(scope, move || {
                let mut collector = common::statistics::StatisticsCollector::new(schema, config)?;
                let mut failure = None;
                loop {
                    let batch = match receiver.lock() {
                        Ok(receiver) => receiver.recv(),
                        Err(_) => return Err(VeinError::internal("statistics worker queue poisoned")),
                    };
                    let Ok(batch) = batch else { break };
                    if failure.is_none()
                        && let Err(error) = tracker.check_cancelled()
                        .and_then(|()| collector.update_input(&batch))
                    {
                        failure = Some(error);
                    }
                }
                failure.map_or(Ok(collector), Err)
            }) {
                Ok(handle) => handles.push(handle),
                Err(error) => {
                    drop(sender);
                    for handle in handles {
                        let _ = handle.join();
                    }
                    return Err(VeinError::internal(format!(
                        "could not start checkpoint statistics worker: {error}",
                    )));
                }
            }
        }

        let mut batches = 0;
        let produced = source.for_each_batch(|view| {
            tracker.check_cancelled()?;
            batches += 1;
            sender.send(view).map_err(|_| {
                VeinError::internal("checkpoint statistics workers disconnected")
            })
        });
        drop(sender);

        let mut combined: Option<common::statistics::StatisticsCollector> = None;
        let mut worker_error = None;
        for handle in handles {
            match handle.join() {
                Ok(Ok(partial)) => {
                    if let Some(collector) = &mut combined {
                        if let Err(error) = collector.merge(&partial) {
                            worker_error.get_or_insert(error);
                        }
                    } else {
                        combined = Some(partial);
                    }
                }
                Ok(Err(error)) => {
                    worker_error.get_or_insert(error);
                }
                Err(_) => {
                    worker_error.get_or_insert_with(|| {
                        VeinError::internal("checkpoint statistics worker panicked")
                    });
                }
            }
        }
        produced?;
        if let Some(error) = worker_error {
            return Err(error);
        }
        let collector = combined.ok_or_else(|| {
            VeinError::internal("checkpoint statistics produced no worker summaries")
        })?;
        finish_collection(collector, expected_rows, batches)
    })
}

fn finish_collection(
    collector: common::statistics::StatisticsCollector,
    expected_rows: u64,
    batches: usize,
) -> Result<(catalog::TableStatistics, usize)> {
    let statistics = collector.finish();
    if statistics.num_rows != Some(expected_rows) {
        return Err(VeinError::internal(
            "checkpoint synopsis does not cover its complete population",
        ));
    }
    Ok((statistics, batches))
}
