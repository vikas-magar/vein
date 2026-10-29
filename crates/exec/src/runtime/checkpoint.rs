//! Query-local intermediate relations used between adaptive execution stages.
//!
//! A checkpoint keeps reference-backed batches while their source arrays fit
//! the query memory budget. If retaining another batch would exceed that
//! budget, existing batches and subsequent input are written to a query-scoped
//! Arrow IPC spill run. Replaying an in-memory checkpoint clones only view
//! metadata and array handles; spilling is the explicit materialization path.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::collections::HashMap;

use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::FileReader;
use common::Result;
use common::identifiers::{IntermediateSourceId, QueryId};
use error::VeinError;

use crate::memory::QueryMemoryTracker;
use crate::pipeline::{DataChunkView, PipelineNode, Source};
use crate::pipeline::view::ViewMemoryLease;
use crate::spill::{IpcRunWriter, PendingSpillRun, SpillRun, SpillWriteHandle};

#[path = "checkpoint_statistics.rs"]
mod statistics;

enum CheckpointState {
    CollectingMemory(Vec<DataChunkView>),
    CollectingSpill(Box<SpillingCheckpoint>),
    SealedMemory(Arc<[DataChunkView]>),
    SealedSpill(SpillRun),
    Failed,
}

/// A completed or in-progress intermediate relation shared by a producer and
/// one or more downstream stage sources.
///
/// The schema is immutable for the relation lifetime. Memory accounting is
/// shared with the rest of the query, and spilled bytes use the existing
/// query-level spill quota and RAII cleanup lifecycle.
#[derive(Clone)]
pub struct IntermediateRelation {
    schema: SchemaRef,
    tracker: QueryMemoryTracker,
    state: Arc<Mutex<CheckpointState>>,
    rows: Arc<AtomicU64>,
}

/// Query-scoped lookup for sealed relations referenced by physical fragments.
///
/// Registration handles own the registry entry and remove it when the
/// continuation stage finishes or is cancelled.
#[derive(Clone, Default)]
pub struct IntermediateSourceRegistry {
    entries: Arc<Mutex<HashMap<(QueryId, IntermediateSourceId), IntermediateRelation>>>,
}

/// RAII registration keeping a checkpoint relation available to a stage.
pub struct IntermediateSourceRegistration {
    key: (QueryId, IntermediateSourceId),
    entries: Weak<Mutex<HashMap<(QueryId, IntermediateSourceId), IntermediateRelation>>>,
}

impl IntermediateSourceRegistry {
    /// Register one relation under a stable query-local source identifier.
    pub fn register(
        &self,
        query_id: QueryId,
        source_id: IntermediateSourceId,
        relation: IntermediateRelation,
    ) -> Result<IntermediateSourceRegistration> {
        let key = (query_id, source_id);
        let mut entries = self.entries.lock()
            .map_err(|_| VeinError::internal("intermediate source registry poisoned"))?;
        if entries.contains_key(&key) {
            return Err(VeinError::internal("intermediate source identifier already registered"));
        }
        entries.insert(key, relation);
        Ok(IntermediateSourceRegistration {
            key,
            entries: Arc::downgrade(&self.entries),
        })
    }

    /// Open a fresh reader for a registered relation.
    pub fn source(
        &self,
        query_id: QueryId,
        source_id: IntermediateSourceId,
    ) -> Result<IntermediateRelationSource> {
        let relation = self.entries.lock()
            .map_err(|_| VeinError::internal("intermediate source registry poisoned"))?
            .get(&(query_id, source_id))
            .cloned()
            .ok_or_else(|| VeinError::internal("intermediate source is not registered for this query"))?;
        relation.source()
    }
}

impl Drop for IntermediateSourceRegistration {
    fn drop(&mut self) {
        if let Some(entries) = self.entries.upgrade()
            && let Ok(mut entries) = entries.lock() {
            entries.remove(&self.key);
        }
    }
}

/// Result of running a physical task into a replayable intermediate relation.
pub struct CheckpointTaskResult {
    /// Sealed reference-backed output, spilling under the query's memory budget.
    pub relation: IntermediateRelation,
    /// Number of rows written to the checkpoint.
    pub row_count: u64,
    /// Source bytes and wall-clock time for the producer task.
    pub metrics: crate::runtime::TaskMetrics,
}

impl IntermediateRelation {
    /// Create an empty relation whose retained arrays share `tracker`'s budget.
    pub fn new(schema: SchemaRef, tracker: QueryMemoryTracker) -> Self {
        Self {
            schema,
            tracker,
            state: Arc::new(Mutex::new(CheckpointState::CollectingMemory(Vec::new()))),
            rows: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Create a pipeline sink that appends batches to this relation.
    pub fn sink(&self) -> IntermediateRelationSink {
        IntermediateRelationSink {
            relation: self.clone(),
            sealed: false,
            rows: 0,
        }
    }

    /// Create a source over a sealed relation.
    pub fn source(&self) -> Result<IntermediateRelationSource> {
        let state = self
            .state
            .lock()
            .map_err(|_| VeinError::internal("intermediate relation state poisoned"))?;
        let source = match &*state {
            CheckpointState::SealedMemory(batches) => CheckpointInput::Memory(Arc::clone(batches)),
            CheckpointState::SealedSpill(run) => CheckpointInput::Spill(run.clone()),
            CheckpointState::CollectingMemory(_) | CheckpointState::CollectingSpill(_) => {
                return Err(VeinError::internal(
                    "cannot read an intermediate relation before its producer seals it",
                ));
            }
            CheckpointState::Failed => {
                return Err(VeinError::internal(
                    "cannot read an intermediate relation whose producer failed",
                ));
            }
        };
        Ok(IntermediateRelationSource {
            schema: Arc::clone(&self.schema),
            input: source,
        })
    }

    /// Number of rows retained after the producer seals this relation.
    pub fn row_count(&self) -> Result<u64> {
        let state = self
            .state
            .lock()
            .map_err(|_| VeinError::internal("intermediate relation state poisoned"))?;
        match &*state {
            CheckpointState::SealedMemory(_) | CheckpointState::SealedSpill(_) => {
                Ok(self.rows.load(Ordering::Acquire))
            }
            CheckpointState::CollectingMemory(_) | CheckpointState::CollectingSpill(_) => {
                Err(VeinError::internal(
                    "intermediate relation row count is unavailable before sealing",
                ))
            }
            CheckpointState::Failed => Err(VeinError::internal(
                "intermediate relation producer failed",
            )),
        }
    }

    fn append(&self, mut view: DataChunkView) -> Result<()> {
        view.validate_schema(&self.schema)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| VeinError::internal("intermediate relation state poisoned"))?;

        match &mut *state {
            CheckpointState::CollectingMemory(batches) => {
                match account_retained_arrays(&self.tracker, &mut view) {
                    Ok(()) => {
                        self.add_rows(view.num_rows());
                        batches.push(view);
                        Ok(())
                    }
                    Err(error) if is_memory_limit_error(&error) => {
                        let previous = std::mem::replace(&mut *state, CheckpointState::Failed);
                        let CheckpointState::CollectingMemory(previous) = previous else {
                            unreachable!("state was checked while holding its lock")
                        };
                        match self.begin_spill(previous) {
                            Ok(mut spilling) => {
                                if let Err(error) = self.write_spill_view(&mut spilling.writer, &view) {
                                    *state = CheckpointState::Failed;
                                    return Err(error);
                                }
                                self.add_rows(view.num_rows());
                                *state = CheckpointState::CollectingSpill(Box::new(spilling));
                                tracing::info!(
                                    target: "vein.statistics",
                                    event = "intermediate_checkpoint_spilling",
                                    reason = "query_memory_limit",
                                    memory_used_bytes = self.tracker.used(),
                                    memory_limit_bytes = self.tracker.limit(),
                                    "intermediate relation switched from shared memory to IPC spill"
                                );
                                Ok(())
                            }
                            Err(error) => {
                                *state = CheckpointState::Failed;
                                Err(error)
                            }
                        }
                    }
                    Err(error) => Err(error),
                }
            }
            CheckpointState::CollectingSpill(spilling) => {
                if let Err(error) = self.write_spill_view(&mut spilling.writer, &view) {
                    *state = CheckpointState::Failed;
                    return Err(error);
                }
                self.add_rows(view.num_rows());
                Ok(())
            }
            CheckpointState::SealedMemory(_) | CheckpointState::SealedSpill(_) => Err(
                VeinError::internal("cannot append to a sealed intermediate relation"),
            ),
            CheckpointState::Failed => Err(VeinError::internal(
                "cannot append to a failed intermediate relation",
            )),
        }
    }

    fn begin_spill(&self, batches: Vec<DataChunkView>) -> Result<SpillingCheckpoint> {
        let mut pending = self.tracker.spill_manager().create_run()?;
        let output = pending.writer()?;
        let mut writer = IpcRunWriter::new(
            output,
            self.schema.as_ref(),
            "create intermediate relation IPC writer",
        )?;
        for batch in batches {
            self.write_spill_view(&mut writer, &batch)?;
        }
        Ok(SpillingCheckpoint { pending, writer })
    }

    fn add_rows(&self, rows: usize) {
        let rows = u64::try_from(rows).unwrap_or(u64::MAX);
        let _ = self.rows.fetch_update(
            Ordering::AcqRel,
            Ordering::Acquire,
            |current| Some(current.saturating_add(rows)),
        );
    }

    fn write_spill_view(
        &self,
        writer: &mut IpcRunWriter<SpillWriteHandle>,
        view: &DataChunkView,
    ) -> Result<()> {
        let report = writer.write_view(view, "write intermediate relation IPC batch")?;
        tracing::debug!(
            target: "vein.audit",
            event = "intermediate_checkpoint_batch_spilled",
            rows = view.num_rows(),
            columns = view.num_columns(),
            shared_columns = report.shared_columns,
            gathered_columns = report.gathered_columns,
            gathered_output_buffer_bytes = report.gathered_output_buffer_bytes,
            "intermediate relation batch spilled"
        );
        Ok(())
    }

    fn seal(&self) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| VeinError::internal("intermediate relation state poisoned"))?;
        let previous = std::mem::replace(&mut *state, CheckpointState::Failed);
        *state = match previous {
            CheckpointState::CollectingMemory(batches) => {
                let rows = self.rows.load(Ordering::Acquire);
                tracing::info!(
                    target: "vein.statistics",
                    event = "intermediate_checkpoint_sealed",
                    storage = "shared_memory",
                    batches = batches.len(),
                    rows,
                    query_reserved_memory_bytes = self.tracker.used(),
                    query_memory_limit_bytes = self.tracker.limit(),
                    "intermediate relation sealed"
                );
                CheckpointState::SealedMemory(Arc::from(batches))
            }
            CheckpointState::CollectingSpill(spilling) => {
                let SpillingCheckpoint { pending, writer } = *spilling;
                let output = writer.finish("finish intermediate relation IPC run")?;
                let run = pending.commit(output)?;
                tracing::info!(
                    target: "vein.statistics",
                    event = "intermediate_checkpoint_sealed",
                    storage = "ipc_spill",
                    spill_bytes = run.bytes(),
                    "intermediate relation sealed"
                );
                CheckpointState::SealedSpill(run)
            }
            CheckpointState::SealedMemory(batches) => CheckpointState::SealedMemory(batches),
            CheckpointState::SealedSpill(run) => CheckpointState::SealedSpill(run),
            CheckpointState::Failed => {
                return Err(VeinError::internal("intermediate relation producer failed"));
            }
        };
        Ok(())
    }
}

struct SpillingCheckpoint {
    pending: PendingSpillRun,
    writer: IpcRunWriter<SpillWriteHandle>,
}

/// Pipeline sink for an [`IntermediateRelation`].
pub struct IntermediateRelationSink {
    relation: IntermediateRelation,
    sealed: bool,
    rows: u64,
}

#[async_trait::async_trait]
impl PipelineNode for IntermediateRelationSink {
    async fn push(&mut self, batch: DataChunkView) -> Result<()> {
        let rows = u64::try_from(batch.num_rows())
            .map_err(|_| VeinError::internal("intermediate relation row count overflow"))?;
        let next_rows = self.rows.checked_add(rows)
            .ok_or_else(|| VeinError::internal("intermediate sink row count overflow"))?;
        self.relation.append(batch)?;
        self.rows = next_rows;
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        self.relation.seal()?;
        self.sealed = true;
        Ok(())
    }
}

impl crate::pipeline::Sink for IntermediateRelationSink {
    fn row_count(&self) -> u64 {
        self.rows
    }
}

impl Drop for IntermediateRelationSink {
    fn drop(&mut self) {
        if !self.sealed
            && let Ok(mut state) = self.relation.state.lock() {
            *state = CheckpointState::Failed;
        }
    }
}

enum CheckpointInput {
    Memory(Arc<[DataChunkView]>),
    Spill(SpillRun),
}

/// Replayable source for a sealed [`IntermediateRelation`].
pub struct IntermediateRelationSource {
    schema: SchemaRef,
    input: CheckpointInput,
}

impl IntermediateRelationSource {
    fn for_each_batch(&mut self, mut visit: impl FnMut(DataChunkView) -> Result<()>) -> Result<()> {
        for batch in self.batches()? {
            visit(batch?)?;
        }
        Ok(())
    }

    fn batches(&self) -> Result<Box<dyn Iterator<Item = Result<DataChunkView>> + Send + '_>> {
        match &self.input {
            CheckpointInput::Memory(batches) => {
                Ok(Box::new(batches.iter().cloned().map(Ok)))
            }
            CheckpointInput::Spill(run) => {
                let reader = FileReader::try_new(run.open()?, None).map_err(|error| {
                    VeinError::internal(format!("open intermediate IPC relation: {error}"))
                })?;
                Ok(Box::new(reader.map(|batch| {
                    let batch = batch.map_err(|error| {
                        VeinError::internal(format!("read intermediate IPC relation: {error}"))
                    })?;
                    if batch.schema().as_ref() != self.schema.as_ref() {
                        return Err(VeinError::internal(
                            "intermediate IPC relation schema changed while reading",
                        ));
                    }
                    crate::pipeline::arrow_adapter::from_record_batch(batch)
                })))
            }
        }
    }
}

#[async_trait::async_trait]
impl Source for IntermediateRelationSource {
    async fn execute(&mut self, next: &mut dyn PipelineNode) -> Result<()> {
        for batch in self.batches()? {
            if next.finished() { break; }
            next.push(batch?).await?;
        }
        next.flush().await
    }
}

fn account_retained_arrays(tracker: &QueryMemoryTracker, view: &mut DataChunkView) -> Result<()> {
    let tracker_id = tracker.identity();
    view.account_source_arrays(tracker_id, |identity, bytes| {
        let reservation = tracker.lease_retained_array(
            identity,
            bytes,
            "intermediate relation retained arrays",
        )?;
        let lease: Arc<dyn ViewMemoryLease> = reservation;
        Ok(lease)
    })
}

fn is_memory_limit_error(error: &VeinError) -> bool {
    matches!(error, VeinError::ResourceExhausted { resource, .. } if resource == "intermediate relation retained arrays")
}

#[cfg(test)]
#[path = "checkpoint_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "checkpoint_statistics_tests.rs"]
mod statistics_tests;
