use std::sync::Arc;
use std::time::Instant;

use arrow::datatypes::DataType;

use crate::memory::QueryMemoryTracker;
use crate::pipeline::{DataChunkView, PipelineNode};
use common::Result;
use error::VeinError;

use super::super::hash_table::{HashTable, JoinKeys};
use super::super::spill::{JoinSpillRun, JoinSpillSchema, JoinSpillWriter};

/// Collects build-side chunks and publishes a reusable hash table at flush.
pub struct BuildSink {
    table: Option<HashTable>,
    spill_writer: Option<JoinSpillWriter>,
    spill_schema: Option<JoinSpillSchema>,
    spill_backup: Vec<DataChunkView>,
    build_keys: JoinKeys,
    tracker: QueryMemoryTracker,
    /// Rows absorbed, and the time spent hashing and inserting them.
    rows: u64,
    insert_us: u64,
    /// Worker-local tables skip spill backups and are merged before probing.
    is_worker_local: bool,
    /// Number of worker-local tables merged into this canonical sink.
    combined_workers: u64,
}

impl BuildSink {
    /// A sink that keys each build chunk on `build_keys` and will be probed on
    /// `probe_keys`.
    pub fn new(probe_keys: JoinKeys, build_keys: JoinKeys) -> Self {
        Self::with_memory_tracker(
            probe_keys,
            build_keys,
            QueryMemoryTracker::unlimited(common::cancellation::CancellationToken::new()),
        )
        .expect("unlimited join memory budget must create a build sink")
    }

    /// Create a build sink that charges its index to `tracker`.
    pub fn with_memory_tracker(
        probe_keys: JoinKeys,
        build_keys: JoinKeys,
        tracker: QueryMemoryTracker,
    ) -> Result<Self> {
        let table =
            HashTable::with_memory_tracker(build_keys.clone(), probe_keys, tracker.clone())?;
        Ok(Self {
            table: Some(table),
            spill_writer: None,
            spill_schema: None,
            spill_backup: Vec::new(),
            build_keys,
            tracker,
            rows: 0,
            insert_us: 0,
            is_worker_local: false,
            combined_workers: 0,
        })
    }

    /// Create a build sink that falls back to an IPC run when retained build
    /// state exceeds the query's memory budget.
    pub(crate) fn with_spill_support(
        probe_keys: JoinKeys,
        build_keys: JoinKeys,
        build_types: &[DataType],
        tracker: QueryMemoryTracker,
    ) -> Result<Self> {
        let schema = JoinSpillSchema::new(build_types);
        let (table, spill_writer) =
            match HashTable::with_memory_tracker(build_keys.clone(), probe_keys, tracker.clone()) {
                Ok(table) => (Some(table), None),
                Err(VeinError::ResourceExhausted { .. }) => (
                    None,
                    Some(JoinSpillWriter::new(tracker.clone(), schema.clone())?),
                ),
                Err(error) => return Err(error),
            };
        Ok(Self {
            table,
            spill_writer,
            spill_schema: Some(schema),
            spill_backup: Vec::new(),
            build_keys,
            tracker,
            rows: 0,
            insert_us: 0,
            is_worker_local: false,
            combined_workers: 0,
        })
    }

    /// Fork a private in-memory build table when the query has no memory cap.
    ///
    /// Bounded queries keep the canonical spill-capable sink: independent local
    /// tables could collectively exceed the budget before they are combined.
    pub(crate) fn fork_worker_local(&self) -> Result<Option<Self>> {
        if !self.tracker.is_unlimited() || self.spill_writer.is_some() {
            return Ok(None);
        }
        let Some(table) = self.table.as_ref() else {
            return Ok(None);
        };

        Ok(Some(Self {
            table: Some(table.fork_empty()?),
            spill_writer: None,
            spill_schema: None,
            spill_backup: Vec::new(),
            build_keys: self.build_keys.clone(),
            tracker: self.tracker.clone(),
            rows: 0,
            insert_us: 0,
            is_worker_local: true,
            combined_workers: 0,
        }))
    }

    /// Merge a finished local table into this canonical build sink.
    pub(crate) fn combine_worker(&mut self, mut local: Self) -> Result<()> {
        if !self.tracker.is_unlimited()
            || !local.is_worker_local
            || self.spill_writer.is_some()
            || local.spill_writer.is_some()
            || local.spill_schema.is_some()
            || !self.spill_backup.is_empty()
            || !local.spill_backup.is_empty()
            || self.build_keys != local.build_keys
            || self.tracker.identity() != local.tracker.identity()
        {
            return Err(VeinError::internal(
                "cannot combine incompatible or spill-capable join build states",
            ));
        }

        let local_table = local
            .table
            .take()
            .ok_or_else(|| VeinError::internal("worker-local join build table is unavailable"))?;
        self.table
            .as_mut()
            .ok_or_else(|| VeinError::internal("canonical join build table is unavailable"))?
            .merge(local_table)?;
        self.rows = self.rows.saturating_add(local.rows);
        self.insert_us = self.insert_us.saturating_add(local.insert_us);
        self.combined_workers = self.combined_workers.saturating_add(1);
        Ok(())
    }

    /// How many build rows have been absorbed.
    pub fn rows(&self) -> usize {
        self.rows as usize
    }

    /// Number of worker-local build tables merged into this canonical sink.
    pub fn combined_workers(&self) -> u64 {
        self.combined_workers
    }

    /// The finished table, ready to probe.
    pub fn finish(self) -> HashTable {
        self.table
            .expect("in-memory build sink cannot finish after spilling")
    }

    /// Finish either the retained hash table or its spill run.
    pub(crate) fn finish_with_spill(mut self) -> Result<BuildResult> {
        if let Some(table) = self.table.take() {
            return Ok(BuildResult::InMemory(Arc::new(table)));
        }
        let run = self
            .spill_writer
            .take()
            .ok_or_else(|| VeinError::internal("spilled join build has no writer"))?
            .finish()?;
        Ok(BuildResult::Spilled {
            run,
            schema: self
                .spill_schema
                .take()
                .ok_or_else(|| VeinError::internal("spilled join build has no schema"))?,
        })
    }
}

pub(crate) enum BuildResult {
    InMemory(Arc<HashTable>),
    Spilled {
        run: JoinSpillRun,
        schema: JoinSpillSchema,
    },
}

impl PipelineNode<DataChunkView> for BuildSink {
    fn push(&mut self, view: DataChunkView) -> Result<()> {
        self.rows += view.num_rows() as u64;
        let started = Instant::now();
        if let Some(writer) = self.spill_writer.as_mut() {
            writer.push(&view, &self.build_keys)?;
        } else {
            let table = self
                .table
                .as_mut()
                .ok_or_else(|| VeinError::internal("join build table is unavailable"))?;
            match table.insert(view.clone()) {
                Ok(()) => {
                    if self.spill_schema.is_some() {
                        self.spill_backup.push(view);
                    }
                }
                Err(VeinError::ResourceExhausted { .. }) if self.spill_schema.is_some() => {
                    let schema = self
                        .spill_schema
                        .as_ref()
                        .ok_or_else(|| VeinError::internal("join spill schema is unavailable"))?
                        .clone();
                    let mut writer = JoinSpillWriter::new(self.tracker.clone(), schema)?;
                    for retained in &self.spill_backup {
                        writer.push(retained, &self.build_keys)?;
                    }
                    writer.push(&view, &self.build_keys)?;
                    self.spill_writer = Some(writer);
                    self.spill_backup.clear();
                    self.table = None;
                    tracing::info!(
                        target: "vein.audit",
                        event = "join_build_spill_started",
                        rows = self.rows,
                        "join_build_spill_started"
                    );
                }
                Err(error) => return Err(error),
            }
        }
        self.insert_us += started.elapsed().as_micros() as u64;
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if self.is_worker_local {
            let table = self.table.as_ref().ok_or_else(|| {
                VeinError::internal("worker-local join build table is unavailable at flush")
            })?;
            tracing::debug!(
                target: "vein.audit",
                event = "join_build_worker_finished",
                rows = self.rows,
                table_rows = table.len(),
                build_chunks = table.chunk_count(),
                insert_us = self.insert_us,
                "join build worker state finished"
            );
            return Ok(());
        }
        if let Some(writer) = self.spill_writer.as_ref() {
            tracing::info!(
                target: "vein.audit",
                event = "join_build_spill_finished",
                rows = self.rows,
                spill_rows = writer.rows(),
                insert_us = self.insert_us,
                "join_build_spill_finished"
            );
            return Ok(());
        }
        let table = self
            .table
            .as_ref()
            .ok_or_else(|| VeinError::internal("join build table is unavailable at flush"))?;
        let (keys, key_materialize_us) = table.build_key_materialization();
        tracing::info!(
            target: "vein.audit",
            event = "join_build_finished",
            rows = self.rows,
            table_rows = table.len(),
            build_chunks = table.chunk_count(),
            insert_us = self.insert_us,
            combined_workers = self.combined_workers,
            key_shared_columns = keys.shared_columns,
            key_gathered_columns = keys.gathered_columns,
            key_empty_columns = keys.empty_columns,
            key_gathered_output_buffer_bytes = keys.gathered_output_buffer_bytes,
            key_materialize_us,
            "join_build_finished"
        );
        Ok(())
    }
}
