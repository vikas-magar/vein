//! Memory-budgeted Arrow IPC writer for aggregate spill runs.

use std::collections::HashSet;
use std::sync::Arc;

use common::Result;

use crate::memory::MemoryReservation;
use crate::spill::{IpcRunOutput, IpcRunWriter};

use super::{AggregateIntermediateGroup, AggregateSpillSchema, SPILL_RECORD_BATCH_SIZE};

/// Bounded writer shared by aggregate flush and recursive repartition paths.
pub(super) struct AggregateSpillWriter<'a, W: IpcRunOutput> {
    layout: &'a AggregateSpillSchema,
    writer: IpcRunWriter<W>,
    target_workspace_bytes: usize,
    pending: Vec<AggregateIntermediateGroup>,
    retained_key_leases: Vec<Arc<MemoryReservation>>,
    retained_key_identities: HashSet<usize>,
}

impl<'a, W: IpcRunOutput> AggregateSpillWriter<'a, W> {
    pub(super) fn new(
        layout: &'a AggregateSpillSchema,
        output: W,
        target_workspace_bytes: usize,
    ) -> Result<Self> {
        let writer = IpcRunWriter::new(output, &layout.schema, "create aggregate IPC writer")?;
        Ok(Self {
            layout,
            writer,
            target_workspace_bytes,
            pending: Vec::with_capacity(SPILL_RECORD_BATCH_SIZE),
            retained_key_leases: Vec::new(),
            retained_key_identities: HashSet::new(),
        })
    }

    pub(super) fn push(&mut self, group: AggregateIntermediateGroup) -> Result<()> {
        self.pending.push(group);
        if self.pending.len() == SPILL_RECORD_BATCH_SIZE {
            self.flush()?;
        }
        Ok(())
    }

    pub(super) fn push_retained(
        &mut self,
        group: AggregateIntermediateGroup,
        leases: &[Arc<MemoryReservation>],
    ) -> Result<()> {
        for lease in leases {
            let identity = Arc::as_ptr(lease) as usize;
            if self.retained_key_identities.insert(identity) {
                self.retained_key_leases.push(Arc::clone(lease));
            }
        }
        self.push(group)
    }

    pub(super) fn flush(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }

        Self::write_bounded_batch(
            self.layout,
            self.target_workspace_bytes,
            &mut self.writer,
            &self.pending,
        )?;
        self.pending.clear();
        self.retained_key_leases.clear();
        self.retained_key_identities.clear();
        Ok(())
    }

    pub(super) fn finish(mut self) -> Result<W> {
        self.flush()?;
        self.writer.finish("finish aggregate IPC run")
    }

    fn write_bounded_batch(
        layout: &AggregateSpillSchema,
        target_workspace_bytes: usize,
        writer: &mut IpcRunWriter<W>,
        groups: &[AggregateIntermediateGroup],
    ) -> Result<()> {
        let encoded = layout.encode_batch(groups)?;
        let memory = layout.decoded_batch_memory(&encoded)?;
        let decoded_bytes = memory.total_bytes()?;
        if decoded_bytes <= target_workspace_bytes || groups.len() == 1 {
            if decoded_bytes > target_workspace_bytes {
                tracing::warn!(
                    target: "vein.audit",
                    event = "aggregate_spill_batch_exceeds_memory_target",
                    rows = groups.len(),
                    workspace_bytes = memory.workspace_bytes,
                    retained_key_bytes = memory.retained_key_bytes,
                    decoded_bytes,
                    target_workspace_bytes,
                    "single spill record cannot be split further"
                );
            }
            return writer
                .write_view(&encoded, "write aggregate IPC batch")
                .map(|_| ());
        }

        let midpoint = groups.len() / 2;
        tracing::debug!(
            target: "vein.audit",
            event = "aggregate_spill_batch_split",
            rows = groups.len(),
            workspace_bytes = memory.workspace_bytes,
            retained_key_bytes = memory.retained_key_bytes,
            decoded_bytes,
            target_workspace_bytes,
            left_rows = midpoint,
            right_rows = groups.len() - midpoint,
            "split spill record batch to fit query memory budget"
        );
        drop(encoded);
        Self::write_bounded_batch(
            layout,
            target_workspace_bytes,
            writer,
            &groups[..midpoint],
        )?;
        Self::write_bounded_batch(
            layout,
            target_workspace_bytes,
            writer,
            &groups[midpoint..],
        )
    }
}
