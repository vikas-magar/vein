//! Memory estimates and reservations for decoded aggregate spill batches.

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::ArrayRef;
use common::Result;
use error::VeinError;

use super::{AggregateSpillSchema, SPILL_WORKSPACE_BUDGET_DIVISOR};
use crate::memory::{MemoryReservation, QueryMemoryTracker};
use crate::pipeline::DataChunkView;

pub(super) struct AggregateSpillBatchMemory {
    pub(super) workspace_bytes: usize,
    pub(super) retained_key_bytes: usize,
}

impl AggregateSpillBatchMemory {
    pub(super) fn total_bytes(&self) -> Result<usize> {
        self.workspace_bytes
            .checked_add(self.retained_key_bytes)
            .ok_or_else(|| VeinError::internal("aggregate spill decoded size overflow"))
    }
}

impl AggregateSpillSchema {
    pub(super) fn spill_workspace_target(&self, tracker: &QueryMemoryTracker) -> usize {
        let used_bytes = tracker.used();
        let available_bytes = tracker.limit().saturating_sub(used_bytes);
        let target_bytes = (available_bytes / 4)
            .min(tracker.limit() / SPILL_WORKSPACE_BUDGET_DIVISOR);
        tracing::debug!(
            target: "vein.audit",
            event = "aggregate_spill_workspace_budget",
            used_bytes,
            available_bytes,
            query_limit_bytes = tracker.limit(),
            target_workspace_bytes = target_bytes,
            "computed bounded spill batch target from query memory budget"
        );
        target_bytes
    }

    pub(super) fn decoded_batch_memory(&self, batch: &DataChunkView) -> Result<AggregateSpillBatchMemory> {
        Ok(AggregateSpillBatchMemory {
            workspace_bytes: self.decode_workspace_bytes(batch)?,
            retained_key_bytes: self.retained_key_bytes(batch)?,
        })
    }

    pub(super) fn decode_workspace_bytes(&self, batch: &DataChunkView) -> Result<usize> {
        let first_key = 2;
        let end_key = first_key + self.key_types.len();
        (0..batch.num_columns())
            .filter(|column| !(first_key..end_key).contains(column))
            .try_fold(0_usize, |total, column| {
                batch.source_arrays_for_column(column)?.iter().try_fold(
                    total,
                    |sum, array| {
                        sum.checked_add(array.get_array_memory_size()).ok_or_else(|| {
                            VeinError::internal("aggregate spill workspace size overflow")
                        })
                    },
                )
            })
    }

    fn retained_key_bytes(&self, batch: &DataChunkView) -> Result<usize> {
        let mut bytes = 0_usize;
        self.for_each_unique_key_array(batch, |_, array| {
            bytes = bytes
                .checked_add(array.get_array_memory_size())
                .ok_or_else(|| VeinError::internal("aggregate spill key size overflow"))?;
            Ok(())
        })?;
        Ok(bytes)
    }

    fn for_each_unique_key_array(
        &self,
        batch: &DataChunkView,
        mut visit: impl FnMut(usize, &ArrayRef) -> Result<()>,
    ) -> Result<()> {
        let first_key = 2;
        let end_key = first_key + self.key_types.len();
        let mut identities = HashSet::with_capacity(self.key_types.len());
        for column in first_key..end_key {
            for array in batch.source_arrays_for_column(column)? {
                let identity = Arc::as_ptr(&array) as *const () as usize;
                if identities.insert(identity) {
                    visit(identity, &array)?;
                }
            }
        }
        Ok(())
    }

    pub(super) fn reserve_decoded_batch(
        &self,
        batch: &DataChunkView,
        tracker: &QueryMemoryTracker,
    ) -> Result<(MemoryReservation, Vec<Arc<MemoryReservation>>)> {
        let workspace_bytes = self.decode_workspace_bytes(batch)?;
        let used_before_keys = tracker.used();
        let mut retained_key_bytes = 0_usize;
        let mut key_leases = Vec::with_capacity(self.key_types.len());
        self.for_each_unique_key_array(batch, |identity, array| {
            let array_bytes = array.get_array_memory_size();
            let lease = tracker
                .lease_retained_array(identity, array_bytes, "aggregate retained spill keys")
                .map_err(|error| {
                    tracing::warn!(
                        target: "vein.audit",
                        event = "aggregate_spill_key_reservation_failed",
                        batch_rows = batch.num_rows(),
                        requested_key_bytes = array_bytes,
                        retained_key_bytes,
                        workspace_bytes,
                        used_bytes = tracker.used(),
                        available_bytes = tracker.limit().saturating_sub(tracker.used()),
                        limit_bytes = tracker.limit(),
                        prior_used_bytes = used_before_keys,
                        "could not reserve retained aggregate spill key memory"
                    );
                    error
                })?;
            retained_key_bytes = retained_key_bytes
                .checked_add(array_bytes)
                .ok_or_else(|| VeinError::internal("aggregate spill key size overflow"))?;
            key_leases.push(lease);
            Ok(())
        })?;

        let used_before_workspace = tracker.used();
        let workspace = tracker.reserve(workspace_bytes, "aggregate spill decode workspace")
            .map_err(|error| {
                tracing::warn!(
                    target: "vein.audit",
                    event = "aggregate_spill_workspace_reservation_failed",
                    batch_rows = batch.num_rows(),
                    requested_workspace_bytes = workspace_bytes,
                    retained_key_bytes,
                    used_bytes = used_before_workspace,
                    available_bytes = tracker.limit().saturating_sub(used_before_workspace),
                    limit_bytes = tracker.limit(),
                    prior_used_bytes = used_before_keys,
                    "could not reserve aggregate spill decode workspace"
                );
                error
            })?;
        Ok((workspace, key_leases))
    }
}
