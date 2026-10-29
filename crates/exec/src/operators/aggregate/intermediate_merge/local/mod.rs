//! Typed worker-state combination; serialization is reserved for actual spills.
use super::*;

impl AggregateSink {
    /// Finds or creates one target group while retaining a worker key batch once.
    /// State updates happen only after preparation succeeds, so a memory failure
    /// here can safely fall back to spilling the still-unmerged source suffix.
    pub(in crate::operators::aggregate) fn prepare_local_merge_group(
        &mut self,
        local: &AggregateSink,
        slot: usize,
        batches: &mut [Option<usize>],
    ) -> Result<usize> {
        let group = &local.groups[slot];
        let (keys, row) = group.key_location.map_or((&[][..], None), |location| {
            (
                local.group_key_batches[location.batch].as_slice(),
                Some(location.row),
            )
        });
        if let Some(slot) = self.matching_merge_group(group.hash, group.grouping_set_id, keys, row)
        {
            return Ok(slot);
        }

        let before = self.memory.bytes();
        self.reserve_merge_group()?;
        let reservation = self.groups.try_reserve(1);
        self.rollback_failed_reservation(before, reservation, "cannot reserve combined group")?;
        let location = match group.key_location {
            Some(location) => {
                let target_batch = if let Some(batch) = batches[location.batch] {
                    batch
                } else {
                    let reservation = self.group_key_batches.try_reserve(1);
                    self.rollback_failed_reservation(
                        before,
                        reservation,
                        "cannot reserve combined key batch",
                    )?;
                    let reservation = self.group_key_batch_arrays.try_reserve(1);
                    self.rollback_failed_reservation(
                        before,
                        reservation,
                        "cannot reserve combined key arrays",
                    )?;
                    let reservation = self.group_key_batch_leases.try_reserve(1);
                    self.rollback_failed_reservation(
                        before,
                        reservation,
                        "cannot reserve combined key leases",
                    )?;
                    let batch = self.group_key_batches.len();
                    self.group_key_batches
                        .push(local.group_key_batches[location.batch].clone());
                    self.group_key_batch_arrays
                        .push(local.group_key_batch_arrays[location.batch].clone());
                    // The sinks share the same tracker. Share the existing RAII leases
                    // as well as arrays; do not charge or copy their payloads again.
                    self.group_key_batch_leases
                        .push(local.group_key_batch_leases[location.batch].clone());
                    batches[location.batch] = Some(batch);
                    batch
                };
                Some(GroupKeyLocation {
                    batch: target_batch,
                    row: location.row,
                })
            }
            None => None,
        };
        let slot = Self::push_group(
            &mut self.groups,
            &mut self.aggregate_states,
            group.hash,
            group.grouping_set_id,
            location,
        )?;
        self.index.insert_unique(GroupIndexEntry {
            hash: group.hash,
            slot,
        })?;
        Ok(slot)
    }
}
