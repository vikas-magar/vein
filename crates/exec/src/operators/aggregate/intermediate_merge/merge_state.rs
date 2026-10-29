use super::*;

impl AggregateSink {
    /// Resolve retained keys identically for local combination and decoded spill.
    pub(super) fn matching_merge_group(
        &self, hash: u64, set_id: usize,
        keys: &[Option<Arc<GroupKeyColumn>>], row: Option<usize>,
    ) -> Option<usize> {
        self.index.find(hash, |entry| {
            let Some(existing) = self.groups.get(entry.slot) else { return false; };
            existing.grouping_set_id == set_id && self.grouping_sets[set_id].iter().all(|&column| {
                let Some(location) = existing.key_location else { return false; };
                let column = column as usize;
                keys[column].as_ref().zip(row).zip(
                    self.group_key_batches[location.batch][column].as_ref()
                ).is_some_and(|((key, row), retained)| key.rows_equal(row, retained, location.row))
            })
        }).map(|entry| entry.slot)
    }

    /// Reserve the common index and accumulator footprint before either merge path mutates it.
    pub(super) fn reserve_merge_group(&mut self) -> Result<()> {
        let bytes = size_of::<Group>().checked_add(
            size_of::<Option<GroupKeyLocation>>().checked_mul(self.plan.groups as usize)
                .ok_or_else(|| VeinError::internal("aggregate merge key size overflow"))?
        ).and_then(|bytes| self.aggregate_states.iter()
            .try_fold(bytes, |total, state| total.checked_add(state.estimated_bytes_per_group())))
            .and_then(|bytes| bytes.checked_add(size_of::<GroupIndexEntry>()))
            .ok_or_else(|| VeinError::internal("aggregate merge state size overflow"))?;
        self.memory.grow(bytes, "aggregate merge state")?;
        if let Err(error) = self.index.reserve(1) {
            self.memory.shrink(bytes);
            return Err(error);
        }
        Ok(())
    }

    pub(super) fn rollback_failed_reservation<E: std::fmt::Display>(
        &mut self,
        previous_bytes: usize,
        reservation: std::result::Result<(), E>,
        context: &str,
    ) -> Result<()> {
        reservation.map_err(|error| {
            self.memory.shrink(self.memory.bytes() - previous_bytes);
            VeinError::internal(format!("{context}: {error}"))
        })
    }

    pub(super) fn merge_aggregate_payloads(
        &mut self,
        slot: usize,
        payloads: &[Arc<Vec<u8>>],
    ) -> Result<()> {
        for (state, payload) in self.aggregate_states.iter_mut().zip(payloads) {
            let mut remaining = payload.as_slice();
            state.merge_encoded_group(slot, &mut remaining)?;
            if !remaining.is_empty() {
                return Err(VeinError::internal(
                    "aggregate kernel left trailing bytes in spill state",
                ));
            }
        }
        Ok(())
    }
}
