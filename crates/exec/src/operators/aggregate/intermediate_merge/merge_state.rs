use super::*;

impl AggregateSink {
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
