use super::*;

mod merge;
mod merge_state;
mod local;

impl AggregateSink {
    /// Returns the number of currently retained groups.
    pub fn group_count(&self) -> usize {
        self.groups.len()
    }

    #[allow(dead_code)] // Used to serialize one group without finalizing its accumulators.
    pub(super) fn intermediate_group_state(&self, slot: usize) -> Result<AggregateIntermediateGroup> {
        let group = self
            .groups
            .get(slot)
            .ok_or_else(|| VeinError::internal("aggregate group slot is out of bounds"))?;
        let aggregate_states = self
            .aggregate_states
            .iter()
            .map(|state| encode_state(state.as_ref(), slot))
            .collect::<Result<Vec<_>>>()?;
        let keys = (0..self.plan.groups as usize)
            .map(|column| -> Result<Option<AggregateGroupKeyRef>> {
                if !self.grouping_sets_by_column[column].contains(&group.grouping_set_id) {
                    return Ok(None);
                }
                let location = group.key_location.ok_or_else(|| {
                    VeinError::internal("aggregate group with keys has no retained key location")
                })?;
                let key = self.group_key_batches[location.batch][column]
                    .as_ref()
                    .ok_or_else(|| VeinError::internal("aggregate group key column is missing"))?;
                Ok(Some(AggregateGroupKeyRef::from_column(
                    key.clone(),
                    location.row,
                )?))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(AggregateIntermediateGroup {
            hash: group.hash,
            grouping_set_id: group.grouping_set_id,
            keys,
            aggregate_states,
        })
    }
}

fn encode_state(state: &dyn AggregateAccumulator, group: usize) -> Result<Arc<Vec<u8>>> {
    let mut payload = Vec::new();
    state.encode_group(group, &mut payload)?;
    Ok(Arc::new(payload))
}
