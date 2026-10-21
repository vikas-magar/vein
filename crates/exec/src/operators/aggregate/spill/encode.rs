use super::*;
use super::keys::key_builder;
use crate::spill::IpcRunOutput;

impl AggregateSpillSchema {
    /// Writes groups incrementally as a versioned Arrow IPC run.
    ///
    /// The iterator should snapshot groups on demand so accumulator snapshots,
    /// Arrow builders, and record batches stay bounded by the run batch size.
    #[cfg(test)]
    pub(super) fn write_run<W, I>(&self, output: W, groups: I) -> Result<W>
    where
        W: IpcRunOutput,
        I: IntoIterator<Item=AggregateIntermediateGroup>,
    {
        let mut writer = AggregateSpillWriter::new(self, output, usize::MAX)?;
        for group in groups {
            writer.push(group)?;
        }
        writer.finish()
    }

    /// Streams snapshots from an indexed sink without retaining a second group
    /// list in memory.
    pub(in crate::operators::aggregate) fn write_run_with<W>(
        &self,
        output: W,
        tracker: &QueryMemoryTracker,
        group_count: usize,
        mut group_at: impl FnMut(usize) -> Result<AggregateIntermediateGroup>,
    ) -> Result<W>
    where
        W: IpcRunOutput,
    {
        let mut writer = AggregateSpillWriter::new(
            self,
            output,
            self.spill_workspace_target(tracker),
        )?;
        for slot in 0..group_count {
            writer.push(group_at(slot)?)?;
        }
        writer.finish()
    }

    pub(super) fn encode_batch(&self, groups: &[AggregateIntermediateGroup]) -> Result<DataChunkView> {
        let mut grouping_set_ids = Vec::with_capacity(groups.len());
        let mut hashes = Vec::with_capacity(groups.len());
        let mut key_builders = self
            .key_types
            .iter()
            .map(|data_type| key_builder(data_type, groups.len()))
            .collect::<Result<Vec<_>>>()?;
        let mut states = BinaryBuilder::new();
        let mut state_bytes = 0_usize;

        for group in groups {
            let grouping_set = self
                .grouping_sets
                .get(group.grouping_set_id)
                .ok_or_else(|| {
                    VeinError::internal("aggregate spill group has an invalid grouping-set ID")
                })?;
            if group.keys.len() != self.key_types.len() {
                return Err(VeinError::internal(
                    "aggregate spill group key count does not match its schema",
                ));
            }
            grouping_set_ids.push(u64::try_from(group.grouping_set_id).map_err(|_| {
                VeinError::internal("aggregate spill grouping-set ID exceeds UInt64")
            })?);
            hashes.push(group.hash);
            for (key_index, (builder, group_key)) in
                key_builders.iter_mut().zip(&group.keys).enumerate()
            {
                let included = grouping_set.contains(&key_index);
                match (included, group_key) {
                    (true, Some(key)) => builder.append(Some(key))?,
                    (true, None) => {
                        return Err(VeinError::internal(
                            "included aggregate spill key is missing from its group",
                        ));
                    }
                    (false, None) => builder.append(None)?,
                    (false, Some(_)) => {
                        return Err(VeinError::internal(
                            "excluded aggregate spill key is present in its group",
                        ));
                    }
                }
            }
            let payload = group.encode_aggregate_states()?;
            if payload.len() > i32::MAX as usize {
                return Err(VeinError::internal(
                    "aggregate spill state exceeds Arrow Binary offset capacity",
                ));
            }
            state_bytes = state_bytes
                .checked_add(payload.len())
                .filter(|&size| size <= i32::MAX as usize)
                .ok_or_else(|| {
                    VeinError::internal(
                        "aggregate spill batch exceeds Arrow Binary offset capacity",
                    )
                })?;
            states.append_value(payload);
        }

        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.key_types.len() + 3);
        columns.push(Arc::new(UInt64Array::from(grouping_set_ids)));
        columns.push(Arc::new(UInt64Array::from(hashes)));
        for builder in key_builders {
            columns.push(builder.finish()?);
        }
        columns.push(Arc::new(states.finish()) as ArrayRef);
        DataChunkView::from_arrays(
            columns,
            groups.len(),
            SelectionVector::new_dense(groups.len()),
        )?
            .with_schema(Arc::clone(&self.schema))
    }
}
