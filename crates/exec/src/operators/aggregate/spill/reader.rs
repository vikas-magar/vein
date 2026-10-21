use super::*;

impl AggregateSpillSchema {
    /// Opens a run after validating its complete schema and format version.
    pub(in crate::operators::aggregate) fn open_reader<R: Read + Seek>(
        &self,
        input: R,
    ) -> Result<AggregateSpillReader<R>> {
        let reader = FileReader::try_new(input, None)
            .map_err(|error| spill_error("open aggregate IPC run", error))?;
        if reader.schema().as_ref() != self.schema.as_ref() {
            return Err(VeinError::internal(
                "aggregate IPC run schema does not match the active aggregate plan",
            ));
        }
        Ok(AggregateSpillReader {
            reader,
            layout: self.clone(),
        })
    }

    fn decode_batch(&self, batch: &DataChunkView) -> Result<Vec<AggregateIntermediateGroup>> {
        if !batch
            .schema()
            .is_some_and(|schema| schema.as_ref() == self.schema.as_ref())
        {
            return Err(VeinError::internal(
                "aggregate IPC batch schema changed within a run",
            ));
        }
        let grouping_set_array = batch.materialize_column(0)?;
        let grouping_set_ids = grouping_set_array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| VeinError::internal("invalid aggregate spill grouping-set column"))?;
        let hash_array = batch.materialize_column(1)?;
        let hashes = hash_array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| VeinError::internal("invalid aggregate spill hash column"))?;
        let state_array = batch.materialize_column(self.key_types.len() + 2)?;
        let state_payloads = state_array
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| VeinError::internal("invalid aggregate spill state column"))?;
        let key_arrays = (0..self.key_types.len())
            .map(|index| batch.materialize_column(index + 2))
            .collect::<Result<Vec<_>>>()?;

        let mut groups = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            if grouping_set_ids.is_null(row) || hashes.is_null(row) || state_payloads.is_null(row) {
                return Err(VeinError::internal(
                    "aggregate spill metadata columns cannot contain NULL",
                ));
            }
            let grouping_set_id = usize::try_from(grouping_set_ids.value(row)).map_err(|_| {
                VeinError::internal("aggregate spill grouping-set ID overflows usize")
            })?;
            let grouping_set = self.grouping_sets.get(grouping_set_id).ok_or_else(|| {
                VeinError::internal("aggregate spill record has an invalid grouping-set ID")
            })?;
            let keys = (0..self.key_types.len())
                .map(|key_index| {
                    if grouping_set.contains(&key_index) {
                        AggregateGroupKeyRef::from_array(Arc::clone(&key_arrays[key_index]), row)
                            .map(Some)
                    } else {
                        Ok(None)
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            let aggregate_states =
                AggregateIntermediateGroup::decode_aggregate_states(state_payloads.value(row))?;
            groups.push(AggregateIntermediateGroup {
                hash: hashes.value(row),
                grouping_set_id,
                keys,
                aggregate_states,
            });
        }
        Ok(groups)
    }
}

impl<R: Read + Seek> AggregateSpillReader<R> {
    #[cfg(test)]
    pub(super) fn next_batch(&mut self) -> Result<Option<Vec<AggregateIntermediateGroup>>> {
        match self.reader.next() {
            Some(Ok(batch)) => {
                let view = crate::pipeline::arrow_adapter::from_record_batch(batch)?;
                self.layout.decode_batch(&view).map(Some)
            }
            Some(Err(error)) => Err(spill_error("read aggregate IPC batch", error)),
            None => Ok(None),
        }
    }

    pub(in crate::operators::aggregate) fn next_batch_charged(
        &mut self,
        tracker: &QueryMemoryTracker,
    ) -> Result<Option<AggregateSpillBatch>> {
        match self.reader.next() {
            Some(Ok(batch)) => {
                let batch = crate::pipeline::arrow_adapter::from_record_batch(batch)?;
                let (workspace, key_leases) = self.layout.reserve_decoded_batch(&batch, tracker)?;
                let groups = self.layout.decode_batch(&batch)?;
                drop(workspace);
                Ok(Some(AggregateSpillBatch {
                    groups,
                    _leases: key_leases,
                }))
            }
            Some(Err(error)) => Err(spill_error("read aggregate IPC batch", error)),
            None => Ok(None),
        }
    }
}
