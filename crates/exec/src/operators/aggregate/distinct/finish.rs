use super::super::*;
use super::DistinctCollector;

impl AggregateSink {
    pub(in crate::operators::aggregate) fn finish_distinct_counts(
        &mut self,
        batch_rows: NonZeroUsize,
    ) -> Result<()> {
        let collectors = std::mem::take(&mut self.distinct_collectors);
        for collector in collectors {
            let DistinctCollector {
                aggregate_index,
                grouping_set_id,
                parent_columns,
                sink,
            } = collector;
            let mut sink = *sink;
            sink.finish_with(batch_rows, |batch| {
                self.merge_distinct_batch(
                    aggregate_index,
                    grouping_set_id,
                    &parent_columns,
                    &batch,
                )
            })?;
        }
        Ok(())
    }

    fn merge_distinct_batch(
        &mut self,
        aggregate_index: usize,
        grouping_set_id: usize,
        parent_columns: &[usize],
        batch: &DataChunkView,
    ) -> Result<()> {
        let distinct_position = parent_columns.len();
        let key_columns = (0..distinct_position)
            .map(|column| batch.project(&[column]).and_then(GroupKeyColumn::from_view))
            .collect::<Result<Vec<_>>>()?;
        let empty_states = self.empty_intermediate_states()?;

        for row in 0..batch.num_rows() {
            if batch.is_null(distinct_position, row)? {
                continue;
            }
            let mut hasher = self.hash_builder.build_hasher();
            grouping_set_id.hash(&mut hasher);
            for (&parent_column, key) in parent_columns.iter().zip(&key_columns) {
                    key.hash_row(parent_column as u32, row, &mut hasher);
            }
            let hash = hasher.finish();
            let mut keys = vec![None; self.plan.groups as usize];
            for (child_column, &parent_column) in parent_columns.iter().enumerate() {
                keys[parent_column] = Some(AggregateGroupKeyRef::from_column(
                    key_columns[child_column].clone(),
                    row,
                )?);
            }
            let mut aggregate_states = empty_states.clone();
            aggregate_states[aggregate_index] = AggregateIntermediateState::Count(1);
            let group = AggregateIntermediateGroup {
                hash,
                grouping_set_id,
                keys,
                aggregate_states,
            };
            match self.merge_spilled_group(group.clone()) {
                Ok(()) => {}
                Err(VeinError::ResourceExhausted { .. }) if !self.groups.is_empty() => {
                    self.flush_current_run()?;
                    self.merge_spilled_group(group)?;
                }
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    fn empty_intermediate_states(&self) -> Result<Vec<AggregateIntermediateState>> {
        self.plan
            .aggs
            .iter()
            .zip(&self.plan.aggregate_input_types)
            .zip(self.plan.types.iter().skip(self.plan.groups as usize))
            .map(|((&op, input_type), result_type)| {
                let mut state = AggregateStateColumn::new(op, input_type, result_type)?;
                state.push_group();
                state.intermediate_state(0)
            })
            .collect()
    }
}
