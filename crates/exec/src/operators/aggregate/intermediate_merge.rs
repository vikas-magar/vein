use super::*;

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
            .map(|state| state.intermediate_state(slot))
            .collect::<Result<Vec<_>>>()?;
        let keys = (0..self.plan.groups as usize)
            .map(|column| {
                if !self.grouping_sets_by_column[column].contains(&group.grouping_set_id) {
                    return None;
                }
                let location = group.key_location?;
                let key = self.group_key_batches[location.batch][column]
                    .as_ref()
                    .expect("group source batch retains every grouping column");
                Some((key.array_ref(), location.row))
            })
            .collect();
        Ok(AggregateIntermediateGroup {
            hash: group.hash,
            grouping_set_id: group.grouping_set_id,
            keys,
            aggregate_states,
        })
    }

    /// Merges one decoded spill record without converting its key arrays to
    /// scalars. Array wrappers and backing buffers remain shared by reference.
    pub(super) fn merge_spilled_group(&mut self, group: AggregateIntermediateGroup) -> Result<()> {
        self.tracker.check_cancelled()?;
        let grouping_set = self
            .grouping_sets
            .get(group.grouping_set_id)
            .ok_or_else(|| VeinError::internal("aggregate spill group has invalid set ID"))?;
        if group.keys.len() != self.plan.groups as usize
            || group.aggregate_states.len() != self.aggregate_states.len()
        {
            return Err(VeinError::internal(
                "aggregate spill group layout does not match its plan",
            ));
        }

        let mut key_columns = std::iter::repeat_with(|| None)
            .take(group.keys.len())
            .collect::<Vec<Option<GroupKeyColumn>>>();
        let mut key_row = None;
        for (column, key) in group.keys.iter().enumerate() {
            match (grouping_set.contains(&(column as u32)), key) {
                (true, Some((array, row))) if *row < array.len() => {
                    key_columns[column] = Some(GroupKeyColumn::new(array)?);
                    key_row.get_or_insert(*row);
                }
                (true, _) => {
                    return Err(VeinError::internal(
                        "aggregate spill record is missing an included key",
                    ));
                }
                (false, None) => {}
                (false, Some(_)) => {
                    return Err(VeinError::internal(
                        "aggregate spill record contains an excluded key",
                    ));
                }
            }
        }

        let matching_slot = self.index.find(group.hash, |entry| {
            let Some(existing) = self.groups.get(entry.slot) else {
                return false;
            };
            existing.grouping_set_id == group.grouping_set_id
                && grouping_set.iter().all(|&column| {
                let column = column as usize;
                let Some(location) = existing.key_location else {
                    return false;
                };
                let Some(existing_key) = self.group_key_batches[location.batch][column].as_ref()
                else {
                    return false;
                };
                key_columns[column]
                    .as_ref()
                    .zip(key_row)
                    .is_some_and(|(key, row)| {
                        key.rows_equal(row, existing_key, location.row)
                    })
            })
        });

        let slot = if let Some(entry) = matching_slot {
            entry.slot
        } else {
            let group_bytes = size_of::<Group>()
                .checked_add(
                    size_of::<Option<GroupKeyLocation>>()
                        .checked_mul(self.plan.groups as usize)
                        .ok_or_else(|| VeinError::internal("aggregate spill key size overflow"))?,
                )
                .and_then(|bytes| {
                    self.aggregate_states
                        .iter()
                        .try_fold(bytes, |total, state| {
                            total.checked_add(state.bytes_per_group())
                        })
                })
                .and_then(|bytes| bytes.checked_add(size_of::<GroupIndexEntry>()))
                .ok_or_else(|| VeinError::internal("aggregate spill group size overflow"))?;
            let before = self.memory.bytes();
            self.memory.grow(group_bytes, "aggregate spill merge state")?;
            if let Err(error) = self.index.reserve(1) {
                self.memory.shrink(self.memory.bytes() - before);
                return Err(error);
            }

            let mut retained_arrays = HashSet::new();
            let mut leases = Vec::new();
            if let Err(error) = leases.try_reserve(key_columns.len()) {
                self.memory.shrink(self.memory.bytes() - before);
                return Err(VeinError::internal(format!(
                    "cannot reserve aggregate spill key leases: {error}"
                )));
            }
            for key in group.keys.iter().flatten() {
                let identity = Arc::as_ptr(&key.0) as *const () as usize;
                if retained_arrays.insert(identity) {
                    match self.tracker.lease_retained_array(
                        identity,
                        key.0.get_array_memory_size(),
                        "aggregate spill merge keys",
                    ) {
                        Ok(lease) => leases.push(lease),
                        Err(error) => {
                            self.memory.shrink(self.memory.bytes() - before);
                            return Err(error);
                        }
                    }
                }
            }
            if let Err(error) = self.group_key_batches.try_reserve(1) {
                self.memory.shrink(self.memory.bytes() - before);
                return Err(VeinError::internal(format!(
                    "cannot reserve aggregate spill key batch: {error}"
                )));
            }
            if let Err(error) = self.group_key_batch_leases.try_reserve(1) {
                self.memory.shrink(self.memory.bytes() - before);
                return Err(VeinError::internal(format!(
                    "cannot reserve aggregate spill key leases: {error}"
                )));
            }

            let location = key_row.map(|row| GroupKeyLocation {
                batch: self.group_key_batches.len(),
                row,
            });
            self.group_key_batches.push(key_columns);
            self.group_key_batch_leases.push(leases);
            let slot = Self::push_group(
                &mut self.groups,
                &mut self.aggregate_states,
                group.hash,
                group.grouping_set_id,
                location,
            )?;
            self.index
                .insert_unique(GroupIndexEntry { hash: group.hash, slot })?;
            slot
        };

        for (state, partial) in self
            .aggregate_states
            .iter_mut()
            .zip(&group.aggregate_states)
        {
            state.merge_intermediate_state(slot, partial)?;
        }
        Ok(())
    }
}
