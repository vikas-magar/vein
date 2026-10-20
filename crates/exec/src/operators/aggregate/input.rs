use super::*;

impl AggregateSink {
    pub(super) fn push_group(
        groups: &mut Vec<Group>,
        aggregate_states: &mut [AggregateStateColumn],
        hash: u64,
        grouping_set_id: usize,
        key_location: Option<GroupKeyLocation>,
    ) -> Result<usize> {
        let slot = groups.len();
        groups.push(Group {
            hash,
            grouping_set_id,
            key_location,
        });
        for column in aggregate_states {
            column.push_group();
        }
        Ok(slot)
    }

    /// Folds one chunk into the groups.
    pub fn accumulate(&mut self, chunk: DataChunk) -> Result<()> {
        if !self.distinct_collectors.is_empty() {
            self.accumulate_distinct(&chunk)?;
        }
        let batch = AggregateBatch::from_chunk(
            &chunk,
            self.plan.groups as usize,
            self.plan.aggs.len(),
            &self.grouping_sets,
        )?;
        self.accumulate_with_spill(&batch)
    }

    /// Folds a reference-backed input while retaining grouping columns as views.
    pub(super) fn accumulate_view(&mut self, view: DataChunkView) -> Result<()> {
        if !self.distinct_collectors.is_empty() {
            self.accumulate_distinct_view(&view)?;
        }
        let batch = AggregateBatch::from_view(
            &view,
            self.plan.groups as usize,
            self.plan.aggs.len(),
            &self.grouping_sets,
        )?;
        self.accumulate_with_spill(&batch)
    }

    /// Retries only resource failures, splitting an oversized selection while
    /// keeping every retained Arrow array shared with the input batch.
    fn accumulate_with_spill(&mut self, batch: &AggregateBatch) -> Result<()> {
        match self.accumulate_batch(batch) {
            Ok(()) => Ok(()),
            Err(VeinError::ResourceExhausted { .. }) if !self.groups.is_empty() => {
                self.flush_current_run()?;
                self.accumulate_with_spill(batch)
            }
            Err(error @ VeinError::ResourceExhausted { .. }) => {
                let rows = batch.selection.size();
                if rows <= 1 {
                    return Err(error);
                }
                let midpoint = rows / 2;
                let left = batch.with_selection(batch.selection.slice(0, midpoint));
                let right = batch.with_selection(batch.selection.slice(midpoint, rows - midpoint));
                self.accumulate_with_spill(&left)?;
                self.accumulate_with_spill(&right)
            }
            Err(error) => Err(error),
        }
    }

    fn accumulate_batch(&mut self, batch: &AggregateBatch) -> Result<()> {
        let groups = self.plan.groups as usize;
        if batch.key_columns.len() != groups
            || batch.aggregate_columns.len() != self.plan.aggs.len()
        {
            return Err(VeinError::internal(format!(
                "aggregate batch has {} grouping columns and {} aggregate columns; expected {groups} and {}",
                batch.key_columns.len(),
                batch.aggregate_columns.len(),
                self.plan.aggs.len()
            )));
        }
        for group_ids in &mut self.group_ids_by_set {
            group_ids.clear();
            if group_ids.capacity() < batch.selection.size() {
                group_ids.reserve(batch.selection.size());
            }
        }
        let key_columns = &batch.key_columns;
        let input_rows = batch.selection.size();
        let grouping_set_count = self.grouping_sets.len();
        // With one Utf8 grouping key, each referenced dictionary entry is a
        // group candidate. The row loop below only fans the resolved IDs out.
        let dictionary_key = (groups == 1
            && self.grouping_sets.len() == 1
            && self.grouping_sets[0].as_slice() == [0])
            .then(|| key_columns[0].as_ref())
            .flatten()
            .and_then(|column| column.dictionary_cardinality().map(|size| (column, size)));
        self.dictionary_ids.clear();
        self.dictionary_first_rows.clear();
        let dictionary_fast_path = dictionary_key.is_some();
        if let Some((column, cardinality)) = dictionary_key {
            self.dictionary_group_slots
                .resize(cardinality + 1, usize::MAX);
            self.dictionary_group_slots.fill(usize::MAX);
            for logical_row in 0..input_rows {
                let row = batch.selection.get_index(logical_row);
                let dictionary_id = column
                    .dictionary_id(row)
                    .expect("dictionary fast path has a dictionary key column");
                if self.dictionary_group_slots[dictionary_id] == usize::MAX {
                    self.dictionary_group_slots[dictionary_id] = self.dictionary_ids.len();
                    self.dictionary_ids.push(dictionary_id);
                    self.dictionary_first_rows.push(row);
                }
            }
        }
        let candidate_count = if dictionary_fast_path {
            self.dictionary_ids.len()
        } else {
            input_rows
                .checked_mul(grouping_set_count)
                .ok_or_else(|| VeinError::internal("aggregate batch candidate count overflow"))?
        };
        if self.metrics.input_batches == 0 && self.plan.estimated_groups.is_none() {
            let reserve_ns = self.index.reserve(candidate_count)?;
            self.metrics.group_index_reserve_ns = self
                .metrics
                .group_index_reserve_ns
                .saturating_add(reserve_ns);
        }
        self.group_hashers.clear();
        self.group_hashers
            .try_reserve(candidate_count)
            .map_err(|error| {
                VeinError::internal(format!("cannot reserve aggregate hash states: {error}"))
            })?;
        self.group_hashes.clear();
        self.group_hashes
            .try_reserve(candidate_count)
            .map_err(|error| {
                VeinError::internal(format!("cannot reserve aggregate group hashes: {error}"))
            })?;
        let hash_generation_started = Instant::now();
        let hasher_setup_started = Instant::now();
        for candidate in 0..candidate_count {
            let set_id = candidate % grouping_set_count;
            let mut hasher = self.hash_builder.build_hasher();
            set_id.hash(&mut hasher);
            self.group_hashers.push(hasher);
        }
        self.metrics.hasher_setup_ns = self
            .metrics
            .hasher_setup_ns
            .saturating_add(hasher_setup_started.elapsed().as_nanos() as u64);
        let key_hash_started = Instant::now();
        if dictionary_fast_path {
            key_columns[0]
                .as_ref()
                .expect("dictionary key column was initialized")
                .hash_dictionary_ids(0, &self.dictionary_ids, &mut self.group_hashers);
        } else {
            for (column, key_column) in key_columns.iter().enumerate() {
                if let Some(key_column) = key_column {
                    key_column.hash_batch(
                        column as u32,
                        &batch.selection,
                        &self.grouping_sets_by_column[column],
                        grouping_set_count,
                        &mut self.group_hashers,
                    );
                }
            }
        }
        self.metrics.key_hash_ns = self
            .metrics
            .key_hash_ns
            .saturating_add(key_hash_started.elapsed().as_nanos() as u64);
        let hash_finalize_started = Instant::now();
        self.group_hashes
            .extend(self.group_hashers.iter().map(Hasher::finish));
        self.metrics.hash_finalize_ns = self
            .metrics
            .hash_finalize_ns
            .saturating_add(hash_finalize_started.elapsed().as_nanos() as u64);
        let hash_generation_ns = hash_generation_started.elapsed().as_nanos() as u64;
        self.metrics.hash_generation_ns = self
            .metrics
            .hash_generation_ns
            .saturating_add(hash_generation_ns);

        self.metrics.grouping_probes = self
            .metrics
            .grouping_probes
            .saturating_add(candidate_count as u64);
        let hashes = &self.group_hashes;
        let memory = &mut self.memory;
        let index = &mut self.index;
        let batch_group_ids = &mut self.batch_group_ids;
        let groups_store = &mut self.groups;
        let group_key_batches = &self.group_key_batches;
        let aggregate_states = &mut self.aggregate_states;
        let metrics = &mut self.metrics;
        let new_groups = &mut self.batch_new_groups;
        let dictionary_ids = &self.dictionary_ids;
        let preallocated_groups = self
            .plan
            .estimated_groups
            .and_then(|groups| usize::try_from(groups).ok())
            .filter(|_| self.spill_runs.is_empty())
            .unwrap_or(0);
        let aggregate_state_bytes = aggregate_states
            .iter()
            .try_fold(0usize, |bytes, state| {
                bytes.checked_add(state.bytes_per_group())
            })
            .ok_or_else(|| VeinError::internal("aggregate state size overflow"))?;
        new_groups.clear();
        new_groups.try_reserve(candidate_count).map_err(|error| {
            VeinError::internal(format!("cannot reserve aggregate batch groups: {error}"))
        })?;
        let group_memory_before_batch = memory.bytes();
        let resolution_started = Instant::now();
        let resolution = index.resolve_rows(hashes, batch_group_ids, |candidate, existing_slot| {
            let set_id = if dictionary_fast_path {
                0
            } else {
                candidate % grouping_set_count
            };
            let logical_row = candidate / grouping_set_count;
            let grouping_set = &self.grouping_sets[set_id];
            let dictionary_id = dictionary_fast_path.then(|| dictionary_ids[candidate]);
            let row = if dictionary_fast_path {
                self.dictionary_first_rows[candidate]
            } else {
                batch.selection.get_index(logical_row)
            };
            match existing_slot {
                Some(slot) => {
                    metrics.full_hash_matches = metrics.full_hash_matches.saturating_add(1);
                    let (existing_set_id, keys_match) = if slot < groups_store.len() {
                        let group = &groups_store[slot];
                        let keys_match = grouping_set.iter().all(|&column| {
                            metrics.group_key_comparisons =
                                metrics.group_key_comparisons.saturating_add(1);
                            let column = column as usize;
                            let location = group
                                .key_location
                                .expect("groups with keys retain their source row");
                            let retained = group_key_batches[location.batch][column]
                                .as_ref()
                                .expect("group source batch retains every grouping column");
                            key_columns[column]
                                .as_ref()
                                .expect("grouping set columns have a batch view")
                                .rows_equal(row, retained, location.row)
                        });
                        (group.grouping_set_id, keys_match)
                    } else {
                        let leader = new_groups[slot - groups_store.len()];
                        let leader_set_id = leader % grouping_set_count;
                        let keys_match = if let Some(dictionary_id) = dictionary_id {
                            metrics.group_key_comparisons =
                                metrics.group_key_comparisons.saturating_add(1);
                            let leader_dictionary_id = dictionary_ids[leader];
                            leader_set_id == set_id
                                && key_columns[0]
                                .as_ref()
                                .expect("dictionary key column has a batch view")
                                .dictionary_ids_equal(dictionary_id, leader_dictionary_id)
                        } else {
                            let leader_row = if dictionary_fast_path {
                                self.dictionary_first_rows[leader]
                            } else {
                                batch.selection.get_index(leader / grouping_set_count)
                            };
                            leader_set_id == set_id
                                && grouping_set.iter().all(|&column| {
                                metrics.group_key_comparisons =
                                    metrics.group_key_comparisons.saturating_add(1);
                                let column = column as usize;
                                let key_column = key_columns[column]
                                    .as_ref()
                                    .expect("grouping set columns have a batch view");
                                key_column.rows_equal(row, key_column, leader_row)
                            })
                        };
                        (leader_set_id, keys_match)
                    };
                    if existing_set_id != set_id {
                        return Ok(ProbeResolution::Retry);
                    }
                    if keys_match {
                        metrics.group_index_hits = metrics.group_index_hits.saturating_add(1);
                        Ok(ProbeResolution::Match(slot))
                    } else {
                        metrics.group_collision_retries =
                            metrics.group_collision_retries.saturating_add(1);
                        Ok(ProbeResolution::Retry)
                    }
                }
                None => {
                    metrics.group_index_misses = metrics.group_index_misses.saturating_add(1);
                    let slot = groups_store.len() + new_groups.len();
                    let key_location_bytes = size_of::<Option<GroupKeyLocation>>()
                        .checked_mul(groups)
                        .ok_or_else(|| {
                            VeinError::internal("aggregate key location size overflow")
                        })?;
                    let group_bytes = size_of::<Group>()
                        .checked_add(key_location_bytes)
                        .and_then(|bytes| bytes.checked_add(aggregate_state_bytes))
                        .and_then(|bytes| {
                            if slot >= preallocated_groups {
                                bytes.checked_add(size_of::<GroupIndexEntry>())
                            } else {
                                Some(bytes)
                            }
                        })
                        .ok_or_else(|| VeinError::internal("aggregate group size overflow"))?;
                    memory.grow(group_bytes, "aggregate group state")?;
                    new_groups.push(candidate);
                    Ok(ProbeResolution::Insert(slot))
                }
            }
        });
        let index_metrics = match resolution {
            Ok(metrics) => metrics,
            Err(error) => {
                rollback_new_groups(
                    index,
                    hashes,
                    groups_store.len(),
                    new_groups,
                    memory,
                    group_memory_before_batch,
                );
                return Err(error);
            }
        };
        metrics.group_index_candidate_checks = metrics
            .group_index_candidate_checks
            .saturating_add(index_metrics.candidate_checks);
        metrics.group_index_reserve_ns = metrics
            .group_index_reserve_ns
            .saturating_add(index_metrics.reserve_ns);
        metrics.group_index_capacity_growth_events = metrics
            .group_index_capacity_growth_events
            .saturating_add(index_metrics.capacity_growth_events);
        metrics.group_index_entries_at_capacity_growth = metrics
            .group_index_entries_at_capacity_growth
            .saturating_add(index_metrics.entries_at_capacity_growth);
        metrics.group_index_capacity = metrics
            .group_index_capacity
            .max(index_metrics.capacity as u64);
        metrics.group_index_entries_ns = metrics
            .group_index_entries_ns
            .saturating_add(index_metrics.entries_ns);
        metrics.group_resolution_ns = metrics
            .group_resolution_ns
            .saturating_add(resolution_started.elapsed().as_nanos() as u64);

        let materialize_started = Instant::now();
        if let Err(error) = groups_store.try_reserve(new_groups.len()) {
            rollback_new_groups(
                index,
                hashes,
                groups_store.len(),
                new_groups,
                memory,
                group_memory_before_batch,
            );
            return Err(VeinError::internal(format!(
                "cannot reserve aggregate groups: {error}"
            )));
        }
        let key_batch = if new_groups.is_empty() {
            None
        } else {
            // Group keys retain their source Arrow arrays. Charge the full array
            // footprint, deduplicated by array identity across operators that
            // share this query tracker, before retaining another Arc to them.
            let mut key_leases = HashMap::new();
            if let Err(error) = key_leases.try_reserve(batch.key_arrays.iter().flatten().count()) {
                rollback_new_groups(
                    index,
                    hashes,
                    groups_store.len(),
                    new_groups,
                    memory,
                    group_memory_before_batch,
                );
                return Err(VeinError::internal(format!(
                    "cannot reserve aggregate group-key leases: {error}"
                )));
            }
            for array in batch.key_arrays.iter().flatten() {
                let identity = Arc::as_ptr(array) as *const () as usize;
                if !key_leases.contains_key(&identity) {
                    let lease = self.tracker.lease_retained_array(
                        identity,
                        array.get_array_memory_size(),
                        "aggregate retained group keys",
                    );
                    match lease {
                        Ok(lease) => {
                            key_leases.insert(identity, lease);
                        }
                        Err(error) => {
                            rollback_new_groups(
                                index,
                                hashes,
                                groups_store.len(),
                                new_groups,
                                memory,
                                group_memory_before_batch,
                            );
                            return Err(error);
                        }
                    }
                }
            }
            if let Err(error) = self.group_key_batches.try_reserve(1) {
                rollback_new_groups(
                    index,
                    hashes,
                    groups_store.len(),
                    new_groups,
                    memory,
                    group_memory_before_batch,
                );
                return Err(VeinError::internal(format!(
                    "cannot reserve aggregate group-key batches: {error}"
                )));
            }
            if let Err(error) = self.group_key_batch_leases.try_reserve(1) {
                rollback_new_groups(
                    index,
                    hashes,
                    groups_store.len(),
                    new_groups,
                    memory,
                    group_memory_before_batch,
                );
                return Err(VeinError::internal(format!(
                    "cannot reserve aggregate group-key lease batches: {error}"
                )));
            }
            if let Err(error) = self.group_key_batch_arrays.try_reserve(1) {
                rollback_new_groups(
                    index,
                    hashes,
                    groups_store.len(),
                    new_groups,
                    memory,
                    group_memory_before_batch,
                );
                return Err(VeinError::internal(format!(
                    "cannot reserve aggregate group-key source arrays: {error}"
                )));
            }
            let key_batch_index = self.group_key_batches.len();
            self.group_key_batches.push(key_columns.to_vec());
            self.group_key_batch_arrays.push(batch.key_arrays.clone());
            self.group_key_batch_leases.push(key_leases);
            Some(key_batch_index)
        };
        let group_slot_base = groups_store.len();
        for (new_group_index, &candidate) in new_groups.iter().enumerate() {
            let set_id = candidate % grouping_set_count;
            let logical_row = candidate / grouping_set_count;
            let row = if dictionary_fast_path {
                self.dictionary_first_rows[candidate]
            } else {
                batch.selection.get_index(logical_row)
            };
            metrics.retained_key_slots_initialized = metrics
                .retained_key_slots_initialized
                .saturating_add(groups as u64);
            let location = key_batch.map(|batch| GroupKeyLocation { batch, row });
            let slot = Self::push_group(groups_store, aggregate_states, hashes[candidate], set_id, location)?;
            debug_assert_eq!(slot, group_slot_base + new_group_index);
        }
        metrics.new_groups = metrics.new_groups.saturating_add(new_groups.len() as u64);
        metrics.group_materialize_ns = metrics
            .group_materialize_ns
            .saturating_add(materialize_started.elapsed().as_nanos() as u64);

        let group_ids_distribution_started = Instant::now();
        for group_ids in &mut self.group_ids_by_set {
            group_ids.clear();
        }
        if dictionary_fast_path {
            let key_column = key_columns[0]
                .as_ref()
                .expect("dictionary key column has a batch view");
            for logical_row in 0..input_rows {
                let row = batch.selection.get_index(logical_row);
                let dictionary_id = key_column
                    .dictionary_id(row)
                    .expect("dictionary fast path has a dictionary key column");
                let candidate = self.dictionary_group_slots[dictionary_id];
                self.group_ids_by_set[0].push(self.batch_group_ids[candidate]);
            }
            for (candidate, &slot) in self.batch_group_ids.iter().enumerate() {
                self.dictionary_group_slots[dictionary_ids[candidate]] = slot;
            }
        } else {
            for (candidate, &slot) in self.batch_group_ids.iter().enumerate() {
                self.group_ids_by_set[candidate % grouping_set_count].push(slot);
            }
        }
        self.metrics.group_ids_distribution_ns = self
            .metrics
            .group_ids_distribution_ns
            .saturating_add(group_ids_distribution_started.elapsed().as_nanos() as u64);

        // Resolve each physical input layout once per aggregate column and
        // batch, then fold the selected rows through the borrowed view.
        let state_update_started = Instant::now();
        for (offset, op) in self.plan.aggs.iter().enumerate() {
            if *op == AggOpCode::CountDistinct {
                continue;
            }
            let input = AggregateInput::new(
                *op,
                &self.plan.aggregate_input_types[offset],
                &batch.aggregate_columns[offset],
            )?;
            for group_ids in &self.group_ids_by_set {
                self.aggregate_states[offset]
                    .update_batch(&input, GroupRows::new(group_ids, &batch.selection))?;
            }
        }
        self.metrics.state_update_ns = self
            .metrics
            .state_update_ns
            .saturating_add(state_update_started.elapsed().as_nanos() as u64);
        self.metrics.input_rows = self
            .metrics
            .input_rows
            .saturating_add(input_rows as u64);
        self.metrics.input_batches = self.metrics.input_batches.saturating_add(1);
        Ok(())
    }
}
