use super::*;

pub(super) fn resolve_candidates(
    sink: &mut AggregateSink,
    batch: &AggregateBatch,
    prepared: PreparedAggregateBatch,
) -> Result<usize> {
    let groups = sink.plan.groups as usize;
    let key_columns = &batch.key_columns;
    let grouping_set_count = sink.grouping_sets.len();
    let candidate_count = prepared.candidate_count;
    let dictionary_fast_path = prepared.dictionary_fast_path;
    sink.metrics.grouping_probes = sink
        .metrics
        .grouping_probes
        .saturating_add(candidate_count as u64);
    let hashes = &sink.group_hashes;
    let memory = &mut sink.memory;
    let index = &mut sink.index;
    let batch_group_ids = &mut sink.batch_group_ids;
    let groups_store = &mut sink.groups;
    let group_key_batches = &sink.group_key_batches;
    let aggregate_states = &mut sink.aggregate_states;
    let metrics = &mut sink.metrics;
    let new_groups = &mut sink.batch_new_groups;
    let dictionary_ids = &sink.dictionary_ids;
    let preallocated_groups = sink
        .plan
        .estimated_groups
        .and_then(|groups| usize::try_from(groups).ok())
        .filter(|_| sink.spill_runs.is_empty())
        .unwrap_or(0);
    let aggregate_state_bytes = aggregate_states
        .iter()
        .try_fold(0usize, |bytes, state| {
            bytes.checked_add(state.estimated_bytes_per_group())
        })
        .ok_or_else(|| VeinError::internal("aggregate state size overflow"))?;
    new_groups.clear();
    new_groups.try_reserve(candidate_count).map_err(|error| {
        VeinError::internal(format!("cannot reserve aggregate batch groups: {error}"))
    })?;
    let group_memory_before_batch = memory.bytes();
    let mut reservation = crate::memory::batch::BatchReservation::new(memory, "aggregate group state");
    let resolution_started = Instant::now();
    let resolution = index.resolve_rows(hashes, batch_group_ids, |candidate, existing_slot| {
        let set_id = if dictionary_fast_path {
            0
        } else {
            candidate % grouping_set_count
        };
        let logical_row = candidate / grouping_set_count;
        let grouping_set = &sink.grouping_sets[set_id];
        let dictionary_id = dictionary_fast_path.then(|| dictionary_ids[candidate]);
        let row = if dictionary_fast_path {
            sink.dictionary_first_rows[candidate]
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
                            sink.dictionary_first_rows[leader]
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
                reservation.charge(group_bytes, candidate_count - candidate)?;
                new_groups.push(candidate);
                Ok(ProbeResolution::Insert(slot))
            }
        }
    });
    drop(reservation);
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
    Ok(group_memory_before_batch)
}
