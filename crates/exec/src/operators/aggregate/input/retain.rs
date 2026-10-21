use super::*;

pub(super) fn retain_new_groups(
    sink: &mut AggregateSink,
    batch: &AggregateBatch,
    prepared: PreparedAggregateBatch,
    group_memory_before_batch: usize,
) -> Result<()> {
    let groups = sink.plan.groups as usize;
    let key_columns = &batch.key_columns;
    let grouping_set_count = sink.grouping_sets.len();
    let dictionary_fast_path = prepared.dictionary_fast_path;
    let hashes = &sink.group_hashes;
    let memory = &mut sink.memory;
    let index = &mut sink.index;
    let groups_store = &mut sink.groups;
    let aggregate_states = &mut sink.aggregate_states;
    let metrics = &mut sink.metrics;
    let new_groups = &mut sink.batch_new_groups;
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
            if let std::collections::hash_map::Entry::Vacant(entry) = key_leases.entry(identity) {
                let lease = sink.tracker.lease_retained_array(
                    identity,
                    array.get_array_memory_size(),
                    "aggregate retained group keys",
                );
                match lease {
                    Ok(lease) => {
                        entry.insert(lease);
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
        if let Err(error) = sink.group_key_batches.try_reserve(1) {
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
        if let Err(error) = sink.group_key_batch_leases.try_reserve(1) {
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
        if let Err(error) = sink.group_key_batch_arrays.try_reserve(1) {
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
        let key_batch_index = sink.group_key_batches.len();
        sink.group_key_batches.push(
            key_columns
                .iter()
                .cloned()
                .map(|column| column.map(Arc::new))
                .collect(),
        );
        sink.group_key_batch_arrays.push(batch.key_arrays.clone());
        sink.group_key_batch_leases.push(key_leases);
        Some(key_batch_index)
    };
    let group_slot_base = groups_store.len();
    for (new_group_index, &candidate) in new_groups.iter().enumerate() {
        let set_id = candidate % grouping_set_count;
        let logical_row = candidate / grouping_set_count;
        let row = if dictionary_fast_path {
            sink.dictionary_first_rows[candidate]
        } else {
            batch.selection.get_index(logical_row)
        };
        metrics.retained_key_slots_initialized = metrics
            .retained_key_slots_initialized
            .saturating_add(groups as u64);
        let location = key_batch.map(|batch| GroupKeyLocation { batch, row });
        let slot = AggregateSink::push_group(groups_store, aggregate_states, hashes[candidate], set_id, location)?;
        debug_assert_eq!(slot, group_slot_base + new_group_index);
    }
    metrics.new_groups = metrics.new_groups.saturating_add(new_groups.len() as u64);
    metrics.group_materialize_ns = metrics
        .group_materialize_ns
        .saturating_add(materialize_started.elapsed().as_nanos() as u64);
    Ok(())
}
