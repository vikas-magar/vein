use super::*;

pub(super) fn prepare_batch(
    sink: &mut AggregateSink,
    batch: &AggregateBatch,
) -> Result<PreparedAggregateBatch> {
    let groups = sink.plan.groups as usize;
    if batch.key_columns.len() != groups
        || batch.aggregate_columns.len() != sink.plan.aggs.len()
    {
        return Err(VeinError::internal(format!(
            "aggregate batch has {} grouping columns and {} aggregate columns; expected {groups} and {}",
            batch.key_columns.len(),
            batch.aggregate_columns.len(),
            sink.plan.aggs.len()
        )));
    }
    for group_ids in &mut sink.group_ids_by_set {
        group_ids.clear();
        if group_ids.capacity() < batch.selection.size() {
            group_ids.reserve(batch.selection.size());
        }
    }
    let key_columns = &batch.key_columns;
    let input_rows = batch.selection.size();
    let grouping_set_count = sink.grouping_sets.len();
    // With one Utf8 grouping key, each referenced dictionary entry is a
    // group candidate. The row loop below only fans the resolved IDs out.
    let dictionary_key = (groups == 1
        && sink.grouping_sets.len() == 1
        && sink.grouping_sets[0].as_slice() == [0])
        .then(|| key_columns[0].as_ref())
        .flatten()
        .and_then(|column| column.dictionary_cardinality().map(|size| (column, size)));
    sink.dictionary_ids.clear();
    sink.dictionary_first_rows.clear();
    let dictionary_fast_path = dictionary_key.is_some();
    if let Some((column, cardinality)) = dictionary_key {
        sink.dictionary_group_slots
            .resize(cardinality + 1, usize::MAX);
        sink.dictionary_group_slots.fill(usize::MAX);
        for logical_row in 0..input_rows {
            let row = batch.selection.get_index(logical_row);
            let dictionary_id = column
                .dictionary_id(row)
                .expect("dictionary fast path has a dictionary key column");
            if sink.dictionary_group_slots[dictionary_id] == usize::MAX {
                sink.dictionary_group_slots[dictionary_id] = sink.dictionary_ids.len();
                sink.dictionary_ids.push(dictionary_id);
                sink.dictionary_first_rows.push(row);
            }
        }
    }
    let candidate_count = if dictionary_fast_path {
        sink.dictionary_ids.len()
    } else {
        input_rows
            .checked_mul(grouping_set_count)
            .ok_or_else(|| VeinError::internal("aggregate batch candidate count overflow"))?
    };
    if sink.metrics.input_batches == 0 && sink.plan.estimated_groups.is_none() {
        let reserve_ns = sink.index.reserve(candidate_count)?;
        sink.metrics.group_index_reserve_ns = sink
            .metrics
            .group_index_reserve_ns
            .saturating_add(reserve_ns);
    }
    sink.group_hashers.clear();
    sink.group_hashers
        .try_reserve(candidate_count)
        .map_err(|error| {
            VeinError::internal(format!("cannot reserve aggregate hash states: {error}"))
        })?;
    sink.group_hashes.clear();
    sink.group_hashes
        .try_reserve(candidate_count)
        .map_err(|error| {
            VeinError::internal(format!("cannot reserve aggregate group hashes: {error}"))
        })?;
    let hash_generation_started = Instant::now();
    let hasher_setup_started = Instant::now();
    for candidate in 0..candidate_count {
        let set_id = candidate % grouping_set_count;
        let mut hasher = sink.hash_builder.build_hasher();
        set_id.hash(&mut hasher);
        sink.group_hashers.push(hasher);
    }
    sink.metrics.hasher_setup_ns = sink
        .metrics
        .hasher_setup_ns
        .saturating_add(hasher_setup_started.elapsed().as_nanos() as u64);
    let key_hash_started = Instant::now();
    if dictionary_fast_path {
        key_columns[0]
            .as_ref()
            .expect("dictionary key column was initialized")
            .hash_dictionary_ids(0, &sink.dictionary_ids, &mut sink.group_hashers);
    } else {
        for (column, key_column) in key_columns.iter().enumerate() {
            if let Some(key_column) = key_column {
                key_column.hash_batch(
                    column as u32,
                    &batch.selection,
                    &sink.grouping_sets_by_column[column],
                    grouping_set_count,
                    &mut sink.group_hashers,
                );
            }
        }
    }
    sink.metrics.key_hash_ns = sink
        .metrics
        .key_hash_ns
        .saturating_add(key_hash_started.elapsed().as_nanos() as u64);
    let hash_finalize_started = Instant::now();
    sink.group_hashes
        .extend(sink.group_hashers.iter().map(Hasher::finish));
    sink.metrics.hash_finalize_ns = sink
        .metrics
        .hash_finalize_ns
        .saturating_add(hash_finalize_started.elapsed().as_nanos() as u64);
    let hash_generation_ns = hash_generation_started.elapsed().as_nanos() as u64;
    sink.metrics.hash_generation_ns = sink
        .metrics
        .hash_generation_ns
        .saturating_add(hash_generation_ns);

    Ok(PreparedAggregateBatch {
        candidate_count,
        dictionary_fast_path,
    })
}
