use super::*;

pub(super) fn update_aggregate_states(
    sink: &mut AggregateSink,
    batch: &AggregateBatch,
    prepared: PreparedAggregateBatch,
) -> Result<()> {
    let input_rows = batch.selection.size();
    let key_columns = &batch.key_columns;
    let grouping_set_count = sink.grouping_sets.len();
    let dictionary_fast_path = prepared.dictionary_fast_path;
    let dictionary_ids = &sink.dictionary_ids;
    let batch_group_ids = &sink.batch_group_ids;
    let group_ids_by_set = &mut sink.group_ids_by_set;
    let group_ids_distribution_started = Instant::now();
    for group_ids in group_ids_by_set.iter_mut() {
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
            let candidate = sink.dictionary_group_slots[dictionary_id];
            group_ids_by_set[0].push(batch_group_ids[candidate]);
        }
        for (candidate, &slot) in sink.batch_group_ids.iter().enumerate() {
            sink.dictionary_group_slots[dictionary_ids[candidate]] = slot;
        }
    } else {
        for (candidate, &slot) in sink.batch_group_ids.iter().enumerate() {
            group_ids_by_set[candidate % grouping_set_count].push(slot);
        }
    }
    sink.metrics.group_ids_distribution_ns = sink
        .metrics
        .group_ids_distribution_ns
        .saturating_add(group_ids_distribution_started.elapsed().as_nanos() as u64);

    // Resolve each physical input layout once per aggregate column and
    // batch, then fold the selected rows through the borrowed view.
    let state_update_started = Instant::now();
    for (offset, op) in sink.plan.aggs.iter().enumerate() {
        if *op == AggOpCode::CountDistinct {
            continue;
        }
        let source = AggregateFunctionInputSource::new(
            &batch.aggregate_columns[offset],
            &batch.selection,
        )?;
        for group_ids in group_ids_by_set.iter() {
            sink.aggregate_states[offset].update_batch(
                &[FunctionInput::new(&source, group_ids.len())],
                group_ids,
            )?;
        }
    }
    sink.metrics.state_update_ns = sink
        .metrics
        .state_update_ns
        .saturating_add(state_update_started.elapsed().as_nanos() as u64);
    sink.metrics.input_rows = sink
        .metrics
        .input_rows
        .saturating_add(input_rows as u64);
    sink.metrics.input_batches = sink.metrics.input_batches.saturating_add(1);
    Ok(())
}
