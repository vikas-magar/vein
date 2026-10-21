use super::*;

#[test]
fn decoded_aggregate_spill_keys_are_charged_until_batch_release() {
    let layout = AggregateSpillSchema::try_new(vec![VeinDataType::Int64], vec![vec![0]])
        .expect("create typed aggregate spill schema");
    let key: ArrayRef = Arc::new(Int64Array::from(vec![11, 23]));
    let groups = [
        sample_group(0, Some((Arc::clone(&key), 0)), 2),
        sample_group(0, Some((Arc::clone(&key), 1)), 3),
    ];
    let mut bytes = Cursor::new(Vec::new());
    layout.write_run(&mut bytes, groups).expect("write IPC run");
    let tracker = QueryMemoryTracker::unlimited(common::cancellation::CancellationToken::new());
    let mut reader = layout
        .open_reader(Cursor::new(bytes.into_inner()))
        .expect("open IPC run");

    let mut batch = reader
        .next_batch_charged(&tracker)
        .expect("read charged batch")
        .expect("one record batch");
    let decoded_bytes = tracker.used();
    assert!(decoded_bytes > 0, "decoded Arrow arrays must be accounted");
    assert_eq!(batch.groups.len(), 2);
    let key_arrays = batch.groups[0].keys[0]
        .as_ref()
        .expect("group retains its decoded key array")
        .source_arrays()
        .expect("decoded key source arrays");
    let key_array = &key_arrays[0];
    assert_eq!(
        decoded_bytes,
        key_array.get_array_memory_size(),
        "only key arrays still retained by groups remain charged after decoding"
    );

    let groups = std::mem::take(&mut batch.groups);
    drop(groups);
    assert_eq!(
        tracker.used(),
        decoded_bytes,
        "batch leases stay live while decoded arrays are retained"
    );
    drop(batch);
    assert_eq!(tracker.used(), 0, "batch leases release with the batch");
}
