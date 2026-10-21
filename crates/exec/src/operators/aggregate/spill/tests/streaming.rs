use super::*;

#[test]
fn aggregate_ipc_run_streams_groups_across_bounded_record_batches() {
    let layout = AggregateSpillSchema::try_new(vec![VeinDataType::Int64], vec![vec![0]])
        .expect("create spill schema");
    let key: ArrayRef = Arc::new(Int64Array::from_iter_values(
        0..(SPILL_RECORD_BATCH_SIZE as i64 + 1),
    ));
    let groups = (0..SPILL_RECORD_BATCH_SIZE + 1).map(|row| {
        sample_group(
            0,
            Some((Arc::clone(&key), row)),
            i64::try_from(row).expect("test group count fits i64"),
        )
    });
    let mut bytes = Cursor::new(Vec::new());
    layout
        .write_run(&mut bytes, groups)
        .expect("write streamed run");

    let mut reader = layout
        .open_reader(Cursor::new(bytes.into_inner()))
        .expect("open streamed run");
    let first = reader
        .next_batch()
        .expect("read first batch")
        .expect("first batch");
    let second = reader
        .next_batch()
        .expect("read second batch")
        .expect("second batch");
    assert_eq!(first.len(), SPILL_RECORD_BATCH_SIZE);
    assert_eq!(second.len(), 1);
    assert_eq!(
        second[0].aggregate_states[0].as_slice(),
        count_state(SPILL_RECORD_BATCH_SIZE)
            .expect("encode expected COUNT state")
            .as_slice(),
    );
    assert!(reader.next_batch().expect("end of run").is_none());
}
