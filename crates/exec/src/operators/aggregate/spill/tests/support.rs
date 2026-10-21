use super::*;

pub(super) fn sample_group(
    grouping_set_id: usize,
    key: Option<(ArrayRef, usize)>,
    count: i64,
) -> AggregateIntermediateGroup {
    AggregateIntermediateGroup {
        hash: u64::try_from(count).expect("test count is non-negative"),
        grouping_set_id,
        keys: vec![key.map(|(array, row)| {
            AggregateGroupKeyRef::from_array(array, row).expect("valid test key row")
        })],
        aggregate_states: vec![count_state(usize::try_from(count).expect("nonnegative count"))
            .expect("encode test COUNT state")],
    }
}

pub(super) fn managed_run(
    layout: &AggregateSpillSchema,
    tracker: &QueryMemoryTracker,
    groups: impl IntoIterator<Item=AggregateIntermediateGroup>,
) -> SpillRun {
    let mut pending = tracker
        .spill_manager()
        .create_run()
        .expect("create managed run");
    let output = layout
        .write_run(pending.writer().expect("managed run writer"), groups)
        .expect("write managed run");
    pending.commit(output).expect("commit managed run")
}
