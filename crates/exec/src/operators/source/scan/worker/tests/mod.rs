use super::ScanWorkerState;

#[test]
fn successive_work_units_reuse_only_their_workers_state() {
    let worker = ScanWorkerState::default();
    for expected in 1..=3 {
        assert_eq!(worker.with::<usize, _>(|count| {
            *count += 1;
            *count
        }).unwrap(), expected);
    }
    let other_worker = ScanWorkerState::default();
    assert_eq!(other_worker.with::<usize, _>(|count| *count).unwrap(), 0);
    let other_query = ScanWorkerState::default();
    assert_eq!(other_query.with::<usize, _>(|count| *count).unwrap(), 0);
}

#[test]
fn provider_extensions_are_isolated_by_type() {
    let worker = ScanWorkerState::default();
    worker.with::<usize, _>(|count| *count = 7).unwrap();
    assert!(worker.with::<Vec<usize>, _>(|values| values.is_empty()).unwrap());
    assert_eq!(worker.with::<usize, _>(|count| *count).unwrap(), 7);
}
