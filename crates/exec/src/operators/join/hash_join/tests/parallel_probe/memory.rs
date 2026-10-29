use super::*;

#[tokio::test]
async fn parallel_outer_match_memory_is_accounted_and_released() {
    let table = Arc::new(build(&[1, 1, 2]));
    let tracker = table.memory_tracker().clone();
    let sink = Collect::default();
    let mut join = HashJoin::new(table, JoinType::Full,
                                 vec![OutputColumn::Probe(0), OutputColumn::Build(0)],
                                 vec![DataType::Int64], vec![DataType::Int64], Box::new(sink.clone()));
    let baseline = tracker.used();
    let mut worker = join.fork_local().unwrap().unwrap();
    assert_eq!(tracker.used(), baseline, "fork allocates no match table eagerly");
    worker.push(crate::pipeline::test_support::dense_view(vec![ints(&[1])])).await.unwrap();
    assert!(tracker.used() > baseline, "worker match state owns an RAII reservation");
    let used = tracker.used();
    for _ in 0..20 {
        worker.push(crate::pipeline::test_support::dense_view(vec![ints(&[1])])).await.unwrap();
    }
    assert_eq!(tracker.used(), used, "duplicate matches do not grow tracking state");
    worker.combine_local(&mut join).unwrap();
    assert!(tracker.used() > baseline, "canonical state keeps its own accounted matches");
    drop(join);
    sink.0.lock().unwrap().clear();
    assert_eq!(tracker.used(), 0, "all join state reservations release on drop");
}
