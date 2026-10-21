use super::*;

#[test]
fn shared_build_sink_combines_unlimited_worker_tables_without_copying_payloads() {
    use arrow::array::Int64Array;

    let tracker = QueryMemoryTracker::unlimited(common::cancellation::CancellationToken::new());
    let build = BuildSink::with_spill_support(
        JoinKeys::new(vec![0]),
        JoinKeys::new(vec![0]),
        &[DataType::Int64, DataType::Int64],
        tracker,
    )
        .expect("create spill-capable canonical join sink");
    let mut canonical = SharedBuildSink(Arc::new(Mutex::new(Some(build))));
    let mut first = canonical
        .fork_local()
        .expect("fork first worker state")
        .expect("unlimited joins support local build state");
    let mut second = canonical
        .fork_local()
        .expect("fork second worker state")
        .expect("unlimited joins support local build state");

    let first_payload: arrow::array::ArrayRef = Arc::new(Int64Array::from(vec![10, 20]));
    let second_payload: arrow::array::ArrayRef = Arc::new(Int64Array::from(vec![21, 30]));
    let first_view = crate::pipeline::test_support::dense_view(vec![
        Arc::new(Int64Array::from(vec![1, 2])),
        Arc::clone(&first_payload),
    ]);
    let second_view = crate::pipeline::test_support::dense_view(vec![
        Arc::new(Int64Array::from(vec![2, 3])),
        Arc::clone(&second_payload),
    ]);

    first.push(first_view).expect("insert first worker chunk");
    second.push(second_view).expect("insert second worker chunk");
    first.flush().expect("finish first worker table");
    second.flush().expect("finish second worker table");
    first
        .combine_local(&mut canonical)
        .expect("merge first worker table");
    second
        .combine_local(&mut canonical)
        .expect("merge second worker table");

    let sink = canonical
        .0
        .lock()
        .expect("canonical build lock")
        .take()
        .expect("canonical build sink remains available");
    assert_eq!(sink.rows(), 4);
    assert_eq!(sink.combined_workers(), 2);
    let BuildResult::InMemory(table) = sink.finish_with_spill().expect("finish in-memory build") else {
        panic!("unlimited build should not spill");
    };

    let probe = crate::pipeline::test_support::dense_view(vec![Arc::new(Int64Array::from(vec![2]))]);
    let mut matches = Vec::new();
    table.probe(&probe, &mut matches).expect("probe merged worker tables");
    assert_eq!(matches.len(), 2, "duplicate keys from both workers are retained");

    let mut payloads = Vec::with_capacity(matches.len());
    for (_, (chunk, row)) in matches {
        let (array, physical_row) = table
            .build_chunk(chunk)
            .resolve(1, row as usize)
            .expect("resolve retained worker payload")
            .expect("build payload is not null-extended");
        payloads.push(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Int64 payload")
                .value(physical_row),
        );
        assert!(
            Arc::ptr_eq(array, &first_payload) || Arc::ptr_eq(array, &second_payload),
            "merge retains the original worker Arrow arrays"
        );
    }
    payloads.sort_unstable();
    assert_eq!(payloads, vec![20, 21]);
}

#[test]
fn shared_build_sink_keeps_bounded_memory_on_the_spill_safe_serial_path() {
    let tracker = QueryMemoryTracker::new(
        1 << 20,
        common::cancellation::CancellationToken::new(),
    );
    let build = BuildSink::with_spill_support(
        JoinKeys::new(vec![0]),
        JoinKeys::new(vec![0]),
        &[DataType::Int64],
        tracker,
    )
        .expect("create bounded spill-capable build sink");
    let canonical = SharedBuildSink(Arc::new(Mutex::new(Some(build))));

    assert!(
        canonical.fork_local().expect("check worker-local support").is_none(),
        "independent tables must not bypass the query memory budget and spill path"
    );
}
