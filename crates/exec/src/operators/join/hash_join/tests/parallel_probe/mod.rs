use super::*;

mod memory;
mod build_existence;

fn parallel_join(kind: JoinType, residual: bool) -> (HashJoin, Collect, ArrayRef) {
    let payload = ints(&[5, 15, 7, 9, 11]);
    let mut table = HashTable::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
    table.insert(crate::pipeline::test_support::dense_view(vec![
        nullable_ints(&[Some(1), Some(1), Some(2), Some(3), None]),
        Arc::clone(&payload),
    ])).unwrap();
    let sink = Collect::default();
    let columns = if kind.build_existence() {
        vec![OutputColumn::Build(0), OutputColumn::Build(1)]
    } else {
        vec![OutputColumn::Probe(0), OutputColumn::Probe(1), OutputColumn::Build(0), OutputColumn::Build(1)]
    };
    let mut join = HashJoin::new(Arc::new(table), kind, columns,
                                 vec![DataType::Int64; 2], vec![DataType::Int64; 2], Box::new(sink.clone()));
    if residual {
        let (transform, collector) = residual_greater_than_filter();
        join = join.with_residual_filter(
            vec![OutputColumn::Probe(1), OutputColumn::Build(1)],
            Box::new(transform), collector);
    }
    (join, sink, payload)
}

#[test]
fn parallel_outer_join_combines_matches_and_emits_unmatched_build_once() {
    for kind in [JoinType::Right, JoinType::Full] {
        for residual in [false, true] {
            let (mut join, sink, payload) = parallel_join(kind, residual);
            let mut workers = Vec::new();
            for (keys, values) in [(vec![Some(1), None], vec![10, 99]),
                (vec![Some(2), Some(1)], vec![20, 1])] {
                let mut worker = join.fork_local().unwrap().expect("parallel outer probe");
                workers.push(std::thread::spawn(move || {
                    worker.push(crate::pipeline::test_support::dense_view(vec![
                        nullable_ints(&keys), ints(&values),
                    ])).unwrap();
                    // Even explicit worker finalization must not emit unmatched build rows.
                    worker.flush().unwrap();
                    worker
                }));
            }
            for worker in workers {
                worker.join().unwrap().combine_local(&mut join).unwrap();
            }
            assert_eq!(join.probed_rows, 4);
            join.flush().unwrap();
            let chunks = sink.0.lock().unwrap();
            let mut actual = chunks.iter().flat_map(selected_rows).collect::<Vec<_>>();
            actual.sort_unstable();
            let mut expected = if residual {
                vec![[None, None, Some(1), Some(15)],
                     [None, None, Some(3), Some(9)],
                     [None, None, None, Some(11)],
                     [Some(1), Some(10), Some(1), Some(5)],
                     [Some(2), Some(20), Some(2), Some(7)]]
            } else {
                vec![[None, None, Some(3), Some(9)],
                     [None, None, None, Some(11)],
                     [Some(1), Some(10), Some(1), Some(5)],
                     [Some(1), Some(10), Some(1), Some(15)],
                     [Some(1), Some(1), Some(1), Some(5)],
                     [Some(1), Some(1), Some(1), Some(15)],
                     [Some(2), Some(20), Some(2), Some(7)]]
            };
            if kind == JoinType::Full {
                expected.push([None, Some(99), None, None]);
                if residual { expected.push([Some(1), Some(1), None, None]); }
            }
            expected.sort_unstable();
            assert_eq!(actual, expected, "{kind:?}, residual={residual}");
            for chunk in chunks.iter() {
                for row in 0..chunk.num_rows() {
                    if let Some((array, _)) = chunk.resolve(3, row).unwrap() {
                        assert!(Arc::ptr_eq(array, &payload), "build payload stays shared");
                    }
                }
            }
        }
    }
}

#[test]
fn parallel_inner_residual_workers_keep_private_collectors() {
    let (mut join, sink, _) = parallel_join(JoinType::Inner, true);
    let mut workers = Vec::new();
    for value in [10, 20, 1] {
        let mut local = join.fork_local().unwrap().expect("fork residual evaluator");
        workers.push(std::thread::spawn(move || {
            for _ in 0..20 {
                local.push(crate::pipeline::test_support::dense_view(vec![
                    ints(&[1]), ints(&[value]),
                ])).unwrap();
            }
            local
        }));
    }
    for worker in workers { worker.join().unwrap().combine_local(&mut join).unwrap(); }
    join.flush().unwrap();
    let chunks = sink.0.lock().unwrap();
    let rows = chunks.iter().flat_map(selected_rows).collect::<Vec<_>>();
    assert_eq!(rows.len(), 60);
    assert_eq!(rows.iter().filter(|row| row[1] == Some(10)).count(), 20);
    assert_eq!(rows.iter().filter(|row| row[1] == Some(20)).count(), 40);
}

#[test]
fn non_forkable_downstream_keeps_explicit_serial_fallback() {
    let mut join = HashJoin::new(
        Arc::new(build(&[1])), JoinType::Full,
        vec![OutputColumn::Build(0)], vec![DataType::Int64],
        vec![DataType::Int64], Box::new(ViewCollect::default()));
    assert!(join.fork_local().unwrap().is_none());
    let (transform, collector) = residual_greater_than_filter();
    join = join.with_residual_filter(
        vec![OutputColumn::Probe(0), OutputColumn::Build(0)], Box::new(transform), collector);
    assert!(join.fork_local().unwrap().is_none());
}

#[test]
fn parallel_join_rejects_different_residual_programs() {
    let (mut join, _, _) = parallel_join(JoinType::Full, true);
    let worker = join.fork_local().unwrap().unwrap();
    let (transform, collector) = residual_greater_than_filter();
    join = join.with_residual_filter(
        vec![OutputColumn::Probe(1), OutputColumn::Build(1)], Box::new(transform), collector);
    let error = worker.combine_local(&mut join).unwrap_err();
    assert!(error.to_string().contains("different expression executor configurations"));
}
