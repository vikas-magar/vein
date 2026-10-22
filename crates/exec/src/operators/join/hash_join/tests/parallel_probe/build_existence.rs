use super::*;

#[test]
fn build_existence_combines_workers_and_preserves_duplicates_nulls_and_payload_identity() {
    for kind in [JoinType::RightSemi, JoinType::RightAnti] {
        for residual in [false, true] {
            let (mut join, sink, payload) = parallel_join(kind, residual);
            let mut workers = Vec::new();
            for (keys, values) in [
                (vec![Some(1), None], vec![10, 99]),
                (vec![Some(2), Some(1), Some(1)], vec![20, 20, 20]),
                (vec![], vec![]),
            ] {
                let mut worker = join.fork_local().unwrap().unwrap();
                worker.push(crate::pipeline::test_support::dense_view(vec![
                    nullable_ints(&keys), ints(&values),
                ])).unwrap();
                worker.flush().unwrap();
                workers.push(worker);
            }
            for worker in workers { worker.combine_local(&mut join).unwrap(); }
            assert!(sink.0.lock().unwrap().is_empty(), "worker flush must not emit preserved build rows");
            join.flush().unwrap();
            let chunks = sink.0.lock().unwrap();
            let mut actual: Vec<_> = chunks.iter().flat_map(|chunk| {
                (0..chunk.num_rows()).map(|row| [selected_value(chunk, 0, row), selected_value(chunk, 1, row)])
            }).collect();
            actual.sort_unstable();
            let mut expected = if kind == JoinType::RightSemi {
                vec![[Some(1), Some(5)], [Some(1), Some(15)], [Some(2), Some(7)]]
            } else { vec![[Some(3), Some(9)], [None, Some(11)]] };
            expected.sort_unstable();
            assert_eq!(actual, expected, "{kind:?} residual={residual}");
            for chunk in chunks.iter() {
                for row in 0..chunk.num_rows() {
                    let (source, _) = chunk.resolve(1, row).unwrap().unwrap();
                    assert!(Arc::ptr_eq(source, &payload));
                }
            }
        }
    }
}

#[test]
fn empty_probe_emits_all_build_rows_only_for_right_anti() {
    for kind in [JoinType::RightSemi, JoinType::RightAnti] {
        let (mut join, sink, _) = parallel_join(kind, false);
        join.flush().unwrap();
        let rows: usize = sink.0.lock().unwrap().iter().map(DataChunkView::num_rows).sum();
        assert_eq!(rows, if kind == JoinType::RightSemi { 0 } else { 5 });
    }
}

#[test]
fn unknown_residual_does_not_mark_a_build_row_as_matched() {
    for kind in [JoinType::RightSemi, JoinType::RightAnti] {
        let (mut join, sink, _) = parallel_join(kind, true);
        join.push(crate::pipeline::test_support::dense_view(vec![
            ints(&[1]), nullable_ints(&[None]),
        ])).unwrap();
        join.flush().unwrap();
        let rows: usize = sink.0.lock().unwrap().iter().map(DataChunkView::num_rows).sum();
        assert_eq!(rows, if kind == JoinType::RightSemi { 0 } else { 5 });
    }
}
