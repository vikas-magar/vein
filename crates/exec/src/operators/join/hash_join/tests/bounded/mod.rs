use super::*;
mod cancellation;

fn expected(kind: JoinType, probe: &[Option<i64>], build: &[Option<i64>]) -> Vec<Vec<Option<i64>>> {
    let mut rows = Vec::new();
    let mut build_matched = vec![false; build.len()];
    for &left in probe {
        let matches = build.iter().enumerate().filter(|(_, right)| {
            kind == JoinType::Cross || left.is_some() && left == **right
        }).collect::<Vec<_>>();
        let matched = !matches.is_empty();
        match kind {
            JoinType::Semi if matched => rows.push(vec![left]),
            JoinType::Anti if !matched => rows.push(vec![left]),
            JoinType::NullAwareAnti if !matched && (build.is_empty() ||
                left.is_some() && build.iter().all(Option::is_some)) => rows.push(vec![left]),
            JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full | JoinType::Cross => {
                for (index, &right) in matches {
                    rows.push(vec![left, right]);
                    build_matched[index] = true;
                }
                if !matched && matches!(kind, JoinType::Left | JoinType::Full) { rows.push(vec![left, None]); }
            }
            _ => {}
        }
    }
    if matches!(kind, JoinType::Right | JoinType::Full) {
        for (index, &right) in build.iter().enumerate() {
            if !build_matched[index] { rows.push(vec![None, right]); }
        }
    }
    rows.sort();
    rows
}

#[tokio::test]
async fn bounded_join_matches_scalar_oracle_for_duplicates_nulls_and_empty_inputs() {
    for kind in [JoinType::Inner, JoinType::Left, JoinType::Right, JoinType::Full,
        JoinType::Semi, JoinType::Anti, JoinType::NullAwareAnti, JoinType::Cross] {
        for probe in [vec![], vec![None], vec![Some(7)], vec![Some(9), Some(7), None]] {
            for build_rows in [vec![], vec![None], vec![Some(7), Some(7), None, Some(8)]] {
                let keys = if kind == JoinType::Cross { vec![] } else { vec![0] };
                let mut table = HashTable::new(JoinKeys::new(keys.clone()), JoinKeys::new(keys));
                for &value in &build_rows {
                    table.insert(crate::pipeline::test_support::dense_view(vec![nullable_ints(&[value])])).unwrap();
                }
                let existence = matches!(kind, JoinType::Semi | JoinType::Anti | JoinType::NullAwareAnti);
                let columns = if existence { vec![OutputColumn::Probe(0)] } else { vec![OutputColumn::Probe(0), OutputColumn::Build(0)] };
                let sink = Collect::default();
                let mut join = HashJoin::new(Arc::new(table), kind, columns,
                                             vec![DataType::Int64], vec![DataType::Int64], Box::new(sink.clone()));
                // One-row input and build chunks force a candidate boundary after every match.
                for &value in &probe {
                    join.push(crate::pipeline::test_support::dense_view(vec![nullable_ints(&[value])])).await.unwrap();
                }
                join.flush().await.unwrap();
                let mut actual: Vec<Vec<Option<i64>>> = Vec::new();
                for batch in sink.0.lock().unwrap().iter() {
                    for row in 0..batch.num_rows() {
                        actual.push((0..batch.num_columns()).map(|column| selected_value(batch, column, row)).collect());
                    }
                }
                actual.sort();
                assert_eq!(actual, expected(kind, &probe, &build_rows), "{kind:?}: {probe:?} / {build_rows:?}");
                assert!(join.max_candidate_batch_rows <= 1);
            }
        }
    }
}

#[tokio::test]
async fn existence_join_skips_duplicate_chain_after_first_accepted_match() {
    let sink = Collect::default();
    let mut join = HashJoin::new(Arc::new(build(&vec![7; 1000])), JoinType::Semi,
                                 vec![OutputColumn::Probe(0)], vec![DataType::Int64], vec![DataType::Int64], Box::new(sink));
    join.push(crate::pipeline::test_support::dense_view(vec![ints(&[7, 7, 8])])).await.unwrap();
    assert_eq!(join.matched_rows, 2);
    assert_eq!(join.candidate_rows_examined, 2);
    assert_eq!(join.max_candidate_batch_rows, 2);
}

#[tokio::test]
async fn existence_residual_retries_rejected_matches_then_stops_accepted_rows() {
    use common::protocol::BinaryExprOp;
    use types::VeinDataType;
    let mut table = HashTable::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
    table.insert(crate::pipeline::test_support::dense_view(vec![
        ints(&[7; 32]), ints(&(0..32).collect::<Vec<_>>()),
    ])).unwrap();
    let output = JoinFilterCollector::default();
    let predicate = expression_binary(BinaryExprOp::Greater, expression_column(0, VeinDataType::Int64),
                                      expression_column(1, VeinDataType::Int64), VeinDataType::Boolean);
    let transform = expression_filter(predicate, output.clone());
    let sink = Collect::default();
    let mut join = HashJoin::new(Arc::new(table), JoinType::Semi,
                                 vec![OutputColumn::Probe(1)], vec![DataType::Int64; 2], vec![DataType::Int64; 2], Box::new(sink.clone()))
        .with_residual_filter(vec![OutputColumn::Probe(1), OutputColumn::Build(1)], Box::new(transform), output);
    join.push(crate::pipeline::test_support::dense_view(vec![ints(&[7, 7]), ints(&[30, 20])])).await.unwrap();
    join.flush().await.unwrap();
    let actual = sink.0.lock().unwrap().iter().flat_map(|batch| cells(batch, 0)).collect::<Vec<_>>();
    assert_eq!(actual, vec![Some(30), Some(20)]);
    assert_eq!(join.candidate_rows_examined, 16, "three candidates then thirteen; no remaining duplicate scans");
}
