use super::*;

fn run_correlated_shape(join_type: JoinType, empty_build: bool) -> Vec<i64> {
    let mut table = HashTable::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
    if !empty_build {
        table
            .insert(DataChunk::from_vectors(vec![
                ints(&[1, 1, 2, 4]),
                nullable_ints(&[Some(10), Some(20), Some(5), None]),
            ]))
            .expect("insert build rows");
    }

    let collector = JoinFilterCollector::default();
    let predicate = expression_binary(
        common::protocol::BinaryExprOp::NotEqual,
        expression_column(0, types::VeinDataType::Int64),
        expression_column(1, types::VeinDataType::Int64),
        types::VeinDataType::Boolean,
    );
    let executor = expression_filter(predicate, collector.clone());
    let sink = Collect::default();
    let mut join = HashJoin::new(
        Arc::new(table),
        join_type,
        vec![OutputColumn::Probe(2)],
        vec![DataType::Int64; 3],
        vec![DataType::Int64; 2],
        Box::new(sink.clone()),
    )
        .with_residual_filter(
            vec![OutputColumn::Probe(1), OutputColumn::Build(1)],
            Box::new(executor),
            collector,
        );

    join.push(DataChunk::from_vectors(vec![
        ints(&[1, 1, 2, 3, 4]),
        nullable_ints(&[Some(10), Some(15), None, Some(8), Some(7)]),
        ints(&[101, 102, 103, 104, 105]),
    ]))
        .expect("probe rows");
    join.flush().expect("finish join");

    let chunks = sink.0.lock().expect("collect lock");
    let mut ids = chunks
        .iter()
        .flat_map(|chunk| {
            (0..chunk.num_rows()).map(|row| {
                selected_value(chunk, 0, row).expect("semi/anti output keeps probe id")
            })
        })
        .collect::<Vec<_>>();
    ids.sort_unstable();
    ids
}

#[test]
fn semi_join_residual_handles_duplicate_matches_and_sql_nulls() {
    assert_eq!(run_correlated_shape(JoinType::Semi, false), vec![101, 102]);
}

#[test]
fn anti_join_residual_handles_duplicate_matches_and_sql_nulls() {
    assert_eq!(run_correlated_shape(JoinType::Anti, false), vec![103, 104, 105]);
}

#[test]
fn empty_build_preserves_exists_and_not_exists_semantics() {
    assert_eq!(run_correlated_shape(JoinType::Semi, true), Vec::<i64>::new());
    assert_eq!(
        run_correlated_shape(JoinType::Anti, true),
        vec![101, 102, 103, 104, 105]
    );
}
