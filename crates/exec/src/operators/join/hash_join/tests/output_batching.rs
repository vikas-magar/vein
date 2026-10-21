use super::*;

fn view_int(view: &DataChunkView, column: usize, row: usize) -> Option<i64> {
    let (array, physical_row) = view.resolve(column, row).expect("resolve join output")?;
    let values = array
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 output");
    values.is_valid(physical_row).then(|| values.value(physical_row))
}

#[test]
fn join_batches_multiple_build_chunks_without_copying_payloads() {
    let mut table = HashTable::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
    let build_payloads = [ints(&[70, 80]), ints(&[71, 81]), ints(&[72, 82])];
    for (keys, payload) in [
        (&[7, 8][..], &build_payloads[0]),
        (&[7, 8][..], &build_payloads[1]),
        (&[7, 8][..], &build_payloads[2]),
    ] {
        table
            .insert(crate::pipeline::test_support::dense_view(vec![ints(keys), Arc::clone(payload)]))
            .expect("insert build batch");
    }

    let probe_payload = ints(&[700, 800, 900]);
    let sink = ViewCollect::default();
    let mut join = HashJoin::new(
        Arc::new(table),
        JoinType::Left,
        vec![OutputColumn::Probe(1), OutputColumn::Build(1)],
        vec![DataType::Int64, DataType::Int64],
        vec![DataType::Int64, DataType::Int64],
        Box::new(sink.clone()),
    );
    join.push(
        crate::pipeline::test_support::dense_view(vec![ints(&[7, 8, 9]), Arc::clone(&probe_payload)]),
    )
        .expect("probe rows across build chunks");
    join.flush().expect("flush join");

    assert_eq!(join.output_batches, 3);
    assert_eq!(join.output_rows, 7);
    assert_eq!(join.max_output_batch_rows, 3);
    let batches = sink.0.lock().expect("view collector lock");
    assert_eq!(batches.len(), 3, "seven rows are emitted in bounded batches");
    assert!(batches.iter().all(|batch| batch.num_rows() <= 3));
    let rows = batches
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).map(|row| {
                let probe = view_int(batch, 0, row);
                if let Some((array, _)) = batch.resolve(0, row).expect("probe source") {
                    assert!(Arc::ptr_eq(array, &probe_payload));
                }
                let build = view_int(batch, 1, row);
                if let Some((array, _)) = batch.resolve(1, row).expect("build source") {
                    assert!(build_payloads.iter().any(|source| Arc::ptr_eq(array, source)));
                }
                (probe, build)
            })
        })
        .collect::<Vec<_>>();

    assert_eq!(
        rows.iter().map(|row| row.0).collect::<Vec<_>>(),
        vec![Some(700), Some(700), Some(700), Some(800), Some(800), Some(800), Some(900)],
        "probe order is preserved across output batch boundaries"
    );
    assert_eq!(rows[0..3].iter().map(|row| row.1).collect::<std::collections::BTreeSet<_>>(), [Some(70), Some(71), Some(72)].into());
    assert_eq!(rows[3..6].iter().map(|row| row.1).collect::<std::collections::BTreeSet<_>>(), [Some(80), Some(81), Some(82)].into());
    assert_eq!(rows[6], (Some(900), None), "left join retains an unmatched probe as NULL");
}
