//! Aggregate sets regression coverage.

use super::*;

#[test]
fn grouping_sets_keep_set_identity_and_null_omitted_keys() {
    let plan = AggregatePlan {
        groups: 2,
        estimated_groups: None,
        grouping_sets: Some(vec![vec![0, 1], vec![0], vec![]]),
        emit_empty_group_on_empty_input: true,
        aggs: vec![AggOpCode::Sum],
        aggregate_input_types: vec![VeinDataType::Int64],
        types: vec![VeinDataType::Int64, VeinDataType::Utf8, VeinDataType::Int64],
    };
    let mut sink = AggregateSink::new(inferred_results(plan)).unwrap();
    sink.accumulate(crate::pipeline::test_support::dense_view(vec![
        Arc::new(Int64Array::from(vec![Some(1), None])),
        Arc::new(StringArray::from(vec![Some("x"), Some("z")])),
        Arc::new(Int64Array::from(vec![10, 20])),
    ]))
        .unwrap();

    let metrics = sink.metrics();
    assert_eq!(metrics.input_batches, 1);
    assert_eq!(metrics.input_rows, 2);
    assert_eq!(metrics.grouping_sets, 3);
    assert_eq!(metrics.grouping_probes, 6);
    assert_eq!(metrics.new_groups, 5);
    assert_eq!(metrics.string_key_bytes_copied, 0);
    assert_eq!(metrics.retained_key_slots_initialized, 10);
    assert_eq!(metrics.string_key_bytes_copied, 0);

    let output = sink.finish().unwrap();
    let batch = &output[0];
    let first_key = crate::pipeline::test_support::materialized_column::<Int64Array>(batch, 0);
    let second_key = crate::pipeline::test_support::materialized_column::<StringArray>(batch, 1);
    let sums = int128_column(batch, 2);

    assert_eq!(batch.num_rows(), 5);
    let mut actual = (0..batch.num_rows())
        .map(|row| {
            (
                (!first_key.is_null(row)).then(|| first_key.value(row)),
                (!second_key.is_null(row)).then(|| second_key.value(row)),
                sums.value(row),
            )
        })
        .collect::<Vec<_>>();
    actual.sort_unstable();
    let mut expected = vec![
        (Some(1), Some("x"), 10),
        (None, Some("z"), 20),
        (Some(1), None, 10),
        (None, None, 20),
        (None, None, 30),
    ];
    expected.sort_unstable();
    assert_eq!(actual, expected);
}

#[test]
fn empty_input_emits_only_empty_grouping_sets() {
    let plan = AggregatePlan {
        groups: 1,
        estimated_groups: None,
        grouping_sets: Some(vec![vec![0], vec![]]),
        emit_empty_group_on_empty_input: true,
        aggs: vec![AggOpCode::Count],
        aggregate_input_types: vec![VeinDataType::Int64],
        types: vec![VeinDataType::Int64, VeinDataType::Int64],
    };
    let mut sink = AggregateSink::new(inferred_results(plan)).unwrap();
    sink.accumulate(crate::pipeline::test_support::dense_view(vec![
        Arc::new(Int64Array::from(Vec::<i64>::new())),
        Arc::new(Int64Array::from(Vec::<i64>::new())),
    ]))
        .unwrap();

    let output = sink.finish().unwrap();
    assert_eq!(output[0].num_rows(), 1);
    assert!(
        crate::pipeline::test_support::materialized_column::<Int64Array>(&output[0], 0)
            .is_null(0)
    );
    assert_eq!(
        crate::pipeline::test_support::materialized_column::<Int64Array>(&output[0], 1)
            .value(0),
        0
    );
}
