//! streaming regression tests for hash aggregation.

use super::*;

#[test]
fn aggregate_finish_streams_ordered_groups_in_bounded_chunks() {
    let plan = AggregatePlan {
        groups: 1,
        estimated_groups: None,
        grouping_sets: Some(vec![vec![0]]),
        emit_empty_group_on_empty_input: false,
        aggs: vec![AggOpCode::Sum],
        aggregate_input_types: vec![VeinDataType::Int64],
        types: vec![VeinDataType::Int64, VeinDataType::Int64],
    };
    let mut sink = AggregateSink::new(inferred_results(plan)).expect("create grouped sum");
    sink.accumulate(crate::pipeline::test_support::dense_view(vec![
        Arc::new(Int64Array::from(vec![Some(1), None, Some(1), Some(3), None])) as ArrayRef,
        Arc::new(Int64Array::from(vec![10, 20, 15, 30, 25])) as ArrayRef,
    ]))
        .expect("accumulate grouped values");

    let mut chunks = Vec::new();
    sink.finish_with(NonZeroUsize::new(2).expect("non-zero batch size"), |chunk| {
        chunks.push(chunk);
        Ok(())
    })
        .expect("stream aggregate output");

    assert_eq!(
        chunks
            .iter()
            .map(|chunk| chunk.num_rows())
            .collect::<Vec<_>>(),
        vec![2, 1],
        "each output chunk must respect the requested group row bound"
    );
    let mut actual = Vec::new();
    for chunk in &chunks {
        let keys = crate::pipeline::test_support::materialized_column::<Int64Array>(chunk, 0);
        let sums = int128_column(chunk, 1);
        for row in 0..chunk.num_rows() {
            actual.push((
                (!keys.is_null(row)).then(|| keys.value(row)),
                sums.value(row),
            ));
        }
    }
    assert_eq!(actual, vec![(Some(1), 25), (None, 45), (Some(3), 30)]);
}

#[test]
fn aggregate_finish_callback_error_stops_emission_without_losing_groups() {
    let plan = AggregatePlan {
        groups: 1,
        estimated_groups: None,
        grouping_sets: Some(vec![vec![0]]),
        emit_empty_group_on_empty_input: false,
        aggs: vec![AggOpCode::Count],
        aggregate_input_types: vec![VeinDataType::Int64],
        types: vec![VeinDataType::Int64, VeinDataType::Int64],
    };
    let mut sink = AggregateSink::new(inferred_results(plan)).expect("create grouped count");
    sink.accumulate(crate::pipeline::test_support::dense_view(vec![
        Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
        Arc::new(Int64Array::from(vec![10, 20, 30])) as ArrayRef,
    ]))
        .expect("accumulate grouped values");

    let mut emitted = 0;
    let error = sink
        .finish_with(NonZeroUsize::new(1).expect("non-zero batch size"), |_| {
            emitted += 1;
            Err(VeinError::internal("downstream stopped"))
        })
        .expect_err("propagate downstream failure");
    assert!(error.to_string().contains("downstream stopped"));
    assert_eq!(emitted, 1, "stop after the first failed push");

    let output = sink.finish().expect("groups remain available for retry");
    assert_eq!(output.iter().map(|chunk| chunk.num_rows()).sum::<usize>(), 3);
}

#[test]
fn aggregate_finish_streaming_preserves_empty_group_semantics() {
    let grouped_plan = AggregatePlan {
        groups: 1,
        estimated_groups: None,
        grouping_sets: Some(vec![vec![0]]),
        emit_empty_group_on_empty_input: false,
        aggs: vec![AggOpCode::Count],
        aggregate_input_types: vec![VeinDataType::Int64],
        types: vec![VeinDataType::Int64, VeinDataType::Int64],
    };
    let mut grouped = AggregateSink::new(grouped_plan).expect("create grouped count");
    let mut grouped_batches = 0;
    grouped
        .finish_with(AGGREGATE_BATCH_ROWS, |_| {
            grouped_batches += 1;
            Ok(())
        })
        .expect("finish empty grouped aggregate");
    assert_eq!(grouped_batches, 0);

    let global_plan = AggregatePlan {
        groups: 0,
        estimated_groups: None,
        grouping_sets: None,
        emit_empty_group_on_empty_input: true,
        aggs: vec![AggOpCode::Count],
        aggregate_input_types: vec![VeinDataType::Int64],
        types: vec![VeinDataType::Int64],
    };
    let mut global = AggregateSink::new(global_plan).expect("create global count");
    let mut global_batches = Vec::new();
    global
        .finish_with(AGGREGATE_BATCH_ROWS, |batch| {
            global_batches.push(batch);
            Ok(())
        })
        .expect("finish empty global aggregate");
    assert_eq!(global_batches.len(), 1);
    assert_eq!(global_batches[0].num_rows(), 1);
    let count = crate::pipeline::test_support::materialized_column::<Int64Array>(
        &global_batches[0],
        0,
    );
    assert_eq!(count.value(0), 0);
}
