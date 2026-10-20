//! Aggregate partial other regression coverage.

use super::*;

#[test]
fn intermediate_single_value_preserves_null_row_and_cardinality() {
    let input: ArrayRef = Arc::new(Int64Array::from(vec![None]));
    let mut column = AggregateStateColumn::SingleValue(vec![Some((Arc::clone(&input), 0))]);
    let partial = column
        .intermediate_state(0)
        .expect("snapshot one NULL row");
    column.push_group();
    column
        .merge_intermediate_state(1, &partial)
        .expect("restore one NULL row");

    let AggregateStateColumn::SingleValue(states) = &mut column else {
        panic!("expected SINGLE_VALUE state column");
    };
    let (retained, row) = states[1].as_ref().expect("NULL is still an input row");
    assert!(retained.is_null(*row));
    assert!(Arc::ptr_eq(retained, &input), "Arrow payload remains shared");

    let error = column
        .merge_intermediate_state(1, &partial)
        .expect_err("two NULL rows still violate scalar cardinality");
    assert!(error.to_string().contains("scalar subquery returned more than one row"));
    let AggregateStateColumn::SingleValue(states) = column else {
        panic!("expected SINGLE_VALUE state column");
    };
    assert!(states[1].as_ref().expect("first row remains").0.is_null(0));
}

#[test]
fn intermediate_group_state_preserves_grouping_set_and_null_key_identity() {
    let plan = AggregatePlan {
        groups: 1,
        estimated_groups: None,
        grouping_sets: Some(vec![vec![0], vec![]]),
        emit_empty_group_on_empty_input: false,
        aggs: vec![AggOpCode::Count],
        aggregate_input_types: vec![VeinDataType::Int64],
        types: vec![VeinDataType::Int64, VeinDataType::Int64],
    };
    let mut sink = AggregateSink::new(plan).expect("create grouping-set aggregate");
    let input_keys = Arc::new(Int64Array::from(vec![None, Some(1)]));
    let input_key_values = input_keys.values().as_ptr();
    sink.accumulate(DataChunk::from_vectors(vec![
        input_keys,
        Arc::new(Int64Array::from(vec![10, 20])) as ArrayRef,
    ]))
        .expect("accumulate grouped and NULL keys");

    let states = (0..sink.group_count())
        .map(|slot| sink.intermediate_group_state(slot).expect("snapshot group"))
        .collect::<Vec<_>>();
    let real_null_group = states
        .iter()
        .find(|state| state.grouping_set_id == 0)
        .expect("real NULL key group has its own grouping-set ID");
    let retained_key = real_null_group.keys[0]
        .as_ref()
        .expect("group key snapshot retains its Arrow column");
    let row = retained_key.row();
    let retained_arrays = retained_key
        .source_arrays()
        .expect("snapshot retains its key sources");
    let retained_keys = retained_arrays[0]
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("snapshot preserves the key array type");
    assert!(retained_keys.is_null(row));
    assert_eq!(
        retained_keys.values().as_ptr(),
        input_key_values,
        "snapshot shares the source Arrow value buffer"
    );
    assert!(states
        .iter()
        .find(|state| state.grouping_set_id == 1)
        .expect("empty grouping set state")
        .keys[0]
        .is_none());
    assert!(
        states.iter().any(|state| state.grouping_set_id == 1),
        "the empty grouping set keeps its identity even when another set retains the source batch"
    );
    assert!(states.iter().all(|state| state.aggregate_states.len() == 1));
    drop(sink);
    assert!(
        retained_keys.is_null(row),
        "the intermediate group keeps its shared key view after the sink drops"
    );
}
