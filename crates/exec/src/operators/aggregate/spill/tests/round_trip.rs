use super::*;

#[test]
fn aggregate_ipc_run_round_trips_typed_keys_nulls_grouping_sets_and_states() {
    let layout =
        AggregateSpillSchema::try_new(vec![VeinDataType::Int64], vec![vec![0], vec![]])
            .expect("create typed aggregate spill schema");
    let key: ArrayRef = Arc::new(Int64Array::from(vec![None, Some(17)]));
    let groups = [
        sample_group(0, Some((Arc::clone(&key), 0)), 2),
        sample_group(0, Some((Arc::clone(&key), 1)), 5),
        sample_group(1, None, 7),
    ];
    let mut bytes = Cursor::new(Vec::new());
    layout
        .write_run(&mut bytes, groups.iter().cloned())
        .expect("write IPC run");

    let mut reader = layout
        .open_reader(Cursor::new(bytes.into_inner()))
        .expect("validate and open IPC run");
    let restored = reader
        .next_batch()
        .expect("read IPC batch")
        .expect("run batch");
    assert_eq!(restored.len(), 3);
    assert_eq!(restored[0].hash, 2);
    assert_eq!(restored[1].hash, 5);
    assert_eq!(restored[2].hash, 7);
    let null_key = restored[0].keys[0]
        .as_ref()
        .expect("real NULL grouping key");
    let (array, row) = null_key
        .source_row()
        .expect("resolve NULL key")
        .expect("a SQL NULL still has an Arrow source row");
    assert!(array.is_null(row));
    let ordinary_key = restored[1].keys[0].as_ref().expect("ordinary key");
    let (array, row) = ordinary_key
        .source_row()
        .expect("resolve ordinary key")
        .expect("ordinary key has a source row");
    assert_eq!(
        array
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(row),
        17
    );
    assert_eq!(restored[0].grouping_set_id, 0);
    assert_eq!(restored[2].grouping_set_id, 1);
    assert!(
        restored[2].keys[0].is_none(),
        "omitted grouping key stays absent"
    );
    assert_eq!(restored[2].aggregate_states.len(), 1);
    assert!(reader.next_batch().expect("end of run").is_none());
}

#[test]
fn aggregate_ipc_run_normalizes_dictionary_strings_to_plan_type() {
    use arrow::array::{DictionaryArray, Int32Array};
    use arrow::datatypes::Int32Type;

    let layout = AggregateSpillSchema::try_new(vec![VeinDataType::Utf8], vec![vec![0]])
        .expect("create UTF-8 spill schema");
    let dict = DictionaryArray::<Int32Type>::try_new(
        Int32Array::from(vec![Some(0), Some(1)]),
        Arc::new(StringArray::from(vec!["alpha", "beta"])),
    )
        .expect("dictionary source key");
    let key: ArrayRef = Arc::new(dict);
    let groups = [sample_group(0, Some((key, 1)), 1)];
    let mut bytes = Cursor::new(Vec::new());
    layout
        .write_run(&mut bytes, groups.iter().cloned())
        .expect("write dictionary key");
    let mut reader = layout.open_reader(Cursor::new(bytes.into_inner())).unwrap();
    let restored = reader.next_batch().unwrap().unwrap();
    let (array, row) = restored[0].keys[0]
        .as_ref()
        .unwrap()
        .source_row()
        .unwrap()
        .unwrap();
    assert_eq!(array.data_type(), &DataType::Utf8);
    assert_eq!(
        array
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(row),
        "beta"
    );
}

#[test]
fn aggregate_ipc_run_rejects_schema_mismatch_and_bad_group_layout() {
    let layout = AggregateSpillSchema::try_new(vec![VeinDataType::Int64], vec![vec![0]])
        .expect("create schema");
    let wrong = AggregateSpillSchema::try_new(vec![VeinDataType::Utf8], vec![vec![0]])
        .expect("create different schema");
    let mut bytes = Cursor::new(Vec::new());
    wrong
        .write_run(&mut bytes, std::iter::empty())
        .expect("write another layout");
    assert!(layout.open_reader(Cursor::new(bytes.into_inner())).is_err());

    let invalid_group = AggregateIntermediateGroup {
        hash: 1,
        grouping_set_id: 0,
        keys: vec![],
        aggregate_states: vec![count_state(1).expect("encode test COUNT state")],
    };
    assert!(layout.encode_batch(&[invalid_group]).is_err());

    let mismatched_key = AggregateIntermediateGroup {
        hash: 1,
        grouping_set_id: 0,
        keys: vec![Some(
            AggregateGroupKeyRef::from_array(
                Arc::new(StringArray::from(vec!["wrong type"])) as ArrayRef,
                0,
            )
                .expect("valid mismatched key row"),
        )],
        aggregate_states: vec![count_state(1).expect("encode test COUNT state")],
    };
    assert!(layout.encode_batch(&[mismatched_key]).is_err());

    let out_of_range_set = AggregateIntermediateGroup {
        hash: 1,
        grouping_set_id: 1,
        keys: vec![None],
        aggregate_states: vec![count_state(1).expect("encode test COUNT state")],
    };
    assert!(layout.encode_batch(&[out_of_range_set]).is_err());
}
