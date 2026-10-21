use super::*;

#[test]
fn aggregate_state_frame_round_trips_opaque_kernel_payloads() {
    let source = AggregateIntermediateGroup {
        hash: 37,
        grouping_set_id: 3,
        keys: Vec::new(),
        aggregate_states: vec![
            Arc::new(vec![0, 1, 2, 255]),
            Arc::new(Vec::new()),
            Arc::new(b"plugin-owned state".to_vec()),
        ],
    };
    let encoded = source
        .encode_aggregate_states()
        .expect("encode state payload");
    let decoded = AggregateIntermediateGroup::decode_aggregate_states(&encoded)
        .expect("decode state payload");
    let restored = AggregateIntermediateGroup {
        hash: source.hash,
        grouping_set_id: source.grouping_set_id,
        keys: Vec::new(),
        aggregate_states: decoded,
    };
    assert_eq!(
        restored
            .encode_aggregate_states()
            .expect("re-encode state payload"),
        encoded,
        "all variants retain exact accumulator bits and metadata"
    );
}

#[test]
fn aggregate_state_frame_rejects_invalid_magic_truncation_and_trailing_data() {
    let valid = AggregateIntermediateGroup {
        hash: 0,
        grouping_set_id: 0,
        keys: Vec::new(),
        aggregate_states: vec![Arc::new(vec![1, 2, 3])],
    }
        .encode_aggregate_states()
        .expect("encode state payload");
    assert!(
        AggregateIntermediateGroup::decode_aggregate_states(&valid[..valid.len() - 1]).is_err()
    );

    let mut invalid = valid.clone();
    invalid[0] = b'X';
    assert!(AggregateIntermediateGroup::decode_aggregate_states(&invalid).is_err());

    let mut trailing = valid;
    trailing.push(0);
    assert!(AggregateIntermediateGroup::decode_aggregate_states(&trailing).is_err());
}
