use super::*;

fn partial<T>(
    op: AggOpCode,
    value: Option<T>,
    count: i64,
    scale: i8,
    decimal: bool,
) -> NumericPartial<T> {
    NumericPartial {
        op,
        value,
        count,
        scale,
        decimal_precision_limit: decimal,
    }
}

#[test]
fn aggregate_state_codec_round_trips_every_grouped_state_variant() {
    let source = AggregateIntermediateGroup {
        hash: 37,
        grouping_set_id: 3,
        keys: Vec::new(),
        aggregate_states: vec![
            AggregateIntermediateState::Count(19),
            AggregateIntermediateState::Int64(partial(AggOpCode::Sum, Some(-17), 0, 0, false)),
            AggregateIntermediateState::Int128Avg(partial(
                AggOpCode::Avg,
                Some(-1_000_000_000_001),
                7,
                0,
                false,
            )),
            AggregateIntermediateState::UInt64(partial(
                AggOpCode::Max,
                Some(u64::MAX),
                0,
                0,
                false,
            )),
            AggregateIntermediateState::UInt128Avg(partial(
                AggOpCode::Avg,
                Some(u128::MAX - 1),
                2,
                0,
                false,
            )),
            AggregateIntermediateState::Float32(partial(
                AggOpCode::Min,
                Some(-1.25),
                0,
                0,
                false,
            )),
            AggregateIntermediateState::Float64(partial(AggOpCode::Sum, None, 0, 0, false)),
            AggregateIntermediateState::Decimal128(partial(
                AggOpCode::Sum,
                Some(-123_456),
                0,
                4,
                true,
            )),
            AggregateIntermediateState::Decimal256(partial(
                AggOpCode::Avg,
                Some(i256::from_parts(u128::MAX - 3, -17)),
                11,
                6,
                false,
            )),
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
fn aggregate_state_codec_rejects_truncated_unknown_and_trailing_data() {
    let valid = AggregateIntermediateGroup {
        hash: 0,
        grouping_set_id: 0,
        keys: Vec::new(),
        aggregate_states: vec![AggregateIntermediateState::Count(1)],
    }
        .encode_aggregate_states()
        .expect("encode state payload");
    assert!(
        AggregateIntermediateGroup::decode_aggregate_states(&valid[..valid.len() - 1]).is_err()
    );

    let mut unknown = valid.clone();
    unknown[8] = 255;
    assert!(AggregateIntermediateGroup::decode_aggregate_states(&unknown).is_err());

    let mut trailing = valid;
    trailing.push(0);
    assert!(AggregateIntermediateGroup::decode_aggregate_states(&trailing).is_err());
}
