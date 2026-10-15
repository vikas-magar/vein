//! Aggregate unsigned regression coverage.

use super::*;

macro_rules! assert_unsigned_layout {
    ($array:ty, $data_type:expr, $sum_type:expr, $sum:expr, $min:expr, $max:expr) => {{
        let input: ArrayRef = Arc::new(<$array>::from(vec![Some(2), None, Some(5)]));
        let plan = AggregatePlan {
            groups: 0,
            estimated_groups: None,
            grouping_sets: None,
            emit_empty_group_on_empty_input: true,
            aggs: vec![
                AggOpCode::Sum,
                AggOpCode::Min,
                AggOpCode::Max,
                AggOpCode::Avg,
            ],
            aggregate_input_types: vec![$data_type.clone(); 4],
            types: vec![
                $sum_type,
                $data_type.clone(),
                $data_type.clone(),
                VeinDataType::Float64,
            ],
        };
        let mut sink = AggregateSink::new(plan).unwrap();
        sink.accumulate(DataChunk::from_vectors(vec![
            input.clone(),
            input.clone(),
            input.clone(),
            input,
        ]))
        .unwrap();
        let output = sink.finish().unwrap();
        assert_eq!(
            output[0]
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(0),
            $sum
        );
        assert_eq!(
            output[0]
                .column(1)
                .as_any()
                .downcast_ref::<$array>()
                .unwrap()
                .value(0),
            $min
        );
        assert_eq!(
            output[0]
                .column(2)
                .as_any()
                .downcast_ref::<$array>()
                .unwrap()
                .value(0),
            $max
        );
        assert_eq!(
            output[0]
                .column(3)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            3.5
        );
    }};
}

#[test]
fn unsigned_layouts_share_typed_states_and_ignore_nulls() {
    assert_unsigned_layout!(
        UInt8Array,
        VeinDataType::UInt8,
        VeinDataType::UInt64,
        7,
        2,
        5
    );
    assert_unsigned_layout!(
        UInt16Array,
        VeinDataType::UInt16,
        VeinDataType::UInt64,
        7,
        2,
        5
    );
    assert_unsigned_layout!(
        UInt32Array,
        VeinDataType::UInt32,
        VeinDataType::UInt64,
        7,
        2,
        5
    );
    assert_unsigned_layout!(
        UInt64Array,
        VeinDataType::UInt64,
        VeinDataType::UInt64,
        7,
        2,
        5
    );
}

#[test]
fn uint64_sum_checks_overflow_and_average_uses_wide_exact_state() {
    let sum_plan = AggregatePlan {
        groups: 0,
        estimated_groups: None,
        grouping_sets: None,
        emit_empty_group_on_empty_input: true,
        aggs: vec![AggOpCode::Sum],
        aggregate_input_types: vec![VeinDataType::UInt64],
        types: vec![VeinDataType::UInt64],
    };
    let mut sum_sink = AggregateSink::new(sum_plan).unwrap();
    let overflow = sum_sink
        .accumulate(DataChunk::from_vectors(vec![Arc::new(UInt64Array::from(
            vec![u64::MAX, 1],
        ))]))
        .unwrap_err();
    assert!(
        overflow
            .to_string()
            .contains("overflow while computing SUM")
    );

    let avg_plan = AggregatePlan {
        groups: 0,
        estimated_groups: None,
        grouping_sets: None,
        emit_empty_group_on_empty_input: true,
        aggs: vec![AggOpCode::Avg],
        aggregate_input_types: vec![VeinDataType::UInt64],
        types: vec![VeinDataType::Float64],
    };
    let mut avg_sink = AggregateSink::new(avg_plan).unwrap();
    avg_sink
        .accumulate(DataChunk::from_vectors(vec![Arc::new(UInt64Array::from(
            vec![u64::MAX],
        ))]))
        .unwrap();
    avg_sink
        .accumulate(DataChunk::from_vectors(vec![Arc::new(UInt64Array::from(
            vec![u64::MAX],
        ))]))
        .unwrap();
    let output = avg_sink.finish().unwrap();
    let average = output[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(average.value(0), u64::MAX as f64);
}
