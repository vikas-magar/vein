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
        sink.accumulate(crate::pipeline::test_support::dense_view(vec![
            input.clone(),
            input.clone(),
            input.clone(),
            input,
        ]))
        .unwrap();
        let output = sink.finish().await.unwrap();
        assert_eq!(
            crate::pipeline::test_support::materialized_column::<UInt64Array>(&output[0], 0)
                .value(0),
            $sum
        );
        assert_eq!(
            crate::pipeline::test_support::materialized_column::<$array>(&output[0], 1)
                .value(0),
            $min
        );
        assert_eq!(
            crate::pipeline::test_support::materialized_column::<$array>(&output[0], 2)
                .value(0),
            $max
        );
        assert_eq!(
            crate::pipeline::test_support::materialized_column::<Float64Array>(&output[0], 3)
                .value(0),
            3.5
        );
    }};
}

#[tokio::test]
async fn unsigned_layouts_share_typed_states_and_ignore_nulls() {
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

#[tokio::test]
async fn uint64_sum_checks_overflow_and_average_uses_wide_exact_state() {
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
        .accumulate(crate::pipeline::test_support::dense_view(vec![Arc::new(UInt64Array::from(
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
        .accumulate(crate::pipeline::test_support::dense_view(vec![Arc::new(UInt64Array::from(
            vec![u64::MAX],
        ))]))
        .unwrap();
    avg_sink
        .accumulate(crate::pipeline::test_support::dense_view(vec![Arc::new(UInt64Array::from(
            vec![u64::MAX],
        ))]))
        .unwrap();
    let output = avg_sink.finish().await.unwrap();
    let average = crate::pipeline::test_support::materialized_column::<Float64Array>(&output[0], 0);
    assert_eq!(average.value(0), u64::MAX as f64);
}
