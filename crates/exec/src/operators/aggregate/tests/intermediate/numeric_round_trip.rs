//! Aggregate numeric round trip regression coverage.

use super::*;

#[test]
fn intermediate_numeric_states_round_trip_without_finalizing_averages() {
    macro_rules! round_trip_numeric {
        ($variant:ident, $column:expr, $expected:expr) => {{
            let mut column = $column;
            let partial = column
                .intermediate_state(0)
                .expect("snapshot numeric aggregate state");
            column.push_group();
            column
                .merge_intermediate_state(1, &partial)
                .expect("restore numeric aggregate state");
            let AggregateStateColumn::$variant { states, .. } = column else {
                panic!("expected {} state column", stringify!($variant));
            };
            assert_eq!(states[1].value, $expected);
            assert_eq!(states[1].count, states[0].count);
            assert_eq!(states[1].op, states[0].op);
            assert_eq!(states[1].scale, states[0].scale);
        }};
    }

    round_trip_numeric!(
        Int64,
        AggregateStateColumn::Int64 {
            op: AggOpCode::Sum,
            states: vec![NumericAccumulator {
                op: AggOpCode::Sum,
                value: Some(31_i64),
                count: 0,
                scale: 0,
                decimal_precision_limit: false,
            }],
        },
        Some(31_i64)
    );
    // This sum cannot be represented exactly as Float64. The intermediate
    // AVG state must preserve it together with its row count.
    round_trip_numeric!(
        Int128Avg,
        AggregateStateColumn::Int128Avg {
            states: vec![NumericAccumulator {
                op: AggOpCode::Avg,
                value: Some(9_007_199_254_740_993_i128),
                count: 2,
                scale: 0,
                decimal_precision_limit: false,
            }],
        },
        Some(9_007_199_254_740_993_i128)
    );
    round_trip_numeric!(
        UInt64,
        AggregateStateColumn::UInt64 {
            op: AggOpCode::Max,
            states: vec![NumericAccumulator {
                op: AggOpCode::Max,
                value: Some(42_u64),
                count: 0,
                scale: 0,
                decimal_precision_limit: false,
            }],
        },
        Some(42_u64)
    );
    round_trip_numeric!(
        UInt128Avg,
        AggregateStateColumn::UInt128Avg {
            states: vec![NumericAccumulator {
                op: AggOpCode::Avg,
                value: Some(42_u128),
                count: 3,
                scale: 0,
                decimal_precision_limit: false,
            }],
        },
        Some(42_u128)
    );
    round_trip_numeric!(
        Float32,
        AggregateStateColumn::Float32 {
            op: AggOpCode::Min,
            states: vec![NumericAccumulator {
                op: AggOpCode::Min,
                value: Some(1.25_f32),
                count: 0,
                scale: 0,
                decimal_precision_limit: false,
            }],
        },
        Some(1.25_f32)
    );
    round_trip_numeric!(
        Float64,
        AggregateStateColumn::Float64 {
            op: AggOpCode::Sum,
            states: vec![NumericAccumulator {
                op: AggOpCode::Sum,
                value: Some(2.5_f64),
                count: 0,
                scale: 0,
                decimal_precision_limit: false,
            }],
        },
        Some(2.5_f64)
    );
    round_trip_numeric!(
        Decimal128,
        AggregateStateColumn::Decimal128 {
            op: AggOpCode::Sum,
            scale: 2,
            states: vec![NumericAccumulator {
                op: AggOpCode::Sum,
                value: Some(1_234_i128),
                count: 0,
                scale: 2,
                decimal_precision_limit: true,
            }],
        },
        Some(1_234_i128)
    );
    round_trip_numeric!(
        Decimal256,
        AggregateStateColumn::Decimal256 {
            op: AggOpCode::Max,
            scale: 3,
            states: vec![NumericAccumulator {
                op: AggOpCode::Max,
                value: Some(i256::from(12_i64)),
                count: 0,
                scale: 3,
                decimal_precision_limit: false,
            }],
        },
        Some(i256::from(12_i64))
    );

    let mut count = AggregateStateColumn::Count(vec![7]);
    let partial = count.intermediate_state(0).expect("snapshot COUNT state");
    count.push_group();
    count
        .merge_intermediate_state(1, &partial)
        .expect("restore COUNT state");
    let AggregateStateColumn::Count(counts) = count else {
        panic!("expected COUNT state column");
    };
    assert_eq!(counts, vec![7, 7]);
}
