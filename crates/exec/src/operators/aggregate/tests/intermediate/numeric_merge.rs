//! Aggregate numeric merge regression coverage.

use super::*;

#[test]
fn intermediate_numeric_merge_combines_partials_and_is_atomic_on_error() {
    let sum_source = AggregateStateColumn::Int64 {
        op: AggOpCode::Sum,
        states: vec![NumericAccumulator {
            op: AggOpCode::Sum,
            value: Some(5_i64),
            count: 0,
            scale: 0,
            decimal_precision_limit: false,
        }],
    };
    let sum_partial = sum_source
        .intermediate_state(0)
        .expect("snapshot SUM partial");
    let mut sum_target = AggregateStateColumn::Int64 {
        op: AggOpCode::Sum,
        states: vec![NumericAccumulator {
            op: AggOpCode::Sum,
            value: Some(10_i64),
            count: 0,
            scale: 0,
            decimal_precision_limit: false,
        }],
    };
    sum_target
        .merge_intermediate_state(0, &sum_partial)
        .expect("merge SUM partial");
    let AggregateStateColumn::Int64 { states, .. } = sum_target else {
        panic!("expected Int64 SUM state");
    };
    assert_eq!(states[0].value, Some(15));

    let avg_partial = AggregateIntermediateState::Int128Avg(NumericPartial {
        op: AggOpCode::Avg,
        value: Some(300_i128),
        count: 3,
        scale: 0,
        decimal_precision_limit: false,
    });
    let mut avg_target = AggregateStateColumn::Int128Avg {
        states: vec![NumericAccumulator {
            op: AggOpCode::Avg,
            value: Some(100_i128),
            count: 2,
            scale: 0,
            decimal_precision_limit: false,
        }],
    };
    avg_target
        .merge_intermediate_state(0, &avg_partial)
        .expect("merge AVG sum and count");
    let AggregateStateColumn::Int128Avg { states } = avg_target else {
        panic!("expected Int128 AVG state");
    };
    assert_eq!(states[0].value, Some(400));
    assert_eq!(states[0].count, 5);

    let overflow_partial = AggregateIntermediateState::Int64(NumericPartial {
        op: AggOpCode::Sum,
        value: Some(1_i64),
        count: 0,
        scale: 0,
        decimal_precision_limit: false,
    });
    let mut overflow_target = AggregateStateColumn::Int64 {
        op: AggOpCode::Sum,
        states: vec![NumericAccumulator {
            op: AggOpCode::Sum,
            value: Some(i64::MAX),
            count: 0,
            scale: 0,
            decimal_precision_limit: false,
        }],
    };
    assert!(overflow_target
        .merge_intermediate_state(0, &overflow_partial)
        .is_err());
    let AggregateStateColumn::Int64 { states, .. } = &overflow_target else {
        panic!("expected Int64 SUM state");
    };
    assert_eq!(states[0].value, Some(i64::MAX));

    let incompatible_partial = AggregateIntermediateState::Int64(NumericPartial {
        op: AggOpCode::Min,
        value: Some(3_i64),
        count: 0,
        scale: 0,
        decimal_precision_limit: false,
    });
    assert!(overflow_target
        .merge_intermediate_state(0, &incompatible_partial)
        .is_err());
    let AggregateStateColumn::Int64 { states, .. } = overflow_target else {
        panic!("expected Int64 SUM state");
    };
    assert_eq!(states[0].value, Some(i64::MAX));
}
