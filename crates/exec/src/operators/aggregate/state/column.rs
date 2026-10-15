//! Typed aggregate state column variants.

use super::*;

/// A typed state vector selected once when the aggregate sink is created.
pub(in crate::operators::aggregate) enum AggregateStateColumn {
    SingleValue(Vec<Option<(ArrayRef, usize)>>),
    Count(Vec<i64>),
    Int64 {
        op: AggOpCode,
        states: Vec<NumericAccumulator<i64>>,
    },
    Int128Avg {
        states: Vec<NumericAccumulator<i128>>,
    },
    UInt64 {
        op: AggOpCode,
        states: Vec<NumericAccumulator<u64>>,
    },
    UInt128Avg {
        states: Vec<NumericAccumulator<u128>>,
    },
    Float32 {
        op: AggOpCode,
        states: Vec<NumericAccumulator<f32>>,
    },
    Float64 {
        op: AggOpCode,
        states: Vec<NumericAccumulator<f64>>,
    },
    Decimal128 {
        op: AggOpCode,
        scale: i8,
        states: Vec<NumericAccumulator<i128>>,
    },
    Decimal256 {
        op: AggOpCode,
        scale: i8,
        states: Vec<NumericAccumulator<i256>>,
    },
}
