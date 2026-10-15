//! Fold selected input rows into typed aggregate state vectors.

use super::*;

impl AggregateStateColumn {
    pub(in crate::operators::aggregate) fn update_batch(&mut self, input: AggregateInput<'_>, group_rows: GroupRows<'_>) -> Result<()> {
        match (self, input) {
            (Self::SingleValue(states), AggregateInput::Any(array)) => {
                for (slot, row) in group_rows {
                    if states[slot].is_some() {
                        return Err(VeinError::stage(
                            "aggregate",
                            "scalar subquery returned more than one row",
                        ));
                    }
                    states[slot] = Some((Arc::clone(array), row));
                }
                Ok(())
            }
            (Self::Count(states), AggregateInput::Count(array)) => {
                for (slot, row) in group_rows {
                    if !array.is_null(row) {
                        states[slot] += 1;
                    }
                }
                Ok(())
            }
            (Self::Int64 { states, .. }, AggregateInput::Int8(array)) => {
                update_numeric_states(states, group_rows, |row| {
                    (!array.is_null(row)).then(|| array.value(row) as i64)
                })
            }
            (Self::Int64 { states, .. }, AggregateInput::Int16(array)) => {
                update_numeric_states(states, group_rows, |row| {
                    (!array.is_null(row)).then(|| array.value(row) as i64)
                })
            }
            (Self::Int64 { states, .. }, AggregateInput::Int32(array)) => {
                update_numeric_states(states, group_rows, |row| {
                    (!array.is_null(row)).then(|| array.value(row) as i64)
                })
            }
            (Self::Int64 { states, .. }, AggregateInput::Int64(array)) => {
                update_numeric_states(states, group_rows, |row| {
                    (!array.is_null(row)).then(|| array.value(row))
                })
            }
            (Self::Int128Avg { states }, AggregateInput::Int8(array)) => {
                update_numeric_states(states, group_rows, |row| {
                    (!array.is_null(row)).then(|| array.value(row) as i128)
                })
            }
            (Self::Int128Avg { states }, AggregateInput::Int16(array)) => {
                update_numeric_states(states, group_rows, |row| {
                    (!array.is_null(row)).then(|| array.value(row) as i128)
                })
            }
            (Self::Int128Avg { states }, AggregateInput::Int32(array)) => {
                update_numeric_states(states, group_rows, |row| {
                    (!array.is_null(row)).then(|| array.value(row) as i128)
                })
            }
            (Self::Int128Avg { states }, AggregateInput::Int64(array)) => {
                update_numeric_states(states, group_rows, |row| {
                    (!array.is_null(row)).then(|| array.value(row) as i128)
                })
            }
            (Self::UInt64 { states, .. }, AggregateInput::UInt8(array)) => {
                update_numeric_states(states, group_rows, |row| {
                    (!array.is_null(row)).then(|| array.value(row) as u64)
                })
            }
            (Self::UInt64 { states, .. }, AggregateInput::UInt16(array)) => {
                update_numeric_states(states, group_rows, |row| {
                    (!array.is_null(row)).then(|| array.value(row) as u64)
                })
            }
            (Self::UInt64 { states, .. }, AggregateInput::UInt32(array)) => {
                update_numeric_states(states, group_rows, |row| {
                    (!array.is_null(row)).then(|| array.value(row) as u64)
                })
            }
            (Self::UInt64 { states, .. }, AggregateInput::UInt64(array)) => {
                update_numeric_states(states, group_rows, |row| {
                    (!array.is_null(row)).then(|| array.value(row))
                })
            }
            (Self::UInt128Avg { states }, AggregateInput::UInt8(array)) => {
                update_numeric_states(states, group_rows, |row| {
                    (!array.is_null(row)).then(|| array.value(row) as u128)
                })
            }
            (Self::UInt128Avg { states }, AggregateInput::UInt16(array)) => {
                update_numeric_states(states, group_rows, |row| {
                    (!array.is_null(row)).then(|| array.value(row) as u128)
                })
            }
            (Self::UInt128Avg { states }, AggregateInput::UInt32(array)) => {
                update_numeric_states(states, group_rows, |row| {
                    (!array.is_null(row)).then(|| array.value(row) as u128)
                })
            }
            (Self::UInt128Avg { states }, AggregateInput::UInt64(array)) => {
                update_numeric_states(states, group_rows, |row| {
                    (!array.is_null(row)).then(|| array.value(row) as u128)
                })
            }
            (Self::Float32 { states, .. }, AggregateInput::Float16(array)) => {
                update_numeric_states(states, group_rows, |row| {
                    (!array.is_null(row)).then(|| array.value(row).to_f32())
                })
            }
            (Self::Float64 { states, .. }, AggregateInput::Float16(array)) => {
                update_numeric_states(states, group_rows, |row| {
                    (!array.is_null(row)).then(|| array.value(row).to_f64())
                })
            }
            (Self::Float32 { states, .. }, AggregateInput::Float32(array)) => {
                update_numeric_states(states, group_rows, |row| {
                    (!array.is_null(row)).then(|| array.value(row))
                })
            }
            (Self::Float64 { states, .. }, AggregateInput::Float32(array)) => {
                update_numeric_states(states, group_rows, |row| {
                    (!array.is_null(row)).then(|| array.value(row) as f64)
                })
            }
            (Self::Float64 { states, .. }, AggregateInput::Float64(array)) => {
                update_numeric_states(states, group_rows, |row| {
                    (!array.is_null(row)).then(|| array.value(row))
                })
            }
            (Self::Decimal128 { states, .. }, AggregateInput::Decimal128(array)) => {
                update_numeric_states(states, group_rows, |row| {
                    (!array.is_null(row)).then(|| array.value(row))
                })
            }
            (Self::Decimal256 { states, .. }, AggregateInput::Decimal256(array)) => {
                update_numeric_states(states, group_rows, |row| {
                    (!array.is_null(row)).then(|| array.value(row))
                })
            }
            _ => Err(VeinError::internal(
                "aggregate input layout does not match its typed state column",
            )),
        }
    }
}
