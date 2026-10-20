//! Fold selected input rows into typed aggregate state vectors.

use super::*;

macro_rules! update_values {
    ($states:expr, $rows:expr, $input:expr, $convert:expr) => {
        update_numeric_states($states, $rows, |row| {
            $input.value(row, |array, physical_row| ($convert)(array.value(physical_row)))
        })
    };
}

impl AggregateStateColumn {
    pub(in crate::operators::aggregate) fn update_batch(
        &mut self,
        input: &AggregateInput<'_>,
        group_rows: GroupRows<'_>,
    ) -> Result<()> {
        match (self, input) {
            (Self::SingleValue(states), AggregateInput::Any(column)) => {
                for (slot, row) in group_rows {
                    if states[slot].is_some() {
                        return Err(VeinError::stage(
                            "aggregate",
                            "scalar subquery returned more than one row",
                        ));
                    }
                    states[slot] = Some(column.source_value(row)?);
                }
                Ok(())
            }
            (Self::Count(states), AggregateInput::Count(column)) => {
                for (slot, row) in group_rows {
                    if !column.is_null(row)? {
                        states[slot] += 1;
                    }
                }
                Ok(())
            }
            (Self::Int64 { states, .. }, AggregateInput::Int8(array)) => {
                update_values!(states, group_rows, array, |value| value as i64)
            }
            (Self::Int64 { states, .. }, AggregateInput::Int16(array)) => {
                update_values!(states, group_rows, array, |value| value as i64)
            }
            (Self::Int64 { states, .. }, AggregateInput::Int32(array)) => {
                update_values!(states, group_rows, array, |value| value as i64)
            }
            (Self::Int64 { states, .. }, AggregateInput::Int64(array)) => {
                update_values!(states, group_rows, array, |value| value)
            }
            (Self::Int128Avg { states }, AggregateInput::Int8(array)) => {
                update_values!(states, group_rows, array, |value| value as i128)
            }
            (Self::Int128Avg { states }, AggregateInput::Int16(array)) => {
                update_values!(states, group_rows, array, |value| value as i128)
            }
            (Self::Int128Avg { states }, AggregateInput::Int32(array)) => {
                update_values!(states, group_rows, array, |value| value as i128)
            }
            (Self::Int128Avg { states }, AggregateInput::Int64(array)) => {
                update_values!(states, group_rows, array, |value| value as i128)
            }
            (Self::UInt64 { states, .. }, AggregateInput::UInt8(array)) => {
                update_values!(states, group_rows, array, |value| value as u64)
            }
            (Self::UInt64 { states, .. }, AggregateInput::UInt16(array)) => {
                update_values!(states, group_rows, array, |value| value as u64)
            }
            (Self::UInt64 { states, .. }, AggregateInput::UInt32(array)) => {
                update_values!(states, group_rows, array, |value| value as u64)
            }
            (Self::UInt64 { states, .. }, AggregateInput::UInt64(array)) => {
                update_values!(states, group_rows, array, |value| value)
            }
            (Self::UInt128Avg { states }, AggregateInput::UInt8(array)) => {
                update_values!(states, group_rows, array, |value| value as u128)
            }
            (Self::UInt128Avg { states }, AggregateInput::UInt16(array)) => {
                update_values!(states, group_rows, array, |value| value as u128)
            }
            (Self::UInt128Avg { states }, AggregateInput::UInt32(array)) => {
                update_values!(states, group_rows, array, |value| value as u128)
            }
            (Self::UInt128Avg { states }, AggregateInput::UInt64(array)) => {
                update_values!(states, group_rows, array, |value| value as u128)
            }
            (Self::Float32 { states, .. }, AggregateInput::Float16(array)) => {
                update_numeric_states(states, group_rows, |row| array.value_as_f32(row))
            }
            (Self::Float64 { states, .. }, AggregateInput::Float16(array)) => {
                update_numeric_states(states, group_rows, |row| array.value_as_f64(row))
            }
            (Self::Float32 { states, .. }, AggregateInput::Float32(array)) => {
                update_values!(states, group_rows, array, |value| value)
            }
            (Self::Float64 { states, .. }, AggregateInput::Float32(array)) => {
                update_values!(states, group_rows, array, |value| value as f64)
            }
            (Self::Float64 { states, .. }, AggregateInput::Float64(array)) => {
                update_values!(states, group_rows, array, |value| value)
            }
            (Self::Decimal128 { states, .. }, AggregateInput::Decimal128(array)) => {
                update_values!(states, group_rows, array, |value| value)
            }
            (Self::Decimal256 { states, .. }, AggregateInput::Decimal256(array)) => {
                update_values!(states, group_rows, array, |value| value)
            }
            _ => Err(VeinError::internal(
                "aggregate input layout does not match its typed state column",
            )),
        }
    }
}
