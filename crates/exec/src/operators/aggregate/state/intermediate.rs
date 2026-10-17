//! Snapshot and merge aggregate partial states for spill and fan-in.

use super::*;

impl AggregateStateColumn {
    #[allow(dead_code)] // Used by aggregate spill serialization.
    pub(in crate::operators::aggregate) fn intermediate_state(&self, slot: usize) -> Result<AggregateIntermediateState> {
        macro_rules! numeric_state {
            ($states:expr, $variant:ident) => {
                AggregateIntermediateState::$variant(
                    $states
                        .get(slot)
                        .ok_or_else(|| VeinError::internal("aggregate state slot is out of bounds"))?
                        .snapshot(),
                )
            };
        }

        Ok(match self {
            Self::SingleValue(states) => {
                let value = states
                    .get(slot)
                    .ok_or_else(|| VeinError::internal("aggregate state slot is out of bounds"))?
                    .clone();
                AggregateIntermediateState::SingleValue {
                    rows: u64::from(value.is_some()),
                    value,
                }
            }
            Self::Count(states) => AggregateIntermediateState::Count(
                *states
                    .get(slot)
                    .ok_or_else(|| VeinError::internal("aggregate state slot is out of bounds"))?,
            ),
            Self::Int64 { states, .. } => numeric_state!(states, Int64),
            Self::Int128Avg { states } => numeric_state!(states, Int128Avg),
            Self::UInt64 { states, .. } => numeric_state!(states, UInt64),
            Self::UInt128Avg { states } => numeric_state!(states, UInt128Avg),
            Self::Float32 { states, .. } => numeric_state!(states, Float32),
            Self::Float64 { states, .. } => numeric_state!(states, Float64),
            Self::Decimal128 { states, .. } => numeric_state!(states, Decimal128),
            Self::Decimal256 { states, .. } => numeric_state!(states, Decimal256),
        })
    }

    #[allow(dead_code)] // Used when restoring aggregate spill state.
    pub(in crate::operators::aggregate) fn merge_intermediate_state(
        &mut self,
        slot: usize,
        partial: &AggregateIntermediateState,
    ) -> Result<()> {
        macro_rules! merge_numeric {
            ($states:expr, $partial:expr) => {{
                $states
                    .get_mut(slot)
                    .ok_or_else(|| VeinError::internal("aggregate state slot is out of bounds"))?
                    .merge_partial($partial)
            }};
        }

        match (self, partial) {
            (Self::SingleValue(states), AggregateIntermediateState::SingleValue { rows, value }) => {
                if *rows > 1 || (*rows == 0) != value.is_none() {
                    return Err(VeinError::internal("invalid SINGLE_VALUE partial state"));
                }
                let current = states
                    .get_mut(slot)
                    .ok_or_else(|| VeinError::internal("aggregate state slot is out of bounds"))?;
                let current_rows = u64::from(current.is_some());
                if current_rows + rows > 1 {
                    return Err(VeinError::stage(
                        "aggregate",
                        "scalar subquery returned more than one row",
                    ));
                }
                if *rows == 1 {
                    *current = value.clone();
                }
                Ok(())
            }
            (Self::Count(states), AggregateIntermediateState::Count(partial)) => {
                let current = states
                    .get_mut(slot)
                    .ok_or_else(|| VeinError::internal("aggregate state slot is out of bounds"))?;
                if *current < 0 || *partial < 0 {
                    return Err(VeinError::internal("invalid COUNT partial state"));
                }
                *current = current
                    .checked_add(*partial)
                    .ok_or_else(|| VeinError::internal("COUNT overflow while merging partial state"))?;
                Ok(())
            }
            (Self::Int64 { states, .. }, AggregateIntermediateState::Int64(partial)) => {
                merge_numeric!(states, partial)
            }
            (Self::Int128Avg { states }, AggregateIntermediateState::Int128Avg(partial)) => {
                merge_numeric!(states, partial)
            }
            (Self::UInt64 { states, .. }, AggregateIntermediateState::UInt64(partial)) => {
                merge_numeric!(states, partial)
            }
            (Self::UInt128Avg { states }, AggregateIntermediateState::UInt128Avg(partial)) => {
                merge_numeric!(states, partial)
            }
            (Self::Float32 { states, .. }, AggregateIntermediateState::Float32(partial)) => {
                merge_numeric!(states, partial)
            }
            (Self::Float64 { states, .. }, AggregateIntermediateState::Float64(partial)) => {
                merge_numeric!(states, partial)
            }
            (Self::Decimal128 { states, .. }, AggregateIntermediateState::Decimal128(partial)) => {
                merge_numeric!(states, partial)
            }
            (Self::Decimal256 { states, .. }, AggregateIntermediateState::Decimal256(partial)) => {
                merge_numeric!(states, partial)
            }
            _ => Err(VeinError::internal("incompatible aggregate partial state variant")),
        }
    }
}
