use error::{Result, VeinError};

use super::{BuiltinState, number::AggregateNumber, numeric::NumericAccumulator};

impl BuiltinState {
    pub(super) fn encode(&self, group: usize, output: &mut Vec<u8>) -> Result<()> {
        match self {
            Self::Count(states) => output.extend_from_slice(&read(states, group, "COUNT")?.to_le_bytes()),
            Self::SingleValue(_) => return Err(VeinError::unsupported(
                "SINGLE_VALUE state cannot be spilled", None,
            )),
            Self::Int64(states) => encode_numeric(states, group, output)?,
            Self::Int128(states) => encode_numeric(states, group, output)?,
            Self::UInt64(states) => encode_numeric(states, group, output)?,
            Self::UInt128(states) => encode_numeric(states, group, output)?,
            Self::Float32(states) => encode_numeric(states, group, output)?,
            Self::Float64(states) => encode_numeric(states, group, output)?,
            Self::Decimal128(states) => encode_numeric(states, group, output)?,
            Self::Decimal256(states) => encode_numeric(states, group, output)?,
        }
        Ok(())
    }

    pub(super) fn merge_encoded(&mut self, group: usize, input: &mut &[u8]) -> Result<()> {
        match self {
            Self::Count(states) => {
                let partial = read_i64(input)?;
                if partial < 0 { return Err(VeinError::internal("negative spilled COUNT state")); }
                let target = states.get_mut(group)
                    .ok_or_else(|| VeinError::internal("COUNT merge group is out of bounds"))?;
                *target = target.checked_add(partial)
                    .ok_or_else(|| VeinError::internal("COUNT overflow while merging spill state"))?;
                Ok(())
            }
            Self::SingleValue(_) => Err(VeinError::unsupported(
                "SINGLE_VALUE state cannot be restored from spill", None,
            )),
            Self::Int64(states) => merge_numeric(states, group, input),
            Self::Int128(states) => merge_numeric(states, group, input),
            Self::UInt64(states) => merge_numeric(states, group, input),
            Self::UInt128(states) => merge_numeric(states, group, input),
            Self::Float32(states) => merge_numeric(states, group, input),
            Self::Float64(states) => merge_numeric(states, group, input),
            Self::Decimal128(states) => merge_numeric(states, group, input),
            Self::Decimal256(states) => merge_numeric(states, group, input),
        }
    }
}

fn encode_numeric<T: AggregateNumber>(
    states: &[NumericAccumulator<T>],
    group: usize,
    output: &mut Vec<u8>,
) -> Result<()> {
    let state = states.get(group)
        .ok_or_else(|| VeinError::internal("aggregate spill group is out of bounds"))?;
    match state.value {
        Some(value) => {
            output.push(1);
            value.encode(output);
        }
        None => output.push(0),
    }
    output.extend_from_slice(&state.count.to_le_bytes());
    output.push(state.scale as u8);
    output.push(u8::from(state.decimal_precision_limit.is_some()));
    Ok(())
}

fn merge_numeric<T: AggregateNumber>(states: &mut [NumericAccumulator<T>], group: usize, input: &mut &[u8]) -> Result<()> {
    let (operation, scale, precision_limit) = {
        let state = states.get(group)
            .ok_or_else(|| VeinError::internal("aggregate merge group is out of bounds"))?;
        (state.operation.clone(), state.scale, state.decimal_precision_limit)
    };
    let value = match read_u8(input)? {
        0 => None,
        1 => Some(T::decode(input)?),
        _ => return Err(VeinError::internal("invalid aggregate spill value marker")),
    };
    let count = read_i64(input)?;
    let incoming_scale = read_u8(input)? as i8;
    let incoming_precision_limit = match read_u8(input)? {
        0 => false,
        1 => true,
        _ => return Err(VeinError::internal("invalid aggregate spill precision flag")),
    };
    if count < 0 || (operation != crate::AggregateOperation::Avg && count != 0)
        || (operation == crate::AggregateOperation::Avg && ((count == 0) != value.is_none()))
        || scale != incoming_scale || precision_limit.is_some() != incoming_precision_limit
    {
        return Err(VeinError::internal("invalid or incompatible aggregate spill state"));
    }
    states.get_mut(group)
        .ok_or_else(|| VeinError::internal("aggregate merge group is out of bounds"))?
        .merge(&NumericAccumulator { operation, value, count, scale, decimal_precision_limit: precision_limit })
}

fn read<T: Copy>(states: &[T], group: usize, name: &str) -> Result<T> {
    states.get(group).copied()
        .ok_or_else(|| VeinError::internal(format!("{name} spill group is out of bounds")))
}

fn read_u8(input: &mut &[u8]) -> Result<u8> {
    let (value, remaining) = input.split_first()
        .ok_or_else(|| VeinError::internal("truncated aggregate spill state"))?;
    *input = remaining;
    Ok(*value)
}

fn read_i64(input: &mut &[u8]) -> Result<i64> {
    let (bytes, remaining) = input.split_first_chunk::<8>()
        .ok_or_else(|| VeinError::internal("truncated aggregate spill count"))?;
    *input = remaining;
    Ok(i64::from_le_bytes(*bytes))
}
