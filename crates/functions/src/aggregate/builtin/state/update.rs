use arrow::array::{Array, Decimal128Array, Decimal256Array, Float16Array, Float32Array, Float64Array,
                   Int8Array, Int16Array, Int32Array, Int64Array, UInt8Array, UInt16Array, UInt32Array, UInt64Array};
use arrow::datatypes::DataType;
use error::{Result, VeinError};

use crate::{AggregateOperation, FunctionInput};

use super::{BuiltinState, number::AggregateNumber, numeric::NumericAccumulator};

impl BuiltinState {
    pub(super) fn update(
        &mut self,
        operation: &AggregateOperation,
        argument_type: Option<&DataType>,
        arguments: &[FunctionInput<'_>],
        group_ids: &[usize],
    ) -> Result<()> {
        validate_inputs(arguments, group_ids)?;
        match self {
            Self::Count(states) => update_count(states, arguments, group_ids),
            Self::SingleValue(states) => update_single(states, arguments, group_ids),
            _ => update_numeric_state(self, operation, argument_type, arguments, group_ids),
        }
    }
}

macro_rules! update {
    ($states:expr, $array:ty, $input:expr, $groups:expr, $convert:expr) => {
        update_values::<_, $array, _>($states, $input, $groups, $convert)
    };
}

fn validate_inputs(arguments: &[FunctionInput<'_>], group_ids: &[usize]) -> Result<()> {
    if arguments.iter().any(|argument| argument.len() != group_ids.len()) {
        return Err(VeinError::internal("aggregate arguments do not match group ID count"));
    }
    Ok(())
}

fn update_count(states: &mut [i64], arguments: &[FunctionInput<'_>], groups: &[usize]) -> Result<()> {
    if arguments.len() > 1 {
        return Err(VeinError::internal("COUNT accepts zero or one argument"));
    }
    let Some(input) = arguments.first().copied() else {
        for &group in groups { increment_count(states, group)?; }
        return Ok(());
    };
    let mut start = 0;
    while start < groups.len() {
        let segment = input.segment(start, groups.len() - start)?;
        if let Some(array) = segment.array() {
            for local in 0..segment.len() {
                let row = if segment.is_scalar() { 0 } else { local };
                if !array.is_null(row) { increment_count(states, groups[start + local])?; }
            }
        }
        start += segment.len();
    }
    Ok(())
}

fn increment_count(states: &mut [i64], group: usize) -> Result<()> {
    let count = states.get_mut(group)
        .ok_or_else(|| VeinError::internal("COUNT group ID is out of bounds"))?;
    *count = count.checked_add(1).ok_or_else(|| VeinError::internal("COUNT overflow"))?;
    Ok(())
}

fn update_single(
    states: &mut [Option<Option<crate::FunctionInputRow>>],
    arguments: &[FunctionInput<'_>],
    groups: &[usize],
) -> Result<()> {
    let [input] = arguments else {
        return Err(VeinError::internal("SINGLE_VALUE requires exactly one argument"));
    };
    for (row, &group) in groups.iter().enumerate() {
        let state = states.get_mut(group)
            .ok_or_else(|| VeinError::internal("SINGLE_VALUE group ID is out of bounds"))?;
        if state.is_some() {
            return Err(VeinError::stage("aggregate", "scalar subquery returned more than one row"));
        }
        *state = Some(input.source_row(row)?);
    }
    Ok(())
}

fn update_numeric_state(
    state: &mut BuiltinState,
    operation: &AggregateOperation,
    argument_type: Option<&DataType>,
    arguments: &[FunctionInput<'_>],
    groups: &[usize],
) -> Result<()> {
    let [input] = arguments else {
        return Err(VeinError::internal("numeric aggregate requires exactly one argument"));
    };
    let input = *input;
    let input_type = argument_type.ok_or_else(|| VeinError::internal("numeric aggregate has no input type"))?;
    match (state, input_type) {
        (BuiltinState::Int64(states), DataType::Int8) => update!(states, Int8Array, input, groups, |a, r| i64::from(a.value(r))),
        (BuiltinState::Int64(states), DataType::Int16) => update!(states, Int16Array, input, groups, |a, r| i64::from(a.value(r))),
        (BuiltinState::Int64(states), DataType::Int32) => update!(states, Int32Array, input, groups, |a, r| i64::from(a.value(r))),
        (BuiltinState::Int64(states), DataType::Int64) => update!(states, Int64Array, input, groups, |a, r| a.value(r)),
        (BuiltinState::Int128(states), DataType::Int8) => update!(states, Int8Array, input, groups, |a, r| i128::from(a.value(r))),
        (BuiltinState::Int128(states), DataType::Int16) => update!(states, Int16Array, input, groups, |a, r| i128::from(a.value(r))),
        (BuiltinState::Int128(states), DataType::Int32) => update!(states, Int32Array, input, groups, |a, r| i128::from(a.value(r))),
        (BuiltinState::Int128(states), DataType::Int64) => update!(states, Int64Array, input, groups, |a, r| i128::from(a.value(r))),
        (BuiltinState::UInt64(states), DataType::UInt8) => update!(states, UInt8Array, input, groups, |a, r| u64::from(a.value(r))),
        (BuiltinState::UInt64(states), DataType::UInt16) => update!(states, UInt16Array, input, groups, |a, r| u64::from(a.value(r))),
        (BuiltinState::UInt64(states), DataType::UInt32) => update!(states, UInt32Array, input, groups, |a, r| u64::from(a.value(r))),
        (BuiltinState::UInt64(states), DataType::UInt64) => update!(states, UInt64Array, input, groups, |a, r| a.value(r)),
        (BuiltinState::UInt128(states), DataType::UInt8) => update!(states, UInt8Array, input, groups, |a, r| u128::from(a.value(r))),
        (BuiltinState::UInt128(states), DataType::UInt16) => update!(states, UInt16Array, input, groups, |a, r| u128::from(a.value(r))),
        (BuiltinState::UInt128(states), DataType::UInt32) => update!(states, UInt32Array, input, groups, |a, r| u128::from(a.value(r))),
        (BuiltinState::UInt128(states), DataType::UInt64) => update!(states, UInt64Array, input, groups, |a, r| u128::from(a.value(r))),
        (BuiltinState::Float32(states), DataType::Float16) => update!(states, Float16Array, input, groups, |a, r| a.value(r).to_f32()),
        (BuiltinState::Float32(states), DataType::Float32) => update!(states, Float32Array, input, groups, |a, r| a.value(r)),
        (BuiltinState::Float64(states), DataType::Float16) => update!(states, Float16Array, input, groups, |a, r| a.value(r).to_f64()),
        (BuiltinState::Float64(states), DataType::Float32) => update!(states, Float32Array, input, groups, |a, r| f64::from(a.value(r))),
        (BuiltinState::Float64(states), DataType::Float64) => update!(states, Float64Array, input, groups, |a, r| a.value(r)),
        (BuiltinState::Decimal128(states), DataType::Decimal128(_, _)) => update!(states, Decimal128Array, input, groups, |a, r| a.value(r)),
        (BuiltinState::Decimal256(states), DataType::Decimal128(_, _)) => update!(states, Decimal128Array, input, groups, |a, r| arrow::datatypes::i256::from(a.value(r))),
        (BuiltinState::Decimal256(states), DataType::Decimal256(_, _)) => update!(states, Decimal256Array, input, groups, |a, r| a.value(r)),
        _ => Err(VeinError::internal(format!("aggregate {operation:?} input {input_type:?} does not match its accumulator"))),
    }
}

fn update_values<T, A, V>(
    states: &mut [NumericAccumulator<T>],
    input: FunctionInput<'_>,
    groups: &[usize],
    mut value: V,
) -> Result<()>
where
    T: AggregateNumber,
    A: Array + 'static,
    V: FnMut(&A, usize) -> T,
{
    let mut start = 0;
    while start < groups.len() {
        let segment = input.segment(start, groups.len() - start)?;
        if let Some(array) = segment.array() {
            let array = array.as_any().downcast_ref::<A>().ok_or_else(|| {
                VeinError::internal("aggregate Arrow array type differs from its bound type")
            })?;
            for local in 0..segment.len() {
                let row = if segment.is_scalar() { 0 } else { local };
                let value = (!array.is_null(row)).then(|| value(array, row));
                let group = groups[start + local];
                states.get_mut(group)
                    .ok_or_else(|| VeinError::internal("aggregate group ID is out of bounds"))?
                    .update(value)?;
            }
        } else {
            for &group in &groups[start..start + segment.len()] {
                states.get_mut(group)
                    .ok_or_else(|| VeinError::internal("aggregate group ID is out of bounds"))?
                    .update(None)?;
            }
        }
        start += segment.len();
    }
    Ok(())
}
