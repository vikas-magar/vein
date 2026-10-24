use std::mem::size_of;
use arrow::datatypes::DataType;
use error::{Result, VeinError};

use crate::AggregateOperation;

use super::{BuiltinAccumulator, BuiltinState, numeric::NumericAccumulator};
use super::number::AggregateNumber;

impl BuiltinAccumulator {
    pub(super) fn create(
        operation: AggregateOperation,
        argument_types: &[types::VeinDataType],
        result_type: &types::VeinDataType,
    ) -> Result<Self> {
        let argument_type = match (operation.clone(), argument_types) {
            (AggregateOperation::Count, []) => None,
            (AggregateOperation::Count, [argument]) => Some(argument.clone()),
            (AggregateOperation::SingleValue | AggregateOperation::CountDistinct, [argument]) => Some(argument.clone()),
            (_, [argument]) => Some(argument.clone()),
            _ => return Err(VeinError::planning(
                format!("aggregate {} accepts at most one argument", operation.name()), None,
            )),
        };
        let native_int128 = result_type.is_int128()
            || argument_type.as_ref().is_some_and(types::VeinDataType::is_int128);
        let argument_type = argument_type.as_ref().map(types::VeinDataType::to_arrow);
        let result_type = result_type.to_arrow();
        let (state, scale, decimal_precision_limit) = match operation.clone() {
            AggregateOperation::Count | AggregateOperation::CountDistinct => {
                (BuiltinState::Count(Vec::new()), 0, false)
            }
            AggregateOperation::SingleValue => (BuiltinState::SingleValue(Vec::new()), 0, false),
            _ if native_int128 => (BuiltinState::Int128(Vec::new()), 0, false),
            _ => numeric_state(operation.clone(), argument_type.as_ref(), &result_type)?,
        };
        Ok(Self {
            operation,
            argument_type,
            result_type,
            scale,
            decimal_precision_limit,
            state,
        })
    }
}

fn numeric_state(
    operation: AggregateOperation,
    input_type: Option<&DataType>,
    result_type: &DataType,
) -> Result<(BuiltinState, i8, bool)> {
    let input = input_type.ok_or_else(|| VeinError::planning(
        format!("aggregate {} requires an argument", operation.name()), None,
    ))?;
    let (state, scale, decimal) = match input {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
        if operation == AggregateOperation::Avg => (BuiltinState::Int128(Vec::new()), 0, false),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            (BuiltinState::Int64(Vec::new()), 0, false)
        }
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64
        if operation == AggregateOperation::Avg => (BuiltinState::UInt128(Vec::new()), 0, false),
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            (BuiltinState::UInt64(Vec::new()), 0, false)
        }
        DataType::Float16 | DataType::Float32 if matches!(result_type, DataType::Float64) => {
            (BuiltinState::Float64(Vec::new()), 0, false)
        }
        DataType::Float16 if operation == AggregateOperation::Avg => {
            (BuiltinState::Float64(Vec::new()), 0, false)
        }
        DataType::Float16 => (BuiltinState::Float32(Vec::new()), 0, false),
        DataType::Float32 if operation == AggregateOperation::Avg => {
            (BuiltinState::Float64(Vec::new()), 0, false)
        }
        DataType::Float32 => (BuiltinState::Float32(Vec::new()), 0, false),
        DataType::Float64 => (BuiltinState::Float64(Vec::new()), 0, false),
        DataType::Decimal128(_, scale) if matches!(result_type, DataType::Decimal256(_, _)) => {
            (BuiltinState::Decimal256(Vec::new()), *scale, operation == AggregateOperation::Sum)
        }
        DataType::Decimal128(_, scale) => {
            (BuiltinState::Decimal128(Vec::new()), *scale, operation == AggregateOperation::Sum)
        }
        DataType::Decimal256(_, scale) => {
            (BuiltinState::Decimal256(Vec::new()), *scale, operation == AggregateOperation::Sum)
        }
        other => return Err(VeinError::planning(
            format!("aggregate {} does not support input type {other:?}", operation.name()), None,
        )),
    };
    Ok((state, scale, decimal))
}

impl BuiltinState {
    pub(super) fn ensure_groups(
        &mut self,
        group_count: usize,
        operation: &AggregateOperation,
        scale: i8,
        decimal_limit: bool,
    ) -> Result<()> {
        match self {
            Self::Count(values) => resize(values, group_count, 0)?,
            Self::SingleValue(values) => resize(values, group_count, None)?,
            Self::Int64(values) => resize_numeric(values, group_count, operation, scale, decimal_limit)?,
            Self::Int128(values) => resize_numeric(values, group_count, operation, scale, decimal_limit)?,
            Self::UInt64(values) => resize_numeric(values, group_count, operation, scale, decimal_limit)?,
            Self::UInt128(values) => resize_numeric(values, group_count, operation, scale, decimal_limit)?,
            Self::Float32(values) => resize_numeric(values, group_count, operation, scale, decimal_limit)?,
            Self::Float64(values) => resize_numeric(values, group_count, operation, scale, decimal_limit)?,
            Self::Decimal128(values) => resize_numeric(values, group_count, operation, scale, decimal_limit)?,
            Self::Decimal256(values) => resize_numeric(values, group_count, operation, scale, decimal_limit)?,
        }
        Ok(())
    }

    pub(super) fn bytes_per_group(&self) -> usize {
        match self {
            Self::Count(_) => size_of::<i64>(),
            Self::SingleValue(_) => size_of::<Option<Option<crate::FunctionInputRow>>>(),
            Self::Int64(_) => size_of::<NumericAccumulator<i64>>(),
            Self::Int128(_) | Self::Decimal128(_) => size_of::<NumericAccumulator<i128>>(),
            Self::UInt64(_) => size_of::<NumericAccumulator<u64>>(),
            Self::UInt128(_) => size_of::<NumericAccumulator<u128>>(),
            Self::Float32(_) => size_of::<NumericAccumulator<f32>>(),
            Self::Float64(_) => size_of::<NumericAccumulator<f64>>(),
            Self::Decimal256(_) => size_of::<NumericAccumulator<arrow::datatypes::i256>>(),
        }
    }
}

fn resize<T: Clone>(values: &mut Vec<T>, count: usize, value: T) -> Result<()> {
    let additional = count.saturating_sub(values.len());
    values.try_reserve(additional)
        .map_err(|error| VeinError::internal(format!("cannot reserve aggregate groups: {error}")))?;
    values.resize(count, value);
    Ok(())
}

fn resize_numeric<T: AggregateNumber>(
    values: &mut Vec<NumericAccumulator<T>>,
    count: usize,
    operation: &AggregateOperation,
    scale: i8,
    decimal_limit: bool,
) -> Result<()> {
    let decimal_limit = if decimal_limit {
        Some(T::decimal_max().ok_or_else(|| {
            VeinError::internal("decimal aggregate state has no precision bound")
        })?)
    } else {
        None
    };
    let additional = count.saturating_sub(values.len());
    values.try_reserve(additional)
        .map_err(|error| VeinError::internal(format!("cannot reserve aggregate numeric groups: {error}")))?;
    values.resize_with(count, || NumericAccumulator::new(operation.clone(), scale, decimal_limit));
    Ok(())
}
