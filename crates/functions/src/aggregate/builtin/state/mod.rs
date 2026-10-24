//! Typed state and algorithms for the built-in aggregate kernels.

mod allocation;
mod codec;
mod combine;
mod finalize;
mod number;
mod numeric;
mod update;

use std::any::Any;
use std::ops::Range;

use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use error::{Result, VeinError};

use crate::{AggregateAccumulator, AggregateOperation, FunctionInput, FunctionInputRow};
use numeric::NumericAccumulator;

enum BuiltinState {
    Count(Vec<i64>),
    SingleValue(Vec<Option<Option<FunctionInputRow>>>),
    Int64(Vec<NumericAccumulator<i64>>),
    Int128(Vec<NumericAccumulator<i128>>),
    UInt64(Vec<NumericAccumulator<u64>>),
    UInt128(Vec<NumericAccumulator<u128>>),
    Float32(Vec<NumericAccumulator<f32>>),
    Float64(Vec<NumericAccumulator<f64>>),
    Decimal128(Vec<NumericAccumulator<i128>>),
    Decimal256(Vec<NumericAccumulator<arrow::datatypes::i256>>),
}

pub(super) struct BuiltinAccumulator {
    operation: AggregateOperation,
    argument_type: Option<DataType>,
    result_type: DataType,
    scale: i8,
    decimal_precision_limit: bool,
    state: BuiltinState,
}

impl BuiltinAccumulator {
    pub(super) fn new(
        operation: AggregateOperation,
        argument_types: &[types::VeinDataType],
        result_type: &types::VeinDataType,
    ) -> Result<Self> {
        Self::create(operation, argument_types, result_type)
    }
}

impl AggregateAccumulator for BuiltinAccumulator {
    fn ensure_groups(&mut self, group_count: usize) -> Result<()> {
        self.state.ensure_groups(
            group_count,
            &self.operation,
            self.scale,
            self.decimal_precision_limit,
        )
    }

    fn update_batch(&mut self, arguments: &[FunctionInput<'_>], group_ids: &[usize]) -> Result<()> {
        self.state.update(&self.operation, self.argument_type.as_ref(), arguments, group_ids)
    }

    fn combine_group(&mut self, target: usize, source: &dyn AggregateAccumulator, source_group: usize) -> Result<()> {
        let source = source.as_any().downcast_ref::<Self>()
            .ok_or_else(|| VeinError::internal("aggregate accumulator types do not match"))?;
        if self.operation != source.operation || self.argument_type != source.argument_type
            || self.result_type != source.result_type
        {
            return Err(VeinError::internal("incompatible built-in aggregate accumulators"));
        }
        self.state.combine(target, &source.state, source_group)
    }

    fn finalize(&self, groups: Range<usize>) -> Result<ArrayRef> {
        self.state.finalize(&self.operation, &self.result_type, groups)
    }

    fn estimated_bytes_per_group(&self) -> usize { self.state.bytes_per_group() }

    fn encode_group(&self, group: usize, output: &mut Vec<u8>) -> Result<()> {
        self.state.encode(group, output)
    }

    fn merge_encoded_group(&mut self, group: usize, input: &mut &[u8]) -> Result<()> {
        self.state.merge_encoded(group, input)
    }

    fn as_any(&self) -> &dyn Any { self }
}

#[cfg(test)]
mod tests;
