use std::any::Any;
use std::ops::Range;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array};
use arrow::datatypes::DataType;
use error::{Result, VeinError};
use types::VeinDataType;

use crate::test_support::DenseInput;
use crate::{
    AggregateAccumulator, AggregateFunction, FunctionDefinition, FunctionInput, FunctionKind,
    FunctionRegistry, FunctionSignature, Volatility,
};

struct SumFunction;

impl AggregateFunction for SumFunction {
    fn return_type(&self, arguments: &[DataType]) -> Result<DataType> {
        match arguments {
            [DataType::Int64] => Ok(DataType::Int64),
            _ => Err(VeinError::planning("sum_test expects one Int64 input", None)),
        }
    }

    fn create_accumulator(
        &self,
        _arguments: &[DataType],
        _result: &DataType,
    ) -> Result<Box<dyn AggregateAccumulator>> {
        Ok(Box::new(SumState(Vec::new())))
    }
}

struct SumState(Vec<i64>);

impl AggregateAccumulator for SumState {
    fn ensure_groups(&mut self, group_count: usize) -> Result<()> {
        self.0.resize(group_count, 0);
        Ok(())
    }

    fn update_batch(&mut self, arguments: &[FunctionInput<'_>], group_ids: &[usize]) -> Result<()> {
        if arguments.len() != 1 || arguments[0].len() != group_ids.len() {
            return Err(VeinError::internal("invalid test aggregate input batch"));
        }
        for (row, &group) in group_ids.iter().enumerate() {
            let Some(value) = arguments[0].source_row(row)? else { continue };
            let array = value.array().as_any().downcast_ref::<Int64Array>()
                .ok_or_else(|| VeinError::internal("expected Int64 test input"))?;
            if !array.is_null(value.row_index()) {
                self.0[group] += array.value(value.row_index());
            }
        }
        Ok(())
    }

    fn combine_group(
        &mut self,
        target: usize,
        source: &dyn AggregateAccumulator,
        source_group: usize,
    ) -> Result<()> {
        let source = source.as_any().downcast_ref::<Self>()
            .ok_or_else(|| VeinError::internal("aggregate state type mismatch"))?;
        self.0[target] += source.0[source_group];
        Ok(())
    }

    fn finalize(&self, groups: Range<usize>) -> Result<ArrayRef> {
        Ok(Arc::new(Int64Array::from(self.0[groups].to_vec())))
    }

    fn estimated_bytes_per_group(&self) -> usize { std::mem::size_of::<i64>() }

    fn encode_group(&self, group: usize, output: &mut Vec<u8>) -> Result<()> {
        output.extend_from_slice(&self.0[group].to_le_bytes());
        Ok(())
    }

    fn merge_encoded_group(&mut self, group: usize, input: &mut &[u8]) -> Result<()> {
        let (value, remaining) = input.split_first_chunk::<8>()
            .ok_or_else(|| VeinError::internal("truncated test aggregate state"))?;
        self.0[group] += i64::from_le_bytes(*value);
        *input = remaining;
        Ok(())
    }

    fn as_any(&self) -> &dyn Any { self }
}

#[test]
fn registered_aggregate_batches_combine_and_spills_without_gathering_inputs() {
    let mut registry = FunctionRegistry::new();
    registry.register(FunctionDefinition::aggregate(
        "sum_test",
        FunctionSignature {
            return_type: VeinDataType::Int64,
            volatility: Volatility::Immutable,
            arg_types: vec![VeinDataType::Int64],
            min_arg_count: 1,
            kind: FunctionKind::Aggregate,
            bytecode_ref: None,
        },
        SumFunction,
    )).unwrap();

    let mut state = registry.create_aggregate_accumulator(
        "SUM_TEST", &[DataType::Int64], &DataType::Int64,
    ).unwrap();
    state.ensure_groups(2).unwrap();
    let input = DenseInput::column(Arc::new(Int64Array::from(vec![4, 6, 3])) as ArrayRef);
    state.update_batch(&[FunctionInput::new(&input, 3)], &[0, 1, 0]).unwrap();

    let mut worker = registry.create_aggregate_accumulator(
        "sum_test", &[DataType::Int64], &DataType::Int64,
    ).unwrap();
    worker.ensure_groups(1).unwrap();
    let input = DenseInput::column(Arc::new(Int64Array::from(vec![5])) as ArrayRef);
    worker.update_batch(&[FunctionInput::new(&input, 1)], &[0]).unwrap();
    state.combine_group(0, worker.as_ref(), 0).unwrap();

    let mut encoded = Vec::new();
    state.encode_group(0, &mut encoded).unwrap();
    let mut restored = registry.create_aggregate_accumulator(
        "sum_test", &[DataType::Int64], &DataType::Int64,
    ).unwrap();
    restored.ensure_groups(1).unwrap();
    let mut remaining = encoded.as_slice();
    restored.merge_encoded_group(0, &mut remaining).unwrap();
    assert!(remaining.is_empty());
    let result = restored.finalize(0..1).unwrap();
    let result = result.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(result.values(), &[12]);
}
