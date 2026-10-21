//! Built-in aggregate implementations registered beside their signatures.

mod state;

use arrow::datatypes::DataType;
use error::{Result, VeinError};

use super::{AggregateAccumulator, AggregateFunction, AggregateOperation};

/// Runtime implementation for a built-in aggregate identity.
#[derive(Clone)]
pub(super) struct BuiltinAggregate {
    operation: AggregateOperation,
}

impl BuiltinAggregate {
    pub(super) const fn new(operation: AggregateOperation) -> Self {
        Self { operation }
    }
}

impl AggregateFunction for BuiltinAggregate {
    fn return_type(&self, argument_types: &[DataType]) -> Result<DataType> {
        if self.operation == AggregateOperation::Count {
            return Ok(DataType::Int64);
        }
        let [argument] = argument_types else {
            return Err(VeinError::planning(
                "aggregate requires exactly one argument",
                None,
            ));
        };
        if self.operation == AggregateOperation::SingleValue {
            return Ok(argument.clone());
        }
        let input = types::VeinDataType::try_from(argument)?;
        let fallback = if self.operation == AggregateOperation::Avg {
            types::VeinDataType::Float64
        } else {
            input.clone()
        };
        Ok(types::aggregate_return_type(
            self.operation.name(),
            Some(&input),
            fallback,
        )
            .to_arrow())
    }

    fn create_accumulator(
        &self,
        argument_types: &[DataType],
        result_type: &DataType,
    ) -> Result<Box<dyn AggregateAccumulator>> {
        Ok(Box::new(state::BuiltinAccumulator::new(
            self.operation.clone(),
            argument_types,
            result_type,
        )?))
    }
}
