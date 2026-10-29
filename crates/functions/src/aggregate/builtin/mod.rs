//! Built-in aggregate implementations registered beside their signatures.

mod state;

use types::VeinDataType;
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
    fn return_type(&self, argument_types: &[VeinDataType]) -> Result<VeinDataType> {
        if self.operation == AggregateOperation::Count {
            return Ok(VeinDataType::Int64);
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
        let input = argument.clone();
        let fallback = if self.operation == AggregateOperation::Avg {
            types::VeinDataType::Float64
        } else {
            input.clone()
        };
        Ok(types::aggregate_return_type(
            self.operation.name(),
            Some(&input),
            fallback,
        ))
    }

    fn create_accumulator(
        &self,
        argument_types: &[VeinDataType],
        result_type: &VeinDataType,
    ) -> Result<Box<dyn AggregateAccumulator>> {
        Ok(Box::new(state::BuiltinAccumulator::new(
            self.operation.clone(),
            argument_types,
            result_type,
        )?))
    }
}
