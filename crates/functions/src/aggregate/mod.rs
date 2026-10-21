mod kernel;
mod builtin;

use serde::{Deserialize, Serialize};
use error::{Result, VeinError};

pub use kernel::{AggregateAccumulator, AggregateFunction};

/// Aggregate operation interpreted by the executor's typed state engine.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AggregateOperation {
    /// Count non-null values.
    Count,
    /// Count distinct non-null values.
    CountDistinct,
    /// Sum values.
    Sum,
    /// Minimum value.
    Min,
    /// Maximum value.
    Max,
    /// Average value.
    Avg,
    /// Enforce scalar-subquery cardinality.
    SingleValue,
    /// Dispatch a registered aggregate by its catalog name.
    Custom(String),
}

impl AggregateOperation {
    /// Canonical registry name for the aggregate implementation.
    pub fn name(&self) -> &str {
        match self {
            Self::Count | Self::CountDistinct => "COUNT",
            Self::Sum => "SUM",
            Self::Min => "MIN",
            Self::Max => "MAX",
            Self::Avg => "AVG",
            Self::SingleValue => "SINGLE_VALUE",
            Self::Custom(name) => name,
        }
    }
}

#[derive(Clone)]
struct BuiltinAggregate {
    name: &'static str,
    operation: AggregateOperation,
}

const BUILTINS: &[BuiltinAggregate] = &[
    BuiltinAggregate { name: "COUNT", operation: AggregateOperation::Count },
    BuiltinAggregate { name: "SUM", operation: AggregateOperation::Sum },
    BuiltinAggregate { name: "MIN", operation: AggregateOperation::Min },
    BuiltinAggregate { name: "MAX", operation: AggregateOperation::Max },
    BuiltinAggregate { name: "AVG", operation: AggregateOperation::Avg },
    BuiltinAggregate { name: "SINGLE_VALUE", operation: AggregateOperation::SingleValue },
];

/// Resolves a built-in operation or preserves a registered aggregate's name.
pub fn aggregate_operation(name: &str, distinct: bool) -> Result<AggregateOperation> {
    let aggregate = BUILTINS
        .iter()
        .find(|aggregate| aggregate.name.eq_ignore_ascii_case(name));
    let Some(aggregate) = aggregate else {
        return if distinct {
            Err(VeinError::unsupported(
                format!("DISTINCT is currently supported only for COUNT, not {name}"),
                None,
            ))
        } else {
            Ok(AggregateOperation::Custom(name.to_owned()))
        };
    };
    if distinct {
        if aggregate.operation == AggregateOperation::Count {
            return Ok(AggregateOperation::CountDistinct);
        }
        return Err(VeinError::unsupported(
            format!("DISTINCT is currently supported only for COUNT, not {name}"), None,
        ));
    }
    Ok(aggregate.operation.clone())
}

/// Registers semantic signatures for aggregates understood by the executor.
pub(crate) fn definitions() -> impl Iterator<Item=crate::FunctionDefinition> {
    BUILTINS.iter().map(|aggregate| {
        let operation = aggregate.operation.clone();
        let return_type = match aggregate.operation {
            AggregateOperation::Count | AggregateOperation::CountDistinct => types::VeinDataType::Int64,
            _ => types::VeinDataType::Float64,
        };
        crate::FunctionDefinition::aggregate(
            aggregate.name,
            crate::FunctionSignature {
                return_type,
                volatility: crate::Volatility::Immutable,
                arg_types: Vec::new(),
                min_arg_count: 0,
                kind: crate::FunctionKind::Aggregate,
                bytecode_ref: None,
            },
            builtin::BuiltinAggregate::new(operation),
        )
    })
}

#[cfg(test)]
mod tests;
