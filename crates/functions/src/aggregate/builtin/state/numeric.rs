use error::{Result, VeinError};

use crate::AggregateOperation;

use super::number::AggregateNumber;

#[derive(Clone)]
pub(super) struct NumericAccumulator<T> {
    pub(super) operation: AggregateOperation,
    pub(super) value: Option<T>,
    pub(super) count: i64,
    pub(super) scale: i8,
    pub(super) decimal_precision_limit: Option<T>,
}

impl<T: AggregateNumber> NumericAccumulator<T> {
    pub(super) fn new(operation: AggregateOperation, scale: i8, decimal_precision_limit: Option<T>) -> Self {
        Self { operation, value: None, count: 0, scale, decimal_precision_limit }
    }

    pub(super) fn update(&mut self, value: Option<T>) -> Result<()> {
        let Some(value) = value else { return Ok(()) };
        match self.operation.clone() {
            AggregateOperation::Sum | AggregateOperation::Avg => {
                self.value = Some(match self.value {
                    Some(current) => current.checked_add(value, self.decimal_precision_limit)
                        .ok_or_else(|| VeinError::internal(format!(
                            "overflow while computing {}", self.operation.name()
                        )))?,
                    None => value,
                });
                if self.operation == AggregateOperation::Avg {
                    self.count = self.count.checked_add(1)
                        .ok_or_else(|| VeinError::internal("AVG count overflow"))?;
                }
            }
            AggregateOperation::Min => self.value = Some(self.value.map_or(value, |v| v.minimum(value))),
            AggregateOperation::Max => self.value = Some(self.value.map_or(value, |v| v.maximum(value))),
            other => return Err(VeinError::internal(format!("unsupported numeric aggregate {other:?}"))),
        }
        Ok(())
    }

    pub(super) fn merge(&mut self, other: &Self) -> Result<()> {
        if self.operation != other.operation || self.scale != other.scale
            || self.decimal_precision_limit != other.decimal_precision_limit
        {
            return Err(VeinError::internal("incompatible numeric aggregate states"));
        }
        let value = match (self.operation.clone(), self.value, other.value) {
            (_, current, None) => current,
            (_, None, incoming) => incoming,
            (AggregateOperation::Sum | AggregateOperation::Avg, Some(left), Some(right)) => {
                Some(left.checked_add(right, self.decimal_precision_limit)
                    .ok_or_else(|| VeinError::internal(format!(
                        "overflow while combining {}", self.operation.name()
                    )))?)
            }
            (AggregateOperation::Min, Some(left), Some(right)) => Some(left.minimum(right)),
            (AggregateOperation::Max, Some(left), Some(right)) => Some(left.maximum(right)),
            _ => return Err(VeinError::internal("invalid numeric aggregate state")),
        };
        let count = if self.operation == AggregateOperation::Avg {
            self.count.checked_add(other.count)
                .ok_or_else(|| VeinError::internal("AVG count overflow while combining"))?
        } else {
            self.count
        };
        self.value = value;
        self.count = count;
        Ok(())
    }
}
