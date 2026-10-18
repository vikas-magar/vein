//! Typed numeric accumulator state and checked partial-state operations.

use super::*;

/// The shared fold logic, monomorphized for the input's physical value type.
#[derive(Debug, Clone, Copy)]
pub(in crate::operators::aggregate) struct NumericAccumulator<T> {
    pub(in crate::operators::aggregate) op: AggOpCode,
    pub(in crate::operators::aggregate) value: Option<T>,
    pub(in crate::operators::aggregate) count: i64,
    pub(in crate::operators::aggregate) scale: i8,
    pub(in crate::operators::aggregate) decimal_precision_limit: bool,
}

impl<T: AggregateValue> NumericAccumulator<T> {
    pub(in crate::operators::aggregate) fn new(op: AggOpCode, scale: i8, decimal_precision_limit: bool) -> Self {
        Self {
            op,
            value: None,
            count: 0,
            scale,
            decimal_precision_limit,
        }
    }

    pub(in crate::operators::aggregate) fn update(&mut self, value: Option<T>) -> Result<()> {
        let Some(value) = value else {
            return Ok(());
        };
        match self.op {
            AggOpCode::Count => self.count += 1,
            AggOpCode::Sum | AggOpCode::Avg => {
                let total = match self.value {
                    Some(current) => current
                        .checked_add(value, self.decimal_precision_limit)
                        .ok_or_else(|| {
                            let operation = match self.op {
                                AggOpCode::Sum => "SUM",
                                AggOpCode::Avg => "AVG",
                                _ => unreachable!("only additive aggregates reach this arm"),
                            };
                            VeinError::internal(format!(
                                "numeric overflow while computing {operation}"
                            ))
                        })?,
                    None => value,
                };
                self.value = Some(total);
                if self.op == AggOpCode::Avg {
                    self.count += 1;
                }
            }
            AggOpCode::Min => {
                self.value = Some(self.value.map_or(value, |current| current.minimum(value)));
            }
            AggOpCode::Max => {
                self.value = Some(self.value.map_or(value, |current| current.maximum(value)));
            }
            AggOpCode::CountDistinct => {
                return Err(VeinError::internal(
                    "COUNT(DISTINCT) cannot use a numeric accumulator",
                ));
            }
            AggOpCode::SingleValue => unreachable!("single-value uses a retained Arrow value"),
        }
        Ok(())
    }

    #[allow(dead_code)] // Used by aggregate spill serialization.
    pub(in crate::operators::aggregate) fn snapshot(&self) -> NumericPartial<T> {
        NumericPartial {
            op: self.op,
            value: self.value,
            count: self.count,
            scale: self.scale,
            decimal_precision_limit: self.decimal_precision_limit,
        }
    }

    #[allow(dead_code)] // Used when restoring aggregate spill state.
    pub(in crate::operators::aggregate) fn merge_partial(&mut self, partial: &NumericPartial<T>) -> Result<()> {
        if self.op != partial.op
            || self.scale != partial.scale
            || self.decimal_precision_limit != partial.decimal_precision_limit
        {
            return Err(VeinError::internal("incompatible numeric aggregate partial state"));
        }
        if partial.count < 0 || (self.op != AggOpCode::Avg && partial.count != 0) {
            return Err(VeinError::internal("invalid numeric aggregate partial count"));
        }
        if (partial.count == 0) != partial.value.is_none() && self.op == AggOpCode::Avg {
            return Err(VeinError::internal("invalid AVG partial state"));
        }

        let value = match (self.op, self.value, partial.value) {
            (_, current, None) => current,
            (_, None, incoming) => incoming,
            (AggOpCode::Sum | AggOpCode::Avg, Some(current), Some(incoming)) => {
                Some(current.checked_add(incoming, self.decimal_precision_limit).ok_or_else(|| {
                    VeinError::internal("numeric overflow while merging aggregate state")
                })?)
            }
            (AggOpCode::Min, Some(current), Some(incoming)) => Some(current.minimum(incoming)),
            (AggOpCode::Max, Some(current), Some(incoming)) => Some(current.maximum(incoming)),
            (op, _, _) => {
                return Err(VeinError::internal(format!("unsupported aggregate operation {op:?}")));
            }
        };
        let count = if self.op == AggOpCode::Avg {
            self.count.checked_add(partial.count).ok_or_else(|| {
                VeinError::internal("numeric overflow while merging AVG count")
            })?
        } else {
            self.count
        };
        self.value = value;
        self.count = count;
        Ok(())
    }
}
