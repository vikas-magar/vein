use error::{Result, VeinError};

use super::{BuiltinState, number::AggregateNumber, numeric::NumericAccumulator};

impl BuiltinState {
    pub(super) fn combine(&mut self, target: usize, source: &Self, source_group: usize) -> Result<()> {
        match (self, source) {
            (Self::Count(targets), Self::Count(sources)) => {
                let target = targets.get_mut(target)
                    .ok_or_else(|| VeinError::internal("COUNT target group is out of bounds"))?;
                let source = *sources.get(source_group)
                    .ok_or_else(|| VeinError::internal("COUNT source group is out of bounds"))?;
                *target = target.checked_add(source)
                    .ok_or_else(|| VeinError::internal("COUNT overflow while combining"))?;
                Ok(())
            }
            (Self::SingleValue(targets), Self::SingleValue(sources)) => {
                let target = targets.get_mut(target)
                    .ok_or_else(|| VeinError::internal("SINGLE_VALUE target group is out of bounds"))?;
                let source = sources.get(source_group)
                    .ok_or_else(|| VeinError::internal("SINGLE_VALUE source group is out of bounds"))?;
                if target.is_some() && source.is_some() {
                    return Err(VeinError::stage(
                        "aggregate", "scalar subquery returned more than one row",
                    ));
                }
                if target.is_none() { *target = source.clone(); }
                Ok(())
            }
            (Self::Int64(targets), Self::Int64(sources)) => merge_numeric(targets, sources, target, source_group),
            (Self::Int128(targets), Self::Int128(sources)) => merge_numeric(targets, sources, target, source_group),
            (Self::UInt64(targets), Self::UInt64(sources)) => merge_numeric(targets, sources, target, source_group),
            (Self::UInt128(targets), Self::UInt128(sources)) => merge_numeric(targets, sources, target, source_group),
            (Self::Float32(targets), Self::Float32(sources)) => merge_numeric(targets, sources, target, source_group),
            (Self::Float64(targets), Self::Float64(sources)) => merge_numeric(targets, sources, target, source_group),
            (Self::Decimal128(targets), Self::Decimal128(sources)) => merge_numeric(targets, sources, target, source_group),
            (Self::Decimal256(targets), Self::Decimal256(sources)) => merge_numeric(targets, sources, target, source_group),
            _ => Err(VeinError::internal("incompatible built-in aggregate state variants")),
        }
    }
}

fn merge_numeric<T: AggregateNumber>(
    targets: &mut [NumericAccumulator<T>],
    sources: &[NumericAccumulator<T>],
    target: usize,
    source_group: usize,
) -> Result<()> {
    let source = sources.get(source_group).cloned()
        .ok_or_else(|| VeinError::internal("aggregate source group is out of bounds"))?;
    targets.get_mut(target)
        .ok_or_else(|| VeinError::internal("aggregate target group is out of bounds"))?
        .merge(&source)
}
