mod extract;
mod lower;
mod round;
mod substring;
mod truncate;
mod vector_distance;

use arrow::compute::kernels::temporal::DatePart;
use types::{TemporalUnit, VeinDataType};

use crate::{
    FunctionDefinition, FunctionKind, FunctionRegistry, FunctionSignature, Volatility,
};
use error::Result;

pub(crate) fn register(registry: &mut FunctionRegistry) -> Result<()> {
    register_aggregates(registry)?;
    register_scalars(registry)?;
    register_catalog_functions(registry)
}

fn register_aggregates(registry: &mut FunctionRegistry) -> Result<()> {
    for definition in crate::aggregate::definitions() {
        registry.register(definition)?;
    }
    Ok(())
}

fn register_scalars(registry: &mut FunctionRegistry) -> Result<()> {
    registry.register(FunctionDefinition::scalar(
        "LOWER", signature(VeinDataType::Utf8, vec![VeinDataType::Utf8], 1, FunctionKind::Scalar),
        lower::LowerFunction,
    ))?;
    registry.register(FunctionDefinition::scalar(
        "ROUND", signature(VeinDataType::Float64, vec![VeinDataType::Float64], 1, FunctionKind::Scalar),
        round::RoundFunction,
    ))?;
    registry.register(FunctionDefinition::scalar(
        "SUBSTRING", signature(
            VeinDataType::Utf8,
            vec![VeinDataType::Utf8, VeinDataType::Int64, VeinDataType::Int64],
            2,
            FunctionKind::Scalar,
        ),
        substring::SubstringFunction,
    ))?;
    registry.register(FunctionDefinition::scalar(
        "VECTOR_DISTANCE", signature(
            VeinDataType::Float64,
            vec![VeinDataType::Vector(0), VeinDataType::Vector(0)],
            2,
            FunctionKind::Scalar,
        ),
        vector_distance::VectorDistanceFunction,
    ))?;
    register_temporal_scalars(registry)
}

fn register_temporal_scalars(registry: &mut FunctionRegistry) -> Result<()> {
    for &(name, part) in EXTRACTORS {
        registry.register(FunctionDefinition::scalar(
            name,
            signature(VeinDataType::Int64, Vec::new(), 1, FunctionKind::TemporalExtract),
            extract::ExtractFunction { part },
        ))?;
    }
    for unit in TemporalUnit::ALL {
        let name = unit.truncate_function_name();
        registry.register(FunctionDefinition::scalar(
            name,
            signature(VeinDataType::Null, Vec::new(), 1, FunctionKind::Scalar),
            truncate::TruncateFunction { unit },
        ))?;
    }
    Ok(())
}

fn register_catalog_functions(registry: &mut FunctionRegistry) -> Result<()> {
    registry.register(FunctionDefinition::catalog_only(
        "EMBED",
        signature(VeinDataType::Vector(1536), vec![VeinDataType::Utf8], 1, FunctionKind::AiOperator),
    ))?;
    registry.register(FunctionDefinition::catalog_only(
        "SHIFT", signature(VeinDataType::Null, Vec::new(), 0, FunctionKind::TemporalShift),
    ))?;
    registry.register(FunctionDefinition::catalog_only(
        "TRUNCATE", signature(VeinDataType::Null, Vec::new(), 0, FunctionKind::TemporalTruncate),
    ))?;
    Ok(())
}

fn signature(
    return_type: VeinDataType,
    arg_types: Vec<VeinDataType>,
    min_arg_count: usize,
    kind: FunctionKind,
) -> FunctionSignature {
    FunctionSignature {
        return_type,
        volatility: Volatility::Immutable,
        arg_types,
        min_arg_count,
        kind,
        bytecode_ref: None,
    }
}

pub(crate) const EXTRACTORS: &[(&str, DatePart)] = &[
    ("YEAR", DatePart::Year), ("ISOYEAR", DatePart::YearISO),
    ("QUARTER", DatePart::Quarter), ("MONTH", DatePart::Month),
    ("WEEK", DatePart::Week), ("ISOWEEK", DatePart::WeekISO),
    ("DAY", DatePart::Day), ("DOW", DatePart::DayOfWeekSunday0),
    ("DOW1", DatePart::DayOfWeekSunday1), ("ISODOW0", DatePart::DayOfWeekMonday0),
    ("ISODOW", DatePart::DayOfWeekMonday1), ("DOY", DatePart::DayOfYear),
    ("HOUR", DatePart::Hour), ("MINUTE", DatePart::Minute),
    ("SECOND", DatePart::Second), ("MILLISECOND", DatePart::Millisecond),
    ("MICROSECOND", DatePart::Microsecond), ("NANOSECOND", DatePart::Nanosecond),
];

#[cfg(test)]
mod tests;
