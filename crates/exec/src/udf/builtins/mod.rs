use std::sync::Arc;

use super::FunctionRegistry;

mod lower;
mod round;
mod substring;
mod vector_distance;
mod extract;
mod truncate;

/// Registers scalar built-ins that have executable implementations.
pub fn register_builtin_scalars(registry: &mut FunctionRegistry) {
    registry.register_scalar(Arc::new(lower::LowerFunction));
    registry.register_scalar(Arc::new(round::RoundFunction));
    registry.register_scalar(Arc::new(substring::SubstringFunction));
    registry.register_scalar(Arc::new(vector_distance::VectorDistanceFunction));
    for (name, part) in [
        ("year", DatePart::Year),
        ("isoyear", DatePart::YearISO),
        ("quarter", DatePart::Quarter),
        ("month", DatePart::Month),
        ("week", DatePart::Week),
        ("isoweek", DatePart::WeekISO),
        ("day", DatePart::Day),
        ("dow", DatePart::DayOfWeekSunday0),
        ("dow1", DatePart::DayOfWeekSunday1),
        ("isodow0", DatePart::DayOfWeekMonday0),
        ("isodow", DatePart::DayOfWeekMonday1),
        ("doy", DatePart::DayOfYear),
        ("hour", DatePart::Hour),
        ("minute", DatePart::Minute),
        ("second", DatePart::Second),
        ("millisecond", DatePart::Millisecond),
        ("microsecond", DatePart::Microsecond),
        ("nanosecond", DatePart::Nanosecond),
    ] {
        registry.register_scalar(Arc::new(extract::ExtractFunction { name, part }));
    }
    for unit in types::TemporalUnit::ALL {
        let name = unit.truncate_function_name();
        registry.register_scalar(Arc::new(truncate::TruncateFunction { name, unit }));
    }
}

use arrow::compute::kernels::temporal::DatePart;
