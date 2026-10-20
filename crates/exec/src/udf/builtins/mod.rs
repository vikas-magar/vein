use super::FunctionRegistry;

mod extract;
mod lower;
#[cfg(test)]
#[path = "reference_input_tests.rs"]
mod reference_input_tests;
mod round;
mod substring;
mod truncate;
mod vector_distance;

/// Registers scalar built-ins that have executable implementations.
pub fn register_builtin_scalars(registry: &mut FunctionRegistry) {
    registry.register(lower::LowerFunction);
    registry.register(round::RoundFunction);
    registry.register(substring::SubstringFunction);
    registry.register(vector_distance::VectorDistanceFunction);
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
        registry.register(extract::ExtractFunction { name, part });
    }
    for unit in types::TemporalUnit::ALL {
        let name = unit.truncate_function_name();
        registry.register(truncate::TruncateFunction { name, unit });
    }
}

use arrow::compute::kernels::temporal::DatePart;
