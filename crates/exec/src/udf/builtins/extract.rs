use arrow::array::new_null_array;
use arrow::compute::kernels::temporal::{DatePart, date_part};
use arrow::datatypes::DataType;
use error::{Result, VeinError};

use super::super::{ScalarFunction, FunctionInput, UdfOutput};
use crate::udf::segments::invoke_segments;

#[derive(Debug)]
pub(super) struct ExtractFunction {
    pub(super) name: &'static str,
    pub(super) part: DatePart,
}

impl ScalarFunction for ExtractFunction {
    fn name(&self) -> &str {
        self.name
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 || !supports_part_type(self.part, &arg_types[0]) {
            return Err(VeinError::planning(
                format!("EXTRACT field '{}' does not support argument types {arg_types:?}", self.name),
                None,
            ));
        }
        Ok(DataType::Int32)
    }

    fn invoke(
        &self,
        args: &[FunctionInput<'_>],
        rows: usize,
        return_type: &DataType,
    ) -> Result<UdfOutput> {
        invoke_segments(args, rows, return_type, |segments, segment_rows| {
            let [input] = segments else {
                return Err(VeinError::internal(format!(
                    "EXTRACT field '{}' expects one argument",
                    self.name
                )));
            };
            let Some(array) = input.array() else {
                return Ok(new_null_array(return_type, segment_rows));
            };
            date_part(array, self.part).map_err(|error| {
                VeinError::internal(format!("EXTRACT field '{}' failed: {error}", self.name))
            })
        })
    }
}

fn supports_part_type(part: DatePart, data_type: &DataType) -> bool {
    match data_type {
        DataType::Date32 | DataType::Date64 => !is_time_part(part),
        DataType::Time32(_) | DataType::Time64(_) => !is_calendar_part(part),
        DataType::Timestamp(_, _) | DataType::Duration(_) | DataType::Interval(_) => true,
        _ => false,
    }
}

fn is_calendar_part(part: DatePart) -> bool {
    matches!(
        part,
        DatePart::Year
            | DatePart::YearISO
            | DatePart::Quarter
            | DatePart::Month
            | DatePart::Week
            | DatePart::WeekISO
            | DatePart::Day
            | DatePart::DayOfWeekSunday0
            | DatePart::DayOfWeekMonday0
            | DatePart::DayOfWeekSunday1
            | DatePart::DayOfWeekMonday1
            | DatePart::DayOfYear
    )
}

fn is_time_part(part: DatePart) -> bool {
    matches!(
        part,
        DatePart::Hour
            | DatePart::Minute
            | DatePart::Second
            | DatePart::Millisecond
            | DatePart::Microsecond
            | DatePart::Nanosecond
    )
}

#[cfg(test)]
#[path = "extract_tests.rs"]
mod tests;
