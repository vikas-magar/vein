use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use arrow::compute::kernels::temporal::{DatePart, date_part};
use error::{Result, VeinError};

use super::super::ScalarUDFImpl;

#[derive(Debug)]
pub(super) struct ExtractFunction {
    pub(super) name: &'static str,
    pub(super) part: DatePart,
}

impl ScalarUDFImpl for ExtractFunction {
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

    fn invoke(&self, args: &[ArrayRef]) -> Result<ArrayRef> {
        let [input] = args else {
            return Err(VeinError::internal(format!(
                "EXTRACT field '{}' expects one argument",
                self.name
            )));
        };
        self.return_type(&[input.data_type().clone()])?;
        date_part(input.as_ref(), self.part).map_err(|error| {
            VeinError::internal(format!("EXTRACT field '{}' failed: {error}", self.name))
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
mod tests {
    use super::*;
    use std::sync::Arc;
    use arrow::array::{
        Array, Date32Array, Date64Array, DurationSecondArray, Int32Array,
        IntervalMonthDayNanoArray, Time32MillisecondArray, Time64MicrosecondArray,
        TimestampMicrosecondArray,
    };
    use arrow::datatypes::IntervalMonthDayNano;

    #[test]
    fn extracts_date_parts_from_date_time_and_timestamp_arrays() {
        let input = Arc::new(Date32Array::from(vec![Some(0), Some(19_723), None])) as ArrayRef;
        let output = ExtractFunction { name: "year", part: DatePart::Year }
            .invoke(&[input])
            .expect("extract years");
        let years = output
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Int32 output");

        assert_eq!(years.value(0), 1970);
        assert_eq!(years.value(1), 2024);
        assert!(years.is_null(2));

        let timestamp = Arc::new(TimestampMicrosecondArray::from(vec![Some(43_200_000_000)])) as ArrayRef;
        let parts = [
            ("year", DatePart::Year, 1970),
            ("isoyear", DatePart::YearISO, 1970),
            ("quarter", DatePart::Quarter, 1),
            ("month", DatePart::Month, 1),
            ("week", DatePart::Week, 1),
            ("isoweek", DatePart::WeekISO, 1),
            ("day", DatePart::Day, 1),
            ("dow", DatePart::DayOfWeekSunday0, 4),
            ("dow1", DatePart::DayOfWeekSunday1, 5),
            ("isodow0", DatePart::DayOfWeekMonday0, 3),
            ("isodow", DatePart::DayOfWeekMonday1, 4),
            ("doy", DatePart::DayOfYear, 1),
            ("hour", DatePart::Hour, 12),
            ("minute", DatePart::Minute, 0),
            ("second", DatePart::Second, 0),
            ("millisecond", DatePart::Millisecond, 0),
            ("microsecond", DatePart::Microsecond, 0),
            ("nanosecond", DatePart::Nanosecond, 0),
        ];
        for (name, part, expected) in parts {
            let values = ExtractFunction { name, part }
                .invoke(std::slice::from_ref(&timestamp))
                .expect("extract timestamp part");
            let values = values.as_any().downcast_ref::<Int32Array>().expect("Int32 result");
            assert_eq!(values.value(0), expected, "EXTRACT({name})");
        }

        let dates = Arc::new(Date64Array::from(vec![Some(1_704_067_200_000)])) as ArrayRef;
        let months = ExtractFunction { name: "month", part: DatePart::Month }
            .invoke(&[dates])
            .expect("extract month");
        assert_eq!(months.as_any().downcast_ref::<Int32Array>().unwrap().value(0), 1);

        let times = Arc::new(Time64MicrosecondArray::from(vec![Some(45_000_000_000)])) as ArrayRef;
        let minutes = ExtractFunction { name: "minute", part: DatePart::Minute }
            .invoke(&[times])
            .expect("extract minute");
        assert_eq!(minutes.as_any().downcast_ref::<Int32Array>().unwrap().value(0), 30);

        let time32 = Arc::new(Time32MillisecondArray::from(vec![Some(43_200_000)])) as ArrayRef;
        let hours = ExtractFunction { name: "hour", part: DatePart::Hour }
            .invoke(&[time32])
            .expect("extract time32 hour");
        assert_eq!(hours.as_any().downcast_ref::<Int32Array>().unwrap().value(0), 12);

        let duration = Arc::new(DurationSecondArray::from(vec![Some(43_200)])) as ArrayRef;
        let duration_hours = ExtractFunction { name: "hour", part: DatePart::Hour }
            .invoke(&[duration])
            .expect("extract duration hour");
        assert_eq!(duration_hours.as_any().downcast_ref::<Int32Array>().unwrap().value(0), 12);

        let interval = Arc::new(IntervalMonthDayNanoArray::from(vec![Some(
            IntervalMonthDayNano::new(16, 35, 0),
        )])) as ArrayRef;
        let interval_years = ExtractFunction { name: "year", part: DatePart::Year }
            .invoke(&[interval])
            .expect("extract interval year");
        assert_eq!(interval_years.as_any().downcast_ref::<Int32Array>().unwrap().value(0), 1);

        let hours = ExtractFunction { name: "hour", part: DatePart::Hour }
            .invoke(&[timestamp])
            .expect("extract hour");
        assert_eq!(hours.as_any().downcast_ref::<Int32Array>().unwrap().value(0), 12);
    }

    #[test]
    fn rejects_non_temporal_values_and_incompatible_date_parts() {
        assert_eq!(
            ExtractFunction { name: "year", part: DatePart::Year }
                .return_type(&[DataType::Date32])
                .expect("valid signature"),
            DataType::Int32
        );
        assert!(ExtractFunction { name: "year", part: DatePart::Year }
            .return_type(&[DataType::Time64(arrow::datatypes::TimeUnit::Microsecond)])
            .is_err());
        assert!(ExtractFunction { name: "hour", part: DatePart::Hour }
            .return_type(&[DataType::Date32])
            .is_err());
        assert!(ExtractFunction { name: "year", part: DatePart::Year }
            .return_type(&[DataType::Utf8])
            .is_err());
        assert!(ExtractFunction { name: "year", part: DatePart::Year }.invoke(&[]).is_err());

        let input = Arc::new(Int32Array::from(vec![2024])) as ArrayRef;
        assert!(ExtractFunction { name: "year", part: DatePart::Year }.invoke(&[input]).is_err());
    }
}
