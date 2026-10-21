use std::sync::Arc;

use arrow::array::{Array, ArrayRef, PrimitiveArray, PrimitiveBuilder, new_null_array};
use arrow::array::types::{
    ArrowPrimitiveType, Date32Type, Date64Type, Time32MillisecondType, Time32SecondType,
    Time64MicrosecondType, Time64NanosecondType, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
};
use arrow::datatypes::{DataType, TimeUnit};
use error::{Result, VeinError};
use types::TemporalUnit;

use crate::segments::invoke_segments;
use crate::{FunctionInput, FunctionOutput, ScalarFunction};

#[derive(Debug)]
pub(super) struct TruncateFunction {
    pub(super) unit: TemporalUnit,
}

impl ScalarFunction for TruncateFunction {
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [data_type] = arg_types else {
            return Err(VeinError::planning(
                format!("{} expects exactly one temporal argument", self.unit.truncate_function_name()),
                None,
            ));
        };
        let vein_type = types::VeinDataType::try_from(data_type)?;
        if !self.unit.supports(&vein_type) {
            return Err(VeinError::planning(
                format!(
                    "{} unit does not support argument type {data_type:?}",
                    self.unit.truncate_function_name()
                ),
                None,
            ));
        }
        Ok(data_type.clone())
    }

    fn invoke(
        &self,
        args: &[FunctionInput<'_>],
        rows: usize,
        return_type: &DataType,
    ) -> Result<FunctionOutput> {
        invoke_segments(args, rows, return_type, |segments, segment_rows| {
            let [input] = segments else {
                return Err(VeinError::internal(format!(
                    "{} expects exactly one argument",
                    self.unit.truncate_function_name()
                )));
            };
            let Some(array) = input.array() else {
                return Ok(new_null_array(return_type, segment_rows));
            };
            self.invoke_segment(array)
        })
    }
}

impl TruncateFunction {
    fn invoke_segment(&self, input: &dyn Array) -> Result<ArrayRef> {
        self.return_type(std::slice::from_ref(input.data_type()))?;

        let (units_per_second, units_per_day, is_date, is_time) = match input.data_type() {
            DataType::Date32 => (None, 1, true, false),
            DataType::Date64 => (Some(1_000), 86_400_000, true, false),
            DataType::Timestamp(unit, None) => temporal_scale(*unit, false)?,
            DataType::Time32(unit) => temporal_scale(*unit, true)?,
            DataType::Time64(unit) => temporal_scale(*unit, true)?,
            DataType::Timestamp(_, Some(_)) => {
                return Err(VeinError::planning(
                    "TRUNCATE on timezone-aware timestamps is not supported yet".to_string(),
                    None,
                ));
            }
            _ => {
                return Err(VeinError::planning(
                    format!("TRUNCATE does not support argument type {:?}", input.data_type()),
                    None,
                ));
            }
        };

        macro_rules! apply {
            ($native:ty) => {{
                let array = input
                    .as_any()
                    .downcast_ref::<PrimitiveArray<$native>>()
                    .ok_or_else(|| VeinError::internal("temporal array had an unexpected layout"))?;
                truncate_primitive(
                    array,
                    self.unit,
                    units_per_second,
                    units_per_day,
                    is_date,
                    is_time,
                )
            }};
        }

        match input.data_type() {
            DataType::Date32 => apply!(Date32Type),
            DataType::Date64 => apply!(Date64Type),
            DataType::Timestamp(TimeUnit::Second, _) => apply!(TimestampSecondType),
            DataType::Timestamp(TimeUnit::Millisecond, _) => apply!(TimestampMillisecondType),
            DataType::Timestamp(TimeUnit::Microsecond, _) => apply!(TimestampMicrosecondType),
            DataType::Timestamp(TimeUnit::Nanosecond, _) => apply!(TimestampNanosecondType),
            DataType::Time32(TimeUnit::Second) => apply!(Time32SecondType),
            DataType::Time32(TimeUnit::Millisecond) => apply!(Time32MillisecondType),
            DataType::Time64(TimeUnit::Microsecond) => apply!(Time64MicrosecondType),
            DataType::Time64(TimeUnit::Nanosecond) => apply!(Time64NanosecondType),
            _ => Err(VeinError::planning(
                format!("TRUNCATE does not support argument type {:?}", input.data_type()),
                None,
            )),
        }
    }
}

fn temporal_scale(unit: TimeUnit, is_time: bool) -> Result<(Option<i64>, i64, bool, bool)> {
    let units_per_second: i64 = match unit {
        TimeUnit::Second => 1,
        TimeUnit::Millisecond => 1_000,
        TimeUnit::Microsecond => 1_000_000,
        TimeUnit::Nanosecond => 1_000_000_000,
    };
    let units_per_day = units_per_second
        .checked_mul(86_400)
        .ok_or_else(|| VeinError::planning("temporal scale is out of range".to_string(), None))?;
    Ok((Some(units_per_second), units_per_day, false, is_time))
}

fn truncate_primitive<I>(
    input: &PrimitiveArray<I>,
    unit: TemporalUnit,
    units_per_second: Option<i64>,
    units_per_day: i64,
    is_date: bool,
    is_time: bool,
) -> Result<ArrayRef>
where
    I: ArrowPrimitiveType,
    I::Native: Into<i64> + TryFrom<i64>,
{
    let mut output = PrimitiveBuilder::<I>::with_capacity(input.len());
    for index in 0..input.len() {
        if input.is_null(index) {
            output.append_null();
            continue;
        }

        let value = input.value(index).into();
        let truncated = types::truncate_epoch_value(
            value,
            unit,
            units_per_second,
            units_per_day,
            is_date,
            is_time,
        )?;
        let truncated = <I::Native as TryFrom<i64>>::try_from(truncated)
            .map_err(|_| VeinError::semantic("truncated temporal value is out of range", None))?;
        output.append_value(truncated);
    }
    Ok(Arc::new(output.finish()))
}

#[cfg(test)]
mod tests;
