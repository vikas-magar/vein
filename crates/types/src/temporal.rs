//! Shared temporal value construction and unit conversion.

use error::{Result, VeinError};

use crate::LogicalLiteral;

mod unit;
pub use unit::TemporalUnit;

/// Converts an integer amount and temporal unit into an interval literal.
///
/// Years and months use the interval month field, days use the day field, and
/// sub-day units use nanoseconds. Conversion is checked so out-of-range values fail during binding.
pub fn interval_from_unit(amount: i64, unit: &str) -> Result<LogicalLiteral> {
    let parsed = TemporalUnit::parse(unit)?;
    let (months, days, nanos) = match parsed {
        TemporalUnit::Year => (checked_i32(amount.checked_mul(12), unit)?, 0, 0),
        TemporalUnit::Quarter => (checked_i32(amount.checked_mul(3), unit)?, 0, 0),
        TemporalUnit::Month => (checked_i32(Some(amount), unit)?, 0, 0),
        TemporalUnit::Week => (0, checked_i32(amount.checked_mul(7), unit)?, 0),
        TemporalUnit::Day => (0, checked_i32(Some(amount), unit)?, 0),
        TemporalUnit::Hour => (0, 0, checked_nanos(amount, 3_600_000_000_000, unit)?),
        TemporalUnit::Minute => (0, 0, checked_nanos(amount, 60_000_000_000, unit)?),
        TemporalUnit::Second => (0, 0, checked_nanos(amount, 1_000_000_000, unit)?),
        TemporalUnit::Millisecond => (0, 0, checked_nanos(amount, 1_000_000, unit)?),
        TemporalUnit::Microsecond => (0, 0, checked_nanos(amount, 1_000, unit)?),
        TemporalUnit::Nanosecond => (0, 0, amount),
    };

    Ok(LogicalLiteral::Interval { months, days, nanos })
}

/// Truncates an epoch-based temporal scalar while preserving its physical scale.
/// `units_per_second` is `None` for Date32, and `units_per_day` is one for Date32.
pub fn truncate_epoch_value(
    value: i64,
    unit: TemporalUnit,
    units_per_second: Option<i64>,
    units_per_day: i64,
    is_date: bool,
    is_time: bool,
) -> Result<i64> {
    if units_per_day <= 0 {
        return Err(VeinError::internal("temporal scale must be positive"));
    }

    if is_time {
        return match unit {
            TemporalUnit::Day => Ok(0),
            TemporalUnit::Year
            | TemporalUnit::Quarter
            | TemporalUnit::Month
            | TemporalUnit::Week => Err(unsupported_truncation(unit)),
            _ => truncate_subday(value, unit, units_per_second),
        };
    }

    if is_date && matches!(
        unit,
        TemporalUnit::Hour
            | TemporalUnit::Minute
            | TemporalUnit::Second
            | TemporalUnit::Millisecond
            | TemporalUnit::Microsecond
            | TemporalUnit::Nanosecond
    ) {
        return Err(unsupported_truncation(unit));
    }

    let epoch_days = value.div_euclid(units_per_day);
    let truncated_days = match unit {
        TemporalUnit::Year | TemporalUnit::Quarter | TemporalUnit::Month | TemporalUnit::Week => {
            let (year, month, _) = civil_from_days(epoch_days);
            match unit {
                TemporalUnit::Year => days_from_civil(year, 1, 1),
                TemporalUnit::Quarter => days_from_civil(year, ((month - 1) / 3) * 3 + 1, 1),
                TemporalUnit::Month => days_from_civil(year, month, 1),
                TemporalUnit::Week => epoch_days - (epoch_days + 3).rem_euclid(7),
                _ => unreachable!(),
            }
        }
        TemporalUnit::Day => epoch_days,
        _ => return truncate_subday(value, unit, units_per_second),
    };

    truncated_days
        .checked_mul(units_per_day)
        .ok_or_else(|| VeinError::semantic("truncated temporal value is out of range", None))
}
fn truncate_subday(value: i64, unit: TemporalUnit, units_per_second: Option<i64>) -> Result<i64> {
    let units_per_second = units_per_second
        .ok_or_else(|| VeinError::internal("sub-day truncation requires a time scale"))?;
    let units_per_bucket = match unit {
        TemporalUnit::Hour => units_per_second.checked_mul(3_600),
        TemporalUnit::Minute => units_per_second.checked_mul(60),
        TemporalUnit::Second => Some(units_per_second),
        TemporalUnit::Millisecond => Some(units_per_second / 1_000),
        TemporalUnit::Microsecond => Some(units_per_second / 1_000_000),
        TemporalUnit::Nanosecond => Some(units_per_second / 1_000_000_000),
        _ => return Err(unsupported_truncation(unit)),
    }
        .ok_or_else(|| VeinError::semantic("temporal truncation scale is out of range", None))?;

    if units_per_bucket <= 1 {
        return Ok(value);
    }
    value
        .div_euclid(units_per_bucket)
        .checked_mul(units_per_bucket)
        .ok_or_else(|| VeinError::semantic("truncated temporal value is out of range", None))
}

fn unsupported_truncation(unit: TemporalUnit) -> VeinError {
    VeinError::semantic(format!("TRUNCATE does not support unit '{}' for this temporal type", unit.as_str()), None)
}

/// Converts Gregorian calendar fields to days since 1970-01-01.
pub fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let day_of_year = (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let shifted_days = days + 719_468;
    let era = if shifted_days >= 0 { shifted_days } else { shifted_days - 146_096 } / 146_097;
    let day_of_era = shifted_days - era * 146_097;
    let year_of_era = (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    (year, month, day)
}

fn checked_i32(value: Option<i64>, unit: &str) -> Result<i32> {
    value
        .and_then(|value| i32::try_from(value).ok())
        .ok_or_else(|| VeinError::semantic(format!("temporal shift {unit} amount is out of range"), None))
}

fn checked_nanos(amount: i64, nanos_per_unit: i64, unit: &str) -> Result<i64> {
    amount.checked_mul(nanos_per_unit).ok_or_else(|| {
        VeinError::semantic(format!("temporal shift {unit} amount is out of range"), None)
    })
}

#[cfg(test)]
mod tests;
