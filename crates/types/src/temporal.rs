//! Shared temporal value construction and unit conversion.

use error::{Result, VeinError};

use crate::LogicalLiteral;

/// A supported temporal field used by temporal functions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TemporalUnit {
    /// Calendar year.
    Year,
    /// Three-month calendar quarter.
    Quarter,
    /// Calendar month.
    Month,
    /// ISO-style week beginning on Monday.
    Week,
    /// Calendar day.
    Day,
    /// Hour.
    Hour,
    /// Minute.
    Minute,
    /// Second.
    Second,
    /// Millisecond.
    Millisecond,
    /// Microsecond.
    Microsecond,
    /// Nanosecond.
    Nanosecond,
}

impl TemporalUnit {
    /// Every unit supported by `SHIFT` and `TRUNCATE`.
    pub const ALL: [Self; 11] = [
        Self::Year,
        Self::Quarter,
        Self::Month,
        Self::Week,
        Self::Day,
        Self::Hour,
        Self::Minute,
        Self::Second,
        Self::Millisecond,
        Self::Microsecond,
        Self::Nanosecond,
    ];

    /// Parses a case-insensitive singular or plural unit name.
    pub fn parse(unit: &str) -> Result<Self> {
        match unit.trim().to_ascii_lowercase().as_str() {
            "year" | "years" => Ok(Self::Year),
            "quarter" | "quarters" => Ok(Self::Quarter),
            "month" | "months" => Ok(Self::Month),
            "week" | "weeks" => Ok(Self::Week),
            "day" | "days" => Ok(Self::Day),
            "hour" | "hours" => Ok(Self::Hour),
            "minute" | "minutes" => Ok(Self::Minute),
            "second" | "seconds" => Ok(Self::Second),
            "millisecond" | "milliseconds" => Ok(Self::Millisecond),
            "microsecond" | "microseconds" => Ok(Self::Microsecond),
            "nanosecond" | "nanoseconds" => Ok(Self::Nanosecond),
            _ => Err(VeinError::semantic(format!("unknown temporal unit '{unit}'"), None)),
        }
    }

    /// Returns the canonical singular unit name used for runtime dispatch.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Year => "year",
            Self::Quarter => "quarter",
            Self::Month => "month",
            Self::Week => "week",
            Self::Day => "day",
            Self::Hour => "hour",
            Self::Minute => "minute",
            Self::Second => "second",
            Self::Millisecond => "millisecond",
            Self::Microsecond => "microsecond",
            Self::Nanosecond => "nanosecond",
        }
    }

    /// Canonical runtime function registered for this truncation unit.
    pub const fn truncate_function_name(self) -> &'static str {
        match self {
            Self::Year => "TRUNCATE_YEAR",
            Self::Quarter => "TRUNCATE_QUARTER",
            Self::Month => "TRUNCATE_MONTH",
            Self::Week => "TRUNCATE_WEEK",
            Self::Day => "TRUNCATE_DAY",
            Self::Hour => "TRUNCATE_HOUR",
            Self::Minute => "TRUNCATE_MINUTE",
            Self::Second => "TRUNCATE_SECOND",
            Self::Millisecond => "TRUNCATE_MILLISECOND",
            Self::Microsecond => "TRUNCATE_MICROSECOND",
            Self::Nanosecond => "TRUNCATE_NANOSECOND",
        }
    }

    /// Whether this unit can truncate the given Vein temporal type.
    pub fn supports(self, data_type: &crate::VeinDataType) -> bool {
        use crate::VeinDataType;
        match data_type {
            VeinDataType::Date32 | VeinDataType::Date64 => matches!(
                self,
                Self::Year | Self::Quarter | Self::Month | Self::Week | Self::Day
            ),
            VeinDataType::Timestamp(_, None) => true,
            VeinDataType::Time32(_) | VeinDataType::Time64(_) => matches!(
                self,
                Self::Day
                    | Self::Hour
                    | Self::Minute
                    | Self::Second
                    | Self::Millisecond
                    | Self::Microsecond
                    | Self::Nanosecond
            ),
            _ => false,
        }
    }
}

/// Converts an integer amount and temporal unit into an interval literal.
///
/// Years and months use the interval month field, days use the day field, and
/// sub-day units use nanoseconds.
/// Conversion is checked so out-of-range values fail during binding.
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
mod tests {
    use super::*;

    #[test]
    fn converts_calendar_and_subday_units() {
        let cases = [
            (2, "YEAR", (24, 0, 0)),
            (-3, "months", (-3, 0, 0)),
            (4, "DAY", (0, 4, 0)),
            (-5, "hours", (0, 0, -18_000_000_000_000)),
            (6, "minute", (0, 0, 360_000_000_000)),
            (7, "seconds", (0, 0, 7_000_000_000)),
            (8, "milliseconds", (0, 0, 8_000_000)),
            (9, "microsecond", (0, 0, 9_000)),
            (10, "nanoseconds", (0, 0, 10)),
        ];

        for (amount, unit, expected) in cases {
            assert_eq!(
                interval_from_unit(amount, unit).unwrap(),
                LogicalLiteral::Interval {
                    months: expected.0,
                    days: expected.1,
                    nanos: expected.2,
                }
            );
        }
    }

    #[test]
    fn rejects_unknown_and_out_of_range_units() {
        assert!(interval_from_unit(1, "fortnight").is_err());
        assert!(interval_from_unit(i64::MAX, "year").is_err());
        assert!(interval_from_unit(i64::MAX, "day").is_err());
        assert!(interval_from_unit(i64::MAX, "second").is_err());
    }

    #[test]
    fn truncates_calendar_units_with_epoch_and_leap_year_edges() {
        let march_31 = days_from_civil(2024, 3, 31);
        assert_eq!(
            truncate_epoch_value(march_31, TemporalUnit::Quarter, None, 1, true, false).unwrap(),
            days_from_civil(2024, 1, 1)
        );
        assert_eq!(
            truncate_epoch_value(march_31, TemporalUnit::Month, None, 1, true, false).unwrap(),
            days_from_civil(2024, 3, 1)
        );
        assert_eq!(
            truncate_epoch_value(0, TemporalUnit::Week, None, 1, true, false).unwrap(),
            -3
        );
        assert_eq!(
            truncate_epoch_value(-1, TemporalUnit::Week, None, 1, true, false).unwrap(),
            -3
        );
        assert_eq!(
            truncate_epoch_value(march_31, TemporalUnit::Year, None, 1, true, false).unwrap(),
            days_from_civil(2024, 1, 1)
        );
    }

    #[test]
    fn truncates_subday_values_using_physical_scale_and_euclidean_floor() {
        assert_eq!(
            truncate_epoch_value(
                -1_000_000_001,
                TemporalUnit::Second,
                Some(1_000_000_000),
                86_400_000_000_000,
                false,
                false,
            )
                .unwrap(),
            -2_000_000_000
        );
        assert_eq!(
            truncate_epoch_value(45_123_456, TemporalUnit::Minute, Some(1_000_000), 86_400_000_000, false, true)
                .unwrap(),
            0
        );
        assert!(truncate_epoch_value(1, TemporalUnit::Hour, None, 1, true, false).is_err());
        assert!(truncate_epoch_value(1, TemporalUnit::Month, None, 86_400_000_000, false, true).is_err());
    }
}
