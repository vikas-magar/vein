//! Parsing of typed date/timestamp string literals into epoch values.
//!
//! The binder converts `DATE '2024-01-01'` and `TIMESTAMP '...'` literals
//! into epoch-based [`plan::LogicalLiteral`] values. Parsing is
//! dependency-free: calendar math uses the classic civil-days algorithm
//! (Howard Hinnant) also used by Arrow's `date32`.

use error::{Result, VeinError};
use parser::ast::data_type::IntervalField;

/// Converts a `YYYY-MM-DD` string into days since the UNIX epoch (Date32).
pub fn parse_date(value: &str) -> Result<i32> {
    let (y, m, d) = split_ymd(value).map_err(|msg| VeinError::semantic(msg, None))?;
    let days = types::days_from_civil(y as i64, m as i64, d as i64);
    i32::try_from(days).map_err(|_| {
        VeinError::semantic(
            format!("DATE literal '{value}' out of range for Date32"),
            None,
        )
    })
}

/// Converts a `YYYY-MM-DD[ HH:MM:SS[.ffffff]]` string into microseconds since
/// the UNIX epoch (UTC; `Timestamp(None)`), as Arrow stores timestamps.
pub fn parse_timestamp(value: &str) -> Result<i64> {
    let (date_part, time_part) = match value.split_once(' ') {
        Some((d, t)) => (d, t),
        None => (value, "00:00:00"),
    };
    let (y, m, d) = split_ymd(date_part).map_err(|msg| VeinError::semantic(msg, None))?;

    let mut total_us = types::days_from_civil(y as i64, m as i64, d as i64)
        .checked_mul(86_400_000_000)
        .ok_or_else(|| {
            VeinError::semantic(format!("TIMESTAMP literal '{value}' out of range"), None)
        })?;

    if !time_part.is_empty() {
        total_us = total_us
            .checked_add(parse_hms_us(time_part)?)
            .ok_or_else(|| {
                VeinError::semantic(format!("TIMESTAMP literal '{value}' out of range"), None)
            })?;
    }

    Ok(total_us)
}

/// Converts a `HH:MM:SS[.ffffff]` string into microseconds since midnight
/// (`Time64`).
pub fn parse_time(value: &str) -> Result<i64> {
    parse_hms_us(value)
}

/// Parses an interval string of the form `N unit [N unit ...]` (GoogleSQL
/// style), e.g. `INTERVAL '1 year 2 months 3 days 04:05:06'`, into a
/// `(months, days, nanos)` triple for [`plan::LogicalLiteral::Interval`].
pub fn parse_interval(value: &str) -> Result<(i32, i32, i64)> {
    let mut months = 0i32;
    let mut days = 0i32;
    let mut nanos = 0i64;

    // Split off an optional trailing `HH:MM:SS[.ffffff]` time-of-day part.
    let (tokens, tod) = match value.rsplit_once(' ') {
        Some((head, part)) if part.contains(':') => (head.split_whitespace(), Some(part)),
        _ => (value.split_whitespace(), None),
    };

    let mut toks = tokens.peekable();
    while let Some(num) = toks.next() {
        let amount: i64 = num.parse().map_err(|_| {
            VeinError::semantic(format!("invalid INTERVAL literal '{value}'"), None)
        })?;
        let unit = toks.next().ok_or_else(|| {
            VeinError::semantic(format!("invalid INTERVAL literal '{value}'"), None)
        })?;
        let types::LogicalLiteral::Interval {
            months: unit_months,
            days: unit_days,
            nanos: unit_nanos,
        } = types::interval_from_unit(amount, unit)?
        else {
            unreachable!("interval_from_unit always returns an interval literal")
        };
        months = months
            .checked_add(unit_months)
            .ok_or_else(|| interval_out_of_range(value))?;
        days = days
            .checked_add(unit_days)
            .ok_or_else(|| interval_out_of_range(value))?;
        nanos = nanos
            .checked_add(unit_nanos)
            .ok_or_else(|| interval_out_of_range(value))?;
    }

    if let Some(tod) = tod {
        nanos = nanos
            .checked_add(parse_hms_us(tod)?.checked_mul(1_000).ok_or_else(|| {
                VeinError::semantic(format!("INTERVAL literal '{value}' out of range"), None)
            })?)
            .ok_or_else(|| {
                VeinError::semantic(format!("INTERVAL literal '{value}' out of range"), None)
            })?;
    }

    Ok((months, days, nanos))
}

/// Parses an SQL-standard interval literal whose unit or field range appears
/// after the quoted value, reusing the shared interval conversion above.
pub fn parse_qualified_interval(
    value: &str,
    field: &IntervalField,
) -> Result<(i32, i32, i64)> {
    let unit = match field {
        IntervalField::Year => Some("year"),
        IntervalField::Month => Some("month"),
        IntervalField::Day => Some("day"),
        IntervalField::Hour => Some("hour"),
        IntervalField::Minute => Some("minute"),
        IntervalField::Second => Some("second"),
        _ => None,
    };
    if let Some(unit) = unit {
        return parse_interval(&format!("{value} {unit}"));
    }

    // Keep the existing unqualified interval form (`'1 year 2 months'`)
    // working with the AST's default DAY TO SECOND field marker.
    if matches!(field, IntervalField::DayToSecond)
        && let Ok(parsed) = parse_interval(value)
    {
        return Ok(parsed);
    }

    let (negative, magnitude) = interval_magnitude(value);
    let normalized = match field {
        IntervalField::YearToMonth => {
            let (years, months) = split_interval_fields(magnitude, '-')?;
            let years = parse_interval_component(years, value)?;
            let months = parse_interval_component(months, value)?;
            if months > 11 {
                return Err(invalid_interval_fields(value));
            }
            format!("{years} years {months} months")
        }
        IntervalField::DayToHour => {
            let (days, hours) = split_once_whitespace(magnitude, value)?;
            let hours = parse_interval_component(hours, value)?;
            if hours > 23 {
                return Err(invalid_interval_fields(value));
            }
            format!("{days} days {hours} hours")
        }
        IntervalField::DayToMinute => {
            let (days, clock) = split_once_whitespace(magnitude, value)?;
            let (hours, minutes) = split_interval_fields(clock, ':')?;
            let hours = parse_interval_component(hours, value)?;
            let minutes = parse_interval_component(minutes, value)?;
            if hours > 23 || minutes > 59 {
                return Err(invalid_interval_fields(value));
            }
            format!("{days} days {hours} hours {minutes} minutes")
        }
        IntervalField::DayToSecond => {
            let (days, clock) = split_once_whitespace(magnitude, value)?;
            validate_clock_fields(clock, 3, value)?;
            format!("{days} days {clock}")
        }
        IntervalField::HourToMinute => {
            let (hours, minutes) = split_interval_fields(magnitude, ':')?;
            let minutes = parse_interval_component(minutes, value)?;
            if minutes > 59 {
                return Err(invalid_interval_fields(value));
            }
            format!("{hours} hours {minutes} minutes")
        }
        IntervalField::HourToSecond => {
            validate_clock_fields(magnitude, 3, value)?;
            format!("0 days {magnitude}")
        }
        IntervalField::MinuteToSecond => {
            let (minutes, seconds) = split_interval_fields(magnitude, ':')?;
            let seconds = parse_interval_component(seconds, value)?;
            if seconds > 59 {
                return Err(invalid_interval_fields(value));
            }
            format!("0 days 00:{minutes}:{seconds}")
        }
        IntervalField::Year | IntervalField::Month | IntervalField::Day |
        IntervalField::Hour | IntervalField::Minute | IntervalField::Second => {
            unreachable!("single-field intervals were handled above")
        }
    };

    let (months, days, nanos) = parse_interval(&normalized)?;
    if negative {
        Ok((
            months.checked_neg().ok_or_else(|| invalid_interval_fields(value))?,
            days.checked_neg().ok_or_else(|| invalid_interval_fields(value))?,
            nanos.checked_neg().ok_or_else(|| invalid_interval_fields(value))?,
        ))
    } else {
        Ok((months, days, nanos))
    }
}

fn interval_magnitude(value: &str) -> (bool, &str) {
    match value.as_bytes().first() {
        Some(b'-') => (true, &value[1..]),
        Some(b'+') => (false, &value[1..]),
        _ => (false, value),
    }
}

fn split_interval_fields(
    value: &str,
    separator: char,
) -> Result<(&str, &str)> {
    let mut fields = value.split(separator);
    let first = fields.next().unwrap_or_default();
    let second = fields.next().unwrap_or_default();
    if first.is_empty() || second.is_empty() || fields.next().is_some() {
        return Err(invalid_interval_fields(value));
    }
    Ok((first, second))
}

fn split_once_whitespace<'a>(value: &'a str, original: &str) -> Result<(&'a str, &'a str)> {
    let mut fields = value.split_whitespace();
    let first = fields.next().unwrap_or_default();
    let second = fields.next().unwrap_or_default();
    if first.is_empty() || second.is_empty() || fields.next().is_some() {
        return Err(invalid_interval_fields(original));
    }
    Ok((first, second))
}

fn validate_clock_fields(clock: &str, expected_fields: usize, original: &str) -> Result<()> {
    if clock.split(':').count() != expected_fields {
        return Err(invalid_interval_fields(original));
    }
    parse_hms_us(clock).map(|_| ())
}

fn parse_interval_component(value: &str, original: &str) -> Result<i64> {
    value
        .parse()
        .map_err(|_| invalid_interval_fields(original))
}

fn invalid_interval_fields(value: &str) -> VeinError {
    VeinError::semantic(format!("invalid INTERVAL literal '{value}'"), None)
}

fn interval_out_of_range(value: &str) -> VeinError {
    VeinError::semantic(format!("INTERVAL literal '{value}' out of range"), None)
}

/// Splits a `YYYY-MM-DD` component into `(year, month, day)`.
fn split_ymd(date: &str) -> std::result::Result<(i32, i32, i32), String> {
    let mut parts = date.split('-');
    let year = parts
        .next()
        .and_then(|p| p.parse::<i32>().ok())
        .ok_or_else(|| format!("invalid DATE literal '{date}'"))?;
    let month = parts
        .next()
        .and_then(|p| p.parse::<i32>().ok())
        .ok_or_else(|| format!("invalid DATE literal '{date}'"))?;
    let day = parts
        .next()
        .and_then(|p| p.parse::<i32>().ok())
        .ok_or_else(|| format!("invalid DATE literal '{date}'"))?;
    if parts.next().is_some() || !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return Err(format!("invalid DATE literal '{date}'"));
    }
    Ok((year, month, day))
}

/// Parses `HH:MM:SS[.ffffff]` into microsecond offset within the day.
fn parse_hms_us(time: &str) -> Result<i64> {
    let (hms, frac) = match time.split_once('.') {
        Some((h, f)) => (h, f),
        None => (time, ""),
    };
    let mut parts = hms.split(':');
    let hour = parts
        .next()
        .and_then(|p| p.parse::<i64>().ok())
        .ok_or_else(|| VeinError::semantic(format!("invalid TIMESTAMP literal '{time}'"), None))?;
    let min = parts
        .next()
        .and_then(|p| p.parse::<i64>().ok())
        .ok_or_else(|| VeinError::semantic(format!("invalid TIMESTAMP literal '{time}'"), None))?;
    let sec = parts
        .next()
        .and_then(|p| p.parse::<i64>().ok())
        .ok_or_else(|| VeinError::semantic(format!("invalid TIMESTAMP literal '{time}'"), None))?;
    if parts.next().is_some()
        || hour > 23
        || min > 59
        || sec > 59
        || frac.len() > 6
        || !frac.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(VeinError::semantic(
            format!("invalid TIMESTAMP literal '{time}'"),
            None,
        ));
    }

    // Fractional seconds are right-padded to microseconds.
    let mut micros = 0i64;
    for b in frac.bytes() {
        micros = micros * 10 + i64::from(b - b'0');
    }
    for _ in frac.len()..6 {
        micros *= 10;
    }

    Ok(((hour * 60 + min) * 60 + sec) * 1_000_000 + micros)
}

#[cfg(test)]
#[allow(missing_docs)]
mod tests {
    use super::*;

    #[test]
    fn parses_epoch_and_common_dates() {
        assert_eq!(parse_date("1970-01-01").unwrap(), 0);
        assert_eq!(parse_date("1970-01-02").unwrap(), 1);
        assert_eq!(parse_date("1969-12-31").unwrap(), -1);
        // 2024-01-01 was a Monday, 19,723 days after the epoch.
        assert_eq!(parse_date("2024-01-01").unwrap(), 19_723);
    }

    #[test]
    fn rejects_malformed_dates() {
        assert!(parse_date("2024-13-01").is_err());
        assert!(parse_date("2024-01-32").is_err());
        assert!(parse_date("yesterday").is_err());
    }

    #[test]
    fn parses_timestamps_with_and_without_time() {
        assert_eq!(parse_timestamp("1970-01-01").unwrap(), 0);
        assert_eq!(parse_timestamp("1970-01-01 00:00:01").unwrap(), 1_000_000);
        assert_eq!(parse_timestamp("1970-01-01 00:00:00.000001").unwrap(), 1);
        assert_eq!(
            parse_timestamp("2024-01-01 12:30:45").unwrap(),
            19_723 * 86_400_000_000 + (12 * 3600 + 30 * 60 + 45) * 1_000_000
        );
    }

    #[test]
    fn rejects_malformed_timestamps() {
        assert!(parse_timestamp("2024-01-01 24:00:00").is_err());
        assert!(parse_timestamp("2024-01-01 12:00:60").is_err());
        assert!(parse_timestamp("2024-01-01 12:00:00.1234567").is_err());
    }

    #[test]
    fn parses_times_as_microseconds() {
        assert_eq!(parse_time("00:00:00").unwrap(), 0);
        assert_eq!(parse_time("00:00:01").unwrap(), 1_000_000);
        assert_eq!(parse_time("00:00:00.000001").unwrap(), 1);
        assert_eq!(
            parse_time("12:30:45").unwrap(),
            (12 * 3600 + 30 * 60 + 45) * 1_000_000
        );
        assert!(parse_time("24:00:00").is_err());
    }

    #[test]
    fn parses_intervals() {
        assert_eq!(parse_interval("0 seconds").unwrap(), (0, 0, 0));
        assert_eq!(parse_interval("1 month").unwrap(), (1, 0, 0));
        assert_eq!(parse_interval("2 years 3 days").unwrap(), (24, 3, 0));
        assert_eq!(
            parse_interval("1 hour 30 minutes").unwrap(),
            (0, 0, (3600 + 1800) * 1_000_000_000)
        );
        assert_eq!(parse_interval("1 day 12:00:00").unwrap(), (0, 1, 12 * 3_600_000_000_000));
        assert!(parse_interval("1 fortnight").is_err());
        assert!(parse_interval("soon").is_err());
    }

    #[test]
    fn parses_sql_qualified_interval_fields_and_ranges() {
        let cases = [
            ("2", IntervalField::Year, (24, 0, 0)),
            ("3", IntervalField::Month, (3, 0, 0)),
            ("4", IntervalField::Day, (0, 4, 0)),
            ("5", IntervalField::Hour, (0, 0, 5 * 3_600_000_000_000)),
            ("6", IntervalField::Minute, (0, 0, 6 * 60_000_000_000)),
            ("7", IntervalField::Second, (0, 0, 7_000_000_000)),
            ("2-3", IntervalField::YearToMonth, (27, 0, 0)),
            ("2 03", IntervalField::DayToHour, (0, 2, 3 * 3_600_000_000_000)),
            (
                "2 03:04",
                IntervalField::DayToMinute,
                (0, 2, (3 * 3600 + 4 * 60) * 1_000_000_000),
            ),
            (
                "2 03:04:05",
                IntervalField::DayToSecond,
                (0, 2, (3 * 3600 + 4 * 60 + 5) * 1_000_000_000),
            ),
            (
                "12:30",
                IntervalField::HourToMinute,
                (0, 0, (12 * 3600 + 30 * 60) * 1_000_000_000),
            ),
            (
                "12:30:45",
                IntervalField::HourToSecond,
                (0, 0, (12 * 3600 + 30 * 60 + 45) * 1_000_000_000),
            ),
            (
                "30:45",
                IntervalField::MinuteToSecond,
                (0, 0, (30 * 60 + 45) * 1_000_000_000),
            ),
        ];

        for (value, field, expected) in cases {
            assert_eq!(
                parse_qualified_interval(value, &field).unwrap(),
                expected,
                "field {field:?} with value {value}"
            );
        }

        assert_eq!(
            parse_qualified_interval("-2 03:04:05", &IntervalField::DayToSecond).unwrap(),
            (0, -2, -(3 * 3600 + 4 * 60 + 5) * 1_000_000_000)
        );
        assert!(parse_qualified_interval("2-12", &IntervalField::YearToMonth).is_err());
    }
}
