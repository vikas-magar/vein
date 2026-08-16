//! Parsing of typed date/timestamp string literals into epoch values.
//!
//! The binder converts `DATE '2024-01-01'` and `TIMESTAMP '...'` literals
//! into epoch-based [`plan::LogicalLiteral`] values. Parsing is
//! dependency-free: calendar math uses the classic civil-days algorithm
//! (Howard Hinnant) also used by Arrow's `date32`.

use error::{Result, VeinError};

/// Converts a `YYYY-MM-DD` string into days since the UNIX epoch (Date32).
pub fn parse_date(value: &str) -> Result<i32> {
    let (y, m, d) = split_ymd(value).map_err(|msg| VeinError::semantic(msg, None))?;
    let days = days_from_civil(y as i64, m as i64, d as i64);
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

    let mut total_us = days_from_civil(y as i64, m as i64, d as i64)
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
        match unit.to_ascii_lowercase().as_str() {
            "year" | "years" => months = months.checked_add(amount as i32 * 12).ok_or_else(|| {
                VeinError::semantic(format!("INTERVAL literal '{value}' out of range"), None)
            })?,
            "month" | "months" => months = months.checked_add(amount as i32).ok_or_else(|| {
                VeinError::semantic(format!("INTERVAL literal '{value}' out of range"), None)
            })?,
            "day" | "days" => days = days.checked_add(amount as i32).ok_or_else(|| {
                VeinError::semantic(format!("INTERVAL literal '{value}' out of range"), None)
            })?,
            "hour" | "hours" => nanos = nanos.checked_add(amount.checked_mul(3_600_000_000_000).ok_or_else(|| {
                VeinError::semantic(format!("INTERVAL literal '{value}' out of range"), None)
            })?).ok_or_else(|| {
                VeinError::semantic(format!("INTERVAL literal '{value}' out of range"), None)
            })?,
            "minute" | "minutes" => nanos = nanos.checked_add(amount.checked_mul(60_000_000_000).ok_or_else(|| {
                VeinError::semantic(format!("INTERVAL literal '{value}' out of range"), None)
            })?).ok_or_else(|| {
                VeinError::semantic(format!("INTERVAL literal '{value}' out of range"), None)
            })?,
            "second" | "seconds" => nanos = nanos.checked_add(amount.checked_mul(1_000_000_000).ok_or_else(|| {
                VeinError::semantic(format!("INTERVAL literal '{value}' out of range"), None)
            })?).ok_or_else(|| {
                VeinError::semantic(format!("INTERVAL literal '{value}' out of range"), None)
            })?,
            "millisecond" | "milliseconds" => nanos = nanos.checked_add(amount.checked_mul(1_000_000).ok_or_else(|| {
                VeinError::semantic(format!("INTERVAL literal '{value}' out of range"), None)
            })?).ok_or_else(|| {
                VeinError::semantic(format!("INTERVAL literal '{value}' out of range"), None)
            })?,
            "microsecond" | "microseconds" => nanos = nanos.checked_add(amount.checked_mul(1_000).ok_or_else(|| {
                VeinError::semantic(format!("INTERVAL literal '{value}' out of range"), None)
            })?).ok_or_else(|| {
                VeinError::semantic(format!("INTERVAL literal '{value}' out of range"), None)
            })?,
            "nanosecond" | "nanoseconds" => nanos = nanos.checked_add(amount).ok_or_else(|| {
                VeinError::semantic(format!("INTERVAL literal '{value}' out of range"), None)
            })?,
            _ => {
                return Err(VeinError::semantic(
                    format!("unknown INTERVAL unit '{unit}'"),
                    None,
                ))
            }
        }
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

/// Days since 1970-01-01 for a proleptic-Gregorian date (Hinnant civil-days).
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
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
}
