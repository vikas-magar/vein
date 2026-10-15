use error::{Result, VeinError};

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

