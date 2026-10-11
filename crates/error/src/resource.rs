//! Units used when reporting resource quota failures.

use std::fmt;

/// A unit for a requested resource amount.
///
/// Engine-wide resource errors use [`ResourceUnit::Bytes`] by default. A custom
/// unit lets other subsystems report quotas without adding subsystem-specific
/// variants to the central error model.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ResourceUnit {
    /// A byte count, used by memory and spill quotas.
    Bytes,
    /// A caller-defined unit for another bounded resource.
    Custom(String),
}

impl fmt::Display for ResourceUnit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bytes => formatter.write_str("bytes"),
            Self::Custom(unit) => formatter.write_str(unit),
        }
    }
}

impl From<String> for ResourceUnit {
    fn from(unit: String) -> Self {
        Self::Custom(unit)
    }
}

impl From<&str> for ResourceUnit {
    fn from(unit: &str) -> Self {
        Self::Custom(unit.to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::ResourceUnit;

    #[test]
    fn standard_and_custom_units_have_stable_display() {
        assert_eq!(ResourceUnit::Bytes.to_string(), "bytes");
        assert_eq!(ResourceUnit::from("partitions").to_string(), "partitions");
    }
}
