//! Format-neutral requests for the physical encoding of scan output columns.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;

/// Stable encoding identifier for Arrow dictionary arrays.
pub const ARROW_DICTIONARY_ENCODING: &str = "arrow_dictionary";

/// Open identifier for an output encoding a scan provider may support.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ColumnEncodingId(Arc<str>);

impl ColumnEncodingId {
    /// Creates an encoding identifier without restricting connector extensions.
    pub fn new(value: impl Into<Arc<str>>) -> Self {
        Self(value.into())
    }

    /// Identifier for Arrow dictionary encoded output.
    pub fn arrow_dictionary() -> Self {
        Self::new(ARROW_DICTIONARY_ENCODING)
    }

    /// Returns the stable registry identifier.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for ColumnEncodingId {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl fmt::Display for ColumnEncodingId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// A best-effort request for one scan output column's physical encoding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnEncodingHint {
    /// Zero-based position within the projected scan schema.
    pub column_index: u32,
    /// Requested output representation; providers may reject or ignore it.
    pub encoding: ColumnEncodingId,
}

impl ColumnEncodingHint {
    /// Constructs a hint for dictionary encoded output.
    pub fn dictionary(column_index: u32) -> Self {
        Self {
            column_index,
            encoding: ColumnEncodingId::arrow_dictionary(),
        }
    }
}

#[cfg(test)]
mod tests;
