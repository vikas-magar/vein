//! Open identifiers for table providers and physical data-file encodings.

use std::fmt;
use std::sync::Arc;

macro_rules! string_identifier {
    ($name:ident, $description:literal) => {
        #[doc = $description]
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
        #[serde(transparent)]
        pub struct $name(Arc<str>);

        impl $name {
            /// Creates an identifier without restricting registered providers.
            pub fn new(value: impl Into<Arc<str>>) -> Self {
                Self(value.into())
            }

            /// Returns the stable identifier used by a runtime registry.
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self::new(value)
            }
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self::new(Arc::<str>::from(value))
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(self.as_str())
            }
        }
    };
}

string_identifier!(
    DataFileFormatId,
    "Open identifier for an encoding used by a physical data file."
);
string_identifier!(
    TableProviderId,
    "Open identifier for the catalog/provider that interprets a logical table."
);

#[cfg(test)]
mod tests;
