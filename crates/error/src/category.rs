//! Stable classifications for errors crossing Vein subsystem boundaries.

use std::fmt;

/// Broad error domain used for diagnostics, telemetry, and caller policy.
///
/// Categories describe what a failure means, not which crate produced it.
/// External errors can use the same categories through [`crate::ExternalError`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ErrorCategory {
    /// A command or API argument is invalid before query parsing begins.
    Input,
    /// The query could not be tokenized or parsed.
    Syntax,
    /// The query is valid syntax but uses an unsupported feature.
    Unsupported,
    /// A name or expression violates query semantics.
    Semantic,
    /// Catalog lookup or metadata publication failed.
    Catalog,
    /// A logical or physical plan could not be produced.
    Planning,
    /// A bounded engine resource could not satisfy a request.
    Resource,
    /// Query or pipeline execution failed.
    Execution,
    /// Reading, writing, or decoding data failed.
    Storage,
    /// A data representation or vector operation failed.
    Data,
    /// A connection or transport operation failed.
    Transport,
    /// An unexpected internal invariant failed.
    Internal,
}

impl fmt::Display for ErrorCategory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Input => "input",
            Self::Syntax => "syntax",
            Self::Unsupported => "unsupported",
            Self::Semantic => "semantic",
            Self::Catalog => "catalog",
            Self::Planning => "planning",
            Self::Resource => "resource",
            Self::Execution => "execution",
            Self::Storage => "storage",
            Self::Data => "data",
            Self::Transport => "transport",
            Self::Internal => "internal",
        };
        formatter.write_str(name)
    }
}
