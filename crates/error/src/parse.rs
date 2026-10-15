//! Structured lexical and syntax errors shared with the parser crate.

use crate::{ErrorCategory, SourceSpan};

/// A failure to tokenize part of the input (for example, an unterminated string).
#[derive(Debug, Clone, thiserror::Error)]
#[error("Lexical error at {line}:{column}: {message}")]
pub struct LexicalError {
    /// Human-readable description of the error.
    pub message: String,
    /// The span the error covers.
    pub span: SourceSpan,
    /// 1-based line of the error location.
    pub line: usize,
    /// 1-based column of the error location.
    pub column: usize,
}

/// An error produced while parsing a query.
#[derive(Debug, Clone, thiserror::Error)]
pub enum ParseError {
    /// A token of a different kind than expected was found.
    #[error("Expected {expected}, found {found} at {span:?}")]
    Expected {
        /// The expected token kind.
        expected: String,
        /// The token kind that was actually found.
        found: String,
        /// Where the unexpected token occurred.
        span: SourceSpan,
    },

    /// An unexpected token was encountered out of context.
    #[error("Unexpected token {token} at {span:?}")]
    Unexpected {
        /// The unexpected token.
        token: String,
        /// Where the token occurred.
        span: SourceSpan,
    },

    /// The input could not be tokenized.
    #[error("{0}")]
    Lexical(#[from] LexicalError),

    /// A numeric literal was lexed but could not be parsed to a number.
    #[error("Number parsing error: {message}")]
    NumberParse {
        /// The parse failure description.
        message: String,
        /// Where the literal occurred.
        span: SourceSpan,
    },

    /// A syntactically valid but unsupported feature was used.
    #[error("Unsupported feature: {0}")]
    Unsupported(String),

    /// The parser recursion depth limit was exceeded.
    #[error("Recursion limit of {limit} exceeded")]
    RecursionLimitExceeded {
        /// The configured recursion limit.
        limit: usize,
    },
}

impl ParseError {
    /// Returns the stable classification for this parser error.
    #[must_use]
    pub const fn category(&self) -> ErrorCategory {
        match self {
            Self::Unsupported(_) => ErrorCategory::Unsupported,
            Self::Expected { .. }
            | Self::Unexpected { .. }
            | Self::Lexical(_)
            | Self::NumberParse { .. }
            | Self::RecursionLimitExceeded { .. } => ErrorCategory::Syntax,
        }
    }
}
