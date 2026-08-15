use crate::token::{LexicalError, Span};

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
        span: Span,
    },

    /// An unexpected token was encountered out of context.
    #[error("Unexpected token {token} at {span:?}")]
    Unexpected {
        /// The unexpected token.
        token: String,
        /// Where the token occurred.
        span: Span,
    },

    /// The input could not be tokenized.
    #[error("Lexical error: {0}")]
    Lexical(#[from] LexicalError),

    /// A numeric literal was lexed but could not be parsed to a number.
    #[error("Number parsing error: {message}")]
    NumberParse {
        /// The parse failure description.
        message: String,
        /// Where the literal occurred.
        span: Span,
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

impl From<String> for ParseError {
    fn from(s: String) -> Self {
        ParseError::Unsupported(s)
    }
}
