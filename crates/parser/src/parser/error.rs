use crate::token::{LexicalError, Span};

#[derive(Debug, Clone, thiserror::Error)]
pub enum ParseError {
    #[error("Expected {expected}, found {found} at {span:?}")]
    Expected { expected: String, found: String, span: Span },

    #[error("Unexpected token {token} at {span:?}")]
    Unexpected { token: String, span: Span },

    #[error("Lexical error: {0}")]
    Lexical(#[from] LexicalError),

    #[error("Number parsing error: {message}")]
    NumberParse { message: String, span: Span },

    #[error("Unsupported feature: {0}")]
    Unsupported(String),

    #[error("Recursion limit of {limit} exceeded")]
    RecursionLimitExceeded { limit: usize },
}

impl From<String> for ParseError {
    fn from(s: String) -> Self {
        ParseError::Unsupported(s)
    }
}
