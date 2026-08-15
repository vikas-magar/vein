use crate::token::Span;

/// A failure to tokenize part of the input (e.g. an unterminated string).
#[derive(Debug, Clone, thiserror::Error)]
#[error("Lexical error at {line}:{column}: {message}")]
pub struct LexicalError {
    /// Human-readable description of the error.
    pub message: String,
    /// The span the error covers.
    pub span: Span,
    /// 1-based line of the error location.
    pub line: usize,
    /// 1-based column of the error location.
    pub column: usize,
}
