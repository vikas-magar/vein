use crate::token::Span;

#[derive(Debug, Clone, thiserror::Error)]
#[error("Lexical error at {line}:{column}: {message}")]
pub struct LexicalError {
    pub message: String,
    pub span: Span,
    pub line: usize,
    pub column: usize,
}
