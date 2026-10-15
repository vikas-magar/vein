pub use ::error::SourceSpan as Span;
use crate::token::TokenKind;

/// A token with its kind, source text, and position.
#[derive(Debug, Clone, Copy)]
pub struct SpannedToken<'a> {
    /// The syntactic token kind.
    pub kind: TokenKind,
    /// The exact source text of the token (borrowed from the input).
    pub text: &'a str,
    /// The token's position in the source.
    pub span: Span,
}

/// A value produced while parsing together with the source span it covers.
#[derive(Debug, Clone)]
pub struct Spanned<T> {
    /// The parsed value.
    pub value: T,
    /// The source span covering the value.
    pub span: Span,
}
