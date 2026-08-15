use crate::token::TokenKind;

/// A half-open byte range `[start, end)` into the source plus its 1-based
/// line/column position for diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct Span {
    /// Byte offset of the first character, relative to the source start.
    pub start: usize,
    /// Byte offset one past the last character.
    pub end: usize,
    /// 1-based line number of `start`.
    pub line: usize,
    /// 1-based column number of `start`.
    pub column: usize,
}

impl Span {
    /// Creates a span covering `[start, end)` at the given position.
    pub fn new(start: usize, end: usize, line: usize, column: usize) -> Self {
        Self {
            start,
            end,
            line,
            column,
        }
    }

    /// Returns the smallest span containing both `self` and `other`.
    pub fn union(self, other: Self) -> Self {
        let start = self.start.min(other.start);
        let end = self.end.max(other.end);
        let line = self.line.min(other.line);
        let column = if self.line < other.line {
            self.column
        } else {
            self.column.min(other.column)
        };
        Self {
            start,
            end,
            line,
            column,
        }
    }
}

impl Default for Span {
    fn default() -> Self {
        Self {
            start: 0,
            end: 0,
            line: 1,
            column: 1,
        }
    }
}

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
