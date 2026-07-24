use crate::token::TokenKind;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct Span {
    pub start: usize,
    pub end: usize,
    pub line: usize,
    pub column: usize,
}

impl Span {
    pub fn new(start: usize, end: usize, line: usize, column: usize) -> Self {
        Self { start, end, line, column }
    }

    pub fn union(self, other: Self) -> Self {
        let start = self.start.min(other.start);
        let end = self.end.max(other.end);
        let line = self.line.min(other.line);
        let column = if self.line < other.line { self.column } else { self.column.min(other.column) };
        Self { start, end, line, column }
    }
}

impl Default for Span {
    fn default() -> Self {
        Self { start: 0, end: 0, line: 1, column: 1 }
    }
}

#[derive(Debug, Clone)]
pub struct SpannedToken {
    pub kind: TokenKind,
    pub text: String,
    pub span: Span,
}

#[derive(Debug, Clone)]
pub struct Spanned<T> {
    pub value: T,
    pub span: Span,
}
