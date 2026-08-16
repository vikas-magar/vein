/// Pretty-printing of tokens.
pub mod display;
/// Lexical error type.
pub mod error;
/// The lexer that turns source text into tokens.
pub mod lexer;
/// Keyword recognition for identifiers.
pub mod scan;
/// Source spans and spanned (token/AST) values.
pub mod span;

pub use error::LexicalError;
pub use lexer::Lexer;
pub use scan::keyword;
pub use span::{Span, Spanned, SpannedToken};

/// A syntactic token kind produced by the lexer.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, strum::EnumIter, serde::Serialize, serde::Deserialize,
)]
pub enum TokenKind {
    /// `SELECT`
    Select,
    /// `FROM`
    From,
    /// `EXPLAIN`
    Explain,
    /// `WHERE`
    Where,
    /// `AND`
    And,
    /// `OR`
    Or,
    /// `NOT`
    Not,
    /// `IN`
    In,
    /// `BETWEEN`
    Between,
    /// `LIKE`
    Like,
    /// `IS`
    Is,
    /// `NULL`
    Null,
    /// `AS`
    As,
    /// `ON`
    On,
    /// `JOIN`
    Join,
    /// `LEFT`
    Left,
    /// `RIGHT`
    Right,
    /// `INNER`
    Inner,
    /// `FULL`
    Full,
    /// `CROSS`
    Cross,
    /// `USING`
    Using,
    /// `GROUP`
    Group,
    /// `BY`
    By,
    /// `ORDER`
    Order,
    /// `ASC`
    Asc,
    /// `DESC`
    Desc,
    /// `NULLS`
    Nulls,
    /// `FIRST`
    First,
    /// `LAST`
    Last,
    /// `LIMIT`
    Limit,
    /// `OFFSET`
    Offset,
    /// `DISTINCT`
    Distinct,
    /// `ALL`
    All,
    /// `UNION`
    Union,
    /// `INTERSECT`
    Intersect,
    /// `EXCEPT`
    Except,
    /// `AGGREGATE`
    Aggregate,
    /// `EXTEND`
    Extend,
    /// `QUALIFY` (filters on window-function results)
    Qualify,
    /// `SET`
    Set,
    /// `DROP`
    Drop,
    /// `RENAME`
    Rename,
    /// `CALL`
    Call,
    /// `ASSERT`
    Assert,
    /// `WRITE`
    Write,
    /// `WITH`
    With,
    /// `FORK`
    Fork,
    /// `TEE`
    Tee,
    /// `RECURSIVE`
    Recursive,
    /// `PIVOT`
    Pivot,
    /// `UNPIVOT`
    Unpivot,
    /// `TABLESAMPLE`
    TableSample,
    /// `CASE`
    Case,
    /// `WHEN`
    When,
    /// `THEN`
    Then,
    /// `ELSE`
    Else,
    /// `END`
    End,
    /// `CAST`
    Cast,
    /// `WINDOW`
    Window,
    /// `OVER`
    Over,
    /// `PARTITION`
    Partition,
    /// `EXISTS`
    Exists,
    /// `TRUE`
    True,
    /// `FALSE`
    False,
    /// `ROWS`
    Rows,
    /// `RANGE`
    Range,
    /// `GROUPS`
    Groups,
    /// `UNBOUNDED`
    Unbounded,
    /// `PRECEDING`
    Preceding,
    /// `FOLLOWING`
    Following,
    /// `CURRENT`
    Current,
    /// `ROW`
    Row,
    /// `+`
    Plus,
    /// `-`
    Minus,
    /// `*`
    Star,
    /// `/`
    Slash,
    /// `%`
    Percent,
    /// `||` string concatenation
    Concat,
    /// `=`
    Eq,
    /// `<>`
    Neq,
    /// `<`
    Lt,
    /// `>`
    Gt,
    /// `<=`
    Leq,
    /// `>=`
    Geq,
    /// `|>` pipe operator
    Pipe,
    /// `->` arrow
    Arrow,
    /// `(`
    LParen,
    /// `)`
    RParen,
    /// `[`
    LBracket,
    /// `]`
    RBracket,
    /// `{`
    LBrace,
    /// `}`
    RBrace,
    /// `,`
    Comma,
    /// `.`
    Dot,
    /// `;`
    Semicolon,
    /// `:`
    Colon,
    /// A numeric literal.
    Number,
    /// A single-quoted string literal.
    String,
    /// An unquoted identifier.
    Ident,
    /// A double-quoted identifier.
    DoubleQuotedIdent,
    /// End of input.
    EOF,
    /// A lexer error (never produced; reserved).
    Error,
}
