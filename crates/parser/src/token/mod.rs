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

mod kind;
pub use kind::TokenKind;
