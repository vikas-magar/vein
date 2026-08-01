#![deny(unsafe_code)]
#![allow(missing_docs)]
#![allow(clippy::return_self_not_must_use)]
#![allow(clippy::pedantic)]
#![allow(clippy::unwrap_used)]
//! Pipe-syntax parser for the Vein data engine.
//!
//! Tokenizes pipe-syntax SQL, builds an AST via recursive-descent parsing,
//! lowers the AST into a logical plan, runs heuristic optimizations, and
//! compiles each pipe segment into execution operators.

pub mod ast;
pub mod parser;
pub mod token;

pub use parser::{ParseError, Parser};
pub use token::lexer::Lexer;
pub use token::{SpannedToken, TokenKind};
