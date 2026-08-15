#![deny(unsafe_code)]
//! Pipe-syntax parser for the Vein data engine.
//!
//! Tokenizes pipe-syntax SQL, builds an AST via recursive-descent parsing,
//! lowers the AST into a logical plan, runs heuristic optimizations, and
//! compiles each pipe segment into execution operators.

/// AST node types and visitors.
pub mod ast;
/// Recursive-descent parser and parse errors.
pub mod parser;
/// Lexer, tokens, and source spans.
pub mod token;

pub use parser::{ParseError, Parser};
