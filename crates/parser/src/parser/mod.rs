/// Expression parsing.
pub mod expr;

mod aggregate;
mod alias;
mod call;
mod extend;
mod join;
mod misc;
mod order;
mod pipe;
mod select;
mod set_op;
mod where_op;
mod with;

#[cfg(test)]
mod tpch;

use crate::ast::query::{Query, QueryNode};
use crate::token::{Lexer, Span, SpannedToken, TokenKind};
use std::borrow::Cow;

pub use error::ParseError;

mod error;

#[cfg(test)]
pub(crate) mod test_utils;

/// Max expression nesting depth. Binary operator chains are parsed
/// iteratively, so this only bounds explicit paren/unary nesting:
/// ~4 stack frames (~1KB) per level keeps this safe on 2MB tokio
/// worker stacks while allowing legitimate deep expressions.
const DEFAULT_RECURSION_LIMIT: usize = 512;

/// A recursive-descent parser for pipe-syntax SQL queries.
///
/// Lazily lexes the input, produces an AST, and verifies the whole input was
/// consumed. Reusable across queries.
pub struct Parser<'a> {
    lexer: Lexer<'a>,
    tokens: Vec<SpannedToken<'a>>,
    token_pos: usize,
    peeked: Option<Result<SpannedToken<'a>, ParseError>>,
    lookahead: Option<Result<SpannedToken<'a>, ParseError>>,
    recursion_limit: usize,
    recursion_depth: usize,
}

impl<'a> Parser<'a> {
    /// Creates a parser over the given source text.
    pub fn new(input: &'a str) -> Self {
        Self {
            lexer: Lexer::new(input),
            tokens: vec![],
            token_pos: 0,
            peeked: None,
            lookahead: None,
            recursion_limit: DEFAULT_RECURSION_LIMIT,
            recursion_depth: 0,
        }
    }

    /// Creates a parser over a pre-tokenized stream (test support).
    pub fn from_tokens(tokens: Vec<SpannedToken<'a>>) -> Self {
        Self {
            lexer: Lexer::new(""),
            tokens,
            token_pos: 0,
            peeked: None,
            lookahead: None,
            recursion_limit: DEFAULT_RECURSION_LIMIT,
            recursion_depth: 0,
        }
    }

    /// Sets the maximum recursion depth for expression nesting.
    pub fn with_recursion_limit(mut self, limit: usize) -> Self {
        self.recursion_limit = limit;
        self
    }

    fn enter_recursive(&mut self) -> Result<(), ParseError> {
        self.recursion_depth += 1;
        if self.recursion_depth > self.recursion_limit {
            Err(ParseError::RecursionLimitExceeded {
                limit: self.recursion_limit,
            })
        } else {
            Ok(())
        }
    }

    fn exit_recursive(&mut self) {
        self.recursion_depth = self.recursion_depth.saturating_sub(1);
    }

    fn next_token(&mut self) -> Result<SpannedToken<'a>, ParseError> {
        if let Some(peeked_result) = self.peeked.take() {
            return peeked_result;
        }
        if let Some(lookahead_result) = self.lookahead.take() {
            return lookahead_result;
        }
        if self.token_pos < self.tokens.len() {
            let token = self.tokens[self.token_pos];
            self.token_pos += 1;
            return Ok(token);
        }
        match self.lexer.next() {
            Some(Ok(token)) => Ok(token),
            Some(Err(lex_err)) => Err(ParseError::Lexical(lex_err)),
            None => Ok(SpannedToken {
                kind: TokenKind::EOF,
                text: "",
                span: Span::default(),
            }),
        }
    }

    fn peek(&mut self) -> Result<&SpannedToken<'a>, &ParseError> {
        if self.peeked.is_none() {
            self.peeked = Some(self.next_token());
        }
        match self.peeked.as_ref() {
            Some(result) => result.as_ref(),
            None => unreachable!("peeked token was just populated"),
        }
    }

    fn peek_span(&mut self) -> Result<Span, &ParseError> {
        Ok(self.peek()?.span)
    }

    /// Consumes a peeked error (if any), so a subsequent `peek()` advances to
    /// the next token instead of surfacing the same error again.
    fn take_peek_error(&mut self) -> Option<ParseError> {
        match self.next_token() {
            Err(e) => Some(e),
            Ok(_) => None,
        }
    }

    fn expect(&mut self, kind: TokenKind) -> Result<SpannedToken<'a>, ParseError> {
        let token = self.next_token()?;
        if token.kind == kind {
            Ok(token)
        } else {
            Err(ParseError::Expected {
                expected: format!("{:?}", kind),
                found: format!("{:?}", token.kind),
                span: token.span,
            })
        }
    }

    fn expect_ident(&mut self) -> Result<&'a str, ParseError> {
        let token = self.next_token()?;
        match token.kind {
            TokenKind::Ident => Ok(token.text),
            TokenKind::DoubleQuotedIdent => Ok(&token.text[1..token.text.len() - 1]),
            _ => Err(ParseError::Expected {
                expected: "identifier".into(),
                found: format!("{:?}", token.kind),
                span: token.span,
            }),
        }
    }

    /// Parses a single statement from the input.
    ///
    /// Supports `EXPLAIN <query>` (returning [`QueryNode::Explain`]) and
    /// plain pipe queries (returning [`QueryNode::Plain`]). An `EXPLAIN`
    /// query is planned but must not be executed.
    pub fn parse_statement(&mut self) -> Result<QueryNode<'a>, ParseError> {
        match self.peek().map(|t| &t.kind) {
            Ok(TokenKind::Explain) => {
                self.next_token().ok();
                let query = self.parse_query()?;
                Ok(QueryNode::Explain {
                    query: Box::new(query),
                })
            }
            _ => Ok(QueryNode::Plain {
                query: self.parse_query()?,
            }),
        }
    }

    /// Parses a full query, failing if any input remains after the pipeline.
    pub fn parse_query(&mut self) -> Result<Query<'a>, ParseError> {
        let result = match self.peek().map(|t| &t.kind) {
            Ok(TokenKind::From) => self.parse_pipe_query(),
            Err(_) => Err(self.take_peek_error().expect("peeked error")),
            _ => {
                let span = self.peek_span().unwrap_or_default();
                let found = self
                    .peek()
                    .map(|t| format!("{:?}", t.kind))
                    .unwrap_or_else(|_| "EOF".into());
                Err(ParseError::Expected {
                    expected: "FROM".into(),
                    found,
                    span,
                })
            }
        };

        let query = result?;

        // Validate the entire input was consumed: anything after the last `|>` clause is an error.
        // An optional trailing semicolon is permitted (statement separator).
        if let Ok(TokenKind::Semicolon) = self.peek().map(|t| &t.kind) {
            self.next_token().ok();
        }
        match self.peek().map(|t| &t.kind) {
            Ok(TokenKind::EOF) => Ok(query),
            Ok(other) => Err(ParseError::Expected {
                expected: "end of query".into(),
                found: format!("{:?}", other),
                span: self.peek_span().unwrap_or_default(),
            }),
            Err(_) => Err(self.take_peek_error().expect("peeked error")),
        }
    }

    pub(crate) fn parse_pipe_query(&mut self) -> Result<Query<'a>, ParseError> {
        self.enter_recursive()?;
        let result = self.parse_pipe_query_inner();
        self.exit_recursive();
        result
    }

    /// Inner body of [`Parser::parse_pipe_query`].
    ///
    /// Kept separate so the recursion-depth guard in [`Parser::parse_pipe_query`]
    /// balances on every path, including the early-return `?` sites. Every query
    /// nesting point (subquery table sources, scalar/IN/EXISTS subqueries, WITH
    /// CTEs, and the right side of set operations) funnels through
    /// [`Parser::parse_pipe_query`], so this single guard bounds the total
    /// nesting depth of any query tree.
    fn parse_pipe_query_inner(&mut self) -> Result<Query<'a>, ParseError> {
        self.expect(TokenKind::From)?;
        let source = self.parse_table_source()?;
        let mut pipes = Vec::new();
        while let Ok(()) = self.expect_pipe() {
            let op = self.parse_pipe_operator()?;
            pipes.push(op);
        }
        Ok(Query { source, pipes })
    }

    fn expect_pipe(&mut self) -> Result<(), ParseError> {
        match self.peek().map(|t| &t.kind) {
            Ok(TokenKind::Pipe) => {
                self.next_token()?;
                Ok(())
            }
            _ => Err(ParseError::Expected {
                expected: "|>".into(),
                found: format!(
                    "{:?}",
                    self.peek().map(|t| &t.kind).unwrap_or(&TokenKind::EOF)
                ),
                span: self.peek_span().unwrap_or_default(),
            }),
        }
    }

    fn parse_table_source(&mut self) -> Result<crate::ast::query::TableSource<'a>, ParseError> {
        let peek_kind = self.peek().map(|t| t.kind).unwrap_or(TokenKind::EOF);
        let mut source = match peek_kind {
            TokenKind::LParen => {
                self.next_token()?; // consume '('
                let query = self.parse_pipe_query()?;
                self.expect(TokenKind::RParen)?;
                crate::ast::query::TableSource::Subquery {
                    query: Box::new(query),
                    alias: None,
                }
            }
            TokenKind::String => {
                let token = self.next_token()?;
                let url = if token.text.starts_with('\'') || token.text.starts_with('"') {
                    &token.text[1..token.text.len() - 1]
                } else {
                    token.text
                };
                let mut inline_options = Vec::new();
                if let Ok(TokenKind::With) = self.peek().map(|t| &t.kind) {
                    self.next_token()?; // consume WITH
                    self.expect(TokenKind::LParen)?;
                    loop {
                        let key = self.expect_ident()?;
                        self.expect(TokenKind::Eq)?;
                        let val_tok = self.next_token()?;
                        let val = match val_tok.kind {
                            TokenKind::String | TokenKind::DoubleQuotedIdent => {
                                &val_tok.text[1..val_tok.text.len() - 1]
                            }
                            TokenKind::Ident | TokenKind::Number => val_tok.text,
                            _ => {
                                return Err(ParseError::Unexpected {
                                    token: format!("{:?}", val_tok.kind),
                                    span: val_tok.span,
                                });
                            }
                        };
                        inline_options.push((key, val));
                        match self.peek().map(|t| &t.kind) {
                            Ok(TokenKind::Comma) => {
                                self.next_token()?;
                            }
                            _ => break,
                        }
                    }
                    self.expect(TokenKind::RParen)?;
                }
                crate::ast::query::TableSource::ObjectStore {
                    url,
                    inline_options,
                    alias: None,
                }
            }
            _ => {
                let first = self.expect_ident()?;
                if let Ok(TokenKind::Dot) = self.peek().map(|t| &t.kind) {
                    // Qualified name (e.g. `a.b.c`): join the parts into an owned name
                    self.next_token()?; // consume '.'
                    let mut name = first.to_string();
                    loop {
                        name.push('.');
                        name.push_str(self.expect_ident()?);
                        if let Ok(TokenKind::Dot) = self.peek().map(|t| &t.kind) {
                            self.next_token()?; // consume '.'
                        } else {
                            break;
                        }
                    }
                    crate::ast::query::TableSource::Table {
                        name: Cow::Owned(name),
                        alias: None,
                    }
                } else {
                    crate::ast::query::TableSource::Table {
                        name: Cow::Borrowed(first),
                        alias: None,
                    }
                }
            }
        };

        // Parse optional alias (e.g., `AS t` or just `t`)
        let has_as = if let Ok(TokenKind::As) = self.peek().map(|t| &t.kind) {
            self.next_token()?; // consume AS
            true
        } else {
            false
        };

        if has_as {
            let alias = self.expect_ident()?;
            match &mut source {
                crate::ast::query::TableSource::Table { alias: a, .. } => *a = Some(alias),
                crate::ast::query::TableSource::ObjectStore { alias: a, .. } => *a = Some(alias),
                crate::ast::query::TableSource::Subquery { alias: a, .. } => *a = Some(alias),
            }
        } else if let Ok(TokenKind::Ident | TokenKind::DoubleQuotedIdent) =
            self.peek().map(|t| &t.kind)
        {
            let alias = self.expect_ident()?;
            match &mut source {
                crate::ast::query::TableSource::Table { alias: a, .. } => *a = Some(alias),
                crate::ast::query::TableSource::ObjectStore { alias: a, .. } => *a = Some(alias),
                crate::ast::query::TableSource::Subquery { alias: a, .. } => *a = Some(alias),
            }
        }

        Ok(source)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::pipe_op::PipeOperator;
    use crate::ast::query::TableSource;
    use crate::parser::test_utils::utils::*;

    #[test]
    fn test_from_tokens_simple_query() {
        let tokens = vec![
            spanned(TokenKind::From, "FROM"),
            spanned(TokenKind::Ident, "users"),
            spanned(TokenKind::Pipe, "|>"),
            spanned(TokenKind::Select, "SELECT"),
            spanned(TokenKind::Ident, "name"),
        ];
        let mut parser = Parser::from_tokens(tokens);
        let q = parser.parse_query().expect("test should succeed");
        assert_eq!(
            q.source,
            TableSource::Table {
                name: "users".into(),
                alias: None
            }
        );
        assert_eq!(q.pipes.len(), 1);
    }

    #[test]
    fn test_from_tokens_not_in() {
        let tokens = vec![
            spanned(TokenKind::From, "FROM"),
            spanned(TokenKind::Ident, "t"),
            spanned(TokenKind::Pipe, "|>"),
            spanned(TokenKind::Select, "SELECT"),
            spanned(TokenKind::Ident, "x"),
            spanned(TokenKind::Comma, ","),
            spanned(TokenKind::Ident, "y"),
            spanned(TokenKind::Pipe, "|>"),
            spanned(TokenKind::Where, "WHERE"),
            spanned(TokenKind::Ident, "x"),
            spanned(TokenKind::Not, "NOT"),
            spanned(TokenKind::In, "IN"),
            spanned(TokenKind::LParen, "("),
            spanned(TokenKind::Number, "1"),
            spanned(TokenKind::Comma, ","),
            spanned(TokenKind::Number, "2"),
            spanned(TokenKind::RParen, ")"),
        ];
        let mut parser = Parser::from_tokens(tokens);
        let q = parser.parse_query().expect("test should succeed");
        assert_eq!(q.pipes.len(), 2);
        assert!(matches!(q.pipes[1], PipeOperator::Where { .. }));
    }

    #[test]
    fn test_from_tokens_not_exists() {
        let tokens = vec![
            spanned(TokenKind::From, "FROM"),
            spanned(TokenKind::Ident, "t"),
            spanned(TokenKind::Pipe, "|>"),
            spanned(TokenKind::Where, "WHERE"),
            spanned(TokenKind::Not, "NOT"),
            spanned(TokenKind::Exists, "EXISTS"),
            spanned(TokenKind::LParen, "("),
            spanned(TokenKind::From, "FROM"),
            spanned(TokenKind::Ident, "u"),
            spanned(TokenKind::Pipe, "|>"),
            spanned(TokenKind::Select, "SELECT"),
            spanned(TokenKind::Number, "1"),
            spanned(TokenKind::RParen, ")"),
        ];
        let mut parser = Parser::from_tokens(tokens);
        let q = parser.parse_query().expect("test should succeed");
        assert_eq!(q.pipes.len(), 1);
    }

    #[test]
    fn test_rejects_trailing_tokens() {
        let mut parser = Parser::new("FROM t |> SELECT name garbage");
        let err = parser.parse_query().expect_err("test should fail");
        assert!(matches!(err, ParseError::Expected { .. }));
    }

    #[test]
    fn test_moderate_subquery_nesting_succeeds() {
        let mut q = String::from("FROM t |> SELECT 1");
        for _ in 0..50 {
            q = format!("FROM ({}) |> SELECT 1", q);
        }
        parse_query(&q).expect("test should succeed");
    }

    #[test]
    fn test_deep_subquery_nesting_hits_recursion_limit() {
        let mut q = String::from("FROM t |> SELECT 1");
        for _ in 0..100_000 {
            q = format!("FROM ({}) |> SELECT 1", q);
        }
        let err = parse_query(&q).expect_err("test should fail");
        assert!(matches!(err, ParseError::RecursionLimitExceeded { .. }));
    }

    #[test]
    fn test_deep_fork_nesting_hits_recursion_limit() {
        // FORK/TEE recurse through parse_pipe_operator; without the guard this
        // would overflow the stack instead of returning a typed error. Wrap so
        // each FORK nests inside the previous one.
        let mut inner = String::from("|> WHERE x = 1");
        for _ in 0..100_000 {
            inner = format!("|> FORK ( {} )", inner);
        }
        let q = format!("FROM t {}", inner);
        let err = parse_query(&q).expect_err("test should fail");
        assert!(matches!(err, ParseError::RecursionLimitExceeded { .. }));
    }

    #[test]
    fn test_moderate_fork_nesting_succeeds() {
        let mut inner = String::from("|> WHERE x = 1");
        for _ in 0..50 {
            inner = format!("|> FORK ( {} )", inner);
        }
        let q = format!("FROM t {}", inner);
        parse_query(&q).expect("test should succeed");
    }

    #[test]
    fn test_accepts_trailing_semicolon() {
        let q = parse_query("FROM t |> SELECT name;").expect("test should succeed");
        assert_eq!(q.pipes.len(), 1);
    }

    #[test]
    fn test_reports_lexical_error_at_start() {
        let err = parse_query("@ FROM t").expect_err("test should fail");
        assert!(matches!(err, ParseError::Lexical(_)));
    }

    #[test]
    fn test_reports_lexical_error_at_end() {
        let err = parse_query("FROM t |> SELECT name @").expect_err("test should fail");
        assert!(matches!(err, ParseError::Lexical(_)));
    }

    #[test]
    fn test_reports_lexical_error_after_trailing_semicolon() {
        let err = parse_query("FROM t |> SELECT name; @").expect_err("test should fail");
        assert!(matches!(err, ParseError::Lexical(_)));
    }

    #[test]
    fn test_unterminated_double_quoted_ident_is_lexical_error_not_panic() {
        let err = parse_query("FROM \"").expect_err("test should fail");
        assert!(matches!(err, ParseError::Lexical(_)));
        let err = parse_query("FROM t |> SELECT x AS \"").expect_err("test should fail");
        assert!(matches!(err, ParseError::Lexical(_)));
    }

    #[test]
    fn test_double_quoted_ident_slices_correctly() {
        let q = parse_query("FROM \"my table\" |> SELECT x").expect("test should succeed");
        assert_eq!(
            q.source,
            TableSource::Table {
                name: "my table".into(),
                alias: None
            }
        );
    }

    #[test]
    fn test_explain_wraps_query() {
        let mut parser = Parser::new("EXPLAIN FROM t |> SELECT name");
        let node = parser.parse_statement().expect("test should succeed");
        let QueryNode::Explain { query } = node else {
            panic!("expected an Explain node")
        };
        assert_eq!(
            query.source,
            TableSource::Table {
                name: "t".into(),
                alias: None
            }
        );
        assert_eq!(query.pipes.len(), 1);
    }

    #[test]
    fn test_plain_statement() {
        let mut parser = Parser::new("FROM t |> SELECT name");
        let node = parser.parse_statement().expect("test should succeed");
        assert!(matches!(node, QueryNode::Plain { .. }));
    }

    #[test]
    fn test_explain_requires_from() {
        let mut parser = Parser::new("EXPLAIN SELECT name");
        let err = parser.parse_statement().expect_err("test should fail");
        assert!(matches!(err, ParseError::Expected { .. }));
    }

    #[test]
    fn test_explain_accepts_trailing_semicolon() {
        let mut parser = Parser::new("EXPLAIN FROM t |> SELECT name;");
        let node = parser.parse_statement().expect("test should succeed");
        assert!(matches!(node, QueryNode::Explain { .. }));
    }

    #[test]
    fn test_explain_rejects_trailing_tokens() {
        let mut parser = Parser::new("EXPLAIN FROM t |> SELECT name garbage");
        let err = parser.parse_statement().expect_err("test should fail");
        assert!(matches!(err, ParseError::Expected { .. }));
    }

    #[test]
    fn test_parse_query_rejects_explain() {
        let mut parser = Parser::new("EXPLAIN FROM t |> SELECT name");
        let err = parser.parse_query().expect_err("test should fail");
        assert!(matches!(err, ParseError::Expected { .. }));
    }
}
