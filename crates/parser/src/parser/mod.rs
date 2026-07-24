pub mod expr;
pub mod stmt;

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

use crate::ast::query::Query;
use crate::token::{Lexer, Span, SpannedToken, TokenKind};

pub use error::ParseError;

mod error;

#[cfg(test)]
mod tests;

const DEFAULT_RECURSION_LIMIT: usize = 50;

pub struct Parser {
    lexer: Lexer,
    tokens: Vec<SpannedToken>,
    token_pos: usize,
    peeked: Option<Result<SpannedToken, ParseError>>,
    lookahead: Option<Result<SpannedToken, ParseError>>,
    recursion_limit: usize,
    recursion_depth: usize,
}

impl Parser {
    pub fn new(input: &str) -> Self {
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

    pub fn from_tokens(tokens: Vec<SpannedToken>) -> Self {
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

    pub fn with_recursion_limit(mut self, limit: usize) -> Self {
        self.recursion_limit = limit;
        self
    }

    fn enter_recursive(&mut self) -> Result<(), ParseError> {
        self.recursion_depth += 1;
        if self.recursion_depth > self.recursion_limit {
            Err(ParseError::RecursionLimitExceeded { limit: self.recursion_limit })
        } else {
            Ok(())
        }
    }

    fn exit_recursive(&mut self) {
        self.recursion_depth = self.recursion_depth.saturating_sub(1);
    }

    fn next_token(&mut self) -> Result<SpannedToken, ParseError> {
        if let Some(peeked_result) = self.peeked.take() {
            return peeked_result;
        }
        if let Some(lookahead_result) = self.lookahead.take() {
            return lookahead_result;
        }
        if self.token_pos < self.tokens.len() {
            let token = self.tokens[self.token_pos].clone();
            self.token_pos += 1;
            return Ok(token);
        }
        match self.lexer.next() {
            Some(Ok(token)) => Ok(token),
            Some(Err(lex_err)) => Err(ParseError::Lexical(lex_err)),
            None => Ok(SpannedToken {
                kind: TokenKind::EOF,
                text: String::new(),
                span: Span::default(),
            }),
        }
    }

    fn peek(&mut self) -> Result<&SpannedToken, &ParseError> {
        if self.peeked.is_none() {
            self.peeked = Some(self.next_token());
        }
        self.peeked.as_ref().unwrap().as_ref()
    }

    fn peek_span(&mut self) -> Result<Span, &ParseError> {
        Ok(self.peek()?.span)
    }

    fn expect(&mut self, kind: TokenKind) -> Result<SpannedToken, ParseError> {
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

    fn expect_ident(&mut self) -> Result<String, ParseError> {
        let token = self.next_token()?;
        match token.kind {
            TokenKind::Ident | TokenKind::DoubleQuotedIdent => Ok(token.text),
            _ => Err(ParseError::Expected {
                expected: "identifier".into(),
                found: format!("{:?}", token.kind),
                span: token.span,
            }),
        }
    }

    pub fn parse_query(&mut self) -> Result<Query, ParseError> {
        match self.peek().map(|t| &t.kind) {
            Ok(TokenKind::From) => self.parse_pipe_query(),
            _ => {
                let span = self.peek_span().unwrap_or_default();
                let found = self.peek().map(|t| format!("{:?}", t.kind)).unwrap_or_else(|_| "EOF".into());
                Err(ParseError::Expected {
                    expected: "FROM".into(),
                    found,
                    span,
                })
            }
        }
    }

    pub(crate) fn parse_pipe_query(&mut self) -> Result<Query, ParseError> {
        self.expect(TokenKind::From)?;
        let source = self.parse_table_source()?;
        let mut pipes = Vec::new();
        while let Ok(()) = self.expect_pipe() {
            let op = self.parse_pipe_operator()?;
            pipes.push(op);
        }
        Ok(Query {
            source,
            joins: vec![],
            pipes,
        })
    }

    fn expect_pipe(&mut self) -> Result<(), ParseError> {
        match self.peek().map(|t| &t.kind) {
            Ok(TokenKind::Pipe) => {
                self.next_token()?;
                Ok(())
            }
            _ => Err(ParseError::Expected {
                expected: "|>".into(),
                found: format!("{:?}", self.peek().map(|t| &t.kind).unwrap_or(&TokenKind::EOF)),
                span: self.peek_span().unwrap_or_default(),
            }),
        }
    }

    fn parse_table_source(&mut self) -> Result<crate::ast::query::TableSource, ParseError> {
        let peek_kind = self.peek().map(|t| t.kind.clone()).unwrap_or(TokenKind::EOF);
        let mut source = match peek_kind {
            TokenKind::LParen => {
                self.next_token()?; // consume '('
                let query = self.parse_pipe_query()?;
                self.expect(TokenKind::RParen)?;
                crate::ast::query::TableSource::Subquery { query: Box::new(query), alias: None }
            }
            TokenKind::String => {
                let token = self.next_token()?;
                let url = if token.text.starts_with('\'') || token.text.starts_with('"') {
                    token.text[1..token.text.len()-1].to_string()
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
                            TokenKind::String | TokenKind::DoubleQuotedIdent => val_tok.text[1..val_tok.text.len()-1].to_string(),
                            TokenKind::Ident | TokenKind::Number => val_tok.text,
                            _ => return Err(ParseError::Unexpected { token: format!("{:?}", val_tok.kind), span: val_tok.span }),
                        };
                        inline_options.push((key, val));
                        match self.peek().map(|t| &t.kind) {
                            Ok(TokenKind::Comma) => { self.next_token()?; }
                            _ => break,
                        }
                    }
                    self.expect(TokenKind::RParen)?;
                }
                crate::ast::query::TableSource::ObjectStore { url, inline_options, alias: None }
            }
            _ => {
                let mut name = self.expect_ident()?;
                while let Ok(TokenKind::Dot) = self.peek().map(|t| &t.kind) {
                    self.next_token()?; // consume '.'
                    name.push('.');
                    name.push_str(&self.expect_ident()?);
                }
                crate::ast::query::TableSource::Table { name, alias: None }
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
        } else if let Ok(TokenKind::Ident | TokenKind::DoubleQuotedIdent) = self.peek().map(|t| &t.kind) {
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
