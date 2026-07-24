use crate::token::TokenKind;
use crate::parser::Parser;

impl Parser {
    pub(crate) fn parse_pipe_join(&mut self) -> Result<crate::ast::pipe_op::PipeOperator, super::ParseError> {
        let join_type = match self.peek().map(|t| &t.kind) {
            Ok(TokenKind::Left) => { self.next_token()?; self.expect(TokenKind::Join)?; crate::logical_plan::JoinType::Left }
            Ok(TokenKind::Right) => { self.next_token()?; self.expect(TokenKind::Join)?; crate::logical_plan::JoinType::Right }
            Ok(TokenKind::Inner) => { self.next_token()?; self.expect(TokenKind::Join)?; crate::logical_plan::JoinType::Inner }
            Ok(TokenKind::Full) => { self.next_token()?; self.expect(TokenKind::Join)?; crate::logical_plan::JoinType::Full }
            Ok(TokenKind::Cross) => { self.next_token()?; self.expect(TokenKind::Join)?; crate::logical_plan::JoinType::Cross }
            Ok(TokenKind::Join) => { self.next_token()?; crate::logical_plan::JoinType::Inner }
            _ => return Err(super::ParseError::Expected {
                expected: "JOIN or join type (LEFT, RIGHT, INNER, FULL, CROSS)".into(),
                found: format!("{:?}", self.peek().map(|t| &t.kind).unwrap_or(&TokenKind::EOF)),
                span: self.peek_span().unwrap_or_default(),
            }),
        };

        let right = self.parse_table_source()?;

        let right = if let Ok(TokenKind::As) = self.peek().map(|t| &t.kind) {
            self.next_token()?;
            let alias = self.expect_ident()?;
            match right {
                crate::ast::query::TableSource::Table { name, alias: _ } =>
                    crate::ast::query::TableSource::Table { name, alias: Some(alias) },
                crate::ast::query::TableSource::ObjectStore { url, inline_options, alias: _ } =>
                    crate::ast::query::TableSource::ObjectStore { url, inline_options, alias: Some(alias) },
                crate::ast::query::TableSource::Subquery { query, alias: _ } =>
                    crate::ast::query::TableSource::Subquery { query, alias: Some(alias) },
            }
        } else if matches!(self.peek().map(|t| &t.kind), Ok(TokenKind::Ident)) {
            let alias = self.expect_ident()?;
            match right {
                crate::ast::query::TableSource::Table { name, alias: _ } =>
                    crate::ast::query::TableSource::Table { name, alias: Some(alias) },
                crate::ast::query::TableSource::ObjectStore { url, inline_options, alias: _ } =>
                    crate::ast::query::TableSource::ObjectStore { url, inline_options, alias: Some(alias) },
                crate::ast::query::TableSource::Subquery { query, alias: _ } =>
                    crate::ast::query::TableSource::Subquery { query, alias: Some(alias) },
            }
        } else {
            right
        };

        let (predicate, using) = if let Ok(TokenKind::On) = self.peek().map(|t| &t.kind) {
            self.next_token()?;
            let predicate = self.parse_expr()?;
            (Some(predicate), vec![])
        } else if let Ok(TokenKind::Using) = self.peek().map(|t| &t.kind) {
            self.next_token()?;
            self.expect(TokenKind::LParen)?;
            let using = self.parse_comma_separated(|p| p.expect_ident())?;
            self.expect(TokenKind::RParen)?;
            (None, using)
        } else if join_type == crate::logical_plan::JoinType::Cross {
            (None, vec![])
        } else {
            return Err(super::ParseError::Expected {
                expected: "ON or USING".into(),
                found: format!("{:?}", self.peek().map(|t| &t.kind).unwrap_or(&TokenKind::EOF)),
                span: self.peek_span().unwrap_or_default(),
            });
        };

        Ok(crate::ast::pipe_op::PipeOperator::Join {
            join: crate::ast::pipe_op::JoinClause {
                join_type,
                right: Box::new(crate::ast::query::Query {
                    source: right,
                    joins: vec![],
                    pipes: vec![],
                }),
                predicate,
                using,
            },
        })
    }
}
