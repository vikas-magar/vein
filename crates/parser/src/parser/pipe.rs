use crate::token::TokenKind;
use crate::parser::Parser;

impl Parser {
    pub(crate) fn parse_pipe_operator(&mut self) -> Result<crate::ast::pipe_op::PipeOperator, super::ParseError> {
        match self.peek().map(|t| &t.kind) {
            Ok(TokenKind::Select) => self.parse_pipe_select(),
            Ok(TokenKind::Where) => self.parse_pipe_where(),
            Ok(TokenKind::Aggregate) => self.parse_pipe_aggregate(),
            Ok(TokenKind::Order) => self.parse_pipe_order_by(),
            Ok(TokenKind::Limit) => self.parse_pipe_limit(),
            Ok(TokenKind::Distinct) => self.parse_pipe_distinct(),
            Ok(TokenKind::Extend) => self.parse_pipe_extend(),
            Ok(TokenKind::Set) => self.parse_pipe_set(),
            Ok(TokenKind::Drop) => self.parse_pipe_drop(),
            Ok(TokenKind::Rename) => self.parse_pipe_rename(),
            Ok(TokenKind::As) => self.parse_pipe_as(),
            Ok(TokenKind::Call) => self.parse_pipe_call(),
            Ok(TokenKind::Assert) => self.parse_pipe_assert(),
            Ok(TokenKind::Write) => self.parse_pipe_write(),
            Ok(TokenKind::Join) | Ok(TokenKind::Left) | Ok(TokenKind::Right)
                | Ok(TokenKind::Inner) | Ok(TokenKind::Full) | Ok(TokenKind::Cross) => self.parse_pipe_join(),
            Ok(TokenKind::TableSample) => self.parse_pipe_tablesample(),
            Ok(TokenKind::Pivot) => self.parse_pipe_pivot(),
            Ok(TokenKind::Unpivot) => self.parse_pipe_unpivot(),
            Ok(TokenKind::With) => self.parse_pipe_with(),
            Ok(TokenKind::Union) | Ok(TokenKind::Intersect) | Ok(TokenKind::Except) => self.parse_pipe_set_op(),
            _ => Err(super::ParseError::Expected {
                expected: "pipe operator (SELECT, WHERE, AGGREGATE, etc.)".into(),
                found: format!("{:?}", self.peek().map(|t| &t.kind).unwrap_or(&TokenKind::EOF)),
                span: self.peek_span().unwrap_or_default(),
            }),
        }
    }
}
