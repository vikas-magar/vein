use crate::token::TokenKind;
use crate::parser::Parser;

impl Parser {
    pub(crate) fn parse_pipe_tablesample(&mut self) -> Result<crate::ast::pipe_op::PipeOperator, super::ParseError> {
        self.expect(TokenKind::TableSample)?;
        let method = self.expect_ident()?;
        self.expect(TokenKind::LParen)?;
        let token = self.next_token()?;
        let percent: f64 = token.text.parse().map_err(|_| super::ParseError::NumberParse {
            message: format!("invalid number: {}", token.text),
            span: token.span,
        })?;
        self.expect(TokenKind::RParen)?;
        Ok(crate::ast::pipe_op::PipeOperator::TableSample { method, percent })
    }

    pub(crate) fn parse_pipe_pivot(&mut self) -> Result<crate::ast::pipe_op::PipeOperator, super::ParseError> {
        Err(super::ParseError::Unsupported("PIVOT pipe operator not yet implemented".into()))
    }

    pub(crate) fn parse_pipe_unpivot(&mut self) -> Result<crate::ast::pipe_op::PipeOperator, super::ParseError> {
        Err(super::ParseError::Unsupported("UNPIVOT pipe operator not yet implemented".into()))
    }
}
