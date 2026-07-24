use crate::token::TokenKind;
use crate::parser::Parser;

impl Parser {
    pub(crate) fn parse_pipe_order_by(&mut self) -> Result<crate::ast::pipe_op::PipeOperator, super::ParseError> {
        self.expect(TokenKind::Order)?;
        let mut order_bys = Vec::new();
        loop {
            let expr = self.parse_expr()?;
            let asc = match self.peek().map(|t| &t.kind) {
                Ok(TokenKind::Asc) => { self.next_token()?; true }
                Ok(TokenKind::Desc) => { self.next_token()?; false }
                _ => true,
            };
            let nulls_first = match self.peek().map(|t| &t.kind) {
                Ok(TokenKind::Nulls) => {
                    self.next_token()?;
                    match self.peek().map(|t| &t.kind) {
                        Ok(TokenKind::First) => { self.next_token()?; true }
                        Ok(TokenKind::Last) => { self.next_token()?; false }
                        _ => asc,
                    }
                }
                _ => asc,
            };
            order_bys.push(crate::ast::pipe_op::OrderByExpr { expr, asc, nulls_first });
            match self.peek().map(|t| &t.kind) {
                Ok(TokenKind::Comma) => { self.next_token()?; }
                _ => break,
            }
        }
        Ok(crate::ast::pipe_op::PipeOperator::OrderBy { order_bys })
    }

    pub(crate) fn parse_pipe_limit(&mut self) -> Result<crate::ast::pipe_op::PipeOperator, super::ParseError> {
        self.expect(TokenKind::Limit)?;
        let token = self.next_token()?;
        let limit_value: u64 = token.text.parse().map_err(|_| super::ParseError::NumberParse {
            message: format!("invalid number: {}", token.text),
            span: token.span,
        })?;
        let offset = if let Ok(TokenKind::Offset) = self.peek().map(|t| &t.kind) {
            self.next_token()?;
            let token = self.next_token()?;
            token.text.parse().map_err(|_| super::ParseError::NumberParse {
                message: format!("invalid number: {}", token.text),
                span: token.span,
            })?
        } else {
            0
        };
        Ok(crate::ast::pipe_op::PipeOperator::Limit { limit: limit_value, offset })
    }
}
