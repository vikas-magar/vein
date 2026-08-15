use crate::parser::Parser;
use crate::token::TokenKind;

impl<'a> Parser<'a> {
    pub(crate) fn parse_pipe_order_by(
        &mut self,
    ) -> Result<crate::ast::pipe_op::PipeOperator<'a>, super::ParseError> {
        self.expect(TokenKind::Order)?;
        let mut order_bys = Vec::new();
        loop {
            let expr = self.parse_expr()?;
            let order_by = self.parse_order_suffix(expr)?;
            order_bys.push(order_by);
            match self.peek().map(|t| &t.kind) {
                Ok(TokenKind::Comma) => {
                    self.next_token()?;
                }
                _ => break,
            }
        }
        Ok(crate::ast::pipe_op::PipeOperator::OrderBy { order_bys })
    }

    pub(crate) fn parse_pipe_limit(
        &mut self,
    ) -> Result<crate::ast::pipe_op::PipeOperator<'a>, super::ParseError> {
        self.expect(TokenKind::Limit)?;
        let token = self.next_token()?;
        let limit_value: u64 = token
            .text
            .parse()
            .map_err(|_| super::ParseError::NumberParse {
                message: format!("invalid number: {}", token.text),
                span: token.span,
            })?;
        let offset = if let Ok(TokenKind::Offset) = self.peek().map(|t| &t.kind) {
            self.next_token()?;
            let token = self.next_token()?;
            token
                .text
                .parse()
                .map_err(|_| super::ParseError::NumberParse {
                    message: format!("invalid number: {}", token.text),
                    span: token.span,
                })?
        } else {
            0
        };
        Ok(crate::ast::pipe_op::PipeOperator::Limit {
            limit: limit_value,
            offset,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::pipe_op::PipeOperator;
    use crate::parser::test_utils::utils::*;

    #[test]
    fn test_order_by_desc() {
        let q = parse_query("FROM t |> ORDER age DESC").expect("test should succeed");
        if let PipeOperator::OrderBy { order_bys } = &q.pipes[0] {
            assert_eq!(order_bys.len(), 1);
            assert!(!order_bys[0].asc);
        } else {
            panic!("expected OrderBy");
        }
    }

    #[test]
    fn test_order_by_nulls_first() {
        let q = parse_query("FROM t |> ORDER age DESC NULLS FIRST").expect("test should succeed");
        if let PipeOperator::OrderBy { order_bys } = &q.pipes[0] {
            assert!(order_bys[0].nulls_first);
        } else {
            panic!("expected OrderBy");
        }
    }
}
