use crate::token::TokenKind;
use crate::parser::Parser;

impl Parser {
    pub(crate) fn parse_pipe_call(&mut self) -> Result<crate::ast::pipe_op::PipeOperator, super::ParseError> {
        self.expect(TokenKind::Call)?;
        let name = self.expect_ident()?;
        self.expect(TokenKind::LParen)?;
        let mut args = Vec::new();
        if let Ok(TokenKind::RParen) = self.peek().map(|t| &t.kind) {
            // no args
        } else {
            args = self.parse_comma_separated(|p| p.parse_expr())?;
        }
        self.expect(TokenKind::RParen)?;
        Ok(crate::ast::pipe_op::PipeOperator::Call { name, args })
    }

    pub(crate) fn parse_pipe_assert(&mut self) -> Result<crate::ast::pipe_op::PipeOperator, super::ParseError> {
        self.expect(TokenKind::Assert)?;
        let condition = self.expect_ident()?;
        let message = if let Ok(TokenKind::String) = self.peek().map(|t| &t.kind) {
            Some(self.next_token()?.text)
        } else {
            None
        };
        Ok(crate::ast::pipe_op::PipeOperator::Assert { condition, message })
    }

    pub(crate) fn parse_pipe_write(&mut self) -> Result<crate::ast::pipe_op::PipeOperator, super::ParseError> {
        self.expect(TokenKind::Write)?;
        let table = self.expect_ident()?;
        Ok(crate::ast::pipe_op::PipeOperator::Write {
            target: crate::ast::pipe_op::WriteTarget { table, options: vec![] },
        })
    }
}


#[cfg(test)]
mod tests {
    use crate::ast::pipe_op::PipeOperator;
    use crate::parser::test_utils::utils::*;

    #[test]
        fn test_call() {
            let q = parse_query("FROM t |> CALL my_func(1, 'x')").unwrap();
            if let PipeOperator::Call { name, args } = &q.pipes[0] {
                assert_eq!(name, "my_func");
                assert_eq!(args.len(), 2);
            } else {
                panic!("expected Call");
            }
        }

}
