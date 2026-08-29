use crate::parser::Parser;
use crate::token::TokenKind;

impl<'a> Parser<'a> {
    pub(crate) fn parse_pipe_with(
        &mut self,
    ) -> Result<crate::ast::pipe_op::PipeOperator<'a>, super::ParseError> {
        self.expect(TokenKind::With)?;
        let mut ctes = Vec::new();
        loop {
            let alias = self.expect_ident()?;
            let columns = if let Ok(TokenKind::LParen) = self.peek().map(|t| &t.kind) {
                self.next_token()?;
                let cols = self.parse_comma_separated(super::Parser::expect_ident)?;
                self.expect(TokenKind::RParen)?;
                Some(cols)
            } else {
                None
            };
            self.expect(TokenKind::As)?;
            self.expect(TokenKind::LParen)?;
            let query = self.parse_pipe_query()?;
            self.expect(TokenKind::RParen)?;
            ctes.push(crate::ast::pipe_op::CommonTableExpr {
                alias,
                columns,
                query: Box::new(query),
            });
            match self.peek().map(|t| &t.kind) {
                Ok(TokenKind::Comma) => {
                    self.next_token()?;
                }
                _ => break,
            }
        }
        Ok(crate::ast::pipe_op::PipeOperator::With { ctes })
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::pipe_op::PipeOperator;
    use crate::parser::test_utils::utils::*;

    #[test]
    fn test_with_cte() {
        let q =
            parse_query("FROM t |> WITH x AS (FROM y |> SELECT 1)").expect("test should succeed");
        if let PipeOperator::With { ctes } = &q.pipes[0] {
            assert_eq!(ctes.len(), 1);
            assert_eq!(ctes[0].alias, "x");
        } else {
            panic!("expected With");
        }
    }
}
