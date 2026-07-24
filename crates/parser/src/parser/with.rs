use crate::token::TokenKind;
use crate::parser::Parser;

impl Parser {
    pub(crate) fn parse_pipe_with(&mut self) -> Result<crate::ast::pipe_op::PipeOperator, super::ParseError> {
        self.expect(TokenKind::With)?;
        let mut ctes = Vec::new();
        loop {
            let alias = self.expect_ident()?;
            let columns = if let Ok(TokenKind::LParen) = self.peek().map(|t| &t.kind) {
                self.next_token()?;
                let cols = self.parse_comma_separated(|p| p.expect_ident())?;
                self.expect(TokenKind::RParen)?;
                Some(cols)
            } else {
                None
            };
            self.expect(TokenKind::As)?;
            self.expect(TokenKind::LParen)?;
            let query = self.parse_pipe_query()?;
            self.expect(TokenKind::RParen)?;
            ctes.push(crate::ast::pipe_op::CommonTableExpr { alias, columns, query: Box::new(query) });
            match self.peek().map(|t| &t.kind) {
                Ok(TokenKind::Comma) => { self.next_token()?; }
                _ => break,
            }
        }
        Ok(crate::ast::pipe_op::PipeOperator::With { ctes })
    }
}
