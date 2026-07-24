use crate::token::TokenKind;
use crate::parser::Parser;

impl Parser {
    pub(crate) fn parse_pipe_select(&mut self) -> Result<crate::ast::pipe_op::PipeOperator, super::ParseError> {
        self.expect(TokenKind::Select)?;
        let mut items = Vec::new();
        loop {
            let expr = self.parse_expr()?;
            let alias = if let Ok(TokenKind::As) = self.peek().map(|t| &t.kind) {
                self.next_token()?;
                Some(self.expect_ident()?)
            } else {
                None
            };
            items.push(crate::ast::pipe_op::SelectItem { expr, alias });
            match self.peek().map(|t| &t.kind) {
                Ok(TokenKind::Comma) => { self.next_token()?; }
                _ => break,
            }
        }
        Ok(crate::ast::pipe_op::PipeOperator::Select { items })
    }
}
