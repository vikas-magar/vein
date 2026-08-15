use crate::parser::Parser;
use crate::token::TokenKind;

impl<'a> Parser<'a> {
    pub(crate) fn parse_pipe_extend(
        &mut self,
    ) -> Result<crate::ast::pipe_op::PipeOperator<'a>, super::ParseError> {
        self.expect(TokenKind::Extend)?;
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
                Ok(TokenKind::Comma) => {
                    self.next_token()?;
                }
                _ => break,
            }
        }
        Ok(crate::ast::pipe_op::PipeOperator::Extend { items })
    }

    pub(crate) fn parse_pipe_set(
        &mut self,
    ) -> Result<crate::ast::pipe_op::PipeOperator<'a>, super::ParseError> {
        self.expect(TokenKind::Set)?;
        let mut assignments = Vec::new();
        loop {
            let col = self.expect_ident()?;
            self.expect(TokenKind::Eq)?;
            let expr = self.parse_expr()?;
            assignments.push((col, expr));
            match self.peek().map(|t| &t.kind) {
                Ok(TokenKind::Comma) => {
                    self.next_token()?;
                }
                _ => break,
            }
        }
        Ok(crate::ast::pipe_op::PipeOperator::Set { assignments })
    }

    pub(crate) fn parse_pipe_drop(
        &mut self,
    ) -> Result<crate::ast::pipe_op::PipeOperator<'a>, super::ParseError> {
        self.expect(TokenKind::Drop)?;
        let mut columns = Vec::new();
        loop {
            columns.push(self.expect_ident()?);
            match self.peek().map(|t| &t.kind) {
                Ok(TokenKind::Comma) => {
                    self.next_token()?;
                }
                _ => break,
            }
        }
        Ok(crate::ast::pipe_op::PipeOperator::Drop { columns })
    }

    pub(crate) fn parse_pipe_rename(
        &mut self,
    ) -> Result<crate::ast::pipe_op::PipeOperator<'a>, super::ParseError> {
        self.expect(TokenKind::Rename)?;
        let mut renames = Vec::new();
        loop {
            let from = self.expect_ident()?;
            self.expect(TokenKind::As)?;
            let to = self.expect_ident()?;
            renames.push((from, to));
            match self.peek().map(|t| &t.kind) {
                Ok(TokenKind::Comma) => {
                    self.next_token()?;
                }
                _ => break,
            }
        }
        Ok(crate::ast::pipe_op::PipeOperator::Rename { renames })
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::pipe_op::PipeOperator;
    use crate::parser::test_utils::utils::*;

    #[test]
    fn test_extend() {
        let q = parse_query("FROM t |> EXTEND salary * 2 AS bonus").expect("test should succeed");
        if let PipeOperator::Extend { items } = &q.pipes[0] {
            assert_eq!(items.len(), 1);
            assert_eq!(items[0].alias, Some("bonus".into()));
        } else {
            panic!("expected Extend");
        }
    }
}
