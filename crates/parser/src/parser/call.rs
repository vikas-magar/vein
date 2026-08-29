use crate::parser::Parser;
use crate::token::TokenKind;

impl<'a> Parser<'a> {
    pub(crate) fn parse_pipe_call(
        &mut self,
    ) -> Result<crate::ast::pipe_op::PipeOperator<'a>, super::ParseError> {
        self.expect(TokenKind::Call)?;
        let name = self.expect_ident()?;
        self.expect(TokenKind::LParen)?;
        let mut args = Vec::new();
        if let Ok(TokenKind::RParen) = self.peek().map(|t| &t.kind) {
            // no args
        } else {
            args = self.parse_comma_separated(super::Parser::parse_expr)?;
        }
        self.expect(TokenKind::RParen)?;
        Ok(crate::ast::pipe_op::PipeOperator::Call { name, args })
    }

    pub(crate) fn parse_pipe_assert(
        &mut self,
    ) -> Result<crate::ast::pipe_op::PipeOperator<'a>, super::ParseError> {
        self.expect(TokenKind::Assert)?;
        let condition = self.parse_expr()?;
        let message = if let Ok(TokenKind::String) = self.peek().map(|t| &t.kind) {
            let tok = self.next_token()?;
            Some(&tok.text[1..tok.text.len() - 1])
        } else {
            None
        };
        Ok(crate::ast::pipe_op::PipeOperator::Assert { condition, message })
    }

    pub(crate) fn parse_pipe_write(
        &mut self,
    ) -> Result<crate::ast::pipe_op::PipeOperator<'a>, super::ParseError> {
        self.expect(TokenKind::Write)?;

        let table = if let Ok(TokenKind::String) = self.peek().map(|t| &t.kind) {
            let tok = self.next_token()?;
            &tok.text[1..tok.text.len() - 1]
        } else {
            self.expect_ident()?
        };

        let mut options = Vec::new();

        // Optionally parse WITH
        if let Ok(TokenKind::With) = self.peek().map(|t| &t.kind) {
            self.next_token()?;
        }

        if let Ok(TokenKind::LParen) = self.peek().map(|t| &t.kind) {
            self.next_token()?; // consume '('
            loop {
                if let Ok(TokenKind::RParen) = self.peek().map(|t| &t.kind) {
                    break;
                }
                let key = self.expect_ident()?;
                self.expect(TokenKind::Eq)?;
                let val_tok = self.next_token()?;
                let val = match val_tok.kind {
                    TokenKind::String | TokenKind::DoubleQuotedIdent => {
                        &val_tok.text[1..val_tok.text.len() - 1]
                    }
                    TokenKind::Ident | TokenKind::Number => val_tok.text,
                    _ => {
                        return Err(super::ParseError::Unexpected {
                            token: format!("{:?}", val_tok.kind),
                            span: val_tok.span,
                        });
                    }
                };
                options.push((key, val));
                match self.peek().map(|t| &t.kind) {
                    Ok(TokenKind::Comma) => {
                        self.next_token()?;
                    }
                    _ => break,
                }
            }
            self.expect(TokenKind::RParen)?;
        }

        Ok(crate::ast::pipe_op::PipeOperator::Write {
            target: crate::ast::pipe_op::WriteTarget { table, options },
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::pipe_op::PipeOperator;
    use crate::parser::test_utils::utils::*;

    #[test]
    fn test_call() {
        let q = parse_query("FROM t |> CALL my_func(1, 'x')").expect("test should succeed");
        if let PipeOperator::Call { name, args } = &q.pipes[0] {
            assert_eq!(*name, "my_func");
            assert_eq!(args.len(), 2);
        } else {
            panic!("expected Call");
        }
    }

    #[test]
    fn test_write_with_options() {
        let q = parse_query("FROM t |> WRITE 's3://bucket' (mode='append', format='parquet')")
            .expect("test should succeed");
        if let PipeOperator::Write { target } = &q.pipes[0] {
            assert_eq!(target.table, "s3://bucket");
            assert_eq!(target.options.len(), 2);
            assert_eq!(target.options[0], ("mode", "append"));
            assert_eq!(target.options[1], ("format", "parquet"));
        } else {
            panic!("expected Write");
        }
    }
}
