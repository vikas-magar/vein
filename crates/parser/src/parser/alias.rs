use crate::token::TokenKind;
use crate::parser::Parser;

impl Parser {
    pub(crate) fn parse_pipe_as(&mut self) -> Result<crate::ast::pipe_op::PipeOperator, super::ParseError> {
        self.expect(TokenKind::As)?;
        let alias = self.expect_ident()?;
        Ok(crate::ast::pipe_op::PipeOperator::As { alias })
    }

    pub(crate) fn parse_pipe_distinct(&mut self) -> Result<crate::ast::pipe_op::PipeOperator, super::ParseError> {
        self.expect(TokenKind::Distinct)?;
        Ok(crate::ast::pipe_op::PipeOperator::Distinct)
    }
}
