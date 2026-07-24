use crate::token::TokenKind;
use crate::parser::Parser;

impl Parser {
    pub(crate) fn parse_pipe_where(&mut self) -> Result<crate::ast::pipe_op::PipeOperator, super::ParseError> {
        self.expect(TokenKind::Where)?;
        let predicate = self.parse_expr()?;
        Ok(crate::ast::pipe_op::PipeOperator::Where { predicate })
    }
}
