use crate::token::TokenKind;
use crate::parser::Parser;

impl<'a> Parser<'a> {
    pub(crate) fn parse_pipe_where(&mut self) -> Result<crate::ast::pipe_op::PipeOperator<'a>, super::ParseError> {
        self.expect(TokenKind::Where)?;
        let predicate = self.parse_expr()?;
        Ok(crate::ast::pipe_op::PipeOperator::Where { predicate })
    }
}


#[cfg(test)]
mod tests {
    use crate::ast::pipe_op::PipeOperator;
    use crate::parser::test_utils::utils::*;

    #[test]
        fn test_where_clause() {
            let q = parse_query("FROM t |> WHERE age > 18").unwrap();
            assert_eq!(q.pipes.len(), 1);
            assert!(matches!(q.pipes[0], PipeOperator::Where { .. }));
        }

}
