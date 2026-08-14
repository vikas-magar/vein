#[cfg(test)]
pub mod utils {
    use crate::ast::pipe_op::PipeOperator;
    use crate::ast::query::Query;
    use crate::parser::{ParseError, Parser};

    pub fn parse_query(input: &str) -> Result<Query<'_>, ParseError> {
        Parser::new(input).parse_query()
    }

    use crate::token::{Span, SpannedToken, TokenKind};

    pub fn spanned<'a>(kind: TokenKind, text: &'a str) -> SpannedToken<'a> {
        SpannedToken { kind, text, span: Span::default() }
    }

    pub fn parse_expr(input: &str) -> Result<crate::ast::expr::SpannedExpr<'_>, ParseError> {
        let wrapped = Box::leak(format!("FROM t |> SELECT {}", input).into_boxed_str());
        let query = parse_query(wrapped)?;
        match query.pipes.into_iter().next() {
            Some(PipeOperator::Select { mut items }) => Ok(items.remove(0).expr),
            Some(other) => panic!("expected Select, got {:?}", other),
            None => panic!("expected at least one pipe operator"),
        }
    }
}
