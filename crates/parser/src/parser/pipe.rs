use crate::parser::Parser;
use crate::token::TokenKind;

impl<'a> Parser<'a> {
    /// Parses one pipe operator, guarded against unbounded nesting.
    ///
    /// FORK/TEE recurse back into this function for each sub-pipeline, so a
    /// pathologically nested input (e.g. `FORK ( |> FORK ( |> ...)`) would
    /// otherwise overflow the stack. Wrapping the dispatch in the recursion
    /// budget bounds that nesting; flat pipe chains stay at depth 1.
    pub(crate) fn parse_pipe_operator(
        &mut self,
    ) -> Result<crate::ast::pipe_op::PipeOperator<'a>, super::ParseError> {
        self.enter_recursive()?;
        let result = self.parse_pipe_operator_inner();
        self.exit_recursive();
        result
    }

    fn parse_pipe_operator_inner(
        &mut self,
    ) -> Result<crate::ast::pipe_op::PipeOperator<'a>, super::ParseError> {
        match self.peek().map(|t| &t.kind) {
            Ok(TokenKind::Select) => self.parse_pipe_select(),
            Ok(TokenKind::Where) => self.parse_pipe_where(),
            Ok(TokenKind::Qualify) => self.parse_pipe_qualify(),
            Ok(TokenKind::Aggregate) => self.parse_pipe_aggregate(),
            Ok(TokenKind::Order) => self.parse_pipe_order_by(),
            Ok(TokenKind::Limit) => self.parse_pipe_limit(),
            Ok(TokenKind::Distinct) => self.parse_pipe_distinct(),
            Ok(TokenKind::Extend) => self.parse_pipe_extend(),
            Ok(TokenKind::Set) => self.parse_pipe_set(),
            Ok(TokenKind::Drop) => self.parse_pipe_drop(),
            Ok(TokenKind::Rename) => self.parse_pipe_rename(),
            Ok(TokenKind::As) => self.parse_pipe_as(),
            Ok(TokenKind::Call) => self.parse_pipe_call(),
            Ok(TokenKind::Assert) => self.parse_pipe_assert(),
            Ok(TokenKind::Write) => self.parse_pipe_write(),
            Ok(TokenKind::Join | TokenKind::Left | TokenKind::Right | TokenKind::Inner |
               TokenKind::Full | TokenKind::Cross) => {
                self.parse_pipe_join()
            }
            Ok(TokenKind::TableSample) => self.parse_pipe_tablesample(),
            Ok(TokenKind::Pivot) => self.parse_pipe_pivot(),
            Ok(TokenKind::Unpivot) => self.parse_pipe_unpivot(),
            Ok(TokenKind::With) => self.parse_pipe_with(),
            Ok(TokenKind::Fork) => self.parse_pipe_fork(),
            Ok(TokenKind::Tee) => self.parse_pipe_tee(),
            Ok(TokenKind::Union | TokenKind::Intersect | TokenKind::Except) => {
                self.parse_pipe_set_op()
            }
            _ => Err(super::ParseError::Expected {
                expected: "pipe operator (SELECT, WHERE, AGGREGATE, etc.)".into(),
                found: format!(
                    "{:?}",
                    self.peek().map_or(&TokenKind::EOF, |t| &t.kind)
                ),
                span: self.peek_span().unwrap_or_default(),
            }),
        }
    }

    pub(crate) fn parse_pipe_fork(
        &mut self,
    ) -> Result<crate::ast::pipe_op::PipeOperator<'a>, super::ParseError> {
        self.expect(TokenKind::Fork)?;
        self.expect(TokenKind::LParen)?;
        let mut pipelines = Vec::new();
        loop {
            let mut pipeline = Vec::new();
            while let Ok(()) = self.expect_pipe() {
                pipeline.push(self.parse_pipe_operator()?);
            }
            pipelines.push(pipeline);
            if let Ok(TokenKind::Comma) = self.peek().map(|t| &t.kind) {
                self.next_token()?;
            } else {
                break;
            }
        }
        self.expect(TokenKind::RParen)?;
        Ok(crate::ast::pipe_op::PipeOperator::Fork { pipelines })
    }

    pub(crate) fn parse_pipe_tee(
        &mut self,
    ) -> Result<crate::ast::pipe_op::PipeOperator<'a>, super::ParseError> {
        self.expect(TokenKind::Tee)?;
        self.expect(TokenKind::LParen)?;
        let mut pipelines = Vec::new();
        loop {
            let mut pipeline = Vec::new();
            while let Ok(()) = self.expect_pipe() {
                pipeline.push(self.parse_pipe_operator()?);
            }
            pipelines.push(pipeline);
            if let Ok(TokenKind::Comma) = self.peek().map(|t| &t.kind) {
                self.next_token()?;
            } else {
                break;
            }
        }
        self.expect(TokenKind::RParen)?;
        Ok(crate::ast::pipe_op::PipeOperator::Tee { pipelines })
    }
}

#[cfg(test)]
mod tests;
