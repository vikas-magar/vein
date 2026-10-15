//! Testing utilities (mock function registry, catalog, and end-to-end bind helper).

mod catalog;
mod functions;

pub(crate) use catalog::TestCatalog;
pub use functions::MockFunctionRegistry;

use crate::pipe::query::QueryBinder;
use crate::traits::LogicalBinder;
use error::VeinError;
use parser::parser::Parser;
use plan::LogicalPlan;
use std::sync::Arc;

/// Binds a SQL string end-to-end: parse → bind with the in-memory catalog and
/// the pre-populated mock registry. Returns a fully-typed logical plan or a
/// typed `VeinError`; never panics on user input.
pub async fn plan(sql: &str) -> error::Result<LogicalPlan> {
    let node = Parser::new(sql)
        .parse_statement()
        .map_err(|e| VeinError::semantic(format!("parse error: {e}"), parse_error_span(&e)))?;

    QueryBinder::new(
        Arc::new(TestCatalog::with_fixtures()),
        Arc::new(MockFunctionRegistry::new()),
    )
        .bind(&node)
        .await
}

/// Extracts the source span of a parse error, when the variant carries one.
fn parse_error_span(err: &parser::parser::ParseError) -> Option<parser::token::Span> {
    use parser::parser::ParseError;
    match err {
        ParseError::Expected { span, .. }
        | ParseError::Unexpected { span, .. }
        | ParseError::NumberParse { span, .. } => Some(*span),
        ParseError::Lexical(_)
        | ParseError::Unsupported(_)
        | ParseError::RecursionLimitExceeded { .. } => None,
    }
}
