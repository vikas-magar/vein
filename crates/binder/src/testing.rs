//! Testing utilities (mock function registry, catalog, and end-to-end bind helper).

mod catalog;

pub(crate) use catalog::TestCatalog;

use crate::pipe::query::QueryBinder;
use crate::traits::LogicalBinder;
use functions::FunctionRegistry;
use error::VeinError;
use parser::parser::Parser;
use plan::LogicalPlan;
use std::sync::Arc;

/// Binds a SQL string end-to-end: parse → bind with the in-memory catalog and
/// the pre-populated mock registry. Returns a fully-typed logical plan or a
/// typed `VeinError`; never panics on user input.
pub async fn plan(sql: &str) -> error::Result<LogicalPlan> {
    plan_with_catalog(sql, Arc::new(TestCatalog::with_fixtures())).await
}

/// Binds against caller-supplied metadata without introducing a format dependency.
pub(crate) async fn plan_with_catalog(
    sql: &str, catalog: Arc<dyn plan::CatalogProvider>,
) -> error::Result<LogicalPlan> {
    let node = Parser::new(sql)
        .parse_statement()
        .map_err(|e| VeinError::semantic(format!("parse error: {e}"), parse_error_span(&e)))?;

    QueryBinder::new(
        catalog,
        Arc::new(FunctionRegistry::with_builtins()),
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
