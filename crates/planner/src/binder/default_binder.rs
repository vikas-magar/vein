use common::Result;
use crate::traits::LogicalBinder;

/// The Default SQL Binder.
/// Responsible for parsing the raw SQL string into an AST, validating it against the catalog,
/// and returning a serialized Semantic Logical Plan.
pub struct DefaultSqlBinder;

impl DefaultSqlBinder {
    /// Creates a new DefaultSqlBinder instance.
    pub fn new() -> Self {
        Self {}
    }
}

impl LogicalBinder for DefaultSqlBinder {
    fn bind(&self, ast_string: String) -> Result<String> {
        // TODO: Call parser::parse(ast_string) and pass to core Binder
        Ok(format!("BoundPlan({})", ast_string))
    }
}
