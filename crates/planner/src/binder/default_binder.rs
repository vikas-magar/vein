use async_trait::async_trait;
use parser::ast::query::Query;
use crate::plan::LogicalPlan;
use crate::traits::LogicalBinder;

/// The default implementation of the logical binder.
/// Transforms an AST into a fully resolved logical plan.
pub struct DefaultSqlBinder;

impl DefaultSqlBinder {
    /// Creates a new instance of the default binder
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl LogicalBinder for DefaultSqlBinder {
    async fn bind(&self, _query: &Query) -> Result<LogicalPlan, error::VeinError> {
        // Implementation will happen in Step 3
        todo!("Step 3: Implement the DefaultSqlBinder::bind logic")
    }
}
