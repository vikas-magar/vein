use binder::LogicalBinder;
use common::Result;
use plan::{PlanTraversal, VeinSchema};

use super::LocalQueryEngine;

impl LocalQueryEngine {
    /// Parses and binds a query for tooling, without optimization, planning or execution.
    /// Catalog providers may read schema metadata, but no scan operator is invoked.
    pub async fn inspect_query(&self, query: &str) -> Result<VeinSchema> {
        let statement = parser::Parser::new(query).parse_statement()?;
        let plan = self.binder.bind(&statement).await?;
        Ok(plan.schema().clone())
    }

    /// Borrowed signatures from the actual function catalog shared with the binder.
    pub fn function_signatures(&self) -> Vec<(&str, &functions::FunctionSignature)> {
        self.binder.functions().list()
    }
}
