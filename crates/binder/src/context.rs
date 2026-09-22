use crate::traits::FunctionRegistry;
use plan::{ColumnId, LogicalPlan};
use std::collections::HashMap;
use vcs::{CatalogReader, PlanningSession};

/// Context for the binder, holding references to external catalogs and functions.
pub struct BinderContext<'a> {
    /// The global catalog for resolving table schemas
    pub catalog: &'a dyn CatalogReader,
    /// The registry for resolving functions
    pub functions: &'a dyn FunctionRegistry,
    /// Immutable catalog metadata pinned for the current query.
    pub planning_session: PlanningSession,
    /// Common Table Expressions (CTEs) bound in the current query scope
    pub ctes: HashMap<String, Box<LogicalPlan>>,
    /// Global column ID generator for the query
    pub next_col_id: ColumnId,
}

impl<'a> BinderContext<'a> {
    /// Creates a binder context at the initial catalog revision.
    pub fn new(catalog: &'a dyn CatalogReader, functions: &'a dyn FunctionRegistry) -> Self {
        Self::with_planning_session(
            catalog,
            functions,
            PlanningSession::new(vcs::CatalogRevision::default()),
        )
    }

    /// Creates a binder context with an already pinned planning session.
    pub fn with_planning_session(
        catalog: &'a dyn CatalogReader,
        functions: &'a dyn FunctionRegistry,
        planning_session: PlanningSession,
    ) -> Self {
        Self {
            catalog,
            functions,
            planning_session,
            ctes: HashMap::new(),
            next_col_id: ColumnId(1),
        }
    }

    /// Resolves a table through the pinned planning session.
    pub async fn resolve_table(
        &mut self,
        name: &str,
    ) -> error::Result<std::sync::Arc<plan::TableMetadata>> {
        self.catalog
            .resolve_table(&mut self.planning_session, name)
            .await
    }

    /// Generates the next monotonically increasing ColumnId
    pub fn next_id(&mut self) -> ColumnId {
        let id = self.next_col_id;
        self.next_col_id = ColumnId(self.next_col_id.0 + 1);
        id
    }
}
