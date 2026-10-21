use async_trait::async_trait;
use error::Result;
pub use functions::{FunctionCatalog as FunctionRegistry, FunctionKind, FunctionSignature, RuntimeKind};
use parser::ast::query::QueryNode;
use plan::{CatalogRevision, CatalogProvider, LogicalPlan};

/// A logical plan together with the catalog revision used to bind it.
#[derive(Debug)]
pub struct BoundQuery {
    /// Bound logical plan.
    pub plan: LogicalPlan,
    /// Catalog revision pinned for all source resolution.
    pub catalog_revision: CatalogRevision,
}

/// The core logical binder trait.
///
/// Binds an AST [`QueryNode`] into a validated, type-coerced [`LogicalPlan`].
/// The binder is a semantic frontend only: it resolves columns, types, scopes
/// and functions; plan optimization happens in a later pipeline stage.
#[async_trait]
pub trait LogicalBinder: Send + Sync {
    /// Binds a parsed statement into a validated, type-coerced LogicalPlan.
    /// `EXPLAIN` statements yield a [`LogicalPlan::Explain`] wrapper over the
    /// bound inner query.
    async fn bind<'a>(&self, node: &'a QueryNode<'a>) -> Result<LogicalPlan>;

    /// Binds a query and returns the catalog revision used for source resolution.
    async fn bind_with_catalog_revision<'a>(&self, node: &'a QueryNode<'a>) -> Result<BoundQuery> {
        Ok(BoundQuery {
            plan: self.bind(node).await?,
            catalog_revision: CatalogRevision::default(),
        })
    }

    /// Returns the underlying catalog, if available.
    fn catalog(&self) -> Option<&std::sync::Arc<dyn CatalogProvider>> {
        None
    }
}
