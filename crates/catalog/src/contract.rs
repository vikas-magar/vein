//! Read-only catalog contracts used while binding and planning a query.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use error::Result;

use crate::metadata::TableMetadata;
use crate::store::MetadataStore;

/// Opaque revision identifying the metadata view used by a query plan.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CatalogRevision(String);

impl CatalogRevision {
    /// Creates a revision from a catalog-owned identifier.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the catalog-owned identifier.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for CatalogRevision {
    fn default() -> Self {
        Self::new("0")
    }
}

/// Table metadata pinned for one query-planning operation.
#[derive(Debug, Clone)]
pub struct PlanningSession {
    /// Metadata revision used by this query.
    pub revision: CatalogRevision,
    tables: HashMap<String, Arc<TableMetadata>>,
}

impl PlanningSession {
    /// Creates a planning session at a catalog revision.
    pub fn new(revision: CatalogRevision) -> Self {
        Self {
            revision,
            tables: HashMap::new(),
        }
    }

    /// Returns a previously resolved table from this session.
    pub fn table(&self, name: &str) -> Option<Arc<TableMetadata>> {
        self.tables.get(name).cloned()
    }

    fn cache_table(&mut self, name: &str, table: Arc<TableMetadata>) {
        self.tables.insert(name.to_owned(), table);
    }
}

/// Read-only metadata interface required by the binder.
#[async_trait]
pub trait CatalogProvider: Send + Sync {
    /// Looks up a table by name.
    async fn get_table(&self, name: &str) -> Result<Arc<TableMetadata>>;

    /// Returns the process-local store of parsed file metadata this catalog fills.
    ///
    /// Execution reads through the same handle so a file's metadata is parsed
    /// once per process instead of once per component. A catalog that parses no
    /// file metadata returns an empty store; the method is required so that
    /// sharing the handle is a deliberate choice rather than a silent omission.
    fn metadata_store(&self) -> MetadataStore;

    /// Returns the metadata revision used for a new planning session.
    async fn catalog_revision(&self) -> Result<CatalogRevision> {
        Ok(CatalogRevision::default())
    }

    /// Starts a planning session with one consistent metadata revision.
    async fn begin_planning_session(&self) -> Result<PlanningSession> {
        Ok(PlanningSession::new(self.catalog_revision().await?))
    }

    /// Resolves and caches a table within the planning session.
    async fn resolve_table(
        &self,
        session: &mut PlanningSession,
        name: &str,
    ) -> Result<Arc<TableMetadata>> {
        if let Some(table) = session.table(name) {
            return Ok(table);
        }
        let table = self.get_table(name).await?;
        session.cache_table(name, table.clone());
        Ok(table)
    }
}
