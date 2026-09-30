//! Catalog snapshot identity pinned by a query plan.

/// Immutable catalog snapshot token carried with a physical scan.
///
/// A planning session resolves a table against one catalog revision. The token
/// records that revision together with the table identity and schema version it
/// saw, so execution can tell a plan that is still valid from one resolved
/// against metadata that has since moved on.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub struct CatalogSnapshotToken {
    /// Stable table identity in the catalog.
    pub table_id: String,
    /// Catalog revision used to plan the query.
    pub catalog_revision: String,
    /// Committed table snapshot selected by the planner.
    pub snapshot_id: String,
    /// Schema version selected by the planner.
    pub schema_version: u64,
}

impl CatalogSnapshotToken {
    /// Creates a catalog snapshot token.
    pub fn new(
        table_id: impl Into<String>,
        catalog_revision: impl Into<String>,
        snapshot_id: impl Into<String>,
        schema_version: u64,
    ) -> Self {
        Self {
            table_id: table_id.into(),
            catalog_revision: catalog_revision.into(),
            snapshot_id: snapshot_id.into(),
            schema_version,
        }
    }
}

// The archived-to-native conversion lives here because the orphan rule pins it
// to the crate that defines the token. It is the O(1) extraction the executor
// uses to read a token out of a zero-copy rkyv buffer.
impl From<&ArchivedCatalogSnapshotToken> for CatalogSnapshotToken {
    fn from(archived: &ArchivedCatalogSnapshotToken) -> Self {
        Self::new(
            archived.table_id.as_str(),
            archived.catalog_revision.as_str(),
            archived.snapshot_id.as_str(),
            archived.schema_version.to_native(),
        )
    }
}
