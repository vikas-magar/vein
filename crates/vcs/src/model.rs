//! Immutable metadata contracts shared by VCS planning, execution, and governance.
//!
//! These types deliberately contain opaque identifiers rather than backend-specific
//! IDs. A catalog backend may use Iceberg snapshot IDs, database revisions, or a
//! distributed log offset internally, while planners and executors use one stable
//! contract.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use plan::TableMetadata;

/// An opaque, monotonically comparable catalog revision.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CatalogRevision(String);

impl CatalogRevision {
    /// Creates a revision from a backend-owned identifier.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the backend-owned revision identifier.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns whether this revision is the initial empty revision.
    pub fn is_initial(&self) -> bool {
        self.0 == "0"
    }
}

impl Default for CatalogRevision {
    fn default() -> Self {
        Self::new("0")
    }
}

/// Stable logical identity of a catalog table.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableId(String);

impl TableId {
    /// Creates a table identity.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the stable table identity.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Stable identity of one committed table snapshot.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SnapshotId(String);

impl SnapshotId {
    /// Creates a snapshot identity.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the stable snapshot identity.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Monotonic schema version associated with a table snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SchemaVersion(u64);

impl SchemaVersion {
    /// Creates a schema version.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the numeric schema version.
    pub const fn value(self) -> u64 {
        self.0
    }
}

/// Capabilities advertised by a table snapshot or storage format.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TableCapability {
    /// The source supports predicate and projection pushdown.
    PredicatePushdown,
    /// The source exposes partition-level statistics.
    PartitionPruning,
    /// The source exposes row-group or granule statistics.
    GranulePruning,
    /// The source exposes canonical address sidecars.
    AddressNative,
    /// The source supports exact key verification from a payload vault.
    CollisionVerification,
    /// The source supports bounded streaming reads.
    StreamingRead,
    /// The source supports immutable snapshot reads.
    SnapshotRead,
}

/// Immutable metadata required to plan one table read.
#[derive(Debug, Clone)]
pub struct TableSnapshot {
    /// Stable logical table identity.
    pub table_id: TableId,
    /// User-visible table name.
    pub name: String,
    /// Catalog revision at which this metadata was observed.
    pub catalog_revision: CatalogRevision,
    /// Committed data snapshot selected for the query.
    pub snapshot_id: SnapshotId,
    /// Schema version selected for the query.
    pub schema_version: SchemaVersion,
    /// Authoritative table metadata used by the binder.
    pub metadata: Arc<TableMetadata>,
    /// Immutable manifest or VFS descriptor reference.
    pub manifest_uri: Option<String>,
    /// Optional statistics index reference.
    pub statistics_uri: Option<String>,
    /// Physical and logical capabilities available for planning.
    pub capabilities: BTreeSet<TableCapability>,
}

impl TableSnapshot {
    /// Builds a stable planning snapshot from authoritative table metadata.
    pub fn from_metadata(metadata: Arc<TableMetadata>, catalog_revision: CatalogRevision) -> Self {
        let table_name = metadata.name.clone();
        let table_id = TableId::new(format!("table:{table_name}"));
        let snapshot_id = SnapshotId::new(format!("{}:{table_name}", catalog_revision.as_str()));
        let mut capabilities = BTreeSet::from([
            TableCapability::SnapshotRead,
            TableCapability::StreamingRead,
        ]);

        if metadata
            .format
            .as_ref()
            .map(|format| format.to_string().eq_ignore_ascii_case("exo"))
            .unwrap_or(false)
        {
            capabilities.extend([
                TableCapability::PredicatePushdown,
                TableCapability::PartitionPruning,
                TableCapability::GranulePruning,
                TableCapability::AddressNative,
                TableCapability::CollisionVerification,
            ]);
        }

        Self {
            table_id,
            name: table_name,
            catalog_revision,
            snapshot_id,
            schema_version: SchemaVersion::new(1),
            manifest_uri: metadata.location.clone(),
            statistics_uri: None,
            metadata,
            capabilities,
        }
    }

    /// Returns whether a capability is available.
    pub fn supports(&self, capability: &TableCapability) -> bool {
        self.capabilities.contains(capability)
    }
}

/// A table handle embedded in logical and physical plans.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableHandle {
    /// Stable logical table identity.
    pub table_id: TableId,
    /// Catalog revision used to resolve the table.
    pub catalog_revision: CatalogRevision,
    /// Committed snapshot used by the plan.
    pub snapshot_id: SnapshotId,
    /// Schema version used by the plan.
    pub schema_version: SchemaVersion,
}

/// Immutable set of metadata pinned for one query-planning session.
#[derive(Debug, Clone)]
pub struct PlanningSession {
    /// Revision that all resolved tables must belong to.
    pub catalog_revision: CatalogRevision,
    /// Resolved table snapshots keyed by canonical name.
    pub tables: BTreeMap<String, Arc<TableSnapshot>>,
}

impl PlanningSession {
    /// Creates an empty planning session at a catalog revision.
    pub fn new(catalog_revision: CatalogRevision) -> Self {
        Self {
            catalog_revision,
            tables: BTreeMap::new(),
        }
    }

    /// Adds a table snapshot, rejecting a revision mismatch.
    pub fn insert(&mut self, snapshot: Arc<TableSnapshot>) -> Result<(), PlanningSessionError> {
        if snapshot.catalog_revision != self.catalog_revision {
            return Err(PlanningSessionError::RevisionMismatch {
                expected: self.catalog_revision.clone(),
                actual: snapshot.catalog_revision.clone(),
            });
        }

        self.tables.insert(snapshot.name.clone(), snapshot);
        Ok(())
    }

    /// Looks up a table snapshot by canonical name.
    pub fn table(&self, name: &str) -> Option<Arc<TableSnapshot>> {
        self.tables.get(name).cloned()
    }
}

/// Errors raised while constructing a consistent planning session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanningSessionError {
    /// A table came from a different catalog revision.
    RevisionMismatch {
        /// Revision pinned by the session.
        expected: CatalogRevision,
        /// Revision carried by the table snapshot.
        actual: CatalogRevision,
    },
}

impl std::fmt::Display for PlanningSessionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RevisionMismatch { expected, actual } => write!(
                formatter,
                "table metadata revision '{}' does not match planning revision '{}'",
                actual.as_str(),
                expected.as_str()
            ),
        }
    }
}

impl std::error::Error for PlanningSessionError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn planning_session_rejects_mixed_revisions() {
        let mut session = PlanningSession::new(CatalogRevision::new("r1"));
        let metadata = Arc::new(TableMetadata::new(
            "orders",
            Arc::new(plan::VeinSchema::empty()),
        ));

        let snapshot = Arc::new(TableSnapshot {
            table_id: TableId::new("table-1"),
            name: "orders".to_string(),
            catalog_revision: CatalogRevision::new("r2"),
            snapshot_id: SnapshotId::new("snapshot-1"),
            schema_version: SchemaVersion::new(1),
            metadata,
            manifest_uri: None,
            statistics_uri: None,
            capabilities: BTreeSet::new(),
        });

        assert!(matches!(
            session.insert(snapshot),
            Err(PlanningSessionError::RevisionMismatch { .. })
        ));
    }
}
