use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{ArtifactRef, CatalogRevision, SnapshotId, TableId, WriteId};

/// Visibility policy for one table write generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum TableWriteMode {
    /// Replace the table snapshot when the generation commits.
    Overwrite,
    /// Publish the generation as an append to the current table snapshot.
    Append,
}

/// Lifecycle state of a catalog-owned table write.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum TableWriteState {
    /// The generation accepts immutable part descriptors.
    Prepared,
    /// The generation was atomically published as the table snapshot.
    Committed,
    /// The generation can never become visible.
    Aborted,
}

/// One immutable physical part produced by an executor for a write generation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TableWritePart {
    /// Stable partition identity within the generation.
    pub partition_id: u32,
    /// Immutable manifest or physical-data descriptor.
    pub artifact: ArtifactRef,
    /// Rows contained by this part.
    pub row_count: u64,
}

/// Durable intent and publication state for one table write.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TableWriteIntent {
    /// Stable write-generation identity.
    pub write_id: WriteId,
    /// Logical table receiving the generation.
    pub table_id: TableId,
    /// Visibility policy selected by the planner.
    pub mode: TableWriteMode,
    /// Catalog revision observed before the generation was prepared.
    pub expected_revision: CatalogRevision,
    /// Monotonic generation allocated by the catalog.
    pub generation: u64,
    /// Number of parts required before commit is legal.
    pub expected_parts: u32,
    /// Current lifecycle state.
    pub state: TableWriteState,
    /// Published immutable parts keyed by partition identity.
    pub parts: BTreeMap<u32, TableWritePart>,
    /// Snapshot replaced by this generation, when one existed at prepare time.
    pub base_snapshot_id: Option<SnapshotId>,
}

impl TableWriteIntent {
    /// Returns whether all expected immutable parts have been published.
    pub fn is_complete(&self) -> bool {
        self.parts.len() == self.expected_parts as usize
            && self
                .parts
                .keys()
                .copied()
                .eq(0..self.expected_parts)
    }
}
