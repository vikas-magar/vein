//! Stable catalog service contracts used by planning and metadata writers.

use std::sync::Arc;

use async_trait::async_trait;
use error::Result;
use plan::TableMetadata;

use crate::{PlanningSession, TableCatalog};
use distributed::{
    CommitReceipt, TableSnapshot, TableWriteIntent, TableWriteMode, TableWritePart, WriteId,
};

/// Read-only catalog contract used by query planning.
///
/// A reader exposes only revision-pinned metadata resolution. It cannot mutate
/// catalog state, which keeps planning deterministic and separates reads from
/// publication workflows.
#[async_trait]
pub trait CatalogReader: Send + Sync {
    /// Starts a session pinned to one catalog revision.
    async fn begin_planning_session(&self) -> Result<PlanningSession>;

    /// Resolves a table through the supplied pinned session.
    async fn resolve_table(
        &self,
        session: &mut PlanningSession,
        name: &str,
    ) -> Result<Arc<TableMetadata>>;
}

/// Write contract for authoritative catalog publication.
#[async_trait]
pub trait CatalogWriter: CatalogReader {
    /// Publishes a table metadata entry at the current revision.
    async fn register_table(&self, metadata: TableMetadata) -> Result<()>;

    /// Publishes a table metadata entry against an expected revision.
    async fn register_table_at(
        &self,
        expected_revision: &crate::CatalogRevision,
        metadata: TableMetadata,
    ) -> Result<()>;
}

/// Atomic table-generation publication contract.
#[async_trait]
pub trait TableWriteCatalog: CatalogReader {
    /// Creates a durable write intent at the current catalog revision.
    async fn prepare_table_write(
        &self,
        write_id: WriteId,
        table_id: distributed::TableId,
        mode: TableWriteMode,
        expected_parts: u32,
    ) -> Result<TableWriteIntent>;

    /// Publishes one immutable physical part for a prepared generation.
    async fn publish_table_write_part(
        &self,
        write_id: WriteId,
        part: TableWritePart,
    ) -> Result<CommitReceipt>;

    /// Atomically makes a complete generation the visible table snapshot.
    async fn commit_table_write(
        &self,
        write_id: WriteId,
        snapshot: TableSnapshot,
    ) -> Result<CommitReceipt>;

    /// Aborts a prepared generation without changing table visibility.
    async fn abort_table_write(&self, write_id: WriteId) -> Result<CommitReceipt>;
}

/// Adapts an existing catalog backend to the planning read contract.
#[async_trait]
impl<T> CatalogReader for T
where
    T: TableCatalog + ?Sized,
{
    async fn begin_planning_session(&self) -> Result<PlanningSession> {
        TableCatalog::begin_planning_session(self).await
    }

    async fn resolve_table(
        &self,
        session: &mut PlanningSession,
        name: &str,
    ) -> Result<Arc<TableMetadata>> {
        TableCatalog::resolve_table(self, session, name).await
    }
}

/// Adapts an existing catalog backend to the planning write contract.
#[async_trait]
impl<T> CatalogWriter for T
where
    T: TableCatalog + ?Sized,
{
    async fn register_table(&self, metadata: TableMetadata) -> Result<()> {
        TableCatalog::register_table(self, metadata).await
    }

    async fn register_table_at(
        &self,
        expected_revision: &crate::CatalogRevision,
        metadata: TableMetadata,
    ) -> Result<()> {
        TableCatalog::register_table_at(self, expected_revision.clone(), metadata).await
    }
}
