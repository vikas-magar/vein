#![deny(missing_docs)]

//! Vein Catalog Service: versioned metadata and the planner-facing catalog seam.

/// The planner-facing catalog seam and its in-memory implementation.
pub mod catalog;

/// Versioned catalog RPC messages.
pub mod catalog_rpc;

/// Remote catalog client for coordinators.
pub mod catalog_client;

/// Broadcast events sent to Master nodes (snapshot changes, heartbeats).
///
/// `missing_docs` is relaxed on this module only: `rkyv`'s `Archive` derive
/// generates public archived structs/functions that cannot carry source
/// documentation, while the hand-written surface is fully documented.
#[allow(missing_docs)]
pub mod event;

/// Authoritative shuffle exchange metadata.
pub mod exchange;

/// QUIC exchange metadata service.
pub mod rpc;

/// Storage backend configuration and file IO construction.
pub mod storage;

/// Smart source discovery and schema resolution.
pub mod discovery;

/// Stable read and write contracts for the catalog service.
pub mod service;

/// Durable revisioned table metadata state.
mod state;

/// Immutable catalog revisions and planning-session metadata.
pub mod model;

pub use catalog::{InMemoryCatalog, TableCatalog, TableEntry};
pub use catalog_client::RemoteVcsCatalog;
pub use catalog_rpc::{VcsCatalogRequest, VcsCatalogResponse};
pub use discovery::{DiscoveredSource, resolve_source};
pub use exchange::{ExchangeCatalog, InMemoryExchangeCatalog, PersistentExchangeCatalog};
pub use model::{
    CatalogRevision, PlanningSession, PlanningSessionError, SchemaVersion, SnapshotId,
    TableCapability, TableHandle, TableId, TableSnapshot,
};

use crate::model::{schema_fingerprint, stable_table_identity};
pub use rpc::VcsExchangeServer;
pub use service::{CatalogReader, CatalogWriter, TableWriteCatalog};

use std::sync::Arc;

pub use state::{decode_table_metadata, encode_table_metadata};

use async_trait::async_trait;
use distributed::{
    ArtifactId as DistributedArtifactId, ArtifactRef as DistributedArtifactRef,
    CatalogRevision as DistributedCatalogRevision, MetadataCommand, ReplicatedMetadataWriter,
    SnapshotId as DistributedSnapshotId, TableId as DistributedTableId,
    TableSnapshot as DistributedTableSnapshot, TableWriteIntent, TableWriteMode, TableWritePart,
    TransactionId as DistributedTransactionId, WriteId,
};
use error::{Result, VeinError};

/// The Vein Catalog Service.
///
/// This is the authoritative Raft-backed catalog service for table identity, schemas,
/// snapshots, and planner metadata. Iceberg is used only for storage I/O; it is never
/// used to resolve or own visible table metadata.
///
/// # Does NOT
/// - Execute queries
/// - Talk to Executor nodes directly
/// - Know anything about QUIC routing or topology
pub struct VcsCatalog {
    writer: Arc<dyn ReplicatedMetadataWriter>,
    exchange_catalog: Arc<dyn ExchangeCatalog>,
}

#[async_trait]
impl TableCatalog for VcsCatalog {
    async fn get_table(&self, name: &str) -> Result<Arc<plan::TableMetadata>> {
        let view = self
            .writer
            .read_view()
            .await
            .map_err(|error| VeinError::internal(error.to_string()))?;
        Ok(Self::table_snapshot_from_view(&view, name)?.metadata.clone())
    }

    async fn resolve_table_snapshot(
        &self,
        session: &mut crate::PlanningSession,
        name: &str,
    ) -> Result<Arc<crate::model::TableSnapshot>> {
        if let Some(snapshot) = session.table(name) {
            return Ok(snapshot);
        }
        let view = self
            .writer
            .read_view()
            .await
            .map_err(|error| VeinError::internal(error.to_string()))?;
        let actual_revision = CatalogRevision::new(view.revision.value().to_string());
        if actual_revision != session.catalog_revision {
            return Err(VeinError::internal(format!(
                "catalog changed during planning: session revision '{}', current revision '{}'; restart planning",
                session.catalog_revision.as_str(),
                actual_revision.as_str()
            )));
        }
        let snapshot = Self::table_snapshot_from_view(&view, name)?;
        session
            .insert(snapshot.clone())
            .map_err(|error| VeinError::internal(error.to_string()))?;
        Ok(snapshot)
    }

    async fn validate_snapshot(
        &self,
        token: &common::protocol::CatalogSnapshotToken,
    ) -> Result<()> {
        let view = self
            .writer
            .read_view()
            .await
            .map_err(|error| VeinError::internal(error.to_string()))?;
        let record = view
            .table(&DistributedTableId::from(token.table_id.clone()))
            .ok_or_else(|| {
                VeinError::not_found(format!(
                    "Table identity '{}' is not registered in VCS Catalog",
                    token.table_id
                ))
            })?;
        let catalog_revision = view.revision.value().to_string();
        let snapshot_id = record.latest.id.as_str().to_owned();
        let schema_version = record.latest.schema_version;
        if token.snapshot_id != snapshot_id || token.schema_version != schema_version {
            return Err(VeinError::internal(format!(
                "stale table snapshot '{}': token revision '{}' snapshot '{}' schema version '{}', current revision '{}' snapshot '{}' schema version '{}'",
                token.table_id,
                token.catalog_revision,
                token.snapshot_id,
                token.schema_version,
                catalog_revision,
                snapshot_id,
                schema_version
            )));
        }
        Ok(())
    }

    async fn catalog_revision(&self) -> Result<CatalogRevision> {
        self.authoritative_revision().await
    }

    async fn register_table(&self, metadata: plan::TableMetadata) -> Result<()> {
        let expected_revision = self.authoritative_revision().await?;
        <Self as TableCatalog>::register_table_at(self, expected_revision, metadata).await
    }

    async fn register_table_at(
        &self,
        expected_revision: CatalogRevision,
        metadata: plan::TableMetadata,
    ) -> Result<()> {
        let table_name = metadata.name.clone();
        let schema_columns = metadata.schema.fields.len();

        let view = self
            .writer
            .read_view()
            .await
            .map_err(|error| VeinError::internal(error.to_string()))?;
        let table_id = view
            .table_names
            .get(&table_name)
            .cloned()
            .unwrap_or_else(|| DistributedTableId::from(stable_table_identity(&metadata)));
        let fingerprint = schema_fingerprint(&metadata);
        let schema_version = view
            .table(&table_id)
            .map(|record| {
                if record.latest.schema_fingerprint == fingerprint {
                    record.latest.schema_version
                } else {
                    record.latest.schema_version.saturating_add(1)
                }
            })
            .unwrap_or(1);
        let revision = self
            .writer
            .propose(metadata_command(
                &expected_revision,
                &metadata,
                table_id,
                schema_version,
            )?)
            .await
            .map(|receipt| CatalogRevision::new(receipt.revision.value().to_string()))
            .map_err(|error| VeinError::internal(error.to_string()))?;

        tracing::info!(
            target: "vein.audit",
            event = "vcs_table_registered",
            table = %table_name,
            columns = schema_columns,
            catalog_revision = %revision.as_str(),
            "vcs_table_registered"
        );

        Ok(())
    }
}

#[async_trait]
impl TableWriteCatalog for VcsCatalog {
    async fn prepare_table_write(
        &self,
        write_id: WriteId,
        table_id: DistributedTableId,
        mode: TableWriteMode,
        expected_parts: u32,
    ) -> Result<TableWriteIntent> {
        let writer = &self.writer;
        let view = writer
            .read_view()
            .await
            .map_err(|error| VeinError::internal(error.to_string()))?;
        let base_snapshot_id = view.table(&table_id).map(|record| record.latest.id.clone());
        let generation = view
            .writes
            .values()
            .filter(|intent| intent.table_id == table_id)
            .map(|intent| intent.generation)
            .max()
            .unwrap_or(0)
            .saturating_add(1);
        let intent = TableWriteIntent {
            write_id: write_id.clone(),
            table_id,
            mode,
            expected_revision: view.revision,
            generation,
            expected_parts,
            state: distributed::TableWriteState::Prepared,
            parts: Default::default(),
            base_snapshot_id,
        };
        writer
            .propose(MetadataCommand::PrepareTableWrite {
                transaction_id: DistributedTransactionId::from(format!(
                    "write:{}:prepare",
                    write_id.as_str()
                )),
                expected_revision: view.revision,
                intent: intent.clone(),
            })
            .await
            .map_err(|error| VeinError::internal(error.to_string()))?;
        tracing::info!(
            target: "vein.audit",
            event = "vcs_table_write_prepared",
            write_id = %write_id,
            table_id = %intent.table_id,
            mode = ?intent.mode,
            generation = intent.generation,
            expected_parts = intent.expected_parts,
            catalog_revision = %intent.expected_revision,
            "vcs_table_write_prepared"
        );
        Ok(intent)
    }

    async fn publish_table_write_part(
        &self,
        write_id: WriteId,
        part: TableWritePart,
    ) -> Result<distributed::CommitReceipt> {
        let writer = &self.writer;
        let partition_id = part.partition_id;
        let row_count = part.row_count;
        let receipt = writer
            .propose(MetadataCommand::PublishTableWritePart {
                transaction_id: DistributedTransactionId::from(format!(
                    "write:{}:part:{}",
                    write_id.as_str(),
                    partition_id
                )),
                write_id: write_id.clone(),
                part,
            })
            .await
            .map_err(|error| VeinError::internal(error.to_string()))?;
        tracing::info!(
            target: "vein.audit",
            event = "vcs_table_write_part_published",
            write_id = %write_id,
            partition_id,
            rows = row_count,
            catalog_revision = %receipt.revision,
            "vcs_table_write_part_published"
        );
        Ok(receipt)
    }

    async fn commit_table_write(
        &self,
        write_id: WriteId,
        snapshot: DistributedTableSnapshot,
    ) -> Result<distributed::CommitReceipt> {
        let writer = &self.writer;
        let view = writer
            .read_view()
            .await
            .map_err(|error| VeinError::internal(error.to_string()))?;
        let receipt = writer
            .propose(MetadataCommand::CommitTableWrite {
                transaction_id: DistributedTransactionId::from(format!(
                    "write:{}:commit",
                    write_id.as_str()
                )),
                write_id: write_id.clone(),
                expected_revision: view.revision,
                snapshot,
            })
            .await
            .map_err(|error| VeinError::internal(error.to_string()))?;
        tracing::info!(
            target: "vein.audit",
            event = "vcs_table_write_committed",
            write_id = %write_id,
            catalog_revision = %receipt.revision,
            "vcs_table_write_committed"
        );
        Ok(receipt)
    }

    async fn abort_table_write(&self, write_id: WriteId) -> Result<distributed::CommitReceipt> {
        let writer = &self.writer;
        let receipt = writer
            .propose(MetadataCommand::AbortTableWrite {
                transaction_id: DistributedTransactionId::from(format!(
                    "write:{}:abort",
                    write_id.as_str()
                )),
                write_id: write_id.clone(),
            })
            .await
            .map_err(|error| VeinError::internal(error.to_string()))?;
        tracing::warn!(
            target: "vein.audit",
            event = "vcs_table_write_aborted",
            write_id = %write_id,
            catalog_revision = %receipt.revision,
            "vcs_table_write_aborted"
        );
        Ok(receipt)
    }
}

fn physical_manifest(
    metadata: &plan::TableMetadata,
    revision: u64,
) -> Result<DistributedArtifactRef> {
    let uri = metadata
        .location
        .clone()
        .or_else(|| metadata.files.first().cloned())
        .ok_or_else(|| {
            VeinError::internal(format!(
                "table '{}' has no physical location or data files",
                metadata.name
            ))
        })?;
    let descriptor = serde_json::to_vec(&(
        &metadata.location,
        &metadata.format,
        &metadata.files,
        &metadata.partition_columns,
        &metadata.sort_order,
        &metadata.statistics,
    ))
    .map_err(|error| {
        VeinError::serialization(format!(
            "failed to encode physical manifest for table '{}': {error}",
            metadata.name
        ))
    })?;

    Ok(DistributedArtifactRef {
        id: DistributedArtifactId::from(format!("manifest:{}:{}", metadata.name, revision)),
        uri,
        checksum: blake3::hash(&descriptor).to_hex().to_string(),
        size_bytes: metadata
            .statistics
            .as_ref()
            .and_then(|statistics| statistics.total_bytes)
            .unwrap_or(0),
    })
}

fn metadata_command(
    expected_revision: &CatalogRevision,
    metadata: &plan::TableMetadata,
    table_id: DistributedTableId,
    schema_version: u64,
) -> Result<MetadataCommand> {
    let revision = expected_revision
        .as_str()
        .parse::<u64>()
        .map_err(|error| VeinError::internal(format!("invalid catalog revision: {error}")))?;
    let distributed_revision = DistributedCatalogRevision::new(revision);
    let transaction_id =
        DistributedTransactionId::from(format!("table:{}:{}", table_id.as_str(), revision));
    let snapshot_id = DistributedSnapshotId::from(format!("{}@{}", table_id.as_str(), revision));

    Ok(MetadataCommand::PublishTableSnapshot {
        transaction_id,
        table_id,
        expected_revision: distributed_revision,
        snapshot: DistributedTableSnapshot {
            id: snapshot_id,
            table_name: metadata.name.clone(),
            schema_version,
            manifest: physical_manifest(metadata, revision)?,
            schema_fingerprint: schema_fingerprint(metadata),
            metadata_payload: encode_table_metadata(metadata)?,
        },
    })
}

impl VcsCatalog {
    fn table_snapshot_from_view(
        view: &distributed::CatalogView,
        name: &str,
    ) -> Result<Arc<crate::model::TableSnapshot>> {
        let table_id = view.table_names.get(name).ok_or_else(|| {
            VeinError::not_found(format!("Table '{name}' is not registered in VCS Catalog"))
        })?;
        let record = view.table(table_id).ok_or_else(|| {
            VeinError::internal(format!(
                "catalog table-name index for '{name}' references missing table '{table_id}'"
            ))
        })?;
        let metadata = decode_table_metadata(&record.latest.metadata_payload)?;
        if metadata.name != name {
            return Err(VeinError::internal(format!(
                "table metadata name '{}' does not match requested table '{}'",
                metadata.name, name
            )));
        }

        let revision = CatalogRevision::new(view.revision.value().to_string());
        let metadata = metadata.with_catalog_snapshot(common::protocol::CatalogSnapshotToken::new(
            table_id.as_str(),
            revision.as_str(),
            record.latest.id.as_str(),
            record.latest.schema_version,
        ));
        let mut snapshot = crate::model::TableSnapshot::from_metadata(Arc::new(metadata), revision);
        snapshot.table_id = TableId::new(table_id.as_str());
        snapshot.snapshot_id = SnapshotId::new(record.latest.id.as_str());
        snapshot.schema_version = SchemaVersion::new(record.latest.schema_version);
        snapshot.manifest_uri = Some(record.latest.manifest.uri.clone());
        Ok(Arc::new(snapshot))
    }

    async fn authoritative_revision(&self) -> Result<CatalogRevision> {
        let view = self
            .writer
            .read_view()
            .await
            .map_err(|error| VeinError::internal(error.to_string()))?;
        Ok(CatalogRevision::new(view.revision.value().to_string()))
    }

    /// Creates a Raft-backed VCS catalog authority.
    pub async fn new(writer: Arc<dyn ReplicatedMetadataWriter>) -> Result<Self> {
        let exchange_catalog = Arc::new(crate::exchange::ReplicatedExchangeCatalog::new(
            Arc::new(crate::exchange::InMemoryExchangeCatalog::default()),
            writer.clone(),
        ));

        Ok(Self {
            writer,
            exchange_catalog,
        })
    }

    /// Returns the VCS-owned exchange catalog.
    pub fn exchange_catalog(&self) -> &dyn ExchangeCatalog {
        self.exchange_catalog.as_ref()
    }

    /// Returns a shareable handle for the VCS exchange RPC service.
    pub fn shared_exchange_catalog(&self) -> Arc<dyn ExchangeCatalog> {
        self.exchange_catalog.clone()
    }

    /// Creates or reopens an exchange through VCS.
    pub async fn create_exchange(
        &self,
        key: common::protocol::ExchangeKey,
        partition_count: u16,
        expected_tasks: Vec<common::identifiers::TaskId>,
        schema_fingerprint: impl Into<String>,
    ) -> Result<common::protocol::ExchangeManifest> {
        let schema_fingerprint = schema_fingerprint.into();
        let result = self
            .exchange_catalog
            .create_exchange(
                key,
                partition_count,
                expected_tasks,
                schema_fingerprint.clone(),
            )
            .await;
        match &result {
            Ok(manifest) => tracing::info!(
                target: "vein.audit",
                event = "vcs_exchange_created",
                exchange = ?key,
                partitions = partition_count,
                schema_fingerprint = %schema_fingerprint,
                state = ?manifest.state,
                "vcs_exchange_created"
            ),
            Err(error) => tracing::error!(
                target: "vein.audit",
                event = "vcs_exchange_create_failed",
                exchange = ?key,
                error = %error,
                "vcs_exchange_create_failed"
            ),
        }
        result
    }

    /// Publishes one durable exchange fragment through VCS.
    pub async fn publish_exchange_fragment(
        &self,
        key: common::protocol::ExchangeKey,
        fragment: common::protocol::ExchangeFragment,
    ) -> Result<()> {
        let partition_id = fragment.partition_id;
        let task_id = fragment.task_id;
        let attempt = fragment.attempt;
        let row_count = fragment.row_count;
        let byte_len = fragment.byte_len;
        let result = self.exchange_catalog.publish_fragment(key, fragment).await;
        match &result {
            Ok(()) => tracing::info!(
                target: "vein.audit",
                event = "vcs_exchange_fragment_published",
                exchange = ?key,
                partition_id,
                %task_id,
                attempt,
                rows = row_count,
                bytes = byte_len,
                "vcs_exchange_fragment_published"
            ),
            Err(error) => tracing::error!(
                target: "vein.audit",
                event = "vcs_exchange_fragment_publish_failed",
                exchange = ?key,
                partition_id,
                %task_id,
                attempt,
                error = %error,
                "vcs_exchange_fragment_publish_failed"
            ),
        }
        result
    }

    /// Seals an exchange after all registered producers publish output.
    pub async fn seal_exchange(
        &self,
        key: common::protocol::ExchangeKey,
    ) -> Result<common::protocol::ExchangeManifest> {
        let result = self.exchange_catalog.seal_exchange(key).await;
        match &result {
            Ok(manifest) => tracing::info!(
                target: "vein.audit",
                event = "vcs_exchange_sealed",
                exchange = ?key,
                fragments = manifest.fragments.len(),
                "vcs_exchange_sealed"
            ),
            Err(error) => tracing::error!(
                target: "vein.audit",
                event = "vcs_exchange_seal_failed",
                exchange = ?key,
                error = %error,
                "vcs_exchange_seal_failed"
            ),
        }
        result
    }

    /// Reads a sealed exchange manifest through VCS.
    pub async fn get_exchange_manifest(
        &self,
        key: common::protocol::ExchangeKey,
    ) -> Result<common::protocol::ExchangeManifest> {
        self.exchange_catalog.get_manifest(key).await
    }

    // -------------------------------------------------------------------------
    // Catalog Management — used by VCS admin / ingestion pipeline
    // -------------------------------------------------------------------------

    // -------------------------------------------------------------------------
    // Query Planning — called by Master node for every incoming query
    // -------------------------------------------------------------------------

    /// Resolves authoritative table metadata for one planning session.
    ///
    /// Replicated catalogs return the committed snapshot identity and manifest
    /// from the Raft view. The returned snapshot is the only legal input to
    /// physical adapter access.
    pub async fn resolve_table_snapshot(
        &self,
        name: &str,
    ) -> Result<Arc<crate::model::TableSnapshot>> {
        let mut session = crate::catalog::TableCatalog::begin_planning_session(self).await?;
        <Self as TableCatalog>::resolve_table_snapshot(self, &mut session, name).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestMetadataWriter {
        view: std::sync::RwLock<distributed::CatalogView>,
    }

    #[async_trait]
    impl ReplicatedMetadataWriter for TestMetadataWriter {
        async fn propose(
            &self,
            _command: MetadataCommand,
        ) -> distributed::Result<distributed::CommitReceipt> {
            Err(distributed::DistributedError::StorePoisoned)
        }

        async fn read_view(&self) -> distributed::Result<distributed::CatalogView> {
            self.view
                .read()
                .map(|view| view.clone())
                .map_err(|_| distributed::DistributedError::StorePoisoned)
        }
    }

    fn test_catalog_view(revision: u64, names: &[&str]) -> distributed::CatalogView {
        let mut view = distributed::CatalogView {
            revision: distributed::CatalogRevision::new(revision),
            ..Default::default()
        };
        for name in names {
            let metadata = plan::TableMetadata::new(
                *name,
                Arc::new(plan::VeinSchema::empty()),
            );
            let table_id = DistributedTableId::from(format!("table:{name}"));
            let snapshot_id = DistributedSnapshotId::from(format!("snapshot:{name}:{revision}"));
            let snapshot = DistributedTableSnapshot {
                id: snapshot_id.clone(),
                table_name: (*name).to_string(),
                schema_version: 1,
                manifest: DistributedArtifactRef {
                    id: DistributedArtifactId::from(format!("manifest:{name}:{revision}")),
                    uri: format!("vfs://tables/{name}"),
                    checksum: "test-checksum".to_string(),
                    size_bytes: 0,
                },
                schema_fingerprint: "test-schema".to_string(),
                metadata_payload: encode_table_metadata(&metadata).expect("metadata encodes"),
            };
            view.table_names.insert((*name).to_string(), table_id.clone());
            view.tables.insert(
                table_id.clone(),
                distributed::TableRecord {
                    table_id,
                    latest: snapshot,
                },
            );
        }
        view
    }

    #[tokio::test]
    async fn planning_resolution_fails_closed_if_catalog_advances_between_tables() {
        let writer = Arc::new(TestMetadataWriter {
            view: std::sync::RwLock::new(test_catalog_view(1, &["orders", "customers"])),
        });
        let catalog = VcsCatalog {
            writer: writer.clone(),
            exchange_catalog: Arc::new(crate::InMemoryExchangeCatalog::default()),
        };
        let mut session = PlanningSession::new(CatalogRevision::new("1"));

        let orders = TableCatalog::resolve_table_snapshot(&catalog, &mut session, "orders")
            .await
            .expect("table resolves at pinned revision");
        assert_eq!(orders.catalog_revision.as_str(), "1");

        *writer.view.write().expect("test view lock") = test_catalog_view(2, &["orders", "customers"]);
        let error = TableCatalog::resolve_table_snapshot(&catalog, &mut session, "customers")
            .await
            .expect_err("a changed catalog must not mix revisions in one plan");

        assert!(error.to_string().contains("restart planning"));
        assert!(session.table("customers").is_none());
        assert_eq!(session.table("orders").unwrap().catalog_revision.as_str(), "1");
    }

    #[test]
    fn physical_manifest_uses_real_location_and_content_checksum() {
        let metadata = plan::TableMetadata::new("orders", Arc::new(plan::VeinSchema::empty()))
            .with_location("vfs://tables/orders")
            .with_files(vec!["vfs://tables/orders/part-0.exo".to_owned()]);

        let manifest = physical_manifest(&metadata, 7).expect("manifest should be created");
        assert_eq!(manifest.uri, "vfs://tables/orders");
        assert_eq!(manifest.size_bytes, 0);
        assert!(!manifest.checksum.is_empty());
        assert_eq!(manifest.id.as_str(), "manifest:orders:7");
    }

    #[test]
    fn physical_manifest_rejects_metadata_without_physical_artifacts() {
        let metadata = plan::TableMetadata::new("orders", Arc::new(plan::VeinSchema::empty()));

        let error = physical_manifest(&metadata, 1).expect_err("manifest must require an artifact");
        assert!(
            error
                .to_string()
                .contains("no physical location or data files")
        );
    }
}
