#![deny(missing_docs)]

//! Vein Catalog Service: versioned metadata, scan planning, and the
//! planner-facing catalog seam.

/// The planner-facing catalog seam and its in-memory implementation.
pub mod catalog;

/// Versioned catalog RPC messages.
pub mod catalog_rpc;

/// Remote catalog client for coordinators.
pub mod catalog_client;

/// Hot table metadata cache.
pub mod cache;

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

/// Cache of pre-computed scan plans for warm query replays.
pub mod plan_cache;

/// Scan planning: translates predicates into file scan tasks.
pub mod scan;

/// Storage backend configuration and file IO construction.
pub mod storage;

/// Smart source discovery and schema resolution.
pub mod discovery;

/// Stable read and write contracts for the catalog service.
pub mod service;

/// Durable revisioned table metadata state.
pub mod state;

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
pub use rpc::VcsExchangeServer;
pub use service::{CatalogReader, CatalogWriter, TableWriteCatalog};
pub use state::PersistentCatalogState;

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use distributed::{
    ArtifactId as DistributedArtifactId, ArtifactRef as DistributedArtifactRef,
    CatalogRevision as DistributedCatalogRevision, MetadataCommand, ReplicatedMetadataWriter,
    SnapshotId as DistributedSnapshotId, TableId as DistributedTableId,
    TableSnapshot as DistributedTableSnapshot, TableWriteIntent, TableWriteMode, TableWritePart,
    TransactionId as DistributedTransactionId, WriteId,
};
use error::{Result, VeinError};
use iceberg::expr::Predicate;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};

use crate::cache::TableCache;
use crate::plan_cache::{ScanPlan, ScanPlanCache};
use crate::scan::plan_scan;
use crate::state::{decode_table_metadata, encode_table_metadata};
use crate::storage::{StorageConfig, build_file_io};

/// The Vein Catalog Service.
///
/// This is the authoritative catalog service for table identity, schemas, snapshots,
/// and planner metadata. Iceberg is retained only as a format-specific physical
/// adapter for manifest and file-scan operations; it is never used to resolve the
/// visible table metadata.
///
/// # Does NOT
/// - Execute queries
/// - Talk to Executor nodes directly
/// - Know anything about QUIC routing or topology
enum CatalogBackend {
    Local,
    Replicated(Arc<dyn ReplicatedMetadataWriter>),
}

/// VCS catalog service state and authority backend.
pub struct VcsCatalog {
    /// Iceberg adapter used only for format-specific manifest/scan operations.
    /// VCS-owned metadata remains authoritative for table identity and schema.
    iceberg_adapter: Arc<dyn Catalog>,
    table_cache: TableCache,
    plan_cache: ScanPlanCache,
    catalog_state: Option<Arc<PersistentCatalogState>>,
    exchange_catalog: Arc<dyn ExchangeCatalog>,
    backend: CatalogBackend,
}

#[async_trait]
impl TableCatalog for VcsCatalog {
    async fn get_table(&self, name: &str) -> Result<Arc<plan::TableMetadata>> {
        // A concrete path is an external source, not a logical VCS table name.
        // Resolve its on-disk format before consulting any stale catalog entry.
        if Path::new(name).exists() {
            let discovered = crate::discovery::resolve_source(name).await?;
            return Ok(Arc::new(
                plan::TableMetadata::new(discovered.name, discovered.schema)
                    .with_location(discovered.location)
                    .with_format(plan::TableFormat::from(discovered.format.as_str()))
                    .with_files(discovered.files),
            ));
        }

        if let CatalogBackend::Replicated(writer) = &self.backend {
            let view = writer
                .read_view()
                .await
                .map_err(|error| VeinError::internal(error.to_string()))?;
            let table_id = DistributedTableId::from(name.to_owned());
            let record = view.table(&table_id).ok_or_else(|| {
                VeinError::not_found(format!("Table '{name}' is not registered in VCS Catalog"))
            })?;
            let metadata = decode_table_metadata(&record.latest.metadata_payload)?;
            if metadata.name != name {
                return Err(VeinError::internal(format!(
                    "table metadata name '{}' does not match requested table '{}'",
                    metadata.name, name
                )));
            }
            let metadata = metadata.with_catalog_snapshot(
                common::protocol::CatalogSnapshotToken::new(
                    table_id.as_str(),
                    view.revision.value().to_string(),
                    record.latest.id.as_str(),
                    1,
                ),
            );
            return Ok(Arc::new(metadata));
        }

        let catalog_state = self.catalog_state.as_ref().ok_or_else(|| {
            VeinError::internal("replicated catalog lookup did not resolve from the Raft view")
        })?;
        if let Some(metadata) = catalog_state.get(name).await {
            let revision = catalog_state.revision().await;
            let snapshot_id = format!("{}:{}", revision.as_str(), metadata.name);
            let metadata = (*metadata).clone().with_catalog_snapshot(
                common::protocol::CatalogSnapshotToken::new(
                    metadata.name.clone(),
                    revision.as_str(),
                    snapshot_id,
                    1,
                ),
            );
            return Ok(Arc::new(metadata));
        }

        let discovered = crate::discovery::resolve_source(name).await?;
        let metadata = plan::TableMetadata::new(discovered.name.clone(), discovered.schema)
            .with_location(discovered.location)
            .with_format(plan::TableFormat::from(discovered.format.as_str()))
            .with_files(discovered.files);
        let expected_revision = self.authoritative_revision().await?;
        <Self as TableCatalog>::register_table_at(self, expected_revision, metadata.clone())
            .await?;
        let revision = self.authoritative_revision().await?;

        tracing::info!(
            target: "vein.audit",
            event = "vcs_table_discovered",
            table = %name,
            format = ?metadata.format,
            catalog_revision = %revision.as_str(),
            "vcs_table_discovered"
        );

        Ok(Arc::new(metadata))
    }

    async fn resolve_table_snapshot(
        &self,
        session: &mut crate::PlanningSession,
        name: &str,
    ) -> Result<Arc<crate::model::TableSnapshot>> {
        if let Some(snapshot) = session.table(name) {
            return Ok(snapshot);
        }
        let snapshot = VcsCatalog::resolve_table_snapshot(self, name).await?;
        session
            .insert(snapshot.clone())
            .map_err(|error| VeinError::internal(error.to_string()))?;
        Ok(snapshot)
    }

    async fn validate_snapshot(
        &self,
        token: &common::protocol::CatalogSnapshotToken,
    ) -> Result<()> {
        let snapshot = self.resolve_table_snapshot(&token.table_id).await?;
        if token.catalog_revision != snapshot.catalog_revision.as_str()
            || token.snapshot_id != snapshot.snapshot_id.as_str()
            || token.schema_version != snapshot.schema_version.value()
        {
            return Err(VeinError::internal(format!(
                "stale table snapshot '{}': token revision '{}' snapshot '{}', current revision '{}' snapshot '{}'",
                token.table_id,
                token.catalog_revision,
                token.snapshot_id,
                snapshot.catalog_revision.as_str(),
                snapshot.snapshot_id.as_str()
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

        let revision = match &self.backend {
            CatalogBackend::Replicated(writer) => writer
                .propose(metadata_command(&expected_revision, &metadata)?)
                .await
                .map(|receipt| CatalogRevision::new(receipt.revision.value().to_string()))
                .map_err(|error| VeinError::internal(error.to_string()))?,
            CatalogBackend::Local => {
                let catalog_state = self.catalog_state.as_ref().ok_or_else(|| {
                    VeinError::internal("local catalog state is not initialized")
                })?;
                PersistentCatalogState::commit_table(
                    catalog_state.as_ref(),
                    &expected_revision,
                    &metadata,
                )
                .await?
            }
        };

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
        let writer = self.replicated_writer()?;
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
        let writer = self.replicated_writer()?;
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
        let writer = self.replicated_writer()?;
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
        let writer = self.replicated_writer()?;
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

fn metadata_command(
    expected_revision: &CatalogRevision,
    metadata: &plan::TableMetadata,
) -> Result<MetadataCommand> {
    let revision = expected_revision
        .as_str()
        .parse::<u64>()
        .map_err(|error| VeinError::internal(format!("invalid catalog revision: {error}")))?;
    let distributed_revision = DistributedCatalogRevision::new(revision);
    let transaction_id =
        DistributedTransactionId::from(format!("table:{}:{}", metadata.name, revision));
    let snapshot_id = DistributedSnapshotId::from(format!("{}@{}", metadata.name, revision));
    let schema_fingerprint = metadata
        .schema
        .fields
        .iter()
        .map(|field| format!("{}:{:?}", field.name, field.data_type))
        .collect::<Vec<_>>()
        .join("|");

    Ok(MetadataCommand::PublishTableSnapshot {
        transaction_id,
        table_id: DistributedTableId::from(metadata.name.clone()),
        expected_revision: distributed_revision,
        snapshot: DistributedTableSnapshot {
            id: snapshot_id.clone(),
            manifest: DistributedArtifactRef {
                id: DistributedArtifactId::from(format!("catalog-{}", metadata.name)),
                uri: format!("catalog://tables/{}", metadata.name),
                checksum: schema_fingerprint.clone(),
                size_bytes: 0,
            },
            schema_fingerprint,
            metadata_payload: encode_table_metadata(metadata)?,
        },
    })
}

fn validate_adapter_request(
    snapshot: &crate::model::TableSnapshot,
    ident: &TableIdent,
) -> Result<()> {
    if snapshot.name != ident.name() {
        return Err(VeinError::internal(format!(
            "physical adapter table '{}' does not match VCS snapshot '{}'",
            ident.name(),
            snapshot.name
        )));
    }
    if snapshot.metadata.format != Some(plan::TableFormat::Iceberg) {
        return Err(VeinError::internal(format!(
            "table '{}' is not registered with the Iceberg physical adapter",
            snapshot.name
        )));
    }
    Ok(())
}

fn table_state_path(warehouse: &str) -> String {
    format!(
        "{}/_vein/catalog/tables.json",
        warehouse.trim_end_matches('/')
    )
}

fn exchange_manifest_path(warehouse: &str) -> String {
    format!(
        "{}/_vein/exchanges/manifests.json",
        warehouse.trim_end_matches('/')
    )
}

impl VcsCatalog {
    async fn authoritative_revision(&self) -> Result<CatalogRevision> {
        match &self.backend {
            CatalogBackend::Local => Ok(self
                .catalog_state
                .as_ref()
                .ok_or_else(|| VeinError::internal("local catalog state is not initialized"))?
                .revision()
                .await),
            CatalogBackend::Replicated(writer) => {
                let view = writer
                    .read_view()
                    .await
                    .map_err(|error| VeinError::internal(error.to_string()))?;
                Ok(CatalogRevision::new(view.revision.value().to_string()))
            }
        }
    }

    fn replicated_writer(&self) -> Result<&Arc<dyn ReplicatedMetadataWriter>> {
        match &self.backend {
            CatalogBackend::Replicated(writer) => Ok(writer),
            CatalogBackend::Local => Err(VeinError::internal(
                "replicated metadata writer is required for authoritative table writes",
            )),
        }
    }

    /// Create a new VcsCatalog with the given storage configuration.
    ///
    /// # Arguments
    /// - `catalog_name`: A name for this catalog (e.g. "vein-prod")
    /// - `warehouse_location`: The root path where Iceberg table metadata lives
    /// - `storage`: Which storage backend to use (local FS or S3)
    pub async fn new(
        catalog_name: impl Into<String>,
        warehouse_location: impl Into<String>,
        storage: StorageConfig,
    ) -> Result<Self> {
        let file_io = build_file_io(&storage)?;
        let warehouse = warehouse_location.into();
        let exchange_manifest_path = exchange_manifest_path(&warehouse);
        let exchange_catalog =
            PersistentExchangeCatalog::open(file_io.clone(), exchange_manifest_path).await?;
        Self::build(
            catalog_name,
            warehouse,
            storage,
            file_io,
            Arc::new(exchange_catalog),
            CatalogBackend::Local,
        )
        .await
    }

    /// Creates a VCS catalog whose exchange metadata is replicated through Raft.
    pub async fn new_with_metadata_writer(
        catalog_name: impl Into<String>,
        warehouse_location: impl Into<String>,
        storage: StorageConfig,
        writer: Arc<dyn ReplicatedMetadataWriter>,
    ) -> Result<Self> {
        let file_io = build_file_io(&storage)?;
        let warehouse = warehouse_location.into();
        let exchange_catalog = Arc::new(crate::exchange::ReplicatedExchangeCatalog::new(
            Arc::new(crate::exchange::InMemoryExchangeCatalog::default()),
            writer.clone(),
        ));
        Self::build(
            catalog_name,
            warehouse,
            storage,
            file_io,
            exchange_catalog,
            CatalogBackend::Replicated(writer),
        )
        .await
    }

    async fn build(
        catalog_name: impl Into<String>,
        warehouse: String,
        storage: StorageConfig,
        file_io: iceberg::io::FileIO,
        exchange_catalog: Arc<dyn ExchangeCatalog>,
        backend: CatalogBackend,
    ) -> Result<Self> {
        let catalog_state = match &backend {
            CatalogBackend::Local => Some(
                PersistentCatalogState::open(file_io.clone(), table_state_path(&warehouse)).await?,
            ),
            CatalogBackend::Replicated(_) => None,
        };

        let factory: Arc<dyn iceberg::io::StorageFactory> = match &storage {
            StorageConfig::LocalFs => Arc::new(iceberg::io::LocalFsStorageFactory),
            StorageConfig::S3 { .. } => {
                Arc::new(iceberg_storage_opendal::OpenDalStorageFactory::S3 {
                    customized_credential_load: None,
                })
            }
        };

        let catalog = MemoryCatalogBuilder::default()
            .with_storage_factory(factory)
            .load(
                catalog_name,
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse)]),
            )
            .await
            .map_err(VeinError::Iceberg)?;

        Ok(Self {
            iceberg_adapter: Arc::new(catalog),
            table_cache: TableCache::new(),
            plan_cache: ScanPlanCache::new(),
            catalog_state,
            exchange_catalog,
            backend,
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
        let result = self
            .exchange_catalog
            .publish_fragment(key, fragment)
            .await;
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

    /// Register a new namespace in the catalog.
    pub async fn create_namespace(&self, ns: &NamespaceIdent) -> Result<()> {
        self.iceberg_adapter
            .create_namespace(ns, HashMap::new())
            .await
            .map_err(VeinError::Iceberg)?;
        Ok(())
    }

    /// Register a new table in the catalog.
    pub async fn create_table(&self, ns: &NamespaceIdent, creation: TableCreation) -> Result<()> {
        self.iceberg_adapter
            .create_table(ns, creation)
            .await
            .map_err(VeinError::Iceberg)?;
        Ok(())
    }

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
        let session = crate::catalog::TableCatalog::begin_planning_session(self).await?;
        let metadata = <Self as crate::catalog::TableCatalog>::get_table(self, name).await?;
        let mut snapshot = crate::model::TableSnapshot::from_metadata(
            metadata.clone(),
            session.catalog_revision.clone(),
        );

        if let CatalogBackend::Replicated(writer) = &self.backend {
            let view = writer
                .read_view()
                .await
                .map_err(|error| VeinError::internal(error.to_string()))?;
            let record = view
                .table(&DistributedTableId::from(name.to_owned()))
                .ok_or_else(|| {
                    VeinError::not_found(format!("Table '{name}' is not registered in VCS Catalog"))
                })?;
            let metadata = if Path::new(name).exists() {
                metadata.as_ref().clone()
            } else {
                decode_table_metadata(&record.latest.metadata_payload)?
            };
            if metadata.name != name {
                return Err(VeinError::internal(format!(
                    "table snapshot metadata name '{}' does not match '{}'",
                    metadata.name, name
                )));
            }
            let metadata = metadata.with_catalog_snapshot(
                common::protocol::CatalogSnapshotToken::new(
                    name,
                    view.revision.value().to_string(),
                    record.latest.id.as_str(),
                    1,
                ),
            );
            snapshot.snapshot_id = SnapshotId::new(record.latest.id.as_str());
            snapshot.manifest_uri = Some(record.latest.manifest.uri.clone());
            snapshot.metadata = Arc::new(metadata);
        }

        Ok(Arc::new(snapshot))
    }

    async fn validate_snapshot_current(
        &self,
        snapshot: &crate::model::TableSnapshot,
    ) -> Result<()> {
        match &self.backend {
            CatalogBackend::Local => {
                let actual = self.authoritative_revision().await?;
                if actual != snapshot.catalog_revision {
                    return Err(VeinError::internal(format!(
                        "stale table snapshot '{}': expected catalog revision '{}', actual '{}'",
                        snapshot.name,
                        snapshot.catalog_revision.as_str(),
                        actual.as_str()
                    )));
                }
            }
            CatalogBackend::Replicated(writer) => {
                let view = writer
                    .read_view()
                    .await
                    .map_err(|error| VeinError::internal(error.to_string()))?;
                let actual_revision = CatalogRevision::new(view.revision.value().to_string());
                let record = view
                    .table(&DistributedTableId::from(snapshot.name.clone()))
                    .ok_or_else(|| {
                        VeinError::not_found(format!(
                            "Table '{}' is not registered in VCS Catalog",
                            snapshot.name
                        ))
                    })?;
                if actual_revision != snapshot.catalog_revision
                    || record.latest.id.as_str() != snapshot.snapshot_id.as_str()
                {
                    return Err(VeinError::internal(format!(
                        "stale table snapshot '{}': planned revision '{}' and snapshot '{}', actual revision '{}' and snapshot '{}'",
                        snapshot.name,
                        snapshot.catalog_revision.as_str(),
                        snapshot.snapshot_id.as_str(),
                        actual_revision.as_str(),
                        record.latest.id
                    )));
                }
            }
        }
        Ok(())
    }

    /// Loads a physical table through the adapter using VCS-pinned metadata.
    pub async fn load_table(
        &self,
        snapshot: &crate::model::TableSnapshot,
        ident: &TableIdent,
    ) -> Result<iceberg::table::Table> {
        self.validate_snapshot_current(snapshot).await?;
        validate_adapter_request(snapshot, ident)?;
        let table = self
            .table_cache
            .get_or_load(ident, snapshot, &*self.iceberg_adapter)
            .await?;
        Ok((*table).clone())
    }

    /// Resolves a scan plan for VCS-pinned metadata and an optional predicate.
    pub async fn resolve_scan(
        &self,
        snapshot: &crate::model::TableSnapshot,
        ident: &TableIdent,
        columns: Option<Vec<String>>,
        predicate: Option<Predicate>,
    ) -> Result<ScanPlan> {
        self.validate_snapshot_current(snapshot).await?;
        validate_adapter_request(snapshot, ident)?;
        let table = self
            .table_cache
            .get_or_load(ident, snapshot, &*self.iceberg_adapter)
            .await?;
        plan_scan(&table, columns, predicate, &self.plan_cache).await
    }

    // -------------------------------------------------------------------------
    // Cache Invalidation — called when new data is ingested into a table
    // -------------------------------------------------------------------------

    /// Invalidate all cached state for a table.
    ///
    /// Call this when a new Iceberg snapshot is committed to the table.
    /// After this call, the next `resolve_scan()` will re-read metadata from storage.
    pub async fn invalidate_table(&self, ident: &TableIdent) {
        self.table_cache.invalidate(ident).await;
        self.plan_cache
            .invalidate_table(ident.namespace().as_ref(), ident.name())
            .await;
    }
}
