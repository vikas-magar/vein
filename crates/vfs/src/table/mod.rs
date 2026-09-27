//! VFS Native Table Subsystem.
//!
//! Owns the complete persistence and scanning lifecycle of Vein native
//! dual-plane (.exo) columnar table partitions directly within VFS
//! memory-mapped blocks, redb extent catalogs, and NVMe cache.
//!
//! Avoids loose table directories by streaming column files through bounded local scratch.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};

mod streaming;
pub use streaming::StreamingPartitionWriter;

use ::types::{VeinSchema, VeinSchemaRef};
use arrow::record_batch::RecordBatch;
use exo::{
    AddressGranuleMark, AddressHeader, PartReader, VaultGranuleMark, VaultHeader,
    classify_storage,
};

use crate::block::{BlockKey, BlockManager, MmapBlockManager, PlaneType};
use crate::metadata::{Extent, MetadataEngine, MetadataFileUpdate, RedbMetadataEngine};
use crate::{Result, VfsConfig, VfsError};

/// Summary returned upon committing a table partition directly into VFS blocks.
#[derive(Debug, Clone)]
pub struct VfsPartitionSummary {
    /// Canonical table identifier.
    pub table_id: u32,
    /// Partition identifier.
    pub part_id: u32,
    /// Canonical logical path prefix for this partition (e.g. "tbl_1/part_000000").
    pub canonical_prefix: String,
    /// Total row count in this partition.
    pub total_rows: u64,
    /// Names of columns persisted.
    pub column_names: Vec<String>,
    /// Physical extents allocated inside VFS blocks.
    pub extents: Vec<Extent>,
}

/// Result of publishing immutable staged partitions into a visible table name.
#[derive(Debug, Clone)]
pub struct VfsTablePublication {
    /// Visible table identifier.
    pub table_id: u32,
    /// Partitions made visible by this publication.
    pub part_ids: Vec<u32>,
    /// Total rows made visible by this publication.
    pub total_rows: u64,
    /// Number of metadata extents aliased into the visible table.
    pub extent_count: usize,
}

/// Core VFS Table Store managing native Dual-Plane columnar table partitions in VFS blocks.
#[derive(Clone)]
pub struct VfsTableStore {
    config: VfsConfig,
    block_manager: Arc<dyn BlockManager>,
    metadata_engine: Arc<dyn MetadataEngine>,
}

/// A bounded reader for append-only exchange frames.
///
/// The reader retains only exchange metadata and the current frame. It never
/// loads the complete logical exchange file into memory.
pub struct ExchangeFrameReader {
    store: VfsTableStore,
    extents: Vec<Extent>,
    logical_len: u64,
    cursor: u64,
}

impl ExchangeFrameReader {
    /// Reads the next complete exchange payload.
    pub async fn next_frame(&mut self) -> Result<Option<Vec<u8>>> {
        if self.cursor == self.logical_len {
            return Ok(None);
        }

        const FRAME_HEADER_LEN: u64 = 16;
        const MAX_FRAME_LEN: u64 = 128 * 1024 * 1024;

        let header = self
            .store
            .read_file_range(&self.extents, self.cursor, FRAME_HEADER_LEN)
            .await?;
        if &header[0..4] != b"VEXF" {
            return Err(VfsError::Internal(format!(
                "Invalid exchange frame header at offset {}",
                self.cursor
            )));
        }

        let version = u32::from_le_bytes(
            header[4..8]
                .try_into()
                .map_err(|_| VfsError::Internal("Invalid exchange frame version".to_string()))?,
        );
        if version != 1 {
            return Err(VfsError::Internal(format!(
                "Unsupported exchange frame version {version}"
            )));
        }

        let payload_len = u64::from_le_bytes(
            header[8..16]
                .try_into()
                .map_err(|_| VfsError::Internal("Invalid exchange frame length".to_string()))?,
        );
        if payload_len > MAX_FRAME_LEN {
            return Err(VfsError::Internal(format!(
                "Exchange frame exceeds bounded payload limit: {payload_len} bytes"
            )));
        }

        let payload_start = self
            .cursor
            .checked_add(FRAME_HEADER_LEN)
            .ok_or_else(|| VfsError::Internal("Exchange frame offset overflow".to_string()))?;
        let payload_end = payload_start
            .checked_add(payload_len)
            .ok_or_else(|| VfsError::Internal("Exchange frame payload overflow".to_string()))?;
        if payload_end > self.logical_len {
            return Err(VfsError::Internal(format!(
                "Truncated exchange frame at offset {}",
                self.cursor
            )));
        }

        let payload = self
            .store
            .read_file_range(&self.extents, payload_start, payload_len)
            .await?;
        self.cursor = payload_end;
        Ok(Some(payload))
    }
}

static SHARED_STORES: OnceLock<Mutex<HashMap<PathBuf, Arc<VfsTableStore>>>> = OnceLock::new();

fn shared_stores() -> &'static Mutex<HashMap<PathBuf, Arc<VfsTableStore>>> {
    SHARED_STORES.get_or_init(|| Mutex::new(HashMap::new()))
}

fn shared_store_key(config: &VfsConfig) -> Result<PathBuf> {
    let path = &config.metadata_db_path;
    if path.is_absolute() {
        Ok(path.clone())
    } else {
        std::env::current_dir()
            .map(|directory| directory.join(path))
            .map_err(|error| VfsError::Internal(format!("resolve VFS metadata path: {error}")))
    }
}

fn configs_match(left: &VfsConfig, right: &VfsConfig) -> bool {
    left.local_cache_dir == right.local_cache_dir
        && left.metadata_db_path == right.metadata_db_path
        && left.block_size_bytes == right.block_size_bytes
        && left.max_ingestion_buffer_bytes == right.max_ingestion_buffer_bytes
        && left.node_ip == right.node_ip
        && left.storage_backend == right.storage_backend
        && left.storage_config == right.storage_config
        && std::mem::discriminant(&left.metadata_hash_algo)
            == std::mem::discriminant(&right.metadata_hash_algo)
}

impl VfsTableStore {
    /// Initializes a new [`VfsTableStore`] from [`VfsConfig`].
    pub async fn init(config: &VfsConfig) -> Result<Self> {
        let metadata_engine = Arc::new(RedbMetadataEngine::init(config).await?);
        let block_manager = Arc::new(MmapBlockManager::init(config).await?);
        Ok(Self {
            config: config.clone(),
            block_manager,
            metadata_engine,
        })
    }

    /// Constructs a [`VfsTableStore`] using existing managers.
    pub fn new(
        config: VfsConfig,
        block_manager: Arc<dyn BlockManager>,
        metadata_engine: Arc<dyn MetadataEngine>,
    ) -> Self {
        Self {
            config,
            block_manager,
            metadata_engine,
        }
    }

    /// Returns the shared VFS store for the environment-derived configuration.
    pub fn shared_or_default() -> Result<Arc<VfsTableStore>> {
        let config = VfsConfig::default();
        let key = shared_store_key(&config)?;
        let stores = shared_stores()
            .lock()
            .map_err(|error| VfsError::Internal(format!("VFS store registry poisoned: {error}")))?;
        if let Some(store) = stores.get(&key) {
            if !configs_match(store.config(), &config) {
                return Err(VfsError::Internal(format!(
                    "conflicting VFS configuration for metadata database '{}'",
                    key.display()
                )));
            }
            return Ok(store.clone());
        }
        if stores.len() == 1 {
            return stores.values().next().cloned().ok_or_else(|| {
                VfsError::Internal("VFS store registry changed unexpectedly".into())
            });
        }
        if stores.len() > 1 {
            return Err(VfsError::Internal(
                "multiple executor VFS stores are registered; select one by configuration".into(),
            ));
        }
        drop(stores);
        Self::shared_for_config(&config)
    }

    /// Returns the process-shared store for this executor configuration.
    ///
    /// Initialization is serialized by metadata path so concurrent workers can
    /// never open duplicate redb handles for one executor's metadata database.
    pub fn shared_for_config(config: &VfsConfig) -> Result<Arc<VfsTableStore>> {
        let key = shared_store_key(config)?;
        let mut stores = shared_stores()
            .lock()
            .map_err(|error| VfsError::Internal(format!("VFS store registry poisoned: {error}")))?;
        if let Some(store) = stores.get(&key) {
            if !configs_match(store.config(), config) {
                return Err(VfsError::Internal(format!(
                    "conflicting VFS configuration for metadata database '{}'",
                    key.display()
                )));
            }
            return Ok(store.clone());
        }

        let config = config.clone();
        let store = Arc::new(Self::run_async(async move { Self::init(&config).await })?);
        stores.insert(key, store.clone());
        Ok(store)
    }

    /// Clears the shared store registry. Intended for isolated test lifecycles.
    pub fn reset_shared() {
        if let Ok(mut stores) = shared_stores().lock() {
            stores.clear();
        }
    }

    /// Helper to safely run an async operation from a synchronous context
    /// across different Tokio runtime configurations.
    pub fn run_async<F, T>(f: F) -> Result<T>
    where
        F: std::future::Future<Output = Result<T>> + Send + 'static,
        T: Send + 'static,
    {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread {
                return tokio::task::block_in_place(|| handle.block_on(f));
            }
            std::thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|e| VfsError::Internal(e.to_string()))?;
                rt.block_on(f)
            })
            .join()
            .map_err(|e| VfsError::Internal(format!("Thread join error: {e:?}")))?
        } else {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| VfsError::Internal(e.to_string()))?;
            rt.block_on(f)
        }
    }

    /// Returns a reference to the active [`VfsConfig`].
    pub fn config(&self) -> &VfsConfig {
        &self.config
    }

    /// Resolves a deterministic `table_id: u32` from any table name, URI, or `tbl_X` alias.
    pub fn resolve_table_id(name: &str) -> u32 {
        let clean = name.trim_start_matches("vfs://").trim_start_matches('/');
        if let Some(num_str) = clean.strip_prefix("tbl_") {
            let id_str = num_str.split('/').next().unwrap_or(num_str);
            if let Ok(id) = id_str.parse::<u32>() {
                return id;
            }
        }
        let hash = blake3::hash(clean.as_bytes());
        let mut bytes = [0u8; 4];
        bytes.copy_from_slice(&hash.as_bytes()[0..4]);
        (u32::from_le_bytes(bytes) & 0x7FFFFFFF).max(1)
    }

    /// Extracts partition ID from a canonical path or URI (e.g. "vfs://tbl_1/part_000002" -> Some(2)).
    pub fn extract_part_id_from_uri(uri: &str) -> Option<u32> {
        let clean = uri.trim_start_matches("vfs://");
        for part in clean.split('/') {
            if let Some(num_str) = part.strip_prefix("part_") {
                if let Ok(id) = num_str.parse::<u32>() {
                    return Some(id);
                }
            }
        }
        None
    }

    /// Writes one EXO partition through the shared incremental writer.
    /// Column files upload in bounded chunks; metadata becomes visible only after
    /// the complete partition has been written.
    pub async fn write_partition(
        &self,
        table_id: u32,
        part_id: u32,
        batch: &RecordBatch,
    ) -> Result<VfsPartitionSummary> {
        let mut writer =
            StreamingPartitionWriter::new(self.clone(), table_id, part_id, batch.schema())?;
        writer.append_batch(batch)?;
        writer.finish().await
    }

    /// Convenience method to write a table partition using a table name string.
    pub async fn write_table(
        &self,
        table_name_or_id: &str,
        part_id: u32,
        batch: &RecordBatch,
    ) -> Result<VfsPartitionSummary> {
        let table_id = Self::resolve_table_id(table_name_or_id);
        self.write_partition(table_id, part_id, batch).await
    }

    /// Reads the authoritative [`VeinSchemaRef`] of a table from its VFS schema manifest.
    pub async fn read_table_schema(&self, table_id: u32) -> Result<VeinSchemaRef> {
        let canonical_path = format!("tbl_{table_id}/_schema.json");
        let extents = self.metadata_engine.get_file(&canonical_path).await?;
        if extents.is_empty() {
            return Err(VfsError::NotFound(canonical_path));
        }

        let mut json_bytes = Vec::new();
        for extent in &extents {
            let chunk = self.block_manager.read_extent(extent).await?;
            json_bytes.extend_from_slice(&chunk);
        }

        let schema: VeinSchema = serde_json::from_slice(&json_bytes)
            .map_err(|e| VfsError::Internal(format!("Failed to parse schema JSON: {e}")))?;
        Ok(Arc::new(schema))
    }

    /// Discovers all partition IDs for a given `table_id` directly from VFS blocks.
    pub async fn resolve_table_parts(&self, table_id: u32) -> Result<Vec<u32>> {
        let parts_canonical = format!("tbl_{table_id}/_partitions.json");
        let extents = match self.metadata_engine.get_file(&parts_canonical).await {
            Ok(extents) => extents,
            Err(VfsError::NotFound(_)) => return Ok(Vec::new()),
            Err(error) => return Err(error),
        };
        if extents.is_empty() {
            return Ok(Vec::new());
        }

        let mut bytes = Vec::new();
        for extent in &extents {
            let chunk = self.block_manager.read_extent(extent).await?;
            bytes.extend_from_slice(&chunk);
        }

        let parts: Vec<u32> = serde_json::from_slice(&bytes)
            .map_err(|e| VfsError::Internal(format!("Failed to parse partition list JSON: {e}")))?;
        Ok(parts)
    }

    /// Loads address-column metadata without reading value granules.
    pub async fn read_address_index(
        &self,
        table_id: u32,
        part_id: u32,
        column_name: &str,
    ) -> Result<(AddressHeader, Vec<AddressGranuleMark>)> {
        let key = BlockKey {
            segment: crate::block::SegmentKey::new(table_id, part_id),
            column_name: column_name.to_string(),
            plane: PlaneType::AddressPlane,
        };
        let extents = self
            .metadata_engine
            .get_file(&key.to_canonical_path())
            .await?;
        let header_bytes = self
            .read_file_range(&extents, 0, exo::addr::HEADER_SIZE as u64)
            .await?;
        let header_array: [u8; exo::addr::HEADER_SIZE] = header_bytes
            .try_into()
            .map_err(|_| VfsError::Internal("Invalid address header length".to_string()))?;
        let header = AddressHeader::decode(&header_array)
            .map_err(|e| VfsError::Internal(format!("Invalid address header: {e}")))?;
        let marks_len = (header.total_granules as usize)
            .checked_mul(exo::addr::MARK_SIZE)
            .ok_or_else(|| VfsError::Internal("Address marks length overflow".to_string()))?;
        let marks_bytes = self
            .read_file_range(&extents, header.marks_offset, marks_len as u64)
            .await?;
        let marks = marks_bytes
            .chunks_exact(exo::addr::MARK_SIZE)
            .map(|chunk| {
                let mark_bytes: [u8; exo::addr::MARK_SIZE] = chunk
                    .try_into()
                    .map_err(|_| VfsError::Internal("Invalid address mark length".to_string()))?;
                Ok(AddressGranuleMark::decode(&mark_bytes))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok((header, marks))
    }

    /// Reads a vault header and its complete granule mark table.
    pub async fn read_vault_index(
        &self,
        table_id: u32,
        part_id: u32,
        column_name: &str,
    ) -> Result<(VaultHeader, Vec<VaultGranuleMark>)> {
        let key = BlockKey {
            segment: crate::block::SegmentKey::new(table_id, part_id),
            column_name: column_name.to_string(),
            plane: PlaneType::PayloadVault,
        };
        let extents = self
            .metadata_engine
            .get_file(&key.to_canonical_path())
            .await?;
        let header_bytes = self
            .read_file_range(&extents, 0, exo::vault::HEADER_SIZE as u64)
            .await?;
        let header_array: [u8; exo::vault::HEADER_SIZE] = header_bytes
            .as_slice()
            .try_into()
            .map_err(|_| VfsError::Internal("Invalid vault header length".to_string()))?;
        let header =
            VaultHeader::decode(&header_array).map_err(|e| VfsError::Internal(e.to_string()))?;
        let marks_len = (header.total_granules as u64)
            .checked_mul(exo::vault::MARK_SIZE as u64)
            .ok_or_else(|| VfsError::Internal("Vault marks length overflow".to_string()))?;
        let marks_bytes = self
            .read_file_range(&extents, header.marks_offset, marks_len)
            .await?;
        let marks = marks_bytes
            .chunks_exact(exo::vault::MARK_SIZE)
            .map(|chunk| {
                let mark_bytes: [u8; exo::vault::MARK_SIZE] = chunk
                    .try_into()
                    .map_err(|_| VfsError::Internal("Invalid vault mark length".to_string()))?;
                Ok(VaultGranuleMark::decode(&mark_bytes))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok((header, marks))
    }

    /// Reads one compressed vault granule using its mark offset and length.
    pub async fn read_vault_granule(
        &self,
        table_id: u32,
        part_id: u32,
        column_name: &str,
        mark: &VaultGranuleMark,
    ) -> Result<Vec<u8>> {
        self.read_partition_plane_range(
            table_id,
            part_id,
            column_name,
            PlaneType::PayloadVault,
            mark.byte_offset,
            mark.compressed_len as u64,
        )
        .await
    }

    /// Returns whether a partition plane has persisted extents.
    pub async fn partition_plane_exists(
        &self,
        table_id: u32,
        part_id: u32,
        column_name: &str,
        plane: PlaneType,
    ) -> Result<bool> {
        let key = BlockKey {
            segment: crate::block::SegmentKey::new(table_id, part_id),
            column_name: column_name.to_string(),
            plane,
        };
        Ok(!self
            .metadata_engine
            .get_file(&key.to_canonical_path())
            .await?
            .is_empty())
    }

    /// Reads a complete partition plane when metadata-only inspection is insufficient.
    pub async fn read_partition_plane_all(
        &self,
        table_id: u32,
        part_id: u32,
        column_name: &str,
        plane: PlaneType,
    ) -> Result<Vec<u8>> {
        let key = BlockKey {
            segment: crate::block::SegmentKey::new(table_id, part_id),
            column_name: column_name.to_string(),
            plane,
        };
        let extents = self
            .metadata_engine
            .get_file(&key.to_canonical_path())
            .await?;
        let total_len: u64 = extents.iter().map(|extent| extent.length).sum();
        self.read_file_range(&extents, 0, total_len).await
    }

    /// Reads one address granule after its index has been loaded.
    pub async fn read_address_granule(
        &self,
        table_id: u32,
        part_id: u32,
        column_name: &str,
        mark: &AddressGranuleMark,
    ) -> Result<Vec<u8>> {
        self.read_partition_plane_range(
            table_id,
            part_id,
            column_name,
            PlaneType::AddressPlane,
            mark.byte_offset,
            mark.compressed_len as u64,
        )
        .await
    }

    /// Opens a bounded reader over an append-only exchange partition.
    pub async fn open_exchange_frame_reader(&self, path: &str) -> Result<ExchangeFrameReader> {
        let extents = match self.metadata_engine.get_file(path).await {
            Ok(extents) => extents,
            Err(VfsError::NotFound(_)) => Vec::new(),
            Err(error) => return Err(error),
        };
        let logical_len = extents.iter().try_fold(0u64, |total, extent| {
            total
                .checked_add(extent.length)
                .ok_or_else(|| VfsError::Internal("Exchange length overflow".to_string()))
        })?;
        Ok(ExchangeFrameReader {
            store: self.clone(),
            extents,
            logical_len,
            cursor: 0,
        })
    }

    /// Appends one self-describing Arrow IPC frame to an exchange partition.
    ///
    /// Exchange partitions are append-only VFS logical files. Each frame carries
    /// its length so independent upstream tasks can append complete IPC streams
    /// without attempting to concatenate Arrow stream headers.
    pub async fn append_exchange_frame(&self, path: &str, payload: &[u8]) -> Result<()> {
        self.append_exchange_frame_owned(path, payload.to_vec())
            .await
    }

    async fn append_exchange_frame_owned(&self, path: &str, mut frame: Vec<u8>) -> Result<()> {
        let payload_len = u64::try_from(frame.len())
            .map_err(|_| VfsError::Internal("Exchange frame is too large".to_string()))?;
        let payload_start = frame.len();
        frame.try_reserve(16).map_err(|error| {
            VfsError::Internal(format!("Unable to reserve exchange frame: {error}"))
        })?;
        frame.resize(payload_start.saturating_add(16), 0);
        frame.copy_within(0..payload_start, 16);
        frame[0..4].copy_from_slice(b"VEXF");
        frame[4..8].copy_from_slice(&1u32.to_le_bytes());
        frame[8..16].copy_from_slice(&payload_len.to_le_bytes());

        let extents = self.block_manager.append(&frame).await?;
        for extent in extents {
            self.metadata_engine.append_extent(path, extent).await?;
        }
        Ok(())
    }

    /// Deletes an exchange or join-spill logical file after successful consumption.
    ///
    /// The metadata entry is removed first; block reclamation remains owned by
    /// the VFS block lifecycle and can safely happen asynchronously.
    pub async fn delete_exchange_path(&self, path: &str) -> Result<()> {
        self.metadata_engine.delete_file(path).await
    }

    /// Reads all complete frames from an exchange partition.
    ///
    /// This compatibility helper uses the bounded frame reader but still
    /// collects the returned payloads for callers that explicitly request a
    /// materialized vector.
    pub async fn read_exchange_frames(&self, path: &str) -> Result<Vec<Vec<u8>>> {
        let mut reader = self.open_exchange_frame_reader(path).await?;
        let mut frames = Vec::new();
        while let Some(frame) = reader.next_frame().await? {
            frames.push(frame);
        }
        Ok(frames)
    }

    /// Synchronously appends an owned exchange payload without cloning its bytes.
    pub fn append_exchange_frame_sync(&self, path: &str, payload: Vec<u8>) -> Result<()> {
        let store = self.clone();
        let path = path.to_string();
        Self::run_async(async move { store.append_exchange_frame_owned(&path, payload).await })
    }

    /// Synchronously deletes an exchange or join-spill logical file.
    pub fn delete_exchange_path_sync(&self, path: &str) -> Result<()> {
        let store = self.clone();
        let path = path.to_string();
        Self::run_async(async move { store.delete_exchange_path(&path).await })
    }

    /// Synchronous wrapper for the exchange frame reader.
    pub fn read_exchange_frames_sync(&self, path: &str) -> Result<Vec<Vec<u8>>> {
        let store = self.clone();
        let path = path.to_string();
        Self::run_async(async move { store.read_exchange_frames(&path).await })
    }

    /// Reads a logical byte range from one partition plane.
    pub async fn read_partition_plane_range(
        &self,
        table_id: u32,
        part_id: u32,
        column_name: &str,
        plane: PlaneType,
        logical_offset: u64,
        length: u64,
    ) -> Result<Vec<u8>> {
        let key = BlockKey {
            segment: crate::block::SegmentKey::new(table_id, part_id),
            column_name: column_name.to_string(),
            plane,
        };
        let extents = self
            .metadata_engine
            .get_file(&key.to_canonical_path())
            .await?;
        self.read_file_range(&extents, logical_offset, length).await
    }

    /// Reads a logical byte range from a file represented by ordered VFS extents.
    /// Extent offsets are physical block offsets; logical ordering is the metadata order.
    pub async fn read_file_range(
        &self,
        extents: &[Extent],
        logical_offset: u64,
        length: u64,
    ) -> Result<Vec<u8>> {
        if length == 0 {
            return Ok(Vec::new());
        }
        let requested_end = logical_offset
            .checked_add(length)
            .ok_or_else(|| VfsError::Internal("Logical file range overflow".to_string()))?;
        let mut output = Vec::with_capacity(length as usize);
        let mut logical_start = 0u64;

        for extent in extents {
            let logical_end = logical_start
                .checked_add(extent.length)
                .ok_or_else(|| VfsError::Internal("Logical extent range overflow".to_string()))?;
            let overlap_start = logical_offset.max(logical_start);
            let overlap_end = requested_end.min(logical_end);
            if overlap_start < overlap_end {
                let extent_offset = overlap_start - logical_start;
                let overlap_len = overlap_end - overlap_start;
                let before = output.len();
                self.block_manager
                    .read_extent_range_into(extent, extent_offset, overlap_len, &mut output)
                    .await?;
                if output.len() - before != overlap_len as usize {
                    return Err(VfsError::Internal(format!(
                        "Short VFS range read: expected {}, got {}",
                        overlap_len,
                        output.len() - before
                    )));
                }
            }
            logical_start = logical_end;
            if logical_start >= requested_end {
                break;
            }
        }

        if output.len() != length as usize {
            return Err(VfsError::Internal(format!(
                "Logical file range is incomplete: expected {}, got {}",
                length,
                output.len()
            )));
        }
        Ok(output)
    }

    /// Opens an [`exo::PartReader`] for zero-copy scanning of a partition directly from VFS blocks.
    pub async fn open_partition_reader(&self, table_id: u32, part_id: u32) -> Result<PartReader> {
        let schema = self.read_table_schema(table_id).await?;
        let mut addr_buffers = std::collections::HashMap::new();
        let mut vault_buffers = std::collections::HashMap::new();

        for field in &schema.fields {
            let col_name = &field.name;
            let storage = classify_storage(&field.data_type.to_arrow()).ok_or_else(|| {
                VfsError::Internal(format!(
                    "EXO has no storage classification for column '{col_name}'"
                ))
            })?;

            // 1. Read address plane bytes from VFS blocks
            let addr_key = BlockKey {
                segment: crate::block::SegmentKey::new(table_id, part_id),
                column_name: col_name.clone(),
                plane: PlaneType::AddressPlane,
            };
            let addr_path = addr_key.to_canonical_path();
            let addr_extents = self.metadata_engine.get_file(&addr_path).await?;
            if addr_extents.is_empty() {
                return Err(VfsError::NotFound(addr_path));
            }
            let mut addr_bytes = Vec::new();
            for extent in &addr_extents {
                let chunk = self.block_manager.read_extent(extent).await?;
                addr_bytes.extend_from_slice(&chunk);
            }
            addr_buffers.insert(col_name.clone(), addr_bytes);

            if storage.uses_vault() {
                let vault_key = BlockKey {
                    segment: crate::block::SegmentKey::new(table_id, part_id),
                    column_name: col_name.clone(),
                    plane: PlaneType::PayloadVault,
                };
                let vault_path = vault_key.to_canonical_path();
                let vault_extents = self.metadata_engine.get_file(&vault_path).await?;
                if vault_extents.is_empty() {
                    return Err(VfsError::NotFound(vault_path));
                }
                let mut vault_bytes = Vec::new();
                for extent in &vault_extents {
                    let chunk = self.block_manager.read_extent(extent).await?;
                    vault_bytes.extend_from_slice(&chunk);
                }
                vault_buffers.insert(col_name.clone(), vault_bytes);
            }
        }

        // 3. Read tombstones (if any)
        let tomb_key = BlockKey {
            segment: crate::block::SegmentKey::new(table_id, part_id),
            column_name: "tombstones".to_string(),
            plane: PlaneType::Tombstone,
        };
        let tomb_path = tomb_key.to_canonical_path();
        let tombstones = match self.metadata_engine.get_file(&tomb_path).await {
            Ok(tomb_extents) if !tomb_extents.is_empty() => {
                let mut tomb_bytes = Vec::new();
                for extent in &tomb_extents {
                    let chunk = self.block_manager.read_extent(extent).await?;
                    tomb_bytes.extend_from_slice(&chunk);
                }
                Some(exo::TombstoneMask::from_bytes(&tomb_bytes)?)
            }
            Ok(_) | Err(VfsError::NotFound(_)) => None,
            Err(error) => return Err(error),
        };

        PartReader::from_buffers(part_id, addr_buffers, vault_buffers, tombstones)
            .map_err(VfsError::from)
    }

    /// Synchronous variant of [`write_table`](Self::write_table).
    pub fn write_table_sync(
        &self,
        table_name_or_id: &str,
        part_id: u32,
        batch: &RecordBatch,
    ) -> Result<VfsPartitionSummary> {
        let store = self.clone();
        let name = table_name_or_id.to_string();
        let batch = batch.clone();
        Self::run_async(async move { store.write_table(&name, part_id, &batch).await })
    }

    /// Appends a new partition to `table_name_or_id`, automatically selecting
    /// the next sequential `part_id` (`max(existing) + 1`, or `0` for a new table).
    ///
    /// This is the preferred write entry-point for all sinks — callers never
    /// need to track or hardcode partition IDs.
    pub async fn append_partition(
        &self,
        table_name_or_id: &str,
        batch: &RecordBatch,
    ) -> Result<VfsPartitionSummary> {
        let table_id = Self::resolve_table_id(table_name_or_id);
        let existing = self.resolve_table_parts(table_id).await?;
        let next_part_id = match existing.iter().max() {
            Some(part_id) => part_id
                .checked_add(1)
                .ok_or_else(|| VfsError::Internal("partition ID overflow".to_string()))?,
            None => 0,
        };
        let started = std::time::Instant::now();
        let result = self.write_partition(table_id, next_part_id, batch).await;
        match &result {
            Ok(summary) => tracing::info!(
                target: "vein.audit",
                event = "vfs_partition_committed",
                table = %table_name_or_id,
                table_id = summary.table_id,
                partition_id = summary.part_id,
                rows = summary.total_rows,
                columns = summary.column_names.len(),
                extents = summary.extents.len(),
                duration_ms = started.elapsed().as_millis() as u64,
                "vfs_partition_committed"
            ),
            Err(error) => tracing::error!(
                target: "vein.audit",
                event = "vfs_partition_commit_failed",
                table = %table_name_or_id,
                table_id,
                partition_id = next_part_id,
                duration_ms = started.elapsed().as_millis() as u64,
                error = %error,
                "vfs_partition_commit_failed"
            ),
        }
        result
    }

    /// Publishes staged table metadata under a visible table name without copying
    /// the underlying immutable block extents.
    pub async fn publish_staged_table(
        &self,
        staging_table: &str,
        visible_table: &str,
        overwrite: bool,
    ) -> Result<VfsTablePublication> {
        let staging_id = Self::resolve_table_id(staging_table);
        let visible_id = Self::resolve_table_id(visible_table);
        let staged_parts = self.resolve_table_parts(staging_id).await?;
        if staged_parts.is_empty() {
            return Err(VfsError::NotFound(format!(
                "staged table '{staging_table}' has no partitions"
            )));
        }

        let schema_path = format!("tbl_{staging_id}/_schema.json");
        let schema_extents = self.metadata_engine.get_file(&schema_path).await?;
        if schema_extents.is_empty() {
            return Err(VfsError::NotFound(schema_path));
        }
        let schema = self.read_table_schema(staging_id).await?;
        let mut visible_parts = if overwrite {
            Vec::new()
        } else {
            self.resolve_table_parts(visible_id).await?
        };
        if !overwrite {
            match self.read_table_schema(visible_id).await {
                Ok(visible_schema) if visible_schema.as_ref() != schema.as_ref() => {
                    return Err(VfsError::Internal(format!(
                        "cannot append staged table '{staging_table}' to '{visible_table}' with a different schema"
                    )));
                }
                Ok(_) | Err(VfsError::NotFound(_)) => {}
                Err(error) => return Err(error),
            }
        }

        let mut destination_ids = Vec::with_capacity(staged_parts.len());
        if overwrite {
            destination_ids.extend(staged_parts.iter().copied());
        } else {
            let first_part_id = match visible_parts.iter().max() {
                Some(part_id) => part_id
                    .checked_add(1)
                    .ok_or_else(|| VfsError::Internal("partition ID overflow".to_string()))?,
                None => 0,
            };
            for offset in 0..staged_parts.len() {
                let offset = u32::try_from(offset)
                    .map_err(|_| VfsError::Internal("partition ID overflow".to_string()))?;
                destination_ids.push(
                    first_part_id
                        .checked_add(offset)
                        .ok_or_else(|| VfsError::Internal("partition ID overflow".to_string()))?,
                );
            }
        }

        let mut total_rows = 0u64;
        for part_id in &staged_parts {
            total_rows = total_rows
                .checked_add(
                    self.open_partition_reader(staging_id, *part_id)
                        .await?
                        .total_rows(),
                )
                .ok_or_else(|| VfsError::Internal("published row count overflow".to_string()))?;
        }

        let mut metadata_updates = vec![MetadataFileUpdate {
            path: format!("tbl_{visible_id}/_schema.json"),
            extents: schema_extents.clone(),
        }];
        let mut aliased_extents = schema_extents.len();
        for (staged_part_id, visible_part_id) in staged_parts.iter().zip(&destination_ids) {
            for field in &schema.fields {
                let storage = classify_storage(&field.data_type.to_arrow()).ok_or_else(|| {
                    VfsError::Internal(format!(
                        "EXO has no storage classification for column '{}'",
                        field.name
                    ))
                })?;
                for plane in [PlaneType::AddressPlane, PlaneType::PayloadVault] {
                    let source = BlockKey {
                        segment: crate::block::SegmentKey::new(staging_id, *staged_part_id),
                        column_name: field.name.clone(),
                        plane,
                    }
                    .to_canonical_path();
                    let destination = BlockKey {
                        segment: crate::block::SegmentKey::new(visible_id, *visible_part_id),
                        column_name: field.name.clone(),
                        plane,
                    }
                    .to_canonical_path();
                    let extents = if plane == PlaneType::PayloadVault && !storage.uses_vault() {
                        Vec::new()
                    } else {
                        let extents = self.metadata_engine.get_file(&source).await?;
                        if extents.is_empty() {
                            return Err(VfsError::NotFound(source));
                        }
                        extents
                    };
                    if !extents.is_empty() {
                        aliased_extents += extents.len();
                    }
                    metadata_updates.push(MetadataFileUpdate {
                        path: destination,
                        extents,
                    });
                }
            }
        }

        visible_parts.extend(destination_ids.iter().copied());
        visible_parts.sort_unstable();
        visible_parts.dedup();
        let parts_json = serde_json::to_vec(&visible_parts).map_err(|error| {
            VfsError::Internal(format!("serialize visible partitions: {error}"))
        })?;
        let parts_extents = self.block_manager.append(&parts_json).await?;
        metadata_updates.push(MetadataFileUpdate {
            path: format!("tbl_{visible_id}/_partitions.json"),
            extents: parts_extents,
        });
        metadata_updates.push(MetadataFileUpdate {
            path: format!("tbl_{visible_id}/_SUCCESS"),
            extents: self.block_manager.append(b"OK").await?,
        });
        self.metadata_engine
            .replace_files_atomically(metadata_updates)
            .await?;

        tracing::info!(
            target: "vein.audit",
            event = "vfs_staged_table_published",
            staging_table = %staging_table,
            visible_table = %visible_table,
            table_id = visible_id,
            partitions = destination_ids.len(),
            rows = total_rows,
            extents = aliased_extents,
            overwrite,
            "vfs_staged_table_published"
        );
        Ok(VfsTablePublication {
            table_id: visible_id,
            part_ids: destination_ids,
            total_rows,
            extent_count: aliased_extents,
        })
    }

    /// Synchronous variant of [`append_partition`](Self::append_partition).
    pub fn append_partition_sync(
        &self,
        table_name_or_id: &str,
        batch: &RecordBatch,
    ) -> Result<VfsPartitionSummary> {
        let store = self.clone();
        let name = table_name_or_id.to_string();
        let batch = batch.clone();
        Self::run_async(async move { store.append_partition(&name, &batch).await })
    }

    /// Synchronous variant of [`read_table_schema`](Self::read_table_schema).
    pub fn read_table_schema_sync(&self, table_id: u32) -> Result<VeinSchemaRef> {
        let store = self.clone();
        Self::run_async(async move { store.read_table_schema(table_id).await })
    }

    /// Synchronous variant of [`resolve_table_parts`](Self::resolve_table_parts).
    pub fn resolve_table_parts_sync(&self, table_id: u32) -> Result<Vec<u32>> {
        let store = self.clone();
        Self::run_async(async move { store.resolve_table_parts(table_id).await })
    }

    /// Synchronous variant of [`open_partition_reader`](Self::open_partition_reader).
    pub fn open_partition_reader_sync(&self, table_id: u32, part_id: u32) -> Result<PartReader> {
        let store = self.clone();
        Self::run_async(async move { store.open_partition_reader(table_id, part_id).await })
    }

    /// Retrieves physical block extents for a canonical logical path.
    pub async fn get_extents(&self, canonical_path: &str) -> Result<Vec<Extent>> {
        self.metadata_engine.get_file(canonical_path).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use tempfile::tempdir;

    fn create_test_config(temp_dir: &std::path::Path) -> VfsConfig {
        let cache_dir = temp_dir.join("cache");
        let metadata_path = temp_dir.join("metadata.redb");
        let infinite_blocks = temp_dir.join("infinite_blocks");
        std::fs::create_dir_all(&cache_dir).unwrap();
        std::fs::create_dir_all(&infinite_blocks).unwrap();

        let mut storage_config = std::collections::HashMap::new();
        storage_config.insert(
            "root".to_string(),
            infinite_blocks.to_string_lossy().to_string(),
        );

        VfsConfig {
            local_cache_dir: cache_dir,
            metadata_db_path: metadata_path,
            block_size_bytes: 4 * 1024 * 1024, // 4MB
            max_ingestion_buffer_bytes: 1024 * 1024,
            node_ip: "127.0.0.1".to_string(),
            storage_backend: "fs".to_string(),
            storage_config,
            metadata_hash_algo: crate::config::HashAlgo::Blake3,
        }
    }

    #[test]
    fn executor_vfs_configs_have_distinct_registry_keys() {
        let dir = tempdir().expect("temporary root");
        let first = VfsConfig::for_executor(dir.path(), "executor-a");
        let second = VfsConfig::for_executor(dir.path(), "executor-b");
        assert_ne!(
            shared_store_key(&first).unwrap(),
            shared_store_key(&second).unwrap()
        );
        assert!(configs_match(&first, &first));
    }

    #[tokio::test]
    async fn shared_store_initialization_is_atomic_per_executor() {
        let dir = tempdir().expect("temporary root");
        let first_config = VfsConfig::for_executor(dir.path(), "executor-a");
        let second_config = VfsConfig::for_executor(dir.path(), "executor-b");
        let first_again_config = first_config.clone();

        let first = tokio::task::spawn_blocking(move || {
            VfsTableStore::shared_for_config(&first_config).expect("initialize first executor")
        });
        let second = tokio::task::spawn_blocking(move || {
            VfsTableStore::shared_for_config(&second_config).expect("initialize second executor")
        });
        let first_again = tokio::task::spawn_blocking(move || {
            VfsTableStore::shared_for_config(&first_again_config)
                .expect("reuse first executor store")
        });

        let first = first.await.expect("first initializer task");
        let second = second.await.expect("second initializer task");
        let first_again = first_again.await.expect("repeated initializer task");
        assert!(Arc::ptr_eq(&first, &first_again));
        assert!(!Arc::ptr_eq(&first, &second));
        assert_ne!(
            first.config().metadata_db_path,
            second.config().metadata_db_path
        );
    }

    #[tokio::test]
    async fn test_vfs_table_store_in_block_roundtrip() -> Result<()> {
        let dir = tempdir()?;
        let config = create_test_config(dir.path());
        let store = VfsTableStore::init(&config).await?;

        // 1. Construct Arrow RecordBatch
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("score", DataType::Float64, false),
        ]));

        let id_array = Arc::new(Int64Array::from(vec![101, 102, 103, 104]));
        let name_array = Arc::new(StringArray::from(vec!["alice", "bob", "charlie", "david"]));
        let score_array = Arc::new(Float64Array::from(vec![98.5, 87.0, 93.2, 99.9]));

        let batch =
            RecordBatch::try_new(schema.clone(), vec![id_array, name_array, score_array]).unwrap();

        let table_id = 42;

        // 2. Persist partition directly into VFS blocks
        let summary = store.write_partition(table_id, 0, &batch).await?;
        assert_eq!(summary.table_id, 42);
        assert_eq!(summary.part_id, 0);
        assert_eq!(summary.total_rows, 4);

        // Verify that NO loose table directory exists on disk!
        let table_dir_candidate = dir.path().join(format!("tbl_{table_id}"));
        assert!(
            !table_dir_candidate.exists(),
            "Table directory should NOT exist on disk!"
        );

        // 3. Read back table schema from VFS blocks
        let read_schema = store.read_table_schema(table_id).await?;
        assert_eq!(read_schema.fields.len(), 3);
        assert_eq!(read_schema.fields[0].name, "id");
        assert_eq!(read_schema.fields[1].name, "name");
        assert_eq!(read_schema.fields[2].name, "score");

        // 4. Resolve partitions from VFS blocks
        let parts = store.resolve_table_parts(table_id).await?;
        assert_eq!(parts, vec![0]);

        // 5. Open partition reader from VFS blocks
        let reader = store.open_partition_reader(table_id, 0).await?;
        assert_eq!(reader.part_id(), 0);
        assert_eq!(reader.total_rows(), 4);

        // Verify primitive slot reading
        let vaddr_101 = reader.read_primitive_slot("id", 0).unwrap();
        assert_eq!(exo::i64_from_canonical(vaddr_101), 101);

        let vaddr_104 = reader.read_primitive_slot("id", 3).unwrap();
        assert_eq!(exo::i64_from_canonical(vaddr_104), 104);

        // Verify payload vault reading
        use exo::traits::VaultReader;
        let mut vault_reader = reader.open_vault_reader("name").unwrap();
        assert_eq!(vault_reader.read_str_slot(0).unwrap(), "alice");
        assert_eq!(vault_reader.read_str_slot(1).unwrap(), "bob");
        assert_eq!(vault_reader.read_str_slot(2).unwrap(), "charlie");
        assert_eq!(vault_reader.read_str_slot(3).unwrap(), "david");

        Ok(())
    }

    #[tokio::test]
    async fn test_vfs_table_store_preserves_nullable_values() -> Result<()> {
        let dir = tempdir()?;
        let config = create_test_config(dir.path());
        let store = VfsTableStore::init(&config).await?;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("enabled", DataType::Boolean, true),
            Field::new("payload", DataType::Binary, true),
        ]));
        let id_array = Arc::new(Int64Array::from(vec![Some(101), None, Some(103)]));
        let enabled_array = Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)]));
        let payload_array = Arc::new(BinaryArray::from(vec![
            Some(b"one".as_ref()),
            None,
            Some(b"three".as_ref()),
        ]));
        let batch =
            RecordBatch::try_new(schema, vec![id_array, enabled_array, payload_array]).unwrap();

        store.write_partition(43, 0, &batch).await?;
        let reader = store.open_partition_reader(43, 0).await?;

        assert_eq!(
            exo::i64_from_canonical(reader.read_primitive_slot("id", 0)?),
            101
        );
        use exo::traits::AddressReader;
        let id_reader = reader.open_address_reader("id")?;
        let enabled_reader = reader.open_address_reader("enabled")?;
        assert!(!id_reader.is_valid(1)?);
        assert!(!enabled_reader.is_valid(1)?);
        assert_eq!(reader.read_primitive_slot("id", 1)?, 0);
        assert_eq!(reader.read_primitive_slot("enabled", 1)?, 0);

        use exo::traits::VaultReader;
        let mut payload_reader = reader.open_vault_reader("payload")?;
        assert_eq!(payload_reader.read_opt_bytes_slot(0)?, Some(&b"one"[..]));
        assert_eq!(payload_reader.read_opt_bytes_slot(1)?, None);
        assert_eq!(payload_reader.read_opt_bytes_slot(2)?, Some(&b"three"[..]));

        Ok(())
    }

    #[tokio::test]
    async fn staged_append_allocates_new_visible_partitions_and_overwrite_replaces_them() -> Result<()> {
        fn int_batch(name: &str, value: i64) -> Result<RecordBatch> {
            let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Int64, false)]));
            let values = Arc::new(Int64Array::from(vec![value]));
            RecordBatch::try_new(schema, vec![values])
                .map_err(|error| VfsError::Internal(error.to_string()))
        }

        let dir = tempdir()?;
        let store = VfsTableStore::init(&create_test_config(dir.path())).await?;
        let visible_id = VfsTableStore::resolve_table_id("append_target");

        let initial_staging = "append_initial_stage";
        store
            .write_partition(
                VfsTableStore::resolve_table_id(initial_staging),
                0,
                &int_batch("id", 10)?,
            )
            .await?;
        store
            .publish_staged_table(initial_staging, "append_target", true)
            .await?;

        let append_staging = "append_next_stage";
        store
            .write_partition(
                VfsTableStore::resolve_table_id(append_staging),
                0,
                &int_batch("id", 20)?,
            )
            .await?;
        let appended = store
            .publish_staged_table(append_staging, "append_target", false)
            .await?;

        assert_eq!(appended.part_ids, vec![1]);
        assert_eq!(store.resolve_table_parts(visible_id).await?, vec![0, 1]);
        assert_eq!(
            exo::i64_from_canonical(
                store.open_partition_reader(visible_id, 0).await?.read_primitive_slot("id", 0)?
            ),
            10
        );
        assert_eq!(
            exo::i64_from_canonical(
                store.open_partition_reader(visible_id, 1).await?.read_primitive_slot("id", 0)?
            ),
            20
        );

        let incompatible_staging = "append_incompatible_stage";
        store
            .write_partition(
                VfsTableStore::resolve_table_id(incompatible_staging),
                0,
                &int_batch("other", 30)?,
            )
            .await?;
        assert!(store
            .publish_staged_table(incompatible_staging, "append_target", false)
            .await
            .is_err());
        assert_eq!(store.resolve_table_parts(visible_id).await?, vec![0, 1]);

        let replacement_staging = "append_replacement_stage";
        store
            .write_partition(
                VfsTableStore::resolve_table_id(replacement_staging),
                0,
                &int_batch("id", 40)?,
            )
            .await?;
        let replacement = store
            .publish_staged_table(replacement_staging, "append_target", true)
            .await?;
        assert_eq!(replacement.part_ids, vec![0]);
        assert_eq!(store.resolve_table_parts(visible_id).await?, vec![0]);
        assert_eq!(
            exo::i64_from_canonical(
                store.open_partition_reader(visible_id, 0).await?.read_primitive_slot("id", 0)?
            ),
            40
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_exchange_frames_roundtrip() -> Result<()> {
        let dir = tempdir()?;
        let config = create_test_config(dir.path());
        let store = VfsTableStore::init(&config).await?;
        let path = "shuffle/test/partition_00000";

        store.append_exchange_frame(path, b"first-frame").await?;
        store.append_exchange_frame(path, b"second-frame").await?;

        let frames = store.read_exchange_frames(path).await?;
        assert_eq!(
            frames,
            vec![b"first-frame".to_vec(), b"second-frame".to_vec()]
        );
        Ok(())
    }
}
