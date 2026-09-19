//! VFS Native Table Subsystem.
//!
//! Owns the complete persistence and scanning lifecycle of Vein native
//! dual-plane (.exo) columnar table partitions directly within VFS
//! memory-mapped blocks, redb extent catalogs, and NVMe cache.
//!
//! Eliminates all temporary filesystem directories and loose table files.

use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use exo::{PartReader, PartWriter};
use ::types::{VeinSchema, VeinSchemaRef};

use crate::block::{BlockKey, BlockManager, MmapBlockManager, PlaneType};
use crate::metadata::{Extent, MetadataEngine, RedbMetadataEngine};
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

/// Core VFS Table Store managing native Dual-Plane columnar table partitions in VFS blocks.
#[derive(Clone)]
pub struct VfsTableStore {
    config: VfsConfig,
    block_manager: Arc<dyn BlockManager>,
    metadata_engine: Arc<dyn MetadataEngine>,
}

static SHARED_STORE: std::sync::RwLock<Option<Arc<VfsTableStore>>> = std::sync::RwLock::new(None);

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

    /// Returns the global shared [`VfsTableStore`] if already initialized, or
    /// lazily initializes one using [`VfsConfig::from_env()`].
    ///
    /// Configuration is driven entirely by environment variables:
    ///
    /// | Variable             | Default       | Description                  |
    /// |----------------------|---------------|------------------------------|
    /// | `VEIN_DATA_DIR`      | `/tmp/veinfs` | Root for all VFS data        |
    /// | `VEIN_BLOCK_SIZE_MB` | `5120` (5 GB) | Block file size in MiB       |
    /// | `VEIN_NODE_IP`       | `127.0.0.1`   | Node IP for data locality    |
    ///
    /// Call [`VfsTableStore::set_shared`] before any queries to override with a
    /// fully configured store (e.g. in tests or when the server reads from a
    /// config file).
    pub fn shared_or_default() -> Result<Arc<VfsTableStore>> {
        {
            if let Ok(guard) = SHARED_STORE.read() {
                if let Some(store) = &*guard {
                    return Ok(store.clone());
                }
            }
        }

        // VfsConfig::default() delegates to VfsConfig::from_env(), so this
        // automatically picks up VEIN_DATA_DIR, VEIN_BLOCK_SIZE_MB, VEIN_NODE_IP.
        let store = Self::run_async(async {
            let config = VfsConfig::default();
            VfsTableStore::init(&config).await
        })?;
        let store_arc = Arc::new(store);

        let mut guard = SHARED_STORE
            .write()
            .map_err(|e| VfsError::Internal(format!("Failed to acquire SHARED_STORE lock: {e}")))?;
        if let Some(existing) = &*guard {
            return Ok(existing.clone());
        }
        *guard = Some(store_arc.clone());
        Ok(store_arc)
    }

    /// Explicitly sets the shared global [`VfsTableStore`].
    pub fn set_shared(store: Arc<VfsTableStore>) {
        if let Ok(mut guard) = SHARED_STORE.write() {
            *guard = Some(store);
        }
    }

    /// Resets the shared global [`VfsTableStore`].
    pub fn reset_shared() {
        if let Ok(mut guard) = SHARED_STORE.write() {
            *guard = None;
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
        let clean = name
            .trim_start_matches("vfs://")
            .trim_start_matches('/');
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

    /// Persists an Arrow [`RecordBatch`] as a native Dual-Plane table partition directly into VFS blocks.
    /// Zero filesystem scratch directories or loose files are created.
    pub async fn write_partition(
        &self,
        table_id: u32,
        part_id: u32,
        batch: &RecordBatch,
    ) -> Result<VfsPartitionSummary> {
        let mut part_writer = PartWriter::create_in_memory(part_id, 8192);

        let schema = batch.schema();
        for (i, field) in schema.fields().iter().enumerate() {
            let col_name = field.name();
            let col_array = batch.column(i);

            match col_array.data_type() {
                DataType::Int8 => {
                    let arr = col_array.as_any().downcast_ref::<Int8Array>().unwrap();
                    part_writer.write_i8_column(col_name, arr.values())
                        .map_err(|e| VfsError::Internal(format!("Error writing i8 column '{col_name}': {e}")))?;
                }
                DataType::UInt8 => {
                    let arr = col_array.as_any().downcast_ref::<UInt8Array>().unwrap();
                    part_writer.write_u8_column(col_name, arr.values())
                        .map_err(|e| VfsError::Internal(format!("Error writing u8 column '{col_name}': {e}")))?;
                }
                DataType::Int16 => {
                    let arr = col_array.as_any().downcast_ref::<Int16Array>().unwrap();
                    part_writer.write_i16_column(col_name, arr.values())
                        .map_err(|e| VfsError::Internal(format!("Error writing i16 column '{col_name}': {e}")))?;
                }
                DataType::UInt16 => {
                    let arr = col_array.as_any().downcast_ref::<UInt16Array>().unwrap();
                    part_writer.write_u16_column(col_name, arr.values())
                        .map_err(|e| VfsError::Internal(format!("Error writing u16 column '{col_name}': {e}")))?;
                }
                DataType::Int32 => {
                    let arr = col_array.as_any().downcast_ref::<Int32Array>().unwrap();
                    part_writer.write_i32_column(col_name, arr.values())
                        .map_err(|e| VfsError::Internal(format!("Error writing i32 column '{col_name}': {e}")))?;
                }
                DataType::UInt32 => {
                    let arr = col_array.as_any().downcast_ref::<UInt32Array>().unwrap();
                    part_writer.write_u32_column(col_name, arr.values())
                        .map_err(|e| VfsError::Internal(format!("Error writing u32 column '{col_name}': {e}")))?;
                }
                DataType::Float32 => {
                    let arr = col_array.as_any().downcast_ref::<Float32Array>().unwrap();
                    part_writer.write_f32_column(col_name, arr.values())
                        .map_err(|e| VfsError::Internal(format!("Error writing f32 column '{col_name}': {e}")))?;
                }
                DataType::Int64 => {
                    let arr = col_array.as_any().downcast_ref::<Int64Array>().unwrap();
                    part_writer.write_primitive_column(col_name, arr.values())
                        .map_err(|e| VfsError::Internal(format!("Error writing i64 column '{col_name}': {e}")))?;
                }
                DataType::UInt64 => {
                    let arr = col_array.as_any().downcast_ref::<UInt64Array>().unwrap();
                    part_writer.write_primitive_column(col_name, arr.values())
                        .map_err(|e| VfsError::Internal(format!("Error writing u64 column '{col_name}': {e}")))?;
                }
                DataType::Float64 => {
                    let arr = col_array.as_any().downcast_ref::<Float64Array>().unwrap();
                    part_writer.write_primitive_column(col_name, arr.values())
                        .map_err(|e| VfsError::Internal(format!("Error writing f64 column '{col_name}': {e}")))?;
                }
                DataType::Boolean => {
                    let arr = col_array.as_any().downcast_ref::<BooleanArray>().unwrap();
                    let bools: Vec<bool> = (0..arr.len()).map(|idx| arr.value(idx)).collect();
                    part_writer.write_bool_column(col_name, &bools)
                        .map_err(|e| VfsError::Internal(format!("Error writing bool column '{col_name}': {e}")))?;
                }
                DataType::Date32 => {
                    let arr = col_array.as_any().downcast_ref::<Date32Array>().unwrap();
                    part_writer.write_i32_column(col_name, arr.values())
                        .map_err(|e| VfsError::Internal(format!("Error writing date32 column '{col_name}': {e}")))?;
                }
                DataType::Date64 => {
                    let arr = col_array.as_any().downcast_ref::<Date64Array>().unwrap();
                    part_writer.write_primitive_column(col_name, arr.values())
                        .map_err(|e| VfsError::Internal(format!("Error writing date64 column '{col_name}': {e}")))?;
                }
                DataType::Timestamp(_, _) => {
                    let int_arr = arrow::compute::cast(col_array.as_ref(), &DataType::Int64)
                        .map_err(|e| VfsError::Internal(format!("Failed to cast timestamp: {e}")))?;
                    let arr = int_arr.as_any().downcast_ref::<Int64Array>().unwrap();
                    part_writer.write_primitive_column(col_name, arr.values())
                        .map_err(|e| VfsError::Internal(format!("Error writing timestamp column '{col_name}': {e}")))?;
                }
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                    let str_arr = arrow::compute::cast(col_array.as_ref(), &DataType::Utf8)
                        .map_err(|e| VfsError::Internal(format!("Failed to cast to utf8: {e}")))?;
                    let arr = str_arr.as_any().downcast_ref::<StringArray>().unwrap();
                    if arr.null_count() > 0 {
                        let opt_strings: Vec<Option<String>> = (0..arr.len())
                            .map(|idx| if arr.is_null(idx) { None } else { Some(arr.value(idx).to_string()) })
                            .collect();
                        part_writer.write_opt_string_column(col_name, &opt_strings)
                            .map_err(|e| VfsError::Internal(format!("Error writing opt_string column '{col_name}': {e}")))?;
                    } else {
                        let strings: Vec<String> = (0..arr.len()).map(|idx| arr.value(idx).to_string()).collect();
                        part_writer.write_string_column(col_name, &strings)
                            .map_err(|e| VfsError::Internal(format!("Error writing string column '{col_name}': {e}")))?;
                    }
                }
                DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
                    let bin_arr = arrow::compute::cast(col_array.as_ref(), &DataType::Binary)
                        .map_err(|e| VfsError::Internal(format!("Failed to cast to binary: {e}")))?;
                    let arr = bin_arr.as_any().downcast_ref::<BinaryArray>().unwrap();
                    let bytes_vec: Vec<Vec<u8>> = (0..arr.len()).map(|idx| arr.value(idx).to_vec()).collect();
                    part_writer.write_bytes_column(col_name, &bytes_vec)
                        .map_err(|e| VfsError::Internal(format!("Error writing bytes column '{col_name}': {e}")))?;
                }
                _ => {
                    let str_arr = arrow::compute::cast(col_array.as_ref(), &DataType::Utf8)
                        .map_err(|e| VfsError::Internal(format!("Failed to cast fallback type to utf8: {e}")))?;
                    let arr = str_arr.as_any().downcast_ref::<StringArray>().unwrap();
                    let strings: Vec<String> = (0..arr.len()).map(|idx| arr.value(idx).to_string()).collect();
                    part_writer.write_string_column(col_name, &strings)
                        .map_err(|e| VfsError::Internal(format!("Error writing string column '{col_name}': {e}")))?;
                }
            }
        }

        let desc = part_writer.finish_in_memory()?;

        // Append encoded buffers directly into VFS active memory-mapped blocks & persist extents in redb
        let mut all_extents = Vec::new();

        // 1. Address plane buffers
        for (col_name, addr_bytes) in &desc.addr_buffers {
            let block_key = BlockKey {
                segment: crate::block::SegmentKey::new(table_id, part_id),
                column_name: col_name.clone(),
                plane: PlaneType::AddressPlane,
            };
            let canonical_path = block_key.to_canonical_path();
            let extents = self.block_manager.append(addr_bytes).await?;
            for extent in &extents {
                self.metadata_engine.append_extent(&canonical_path, extent.clone()).await?;
            }
            all_extents.extend(extents);
        }

        // 2. Payload vault buffers
        for (col_name, vault_bytes) in &desc.vault_buffers {
            let block_key = BlockKey {
                segment: crate::block::SegmentKey::new(table_id, part_id),
                column_name: col_name.clone(),
                plane: PlaneType::PayloadVault,
            };
            let canonical_path = block_key.to_canonical_path();
            let extents = self.block_manager.append(vault_bytes).await?;
            for extent in &extents {
                self.metadata_engine.append_extent(&canonical_path, extent.clone()).await?;
            }
            all_extents.extend(extents);
        }

        // 3. Persist _schema.json directly to VFS blocks and redb metadata.
        //    Use replace_file so the schema manifest is always a single clean entry.
        let vein_schema = VeinSchema::from(batch.schema().as_ref());
        let json_str = serde_json::to_string_pretty(&vein_schema)
            .map_err(|e| VfsError::Internal(format!("Failed to serialize VeinSchema to JSON: {e}")))?;
        let schema_bytes = json_str.as_bytes();
        let schema_canonical = format!("tbl_{table_id}/_schema.json");
        let schema_extents = self.block_manager.append(schema_bytes).await?;
        self.metadata_engine.replace_file(&schema_canonical, schema_extents).await?;

        // 4. Update partition registry in VFS blocks: tbl_{table_id}/_partitions.json.
        //    Read current list, merge the new part_id, then replace_file so only
        //    one valid JSON document lives at this path.
        let parts_canonical = format!("tbl_{table_id}/_partitions.json");
        let mut parts: Vec<u32> = match self.metadata_engine.get_file(&parts_canonical).await {
            Ok(extents) if !extents.is_empty() => {
                let mut bytes = Vec::new();
                for extent in &extents {
                    if let Ok(chunk) = self.block_manager.read_extent(extent).await {
                        bytes.extend_from_slice(&chunk);
                    }
                }
                serde_json::from_slice(&bytes).unwrap_or_default()
            }
            _ => Vec::new(),
        };
        if !parts.contains(&part_id) {
            parts.push(part_id);
            parts.sort();
        }
        let parts_json = serde_json::to_vec(&parts)
            .map_err(|e| VfsError::Internal(format!("Failed to serialize partition list: {e}")))?;
        let parts_extents = self.block_manager.append(&parts_json).await?;
        self.metadata_engine.replace_file(&parts_canonical, parts_extents).await?;

        // 5. Write _SUCCESS marker to VFS blocks
        let success_canonical = format!("tbl_{table_id}/_SUCCESS");
        let success_extents = self.block_manager.append(b"OK").await?;
        for extent in success_extents {
            self.metadata_engine.append_extent(&success_canonical, extent).await?;
        }

        tracing::info!(
            table_id = table_id,
            part_id = part_id,
            rows = desc.total_rows,
            columns = ?desc.column_names,
            "Successfully committed partition directly into VFS blocks and redb extents"
        );

        Ok(VfsPartitionSummary {
            table_id,
            part_id,
            canonical_prefix: format!("tbl_{table_id}/part_{part_id:06}"),
            total_rows: desc.total_rows,
            column_names: desc.column_names,
            extents: all_extents,
        })
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
        let extents = self.metadata_engine.get_file(&parts_canonical).await?;
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

    /// Opens an [`exo::PartReader`] for zero-copy scanning of a partition directly from VFS blocks.
    pub async fn open_partition_reader(&self, table_id: u32, part_id: u32) -> Result<PartReader> {
        let schema = self.read_table_schema(table_id).await?;
        let mut addr_buffers = std::collections::HashMap::new();
        let mut vault_buffers = std::collections::HashMap::new();

        for field in &schema.fields {
            let col_name = &field.name;

            // 1. Read address plane bytes from VFS blocks
            let addr_key = BlockKey {
                segment: crate::block::SegmentKey::new(table_id, part_id),
                column_name: col_name.clone(),
                plane: PlaneType::AddressPlane,
            };
            let addr_path = addr_key.to_canonical_path();
            let addr_extents = self.metadata_engine.get_file(&addr_path).await?;
            if !addr_extents.is_empty() {
                let mut addr_bytes = Vec::new();
                for extent in &addr_extents {
                    let chunk = self.block_manager.read_extent(extent).await?;
                    addr_bytes.extend_from_slice(&chunk);
                }
                addr_buffers.insert(col_name.clone(), addr_bytes);
            }

            // 2. Read payload vault bytes from VFS blocks (if column has variable-length payload)
            let vault_key = BlockKey {
                segment: crate::block::SegmentKey::new(table_id, part_id),
                column_name: col_name.clone(),
                plane: PlaneType::PayloadVault,
            };
            let vault_path = vault_key.to_canonical_path();
            if let Ok(vault_extents) = self.metadata_engine.get_file(&vault_path).await {
                if !vault_extents.is_empty() {
                    let mut vault_bytes = Vec::new();
                    for extent in &vault_extents {
                        let chunk = self.block_manager.read_extent(extent).await?;
                        vault_bytes.extend_from_slice(&chunk);
                    }
                    vault_buffers.insert(col_name.clone(), vault_bytes);
                }
            }
        }

        // 3. Read tombstones (if any)
        let tomb_key = BlockKey {
            segment: crate::block::SegmentKey::new(table_id, part_id),
            column_name: "tombstones".to_string(),
            plane: PlaneType::Tombstone,
        };
        let tomb_path = tomb_key.to_canonical_path();
        let tomb_extents = self.metadata_engine.get_file(&tomb_path).await.unwrap_or_default();
        let tombstones = if !tomb_extents.is_empty() {
            let mut tomb_bytes = Vec::new();
            for extent in &tomb_extents {
                if let Ok(chunk) = self.block_manager.read_extent(extent).await {
                    tomb_bytes.extend_from_slice(&chunk);
                }
            }
            exo::TombstoneMask::from_bytes(&tomb_bytes).ok()
        } else {
            None
        };

        PartReader::from_buffers(part_id, addr_buffers, vault_buffers, tombstones).map_err(VfsError::from)
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
        Self::run_async(async move {
            store.write_table(&name, part_id, &batch).await
        })
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
        let existing = self.resolve_table_parts(table_id).await.unwrap_or_default();
        let next_part_id = existing.iter().max().map(|m| m + 1).unwrap_or(0);
        self.write_partition(table_id, next_part_id, batch).await
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
        Self::run_async(async move {
            store.append_partition(&name, &batch).await
        })
    }


    /// Synchronous variant of [`read_table_schema`](Self::read_table_schema).
    pub fn read_table_schema_sync(&self, table_id: u32) -> Result<VeinSchemaRef> {
        let store = self.clone();
        Self::run_async(async move {
            store.read_table_schema(table_id).await
        })
    }

    /// Synchronous variant of [`resolve_table_parts`](Self::resolve_table_parts).
    pub fn resolve_table_parts_sync(&self, table_id: u32) -> Result<Vec<u32>> {
        let store = self.clone();
        Self::run_async(async move {
            store.resolve_table_parts(table_id).await
        })
    }

    /// Synchronous variant of [`open_partition_reader`](Self::open_partition_reader).
    pub fn open_partition_reader_sync(&self, table_id: u32, part_id: u32) -> Result<PartReader> {
        let store = self.clone();
        Self::run_async(async move {
            store.open_partition_reader(table_id, part_id).await
        })
    }

    /// Retrieves physical block extents for a canonical logical path.
    pub async fn get_extents(&self, canonical_path: &str) -> Result<Vec<Extent>> {
        self.metadata_engine.get_file(canonical_path).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, Schema};
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

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![id_array, name_array, score_array],
        ).unwrap();

        let table_id = 42;

        // 2. Persist partition directly into VFS blocks
        let summary = store.write_partition(table_id, 0, &batch).await?;
        assert_eq!(summary.table_id, 42);
        assert_eq!(summary.part_id, 0);
        assert_eq!(summary.total_rows, 4);

        // Verify that NO loose table directory exists on disk!
        let table_dir_candidate = dir.path().join(format!("tbl_{table_id}"));
        assert!(!table_dir_candidate.exists(), "Table directory should NOT exist on disk!");

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
}
