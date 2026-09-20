//! VFS Native Table Subsystem.
//!
//! Owns the complete persistence and scanning lifecycle of Vein native
//! dual-plane (.exo) columnar table partitions directly within VFS
//! memory-mapped blocks, redb extent catalogs, and NVMe cache.
//!
//! Eliminates all temporary filesystem directories and loose table files.

use std::sync::Arc;

use ::types::{VeinSchema, VeinSchemaRef};
use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use exo::{
    AddressGranuleMark, AddressHeader, NULL_VADDR, PartReader, PartWriter, ToCanonicalAddress,
    VaultGranuleMark, VaultHeader,
};

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

static SHARED_STORE: std::sync::RwLock<Option<Arc<VfsTableStore>>> = std::sync::RwLock::new(None);

fn nullable_primitive_vaddrs<T>(array: &PrimitiveArray<T>) -> (Vec<u64>, Vec<bool>)
where
    T: ArrowPrimitiveType,
    T::Native: ToCanonicalAddress,
{
    let values = (0..array.len())
        .map(|idx| {
            if array.is_null(idx) {
                0
            } else {
                array.value(idx).to_canonical()
            }
        })
        .collect();
    let validity = (0..array.len()).map(|idx| array.is_valid(idx)).collect();
    (values, validity)
}

fn nullable_binary_values(array: &BinaryArray) -> (Vec<u64>, Vec<Option<Vec<u8>>>) {
    let vaddrs = (0..array.len())
        .map(|idx| {
            if array.is_null(idx) {
                NULL_VADDR
            } else {
                array.value(idx).to_canonical()
            }
        })
        .collect();
    let payloads = (0..array.len())
        .map(|idx| {
            if array.is_null(idx) {
                None
            } else {
                Some(array.value(idx).to_vec())
            }
        })
        .collect();
    (vaddrs, payloads)
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
                    if arr.null_count() > 0 {
                        let (vaddrs, validity) = nullable_primitive_vaddrs(arr);
                        part_writer
                            .write_raw_address_column_with_validity(col_name, &vaddrs, &validity, 1)
                            .map_err(|e| {
                                VfsError::Internal(format!(
                                    "Error writing nullable i8 column '{col_name}': {e}"
                                ))
                            })?;
                    } else {
                        part_writer
                            .write_i8_column(col_name, arr.values())
                            .map_err(|e| {
                                VfsError::Internal(format!(
                                    "Error writing i8 column '{col_name}': {e}"
                                ))
                            })?;
                    }
                }
                DataType::UInt8 => {
                    let arr = col_array.as_any().downcast_ref::<UInt8Array>().unwrap();
                    if arr.null_count() > 0 {
                        let (vaddrs, validity) = nullable_primitive_vaddrs(arr);
                        part_writer
                            .write_raw_address_column_with_validity(col_name, &vaddrs, &validity, 1)
                            .map_err(|e| {
                                VfsError::Internal(format!(
                                    "Error writing nullable u8 column '{col_name}': {e}"
                                ))
                            })?;
                    } else {
                        part_writer
                            .write_u8_column(col_name, arr.values())
                            .map_err(|e| {
                                VfsError::Internal(format!(
                                    "Error writing u8 column '{col_name}': {e}"
                                ))
                            })?;
                    }
                }
                DataType::Int16 => {
                    let arr = col_array.as_any().downcast_ref::<Int16Array>().unwrap();
                    if arr.null_count() > 0 {
                        let (vaddrs, validity) = nullable_primitive_vaddrs(arr);
                        part_writer
                            .write_raw_address_column_with_validity(col_name, &vaddrs, &validity, 1)
                            .map_err(|e| {
                                VfsError::Internal(format!(
                                    "Error writing nullable i16 column '{col_name}': {e}"
                                ))
                            })?;
                    } else {
                        part_writer
                            .write_i16_column(col_name, arr.values())
                            .map_err(|e| {
                                VfsError::Internal(format!(
                                    "Error writing i16 column '{col_name}': {e}"
                                ))
                            })?;
                    }
                }
                DataType::UInt16 => {
                    let arr = col_array.as_any().downcast_ref::<UInt16Array>().unwrap();
                    if arr.null_count() > 0 {
                        let (vaddrs, validity) = nullable_primitive_vaddrs(arr);
                        part_writer
                            .write_raw_address_column_with_validity(col_name, &vaddrs, &validity, 2)
                            .map_err(|e| {
                                VfsError::Internal(format!(
                                    "Error writing nullable u16 column '{col_name}': {e}"
                                ))
                            })?;
                    } else {
                        part_writer
                            .write_u16_column(col_name, arr.values())
                            .map_err(|e| {
                                VfsError::Internal(format!(
                                    "Error writing u16 column '{col_name}': {e}"
                                ))
                            })?;
                    }
                }
                DataType::Int32 => {
                    let arr = col_array.as_any().downcast_ref::<Int32Array>().unwrap();
                    if arr.null_count() > 0 {
                        let (vaddrs, validity) = nullable_primitive_vaddrs(arr);
                        part_writer
                            .write_raw_address_column_with_validity(col_name, &vaddrs, &validity, 2)
                            .map_err(|e| {
                                VfsError::Internal(format!(
                                    "Error writing nullable i32 column '{col_name}': {e}"
                                ))
                            })?;
                    } else {
                        part_writer
                            .write_i32_column(col_name, arr.values())
                            .map_err(|e| {
                                VfsError::Internal(format!(
                                    "Error writing i32 column '{col_name}': {e}"
                                ))
                            })?;
                    }
                }
                DataType::UInt32 => {
                    let arr = col_array.as_any().downcast_ref::<UInt32Array>().unwrap();
                    if arr.null_count() > 0 {
                        let (vaddrs, validity) = nullable_primitive_vaddrs(arr);
                        part_writer
                            .write_raw_address_column_with_validity(col_name, &vaddrs, &validity, 4)
                            .map_err(|e| {
                                VfsError::Internal(format!(
                                    "Error writing nullable u32 column '{col_name}': {e}"
                                ))
                            })?;
                    } else {
                        part_writer
                            .write_u32_column(col_name, arr.values())
                            .map_err(|e| {
                                VfsError::Internal(format!(
                                    "Error writing u32 column '{col_name}': {e}"
                                ))
                            })?;
                    }
                }
                DataType::Float32 => {
                    let arr = col_array.as_any().downcast_ref::<Float32Array>().unwrap();
                    if arr.null_count() > 0 {
                        let (vaddrs, validity) = nullable_primitive_vaddrs(arr);
                        part_writer
                            .write_raw_address_column_with_validity(col_name, &vaddrs, &validity, 4)
                            .map_err(|e| {
                                VfsError::Internal(format!(
                                    "Error writing nullable f32 column '{col_name}': {e}"
                                ))
                            })?;
                    } else {
                        part_writer
                            .write_f32_column(col_name, arr.values())
                            .map_err(|e| {
                                VfsError::Internal(format!(
                                    "Error writing f32 column '{col_name}': {e}"
                                ))
                            })?;
                    }
                }
                DataType::Int64 => {
                    let arr = col_array.as_any().downcast_ref::<Int64Array>().unwrap();
                    if arr.null_count() > 0 {
                        let values: Vec<Option<i64>> = (0..arr.len())
                            .map(|idx| arr.is_valid(idx).then(|| arr.value(idx)))
                            .collect();
                        part_writer
                            .write_nullable_primitive_column(col_name, &values)
                            .map_err(|e| {
                                VfsError::Internal(format!(
                                    "Error writing nullable i64 column '{col_name}': {e}"
                                ))
                            })?;
                    } else {
                        part_writer
                            .write_primitive_column(col_name, arr.values())
                            .map_err(|e| {
                                VfsError::Internal(format!(
                                    "Error writing i64 column '{col_name}': {e}"
                                ))
                            })?;
                    }
                }
                DataType::UInt64 => {
                    let arr = col_array.as_any().downcast_ref::<UInt64Array>().unwrap();
                    if arr.null_count() > 0 {
                        let values: Vec<Option<u64>> = (0..arr.len())
                            .map(|idx| arr.is_valid(idx).then(|| arr.value(idx)))
                            .collect();
                        part_writer
                            .write_nullable_primitive_column(col_name, &values)
                            .map_err(|e| {
                                VfsError::Internal(format!(
                                    "Error writing nullable u64 column '{col_name}': {e}"
                                ))
                            })?;
                    } else {
                        part_writer
                            .write_primitive_column(col_name, arr.values())
                            .map_err(|e| {
                                VfsError::Internal(format!(
                                    "Error writing u64 column '{col_name}': {e}"
                                ))
                            })?;
                    }
                }
                DataType::Float64 => {
                    let arr = col_array.as_any().downcast_ref::<Float64Array>().unwrap();
                    if arr.null_count() > 0 {
                        let values: Vec<Option<f64>> = (0..arr.len())
                            .map(|idx| arr.is_valid(idx).then(|| arr.value(idx)))
                            .collect();
                        part_writer
                            .write_nullable_primitive_column(col_name, &values)
                            .map_err(|e| {
                                VfsError::Internal(format!(
                                    "Error writing nullable f64 column '{col_name}': {e}"
                                ))
                            })?;
                    } else {
                        part_writer
                            .write_primitive_column(col_name, arr.values())
                            .map_err(|e| {
                                VfsError::Internal(format!(
                                    "Error writing f64 column '{col_name}': {e}"
                                ))
                            })?;
                    }
                }
                DataType::Boolean => {
                    let arr = col_array.as_any().downcast_ref::<BooleanArray>().unwrap();
                    if arr.null_count() > 0 {
                        let vaddrs: Vec<u64> = (0..arr.len())
                            .map(|idx| {
                                if arr.is_null(idx) {
                                    0
                                } else {
                                    arr.value(idx).to_canonical()
                                }
                            })
                            .collect();
                        let validity: Vec<bool> =
                            (0..arr.len()).map(|idx| arr.is_valid(idx)).collect();
                        part_writer
                            .write_raw_address_column_with_validity(col_name, &vaddrs, &validity, 1)
                            .map_err(|e| {
                                VfsError::Internal(format!(
                                    "Error writing nullable bool column '{col_name}': {e}"
                                ))
                            })?;
                    } else {
                        let bools: Vec<bool> = (0..arr.len()).map(|idx| arr.value(idx)).collect();
                        part_writer
                            .write_bool_column(col_name, &bools)
                            .map_err(|e| {
                                VfsError::Internal(format!(
                                    "Error writing bool column '{col_name}': {e}"
                                ))
                            })?;
                    }
                }
                DataType::Date32 => {
                    let arr = col_array.as_any().downcast_ref::<Date32Array>().unwrap();
                    if arr.null_count() > 0 {
                        let (vaddrs, validity) = nullable_primitive_vaddrs(arr);
                        part_writer
                            .write_raw_address_column_with_validity(col_name, &vaddrs, &validity, 8)
                            .map_err(|e| {
                                VfsError::Internal(format!(
                                    "Error writing nullable date32 column '{col_name}': {e}"
                                ))
                            })?;
                    } else {
                        part_writer
                            .write_i32_column(col_name, arr.values())
                            .map_err(|e| {
                                VfsError::Internal(format!(
                                    "Error writing date32 column '{col_name}': {e}"
                                ))
                            })?;
                    }
                }
                DataType::Date64 => {
                    let arr = col_array.as_any().downcast_ref::<Date64Array>().unwrap();
                    if arr.null_count() > 0 {
                        let values: Vec<Option<i64>> = (0..arr.len())
                            .map(|idx| arr.is_valid(idx).then(|| arr.value(idx)))
                            .collect();
                        part_writer
                            .write_nullable_primitive_column(col_name, &values)
                            .map_err(|e| {
                                VfsError::Internal(format!(
                                    "Error writing nullable date64 column '{col_name}': {e}"
                                ))
                            })?;
                    } else {
                        part_writer
                            .write_primitive_column(col_name, arr.values())
                            .map_err(|e| {
                                VfsError::Internal(format!(
                                    "Error writing date64 column '{col_name}': {e}"
                                ))
                            })?;
                    }
                }
                DataType::Time64(_) | DataType::Duration(_) => {
                    let int_arr = arrow::compute::cast(col_array.as_ref(), &DataType::Int64)
                        .map_err(|e| VfsError::Internal(format!("Failed to cast temporal column: {e}")))?;
                    let arr = int_arr.as_any().downcast_ref::<Int64Array>().unwrap();
                    let values: Vec<Option<i64>> = (0..arr.len())
                        .map(|idx| arr.is_valid(idx).then(|| arr.value(idx)))
                        .collect();
                    part_writer
                        .write_nullable_primitive_column(col_name, &values)
                        .map_err(|e| VfsError::Internal(format!("Error writing temporal column '{col_name}': {e}")))?;
                }
                DataType::Timestamp(_, _) => {
                    let int_arr = arrow::compute::cast(col_array.as_ref(), &DataType::Int64)
                        .map_err(|e| {
                            VfsError::Internal(format!("Failed to cast timestamp: {e}"))
                        })?;
                    let arr = int_arr.as_any().downcast_ref::<Int64Array>().unwrap();
                    if arr.null_count() > 0 {
                        let values: Vec<Option<i64>> = (0..arr.len())
                            .map(|idx| arr.is_valid(idx).then(|| arr.value(idx)))
                            .collect();
                        part_writer
                            .write_nullable_primitive_column(col_name, &values)
                            .map_err(|e| {
                                VfsError::Internal(format!(
                                    "Error writing nullable timestamp column '{col_name}': {e}"
                                ))
                            })?;
                    } else {
                        part_writer
                            .write_primitive_column(col_name, arr.values())
                            .map_err(|e| {
                                VfsError::Internal(format!(
                                    "Error writing timestamp column '{col_name}': {e}"
                                ))
                            })?;
                    }
                }
                DataType::FixedSizeBinary(_) => {
                    let arr = col_array.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
                    let vaddrs: Vec<u64> = (0..arr.len())
                        .map(|idx| if arr.is_null(idx) { 0 } else { arr.value(idx).to_canonical() })
                        .collect();
                    let payloads: Vec<Option<Vec<u8>>> = (0..arr.len())
                        .map(|idx| arr.is_valid(idx).then(|| arr.value(idx).to_vec()))
                        .collect();
                    part_writer
                        .write_raw_column(col_name, &vaddrs, &payloads)
                        .map_err(|e| VfsError::Internal(format!("Error writing fixed binary column '{col_name}': {e}")))?;
                }
                DataType::Decimal128(_, _) => {
                    let arr = col_array.as_any().downcast_ref::<Decimal128Array>().unwrap();
                    if arr.null_count() > 0 {
                        let values: Vec<Option<i128>> = (0..arr.len())
                            .map(|idx| arr.is_valid(idx).then(|| arr.value(idx)))
                            .collect();
                        part_writer
                            .write_nullable_primitive_column(col_name, &values)
                            .map_err(|e| VfsError::Internal(format!("Error writing Decimal128 column '{col_name}': {e}")))?;
                    } else {
                        part_writer
                            .write_primitive_column(col_name, arr.values())
                            .map_err(|e| VfsError::Internal(format!("Error writing Decimal128 column '{col_name}': {e}")))?;
                    }
                }
                DataType::Decimal256(_, _) => {
                    let arr = col_array.as_any().downcast_ref::<Decimal256Array>().unwrap();
                    if arr.null_count() > 0 {
                        let values: Vec<Option<i256>> = (0..arr.len())
                            .map(|idx| arr.is_valid(idx).then(|| arr.value(idx)))
                            .collect();
                        part_writer
                            .write_nullable_primitive_column(col_name, &values)
                            .map_err(|e| VfsError::Internal(format!("Error writing Decimal256 column '{col_name}': {e}")))?;
                    } else {
                        part_writer
                            .write_primitive_column(col_name, arr.values())
                            .map_err(|e| VfsError::Internal(format!("Error writing Decimal256 column '{col_name}': {e}")))?;
                    }
                }
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                    let str_arr = arrow::compute::cast(col_array.as_ref(), &DataType::Utf8)
                        .map_err(|e| VfsError::Internal(format!("Failed to cast to utf8: {e}")))?;
                    let arr = str_arr.as_any().downcast_ref::<StringArray>().unwrap();
                    if arr.null_count() > 0 {
                        let opt_strings: Vec<Option<String>> = (0..arr.len())
                            .map(|idx| {
                                if arr.is_null(idx) {
                                    None
                                } else {
                                    Some(arr.value(idx).to_string())
                                }
                            })
                            .collect();
                        part_writer
                            .write_opt_string_column(col_name, &opt_strings)
                            .map_err(|e| {
                                VfsError::Internal(format!(
                                    "Error writing opt_string column '{col_name}': {e}"
                                ))
                            })?;
                    } else {
                        let strings: Vec<String> = (0..arr.len())
                            .map(|idx| arr.value(idx).to_string())
                            .collect();
                        part_writer
                            .write_string_column(col_name, &strings)
                            .map_err(|e| {
                                VfsError::Internal(format!(
                                    "Error writing string column '{col_name}': {e}"
                                ))
                            })?;
                    }
                }
                DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
                    let bin_arr = arrow::compute::cast(col_array.as_ref(), &DataType::Binary)
                        .map_err(|e| {
                            VfsError::Internal(format!("Failed to cast to binary: {e}"))
                        })?;
                    let arr = bin_arr.as_any().downcast_ref::<BinaryArray>().unwrap();
                    if arr.null_count() > 0 {
                        let (vaddrs, payloads) = nullable_binary_values(arr);
                        part_writer
                            .write_raw_column(col_name, &vaddrs, &payloads)
                            .map_err(|e| {
                                VfsError::Internal(format!(
                                    "Error writing nullable bytes column '{col_name}': {e}"
                                ))
                            })?;
                    } else {
                        let bytes_vec: Vec<Vec<u8>> =
                            (0..arr.len()).map(|idx| arr.value(idx).to_vec()).collect();
                        part_writer
                            .write_bytes_column(col_name, &bytes_vec)
                            .map_err(|e| {
                                VfsError::Internal(format!(
                                    "Error writing bytes column '{col_name}': {e}"
                                ))
                            })?;
                    }
                }
                _ => {
                    return Err(VfsError::Internal(format!(
                        "EXO does not support safe typed storage for Arrow data type {:?}",
                        col_array.data_type()
                    )));
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
                self.metadata_engine
                    .append_extent(&canonical_path, extent.clone())
                    .await?;
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
                self.metadata_engine
                    .append_extent(&canonical_path, extent.clone())
                    .await?;
            }
            all_extents.extend(extents);
        }

        // 3. Persist _schema.json directly to VFS blocks and redb metadata.
        //    Use replace_file so the schema manifest is always a single clean entry.
        let vein_schema = VeinSchema::from(batch.schema().as_ref());
        let json_str = serde_json::to_string_pretty(&vein_schema).map_err(|e| {
            VfsError::Internal(format!("Failed to serialize VeinSchema to JSON: {e}"))
        })?;
        let schema_bytes = json_str.as_bytes();
        let schema_canonical = format!("tbl_{table_id}/_schema.json");
        let schema_extents = self.block_manager.append(schema_bytes).await?;
        self.metadata_engine
            .replace_file(&schema_canonical, schema_extents)
            .await?;

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
        self.metadata_engine
            .replace_file(&parts_canonical, parts_extents)
            .await?;

        // 5. Write _SUCCESS marker to VFS blocks
        let success_canonical = format!("tbl_{table_id}/_SUCCESS");
        let success_extents = self.block_manager.append(b"OK").await?;
        for extent in success_extents {
            self.metadata_engine
                .append_extent(&success_canonical, extent)
                .await?;
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
        let payload_len = u64::try_from(payload.len())
            .map_err(|_| VfsError::Internal("Exchange frame is too large".to_string()))?;
        let mut frame = Vec::with_capacity(16usize.saturating_add(payload.len()));
        frame.extend_from_slice(b"VEXF");
        frame.extend_from_slice(&1u32.to_le_bytes());
        frame.extend_from_slice(&payload_len.to_le_bytes());
        frame.extend_from_slice(payload);

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

    /// Synchronous wrapper for the exchange frame writer.
    pub fn append_exchange_frame_sync(&self, path: &str, payload: &[u8]) -> Result<()> {
        let store = self.clone();
        let path = path.to_string();
        let payload = payload.to_vec();
        Self::run_async(async move { store.append_exchange_frame(&path, &payload).await })
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
                let bytes = self
                    .block_manager
                    .read_extent_range(extent, extent_offset, overlap_len)
                    .await?;
                if bytes.len() != overlap_len as usize {
                    return Err(VfsError::Internal(format!(
                        "Short VFS range read: expected {}, got {}",
                        overlap_len,
                        bytes.len()
                    )));
                }
                output.extend_from_slice(&bytes);
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
        let tomb_extents = self
            .metadata_engine
            .get_file(&tomb_path)
            .await
            .unwrap_or_default();
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
        let existing = self.resolve_table_parts(table_id).await.unwrap_or_default();
        let next_part_id = existing.iter().max().map(|m| m + 1).unwrap_or(0);
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
