use std::fs::{File, create_dir, create_dir_all, remove_dir_all};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::array::*;
use arrow::compute::cast;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use exo::{
    AddressWriter, ExactValueCodec, ExoAddressWriter, ExoVaultWriter, ToCanonicalAddress,
    VaultWriter, classify_storage,
};

use crate::{Result, VfsError};

use super::VfsTableStore;

static NEXT_PARTITION_WRITE_ID: AtomicU64 = AtomicU64::new(0);
const GRANULE_SIZE: usize = 8192;

struct ScratchDirectory(PathBuf);

impl ScratchDirectory {
    fn create(root: &Path) -> Result<Self> {
        create_dir_all(root)?;
        for _ in 0..32 {
            let id = NEXT_PARTITION_WRITE_ID.fetch_add(1, Ordering::Relaxed);
            let path = root.join(format!(".exo-write-{}-{id}", std::process::id()));
            match create_dir(&path) {
                Ok(()) => return Ok(Self(path)),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Err(VfsError::Internal(
            "could not allocate unique EXO partition scratch directory".to_string(),
        ))
    }
}

impl Drop for ScratchDirectory {
    fn drop(&mut self) {
        let _ = remove_dir_all(&self.0);
    }
}

struct ColumnWriter {
    name: String,
    address: ExoAddressWriter<File>,
    vault: Option<ExoVaultWriter<File>>,
}

impl ColumnWriter {
    fn create(name: &str, data_type: &DataType, index: usize, scratch: &Path) -> Result<Self> {
        let storage = classify_storage(data_type).ok_or_else(|| {
            VfsError::Internal(format!(
                "EXO does not support safe typed storage for Arrow data type {data_type:?}"
            ))
        })?;
        let prefix = format!("column_{index:05}");
        let address_path = scratch.join(format!("{prefix}.addr"));
        let address = ExoAddressWriter::create_with_slot_width(
            &address_path,
            GRANULE_SIZE,
            storage.slot_width(),
        )
        .map_err(|error| {
            VfsError::Internal(format!("create address writer for '{name}': {error}"))
        })?;
        let vault = if storage.uses_vault() {
            Some(
                ExoVaultWriter::create(&scratch.join(format!("{prefix}.vault")), GRANULE_SIZE)
                    .map_err(|error| {
                        VfsError::Internal(format!("create vault writer for '{name}': {error}"))
                    })?,
            )
        } else {
            None
        };
        Ok(Self {
            name: name.to_owned(),
            address,
            vault,
        })
    }

    fn append(&mut self, array: &dyn Array) -> Result<()> {
        match array.data_type() {
            DataType::Boolean => {
                let values = downcast::<BooleanArray>(array, &self.name)?;
                for row in 0..values.len() {
                    let valid = values.is_valid(row);
                    let value = if valid {
                        values.value(row).to_canonical()
                    } else {
                        0
                    };
                    self.address.append_nullable_vaddr(value, valid)?;
                }
            }
            DataType::Int8 => self.append_primitive::<Int8Type>(array)?,
            DataType::UInt8 => self.append_primitive::<UInt8Type>(array)?,
            DataType::Int16 => self.append_primitive::<Int16Type>(array)?,
            DataType::UInt16 => self.append_primitive::<UInt16Type>(array)?,
            DataType::Int32 => self.append_primitive::<Int32Type>(array)?,
            DataType::UInt32 => self.append_primitive::<UInt32Type>(array)?,
            DataType::Float32 => self.append_primitive::<Float32Type>(array)?,
            DataType::Int64 => self.append_primitive::<Int64Type>(array)?,
            DataType::UInt64 => self.append_primitive::<UInt64Type>(array)?,
            DataType::Float64 => self.append_primitive::<Float64Type>(array)?,
            DataType::Date32 => self.append_primitive::<Date32Type>(array)?,
            DataType::Date64 => self.append_primitive::<Date64Type>(array)?,
            DataType::Time32(_) => {
                let casted = cast(array, &DataType::Int32).map_err(|error| {
                    VfsError::Internal(format!("cast time32 column '{}': {error}", self.name))
                })?;
                self.append_primitive::<Int32Type>(casted.as_ref())?;
            }
            DataType::Time64(_) | DataType::Duration(_) | DataType::Timestamp(_, _) => {
                let casted = cast(array, &DataType::Int64).map_err(|error| {
                    VfsError::Internal(format!("cast temporal column '{}': {error}", self.name))
                })?;
                self.append_primitive::<Int64Type>(casted.as_ref())?;
            }
            DataType::Decimal128(_, _) => self.append_primitive::<Decimal128Type>(array)?,
            DataType::Decimal256(_, _) => self.append_primitive::<Decimal256Type>(array)?,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                let casted = cast(array, &DataType::Utf8).map_err(|error| {
                    VfsError::Internal(format!("cast string column '{}': {error}", self.name))
                })?;
                let values = downcast::<StringArray>(casted.as_ref(), &self.name)?;
                for row in 0..values.len() {
                    let value = values.is_valid(row).then(|| values.value(row));
                    self.address.append_nullable_vaddr(
                        value.map_or(0, ToCanonicalAddress::to_canonical),
                        value.is_some(),
                    )?;
                    if let Some(vault) = &mut self.vault {
                        vault.append_opt_str(value)?;
                    }
                }
            }
            DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
                let casted = cast(array, &DataType::Binary).map_err(|error| {
                    VfsError::Internal(format!("cast binary column '{}': {error}", self.name))
                })?;
                let values = downcast::<BinaryArray>(casted.as_ref(), &self.name)?;
                for row in 0..values.len() {
                    let value = values.is_valid(row).then(|| values.value(row));
                    self.address.append_nullable_vaddr(
                        value.map_or(0, ToCanonicalAddress::to_canonical),
                        value.is_some(),
                    )?;
                    if let Some(vault) = &mut self.vault {
                        vault.append_opt_bytes(value)?;
                    }
                }
            }
            DataType::FixedSizeBinary(_) => {
                let values = downcast::<FixedSizeBinaryArray>(array, &self.name)?;
                for row in 0..values.len() {
                    let value = values.is_valid(row).then(|| values.value(row));
                    self.address.append_nullable_vaddr(
                        value.map_or(0, ToCanonicalAddress::to_canonical),
                        value.is_some(),
                    )?;
                    if let Some(vault) = &mut self.vault {
                        vault.append_opt_bytes(value)?;
                    }
                }
            }
            unsupported => {
                return Err(VfsError::Internal(format!(
                    "EXO streaming writer does not support Arrow data type {unsupported:?} in column '{}'",
                    self.name
                )));
            }
        }
        Ok(())
    }

    fn append_primitive<T>(&mut self, array: &dyn Array) -> Result<()>
    where
        T: ArrowPrimitiveType,
        T::Native: ExactValueCodec,
    {
        let values = downcast::<PrimitiveArray<T>>(array, &self.name)?;
        let mut encoded = [0u8; 32];
        for row in 0..values.len() {
            let valid = values.is_valid(row);
            let value = values.value(row);
            self.address
                .append_nullable_vaddr(if valid { value.to_canonical() } else { 0 }, valid)?;
            if let Some(vault) = &mut self.vault {
                if valid {
                    value.encode_exact_into(&mut encoded[..T::Native::WIDTH as usize])?;
                    vault.append_fixed_bytes(&encoded[..T::Native::WIDTH as usize])?;
                } else {
                    vault.append_null()?;
                }
            }
        }
        Ok(())
    }

    fn finish(self) -> Result<(String, PathBuf, Option<PathBuf>)> {
        let address_path = self.address.path().to_path_buf();
        self.address.finish().map_err(|error| {
            VfsError::Internal(format!("finish address column '{}': {error}", self.name))
        })?;
        let vault_path = if let Some(vault) = self.vault {
            let path = vault.path().to_path_buf();
            vault.finish().map_err(|error| {
                VfsError::Internal(format!("finish vault column '{}': {error}", self.name))
            })?;
            Some(path)
        } else {
            None
        };
        Ok((self.name, address_path, vault_path))
    }
}

fn downcast<'a, T: 'static>(array: &'a dyn Array, column: &str) -> Result<&'a T> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        VfsError::Internal(format!("Arrow array type mismatch in column '{column}'"))
    })
}

pub struct StreamingPartitionWriter {
    store: VfsTableStore,
    table_id: u32,
    part_id: u32,
    schema: SchemaRef,
    _scratch: ScratchDirectory,
    columns: Vec<ColumnWriter>,
    rows: u64,
    failed: bool,
}

impl StreamingPartitionWriter {
    pub(crate) fn new(
        store: VfsTableStore,
        table_id: u32,
        part_id: u32,
        schema: SchemaRef,
    ) -> Result<Self> {
        let scratch = ScratchDirectory::create(&store.config.local_cache_dir)?;
        let columns = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                ColumnWriter::create(field.name(), field.data_type(), index, &scratch.0)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            store,
            table_id,
            part_id,
            schema,
            _scratch: scratch,
            columns,
            rows: 0,
            failed: false,
        })
    }

    pub fn append_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        if self.failed {
            return Err(VfsError::Internal(
                "cannot append to a failed VFS partition writer".to_string(),
            ));
        }
        let result = (|| {
            if batch.schema().as_ref() != self.schema.as_ref() {
                return Err(VfsError::Internal(
                    "schema changed while streaming a VFS partition".to_string(),
                ));
            }
            for (writer, array) in self.columns.iter_mut().zip(batch.columns()) {
                writer.append(array.as_ref())?;
            }
            self.rows
                .checked_add(batch.num_rows() as u64)
                .ok_or_else(|| VfsError::Internal("partition row count overflow".to_string()))
        })();
        match result {
            Ok(rows) => {
                self.rows = rows;
                Ok(())
            }
            Err(error) => {
                self.failed = true;
                Err(error)
            }
        }
    }

    pub(crate) async fn finish(self) -> Result<super::VfsPartitionSummary> {
        if self.failed {
            return Err(VfsError::Internal(
                "cannot commit a failed VFS partition writer".to_string(),
            ));
        }
        if self.rows == 0 {
            return Err(VfsError::Internal(
                "cannot commit an empty streamed partition".to_string(),
            ));
        }
        let mut files = Vec::with_capacity(self.columns.len());
        for column in self.columns {
            files.push(column.finish()?);
        }
        self.store
            .commit_streamed_partition(self.table_id, self.part_id, self.schema, self.rows, files)
            .await
    }

    pub fn finish_sync(self) -> Result<super::VfsPartitionSummary> {
        VfsTableStore::run_async(self.finish())
    }
}

impl VfsTableStore {
    pub fn begin_partition_writer(
        &self,
        table_name_or_id: &str,
        part_id: Option<u32>,
        schema: SchemaRef,
    ) -> Result<StreamingPartitionWriter> {
        let table_id = Self::resolve_table_id(table_name_or_id);
        let part_id = match part_id {
            Some(part_id) => part_id,
            None => match self.resolve_table_parts_sync(table_id)?.iter().max() {
                Some(part_id) => part_id
                    .checked_add(1)
                    .ok_or_else(|| VfsError::Internal("partition ID overflow".to_string()))?,
                None => 0,
            },
        };
        StreamingPartitionWriter::new(self.clone(), table_id, part_id, schema)
    }

    async fn commit_streamed_partition(
        &self,
        table_id: u32,
        part_id: u32,
        schema: SchemaRef,
        rows: u64,
        files: Vec<(String, PathBuf, Option<PathBuf>)>,
    ) -> Result<super::VfsPartitionSummary> {
        let mut updates = Vec::with_capacity(files.len() * 2 + 3);
        let mut all_extents = Vec::new();
        let mut column_names = Vec::with_capacity(files.len());

        for (column_name, address_path, vault_path) in files {
            column_names.push(column_name.clone());
            let address_key = crate::block::BlockKey {
                segment: crate::block::SegmentKey::new(table_id, part_id),
                column_name: column_name.clone(),
                plane: crate::block::PlaneType::AddressPlane,
            };
            let address_extents = self.append_local_file(&address_path).await?;
            all_extents.extend(address_extents.iter().cloned());
            updates.push(crate::metadata::MetadataFileUpdate {
                path: address_key.to_canonical_path(),
                extents: address_extents,
            });

            let vault_key = crate::block::BlockKey {
                segment: crate::block::SegmentKey::new(table_id, part_id),
                column_name,
                plane: crate::block::PlaneType::PayloadVault,
            };
            let vault_extents = match vault_path {
                Some(path) => self.append_local_file(&path).await?,
                None => Vec::new(),
            };
            all_extents.extend(vault_extents.iter().cloned());
            updates.push(crate::metadata::MetadataFileUpdate {
                path: vault_key.to_canonical_path(),
                extents: vault_extents,
            });
        }

        let vein_schema = ::types::VeinSchema::from(schema.as_ref());
        let schema_bytes = serde_json::to_vec(&vein_schema)
            .map_err(|error| VfsError::Internal(format!("serialize VFS schema: {error}")))?;
        let schema_extents = self.block_manager.append(&schema_bytes).await?;
        updates.push(crate::metadata::MetadataFileUpdate {
            path: format!("tbl_{table_id}/_schema.json"),
            extents: schema_extents,
        });

        let mut parts = self.resolve_table_parts(table_id).await?;
        if !parts.contains(&part_id) {
            parts.push(part_id);
            parts.sort_unstable();
        }
        let parts_bytes = serde_json::to_vec(&parts)
            .map_err(|error| VfsError::Internal(format!("serialize VFS partitions: {error}")))?;
        let parts_extents = self.block_manager.append(&parts_bytes).await?;
        updates.push(crate::metadata::MetadataFileUpdate {
            path: format!("tbl_{table_id}/_partitions.json"),
            extents: parts_extents,
        });

        let success_extents = self.block_manager.append(b"OK").await?;
        updates.push(crate::metadata::MetadataFileUpdate {
            path: format!("tbl_{table_id}/_SUCCESS"),
            extents: success_extents,
        });

        self.metadata_engine
            .replace_files_atomically(updates)
            .await?;
        tracing::info!(
            target: "vein.audit",
            event = "vfs_partition_committed",
            table_id,
            part_id,
            rows,
            columns = column_names.len(),
            extents = all_extents.len(),
            "vfs_partition_committed"
        );

        Ok(super::VfsPartitionSummary {
            table_id,
            part_id,
            canonical_prefix: format!("tbl_{table_id}/part_{part_id:06}"),
            total_rows: rows,
            column_names,
            extents: all_extents,
        })
    }

    async fn append_local_file(&self, path: &Path) -> Result<Vec<crate::metadata::Extent>> {
        use tokio::io::AsyncReadExt;

        const UPLOAD_CHUNK_BYTES: usize = 4 * 1024 * 1024;
        let mut file = tokio::fs::File::open(path).await?;
        let mut buffer = vec![0u8; UPLOAD_CHUNK_BYTES];
        let mut extents = Vec::new();
        loop {
            let read = file.read(&mut buffer).await?;
            if read == 0 {
                break;
            }
            extents.extend(self.block_manager.append(&buffer[..read]).await?);
        }
        Ok(extents)
    }
}
