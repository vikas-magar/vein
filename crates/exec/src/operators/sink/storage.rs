//! Storage sink operator writing pipeline output to files or VFS tables (.exo, .parquet).

use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::*;
use arrow::compute::take;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use common::Result;
use common::protocol::StorageTarget;
use error::VeinError;
use parquet::arrow::ArrowWriter;

use crate::pipeline::{DataChunk, PipelineNode, Sink};

/// A terminal pipeline sink writing rows to storage (.exo or .parquet).
pub struct StorageSink {
    target: StorageTarget,
    batches: Vec<RecordBatch>,
    field_names: Vec<String>,
    vfs_store: Option<Arc<vfs::VfsTableStore>>,
    rows_written: u64,
}

impl StorageSink {
    /// Creates a new [`StorageSink`] for the given storage target.
    pub fn new(target: StorageTarget) -> Self {
        Self::with_vfs_store(target, None)
    }

    /// Creates a new [`StorageSink`] with an explicit [`VfsTableStore`].
    pub fn with_vfs_store(
        target: StorageTarget,
        vfs_store: Option<Arc<vfs::VfsTableStore>>,
    ) -> Self {
        let field_names = target.schema.iter().map(|f| f.name.clone()).collect();
        Self {
            target,
            batches: Vec::new(),
            field_names,
            vfs_store,
            rows_written: 0,
        }
    }
}

impl PipelineNode<DataChunk> for StorageSink {
    fn push(&mut self, chunk: DataChunk) -> Result<()> {
        let selected = chunk.selection.size();
        if selected == 0 {
            return Ok(());
        }

        let mut idx = UInt32Array::builder(selected);
        for i in 0..selected {
            idx.append_value(chunk.selection.get_index(i) as u32);
        }
        let indices = idx.finish();

        let mut fields = Vec::with_capacity(chunk.vectors.len());
        let mut columns = Vec::with_capacity(chunk.vectors.len());
        for (i, vec) in chunk.vectors.iter().enumerate() {
            let col_name = self
                .field_names
                .get(i)
                .cloned()
                .unwrap_or_else(|| format!("col_{i}"));
            fields.push(Field::new(col_name, vec.data_type().clone(), true));
            let array = take(vec.as_ref(), &indices, None)
                .map_err(|e| VeinError::internal(format!("storage sink take failed: {e}")))?;
            columns.push(array);
        }

        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema, columns)
            .map_err(|e| VeinError::internal(format!("storage sink record batch failed: {e}")))?;
        self.rows_written += batch.num_rows() as u64;
        self.batches.push(batch);
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if self.batches.is_empty() {
            tracing::info!(target: "storage_sink", "No batches to persist");
            return Ok(());
        }

        let schema = self.batches[0].schema();
        let combined_batch =
            arrow::compute::concat_batches(&schema, &self.batches).map_err(|e| {
                VeinError::internal(format!("Failed to concatenate record batches: {e}"))
            })?;

        let dest_uri = self
            .target
            .get_option("staging_table")
            .unwrap_or(&self.target.destination_uri)
            .to_string();
        let format = self
            .target
            .get_option("format")
            .unwrap_or("exo")
            .to_ascii_lowercase();

        if format == "parquet" {
            self.write_parquet(&dest_uri, &combined_batch)?;
        } else {
            self.write_exo(&dest_uri, &combined_batch)?;
        }

        self.batches.clear();
        Ok(())
    }
}

impl StorageSink {
    fn write_parquet(&self, dest_uri: &str, batch: &RecordBatch) -> Result<()> {
        let file_path = if dest_uri.ends_with(".parquet") {
            PathBuf::from(dest_uri)
        } else {
            PathBuf::from(format!("data/{dest_uri}.parquet"))
        };

        if let Some(parent) = file_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                VeinError::io(
                    format!("Failed to create directory '{}'", parent.display()),
                    e,
                )
            })?;
        }

        let file = File::create(&file_path).map_err(|e| {
            VeinError::io(
                format!("Failed to create Parquet file '{}'", file_path.display()),
                e,
            )
        })?;

        let mut writer = ArrowWriter::try_new(file, batch.schema(), None).map_err(|e| {
            VeinError::internal(format!("Failed to initialize Parquet writer: {e}"))
        })?;

        writer.write(batch).map_err(|e| {
            VeinError::internal(format!("Failed to write Parquet record batch: {e}"))
        })?;

        writer
            .close()
            .map_err(|e| VeinError::internal(format!("Failed to close Parquet writer: {e}")))?;

        tracing::info!(
            path = %file_path.display(),
            rows = batch.num_rows(),
            "Successfully wrote Parquet dataset"
        );
        Ok(())
    }

    fn write_exo(&self, dest_uri: &str, batch: &RecordBatch) -> Result<()> {
        let store = match &self.vfs_store {
            Some(s) => s.clone(),
            None => vfs::VfsTableStore::shared_or_default()
                .map_err(|e| VeinError::internal(format!("Failed to get VfsTableStore: {e}")))?,
        };

        let summary = if self.target.get_option("staging_table").is_some() {
            let partition_id = self.target.get_option("partition_id").ok_or_else(|| {
                VeinError::internal("staged VFS write is missing partition_id")
            })?.parse::<u32>().map_err(|error| {
                VeinError::internal(format!("invalid storage partition_id: {error}"))
            })?;
            store.write_table_sync(dest_uri, partition_id, batch)
        } else {
            store.append_partition_sync(dest_uri, batch)
        }
        .map_err(|e| {
            VeinError::internal(format!("Failed to write table '{dest_uri}' to VFS: {e}"))
        })?;

        tracing::info!(
            table = %dest_uri,
            table_id = summary.table_id,
            part_id = summary.part_id,
            rows = summary.total_rows,
            extents = summary.extents.len(),
            "Successfully wrote native table into VFS memory-mapped blocks"
        );
        Ok(())
    }
}

impl Sink<DataChunk> for StorageSink {
    fn collect(&mut self) -> Vec<RecordBatch> {
        let _ = self.flush();
        Vec::new()
    }

    fn row_count(&self) -> u64 {
        self.rows_written
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::arrow::SelectionVector;
    use common::protocol::{FieldSchema, KeyValue};

    fn make_target(table_name: &str) -> StorageTarget {
        StorageTarget::new(
            table_name.to_string(),
            vec![KeyValue {
                key: "format".to_string(),
                value: "exo".to_string(),
            }],
            vec![
                FieldSchema {
                    field_id: 0,
                    name: "id".to_string(),
                    data_type: "Int64".to_string(),
                },
                FieldSchema {
                    field_id: 1,
                    name: "name".to_string(),
                    data_type: "Utf8".to_string(),
                },
            ],
        )
    }

    /// Creates a fresh [`VfsTableStore`] backed by a temp directory.
    /// Using a tempdir ensures zero state bleeds in from previous test runs.
    fn fresh_vfs_store() -> (Arc<vfs::VfsTableStore>, tempfile::TempDir) {
        let tmp = tempfile::tempdir().expect("tempdir");
        let cfg = vfs::VfsConfig::from_data_dir(tmp.path());
        let store =
            vfs::VfsTableStore::run_async(async move { vfs::VfsTableStore::init(&cfg).await })
                .expect("vfs store init");
        (Arc::new(store), tmp)
    }

    #[test]
    fn test_storage_sink_exo() {
        let table_name = "test_sink_table_v2";
        let (store, _tmp) = fresh_vfs_store();

        // ── first flush → part_id = 0 ────────────────────────────────────────
        let mut sink = StorageSink::with_vfs_store(make_target(table_name), Some(store.clone()));
        sink.push(DataChunk {
            vectors: vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["alice", "bob", "charlie"])),
            ],
            selection: SelectionVector::new_dense(3),
            address_columns: vec![None, None],
        })
        .unwrap();
        sink.flush().unwrap();

        // ── second flush → part_id = 1  (multi-partition append) ─────────────
        let mut sink2 = StorageSink::with_vfs_store(make_target(table_name), Some(store.clone()));
        sink2
            .push(DataChunk {
                vectors: vec![
                    Arc::new(Int64Array::from(vec![4, 5])),
                    Arc::new(StringArray::from(vec!["dave", "eve"])),
                ],
                selection: SelectionVector::new_dense(2),
                address_columns: vec![None, None],
            })
            .unwrap();
        sink2.flush().unwrap();

        // Verify NO loose directories were created on disk.
        assert!(
            !std::path::Path::new(table_name).exists(),
            "No loose table directory on disk!"
        );
        assert!(
            !std::path::Path::new(&format!("data/{table_name}")).exists(),
            "No data/table directory on disk!"
        );

        // Verify both partitions exist in VFS blocks with correct row counts.
        let table_id = vfs::VfsTableStore::resolve_table_id(table_name);

        let parts = store.resolve_table_parts_sync(table_id).unwrap();
        assert_eq!(parts, vec![0, 1], "expected two partitions [0, 1]");

        let reader0 = store.open_partition_reader_sync(table_id, 0).unwrap();
        assert_eq!(reader0.total_rows(), 3, "part 0 should have 3 rows");

        let reader1 = store.open_partition_reader_sync(table_id, 1).unwrap();
        assert_eq!(reader1.total_rows(), 2, "part 1 should have 2 rows");

        // Schema should still resolve correctly.
        let schema = store.read_table_schema_sync(table_id).unwrap();
        assert_eq!(schema.fields.len(), 2);
        assert_eq!(schema.fields[0].name, "id");
        assert_eq!(schema.fields[1].name, "name");
    }
}
