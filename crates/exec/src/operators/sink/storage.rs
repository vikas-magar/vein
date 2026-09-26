//! Storage sink operator writing pipeline output to files or VFS tables (.exo, .parquet).

use std::fs::File;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use arrow::array::*;
use arrow::compute::take;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use common::Result;
use common::protocol::StorageTarget;
use distributed::{ArtifactId, ArtifactRef, TableWritePart};
use error::VeinError;
use parquet::arrow::ArrowWriter;

use crate::pipeline::{DataChunk, PipelineNode, Sink};

/// A terminal pipeline sink writing rows to storage (.exo or .parquet).
pub struct StorageSink {
    target: StorageTarget,
    exo_writer: Option<vfs::StreamingPartitionWriter>,
    parquet_writer: Option<Mutex<ArrowWriter<File>>>,
    field_names: Vec<String>,
    vfs_store: Option<Arc<vfs::VfsTableStore>>,
    rows_written: u64,
    write_parts: Vec<TableWritePart>,
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
            exo_writer: None,
            parquet_writer: None,
            field_names,
            vfs_store,
            rows_written: 0,
            write_parts: Vec::new(),
        }
    }
}

impl PipelineNode<DataChunk> for StorageSink {
    fn push(&mut self, chunk: DataChunk) -> Result<()> {
        let selected = chunk.selection.size();
        if selected == 0 {
            return Ok(());
        }

        let contiguous = chunk.selection.contiguous_range();
        let indices = contiguous.is_none().then(|| {
            let mut builder = UInt32Array::builder(selected);
            for i in 0..selected {
                builder.append_value(chunk.selection.get_index(i) as u32);
            }
            builder.finish()
        });

        let mut fields = Vec::with_capacity(chunk.vectors.len());
        let mut columns = Vec::with_capacity(chunk.vectors.len());
        for (i, vec) in chunk.vectors.iter().enumerate() {
            let col_name = self
                .field_names
                .get(i)
                .cloned()
                .unwrap_or_else(|| format!("col_{i}"));
            fields.push(Field::new(col_name, vec.data_type().clone(), true));
            let array = match (&contiguous, &indices) {
                (Some(range), _) => vec.slice(range.start, range.len()),
                (None, Some(indices)) => take(vec.as_ref(), indices, None)
                    .map_err(|e| VeinError::internal(format!("storage sink take failed: {e}")))?,
                (None, None) => {
                    return Err(VeinError::internal(
                        "storage sink selection indices were not materialized",
                    ));
                }
            };
            columns.push(array);
        }

        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema, columns)
            .map_err(|e| VeinError::internal(format!("storage sink record batch failed: {e}")))?;
        if self.output_format() == "parquet" {
            self.write_parquet_batch(&batch)?;
        } else {
            if self.exo_writer.is_none() {
                let dest_uri = self
                    .target
                    .get_option("staging_table")
                    .unwrap_or(&self.target.destination_uri)
                    .to_string();
                let partition_id = self
                    .target
                    .get_option("partition_id")
                    .map(|value| {
                        value.parse::<u32>().map_err(|error| {
                            VeinError::internal(format!("invalid storage partition_id: {error}"))
                        })
                    })
                    .transpose()?;
                let store = match &self.vfs_store {
                    Some(store) => store.clone(),
                    None => vfs::VfsTableStore::shared_or_default().map_err(|error| {
                        VeinError::internal(format!("Failed to get VfsTableStore: {error}"))
                    })?,
                };
                self.exo_writer = Some(
                    store
                        .begin_partition_writer(&dest_uri, partition_id, batch.schema())
                        .map_err(|error| {
                            VeinError::internal(format!(
                                "Failed to initialize VFS partition writer for '{dest_uri}': {error}"
                            ))
                        })?,
                );
            }
            self.exo_writer
                .as_mut()
                .ok_or_else(|| VeinError::internal("EXO partition writer was not initialized"))?
                .append_batch(&batch)
                .map_err(|error| {
                    VeinError::internal(format!("Failed to append EXO output batch: {error}"))
                })?;
        }
        self.rows_written = self
            .rows_written
            .checked_add(batch.num_rows() as u64)
            .ok_or_else(|| VeinError::internal("storage sink row count overflow"))?;
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if let Some(writer) = self.parquet_writer.take() {
            let writer = writer.into_inner().map_err(|error| {
                VeinError::internal(format!("Parquet writer lock poisoned: {error}"))
            })?;
            let metadata = writer.close().map_err(|error| {
                VeinError::internal(format!("Failed to close Parquet writer: {error}"))
            })?;
            tracing::info!(
                rows = self.rows_written,
                row_groups = metadata.num_row_groups(),
                "Successfully wrote Parquet dataset"
            );
            return Ok(());
        }

        let Some(writer) = self.exo_writer.take() else {
            tracing::info!(target: "storage_sink", "No batches to persist");
            return Ok(());
        };
        let dest_uri = self
            .target
            .get_option("staging_table")
            .unwrap_or(&self.target.destination_uri)
            .to_string();
        let summary = writer.finish_sync().map_err(|error| {
            VeinError::internal(format!(
                "Failed to commit VFS partition '{dest_uri}': {error}"
            ))
        })?;
        self.write_exo(&dest_uri, summary)?;
        Ok(())
    }
}

impl StorageSink {
    fn output_format(&self) -> String {
        self.target
            .get_option("format")
            .unwrap_or("exo")
            .to_ascii_lowercase()
    }

    fn write_parquet_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        if self.parquet_writer.is_none() {
            let dest_uri = self
                .target
                .get_option("staging_table")
                .unwrap_or(&self.target.destination_uri);
            let file_path = if dest_uri.ends_with(".parquet") {
                PathBuf::from(dest_uri)
            } else {
                PathBuf::from(format!("data/{dest_uri}.parquet"))
            };

            if let Some(parent) = file_path.parent() {
                std::fs::create_dir_all(parent).map_err(|error| {
                    VeinError::io(
                        format!("Failed to create directory '{}'", parent.display()),
                        error,
                    )
                })?;
            }
            let file = File::create(&file_path).map_err(|error| {
                VeinError::io(
                    format!("Failed to create Parquet file '{}'", file_path.display()),
                    error,
                )
            })?;
            self.parquet_writer = Some(Mutex::new(
                ArrowWriter::try_new(file, batch.schema(), None).map_err(|error| {
                    VeinError::internal(format!("Failed to initialize Parquet writer: {error}"))
                })?,
            ));
        }

        self.parquet_writer
            .as_ref()
            .ok_or_else(|| VeinError::internal("Parquet writer was not initialized"))?
            .lock()
            .map_err(|error| VeinError::internal(format!("Parquet writer lock poisoned: {error}")))?
            .write(batch)
            .map_err(|error| {
                VeinError::internal(format!("Failed to write Parquet record batch: {error}"))
            })
    }

    fn write_exo(&mut self, dest_uri: &str, summary: vfs::VfsPartitionSummary) -> Result<()> {
        if let Some(partition_id) = self
            .target
            .get_option("partition_id")
            .map(|value| {
                value.parse::<u32>().map_err(|error| {
                    VeinError::internal(format!("invalid storage partition_id: {error}"))
                })
            })
            .transpose()?
        {
            if summary.part_id != partition_id {
                return Err(VeinError::internal(format!(
                    "VFS committed partition {}, expected staged partition {partition_id}",
                    summary.part_id
                )));
            }
            let write_id = self
                .target
                .get_option("write_id")
                .ok_or_else(|| VeinError::internal("staged VFS write is missing write_id"))?;
            let staging_table = self
                .target
                .get_option("staging_table")
                .ok_or_else(|| VeinError::internal("staged VFS write is missing staging_table"))?;
            self.write_parts.push(TableWritePart {
                partition_id,
                artifact: ArtifactRef {
                    id: ArtifactId::from(format!("{write_id}:part:{partition_id}")),
                    uri: format!("vfs://{staging_table}/part_{partition_id:06}"),
                    checksum: format!(
                        "{}:{}:{}",
                        summary.table_id, partition_id, summary.total_rows
                    ),
                    size_bytes: summary.extents.iter().map(|extent| extent.length).sum(),
                },
                row_count: summary.total_rows,
            });
        }

        tracing::info!(
            table = %dest_uri,
            table_id = summary.table_id,
            part_id = summary.part_id,
            rows = summary.total_rows,
            extents = summary.extents.len(),
            "Successfully committed streamed EXO partition into VFS"
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

    fn write_parts(&mut self) -> Vec<TableWritePart> {
        std::mem::take(&mut self.write_parts)
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
        sink.push(DataChunk {
            vectors: vec![
                Arc::new(Int64Array::from(vec![4, 5])),
                Arc::new(StringArray::from(vec!["dave", "eve"])),
            ],
            selection: SelectionVector::new_dense(2),
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
        assert_eq!(
            reader0.total_rows(),
            5,
            "part 0 should contain rows from both input batches"
        );

        let reader1 = store.open_partition_reader_sync(table_id, 1).unwrap();
        assert_eq!(reader1.total_rows(), 2, "part 1 should have 2 rows");

        // Schema should still resolve correctly.
        let schema = store.read_table_schema_sync(table_id).unwrap();
        assert_eq!(schema.fields.len(), 2);
        assert_eq!(schema.fields[0].name, "id");
        assert_eq!(schema.fields[1].name, "name");
    }
}
