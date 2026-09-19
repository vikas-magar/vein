//! Hash-partitioned VFS exchange sink.
use std::sync::Arc;

use arrow::array::{ArrayRef, UInt32Array};
use arrow::compute::take;
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use common::arrow::IpcStreamEncoder;
use common::protocol::{ExchangeAddressColumn, ExchangeBatch, JoinKeyMode, ShuffleTarget};
use common::Result;
use error::VeinError;
use exo::AddressBatch;

use crate::exchange::SharedExchangeMetadata;
use crate::operators::join::{
    try_extract_address_keys, ArrowJoinKeyExtractor, JoinKeyExtractor, JoinPartitioner,
};
use crate::pipeline::{DataChunk, PipelineNode, Sink};

fn stable_payload_hash(payload: &[u8]) -> u64 {
    payload.iter().fold(0xcbf29ce484222325u64, |hash, byte| {
        (hash ^ *byte as u64).wrapping_mul(0x100000001b3)
    })
}

/// Writes selected input rows into deterministic, append-only VFS exchange partitions.
///
/// Each input chunk becomes one independently decodable Arrow IPC frame. This
/// lets multiple upstream tasks append concurrently without sharing an Arrow
/// stream writer or buffering an entire stage in memory.
#[derive(Default)]
struct PartitionStats {
    byte_len: u64,
    row_count: u64,
    checksum: u64,
}

/// Writes selected input rows into deterministic VFS exchange partitions.
pub struct ShuffleSink {
    target: ShuffleTarget,
    vfs_store: Option<Arc<vfs::VfsTableStore>>,
    metadata: Option<SharedExchangeMetadata>,
    key_columns: Option<Vec<usize>>,
    partition_stats: Vec<PartitionStats>,
}

impl ShuffleSink {
    /// Creates a shuffle sink backed by the shared VFS store.
    pub fn new(target: ShuffleTarget) -> Self {
        Self::with_vfs_store_and_metadata(target, None, None)
    }

    /// Creates a shuffle sink with an injected store for tests and embedding.
    pub fn with_vfs_store(
        target: ShuffleTarget,
        vfs_store: Option<Arc<vfs::VfsTableStore>>,
    ) -> Self {
        Self::with_vfs_store_and_metadata(target, vfs_store, None)
    }

    /// Creates a shuffle sink with VCS metadata publication.
    pub fn with_vfs_store_and_metadata(
        target: ShuffleTarget,
        vfs_store: Option<Arc<vfs::VfsTableStore>>,
        metadata: Option<SharedExchangeMetadata>,
    ) -> Self {
        let partition_stats = (0..target.num_partitions)
            .map(|_| PartitionStats::default())
            .collect();
        Self {
            target,
            vfs_store,
            metadata,
            key_columns: None,
            partition_stats,
        }
    }

    fn store(&self) -> Result<Arc<vfs::VfsTableStore>> {
        match &self.vfs_store {
            Some(store) => Ok(store.clone()),
            None => vfs::VfsTableStore::shared_or_default()
                .map_err(|e| VeinError::internal(format!("failed to open VFS exchange store: {e}"))),
        }
    }

    fn resolve_key_columns(&mut self, width: usize) -> Result<&[usize]> {
        if self.key_columns.is_none() {
            let columns = self
                .target
                .partition_keys
                .iter()
                .map(|key| {
                    self.target
                        .schema
                        .iter()
                        .position(|field| field.field_id == key.field_id || field.name == key.column_name)
                        .ok_or_else(|| {
                            VeinError::internal(format!(
                                "shuffle key '{}' is absent from exchange schema",
                                key.column_name
                            ))
                        })
                })
                .collect::<Result<Vec<_>>>()?;
            if columns.iter().any(|&column| column >= width) {
                return Err(VeinError::internal("shuffle key column is out of bounds"));
            }
            self.key_columns = Some(columns);
        }
        self.key_columns
            .as_deref()
            .ok_or_else(|| VeinError::internal("shuffle key columns were not initialized"))
    }

    fn partition_path(&self, partition: u16) -> String {
        if self.metadata.is_some() {
            format!(
                "{}/task_{}/partition_{partition:05}",
                self.target.vfs_path_prefix, self.target.producer_task_id
            )
        } else {
            format!("{}/partition_{partition:05}", self.target.vfs_path_prefix)
        }
    }

    fn materialize_batch(&self, chunk: &DataChunk, indices: &[u32]) -> Result<RecordBatch> {
        let index_array = UInt32Array::from(indices.to_vec());
        let columns: Vec<ArrayRef> = chunk
            .vectors
            .iter()
            .map(|array| {
                take(array.as_ref(), &index_array, None)
                    .map_err(|e| VeinError::internal(format!("shuffle row materialization failed: {e}")))
            })
            .collect::<Result<_>>()?;

        let fields: Vec<Field> = if self.target.schema.len() == columns.len() {
            self.target
                .schema
                .iter()
                .zip(columns.iter())
                .map(|(field, array)| {
                    Field::new(field.name.clone(), array.data_type().clone(), true)
                })
                .collect()
        } else {
            columns
                .iter()
                .enumerate()
                .map(|(index, array)| Field::new(format!("col_{index}"), array.data_type().clone(), true))
                .collect()
        };
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .map_err(|e| VeinError::internal(format!("shuffle record batch failed: {e}")))
    }

    fn materialize_addresses(
        &self,
        chunk: &DataChunk,
        indices: &[u32],
    ) -> Result<Vec<Option<ExchangeAddressColumn>>> {
        chunk
            .address_columns
            .iter()
            .map(|sidecar| {
                let Some(sidecar) = sidecar else {
                    return Ok(None);
                };
                let mut addresses = Vec::with_capacity(indices.len());
                let mut validity = Vec::with_capacity(indices.len());
                for &index in indices {
                    let Some((address, valid)) = sidecar.get(index as usize) else {
                        return Err(VeinError::internal(
                            "shuffle address sidecar row is out of bounds",
                        ));
                    };
                    addresses.push(address);
                    validity.push(valid);
                }
                let selected = AddressBatch::from_nullable(addresses, &validity)
                    .map_err(|error| VeinError::internal(format!("shuffle address sidecar failed: {error}")))?;
                let (addresses, validity) = selected.into_parts();
                Ok(Some(ExchangeAddressColumn { addresses, validity }))
            })
            .collect()
    }
}

impl PipelineNode<DataChunk> for ShuffleSink {
    fn push(&mut self, chunk: DataChunk) -> Result<()> {
        if chunk.is_empty() {
            return Ok(());
        }

        let key_columns = self.resolve_key_columns(chunk.vectors.len())?.to_vec();
        let address_requested = self
            .target
            .partition_keys
            .iter()
            .all(|key| !matches!(key.mode, JoinKeyMode::ValueNative));
        let key_batch = if address_requested {
            match try_extract_address_keys(&chunk, &key_columns)? {
                Some(batch) => batch,
                None if self
                    .target
                    .partition_keys
                    .iter()
                    .any(|key| matches!(key.mode, JoinKeyMode::AddressNative)) => {
                    return Err(VeinError::internal(
                        "address-native shuffle key requested but source did not provide address sidecars",
                    ));
                }
                None => ArrowJoinKeyExtractor.extract(&chunk, &key_columns)?,
            }
        } else {
            ArrowJoinKeyExtractor.extract(&chunk, &key_columns)?
        };
        let partitioner = JoinPartitioner::new(self.target.num_partitions)?;
        let mut row_partitions = (0..self.target.num_partitions)
            .map(|_| Vec::<u32>::new())
            .collect::<Vec<_>>();

        for row in key_batch.rows() {
            let partition = partitioner.partition_for_hash(row.hash) as usize;
            row_partitions[partition].push(row.row_index);
        }

        let store = self.store()?;
        for (partition, indices) in row_partitions.iter().enumerate() {
            if indices.is_empty() {
                continue;
            }
            let batch = self.materialize_batch(&chunk, indices)?;
            let mut encoder = IpcStreamEncoder::new();
            encoder.write_batch(&batch)?;
            let arrow_ipc = encoder.finish()?;
            let exchange_batch = ExchangeBatch {
                arrow_ipc,
                address_columns: self.materialize_addresses(&chunk, indices)?,
            };
            let payload = exchange_batch
                .encode()
                .map_err(|error| VeinError::internal(format!("failed to encode shuffle batch: {error}")))?;
            store.append_exchange_frame_sync(&self.partition_path(partition as u16), &payload)
                .map_err(|e| VeinError::internal(format!("failed to append shuffle frame: {e}")))?;
            let stats = self
                .partition_stats
                .get_mut(partition)
                .ok_or_else(|| VeinError::internal("shuffle partition statistics are out of bounds"))?;
            stats.byte_len = stats.byte_len.saturating_add(payload.len() as u64);
            stats.row_count = stats.row_count.saturating_add(indices.len() as u64);
            stats.checksum = stats.checksum.rotate_left(5) ^ stable_payload_hash(&payload);
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        let Some(metadata) = &self.metadata else {
            return Ok(());
        };

        metadata.create_exchange(
            self.target.exchange_key,
            self.target.num_partitions,
            self.target.expected_tasks.clone(),
            self.target.schema_fingerprint.clone(),
        )?;

        for partition in 0..self.target.num_partitions {
            let stats = self
                .partition_stats
                .get(partition as usize)
                .ok_or_else(|| VeinError::internal("shuffle partition statistics are out of bounds"))?;
            metadata.publish_fragment(
                self.target.exchange_key,
                common::protocol::ExchangeFragment {
                    partition_id: partition as u32,
                    task_id: self.target.producer_task_id,
                    attempt: self.target.attempt,
                    storage_path: self.partition_path(partition),
                    node_id: None,
                    byte_len: stats.byte_len,
                    row_count: stats.row_count,
                    checksum: stats.checksum,
                },
            )?;
        }
        Ok(())
    }
}

impl Sink<DataChunk> for ShuffleSink {}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use common::arrow::SelectionVector;
    use common::identifiers::{QueryId, StageId, TaskId};
    use common::protocol::{ExchangeKey, FieldSchema, JoinKeyMode, JoinKeySpec};

    fn target() -> ShuffleTarget {
        let task_id = TaskId::from_raw(2);
        ShuffleTarget {
            exchange_key: ExchangeKey {
                query_id: QueryId::from_raw(1),
                stage_id: StageId::from_raw(1),
            },
            vfs_path_prefix: "test-exchange".to_string(),
            producer_task_id: task_id,
            attempt: 0,
            expected_tasks: vec![task_id],
            schema_fingerprint: "test".to_string(),
            num_partitions: 2,
            partition_keys: vec![JoinKeySpec {
                field_id: 0,
                column_name: "id".to_string(),
                mode: JoinKeyMode::ValueNative,
            }],
            schema: vec![
                FieldSchema { field_id: 0, name: "id".to_string(), data_type: "Int64".to_string() },
                FieldSchema { field_id: 1, name: "value".to_string(), data_type: "Utf8".to_string() },
            ],
        }
    }

    #[test]
    fn resolves_keys_and_partitions_selected_rows() {
        let mut sink = ShuffleSink::with_vfs_store(target(), None);
        let chunk = DataChunk {
            vectors: vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
                Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef,
            ],
            selection: SelectionVector::new_sparse(vec![0, 2]),
            address_columns: vec![None, None],
        };
        let keys = sink.resolve_key_columns(chunk.vectors.len()).unwrap();
        assert_eq!(keys, &[0]);
        let extracted = ArrowJoinKeyExtractor.extract(&chunk, keys).unwrap();
        assert_eq!(extracted.len(), 2);
    }
}
