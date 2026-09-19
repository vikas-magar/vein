//! Partition-local hash join source over VFS exchange partitions.
use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::UInt32Array;
use arrow::compute::take;
use common::identifiers::PartitionId;
use common::protocol::{
    ExchangeInput, FieldSchema, JoinKeyMode, JoinKeySpec, JoinSource as ProtocolJoinSource,
    PhysicalJoinType,
};
use common::Result;
use error::VeinError;
use exo::AddressBatch;

use crate::operators::join::{
    try_extract_address_keys, ArrowJoinKeyExtractor, HashJoinTable, JoinKeyBatch, JoinKeyExtractor,
};
use crate::exchange::SharedExchangeMetadata;
use crate::operators::source::exchange::{
    read_exchange_batches, read_exchange_fragments, DecodedExchangeBatch,
};
use crate::pipeline::{DataChunk, PipelineNode, Source};

/// Executes one partition-local physical join.
pub struct HashJoinSource {
    source: ProtocolJoinSource,
    store: Arc<vfs::VfsTableStore>,
    metadata: Option<SharedExchangeMetadata>,
    bytes_read: u64,
}

impl HashJoinSource {
    /// Creates a join source with an injected VFS store.
    pub fn with_store(source: ProtocolJoinSource, store: Arc<vfs::VfsTableStore>) -> Self {
        Self {
            source,
            store,
            metadata: None,
            bytes_read: 0,
        }
    }

    /// Creates a join source with VCS-backed exchange discovery.
    pub fn with_store_and_metadata(
        source: ProtocolJoinSource,
        store: Arc<vfs::VfsTableStore>,
        metadata: SharedExchangeMetadata,
    ) -> Self {
        Self {
            source,
            store,
            metadata: Some(metadata),
            bytes_read: 0,
        }
    }

    async fn read_input(
        &self,
        input: &ExchangeInput,
        schema: &[FieldSchema],
    ) -> Result<(Vec<DecodedExchangeBatch>, u64)> {
        if let Some(metadata) = &self.metadata {
            let fragments = metadata.get_partition(input.exchange_key, PartitionId::from_raw(input.partition_id as u16))?;
            return read_exchange_fragments(&self.store, &fragments, schema).await;
        }
        read_exchange_batches(&self.store, &Self::partition_path(input), schema).await
    }

    fn partition_path(input: &ExchangeInput) -> String {
        format!("{}/partition_{:05}", input.path_prefix, input.partition_id)
    }

    fn key_columns(schema: &[FieldSchema], keys: &[JoinKeySpec]) -> Result<Vec<usize>> {
        keys.iter()
            .map(|key| {
                schema
                    .iter()
                    .position(|field| field.field_id == key.field_id || field.name == key.column_name)
                    .ok_or_else(|| {
                        VeinError::internal(format!(
                            "join key '{}' is absent from exchange schema",
                            key.column_name
                        ))
                    })
            })
            .collect()
    }

    fn chunk_for_batch(batch: &DecodedExchangeBatch) -> Result<DataChunk> {
        DataChunk::from_vectors_with_addresses(
            batch.record_batch.columns().to_vec(),
            batch.address_columns.clone(),
        )
    }

    fn extract_keys(
        chunk: &DataChunk,
        key_columns: &[usize],
        key_specs: &[JoinKeySpec],
    ) -> Result<JoinKeyBatch> {
        let address_requested = key_specs
            .iter()
            .all(|spec| !matches!(spec.mode, JoinKeyMode::ValueNative));
        if address_requested {
            if let Some(keys) = try_extract_address_keys(chunk, key_columns)? {
                return Ok(keys);
            }
            if key_specs
                .iter()
                .any(|spec| matches!(spec.mode, JoinKeyMode::AddressNative))
            {
                return Err(VeinError::internal(
                    "address-native join key requested but source did not provide address sidecars",
                ));
            }
        }
        ArrowJoinKeyExtractor.extract(chunk, key_columns)
    }

    fn materialize_addresses(
        columns: &[Option<AddressBatch>],
        rows: &[u32],
    ) -> Result<Vec<Option<AddressBatch>>> {
        columns
            .iter()
            .map(|column| {
                let Some(column) = column else {
                    return Ok(None);
                };
                let mut addresses = Vec::with_capacity(rows.len());
                let mut validity = Vec::with_capacity(rows.len());
                for &row in rows {
                    let Some((address, valid)) = column.get(row as usize) else {
                        return Err(VeinError::internal(
                            "join address sidecar row is out of bounds",
                        ));
                    };
                    addresses.push(address);
                    validity.push(valid);
                }
                AddressBatch::from_nullable(addresses, &validity)
                    .map(Some)
                    .map_err(|error| VeinError::internal(format!("join address sidecar failed: {error}")))
            })
            .collect()
    }

    fn materialize_output(
        left: &DecodedExchangeBatch,
        right: &DecodedExchangeBatch,
        left_rows: &[u32],
        right_rows: &[u32],
    ) -> Result<DataChunk> {
        let left_indices = UInt32Array::from(left_rows.to_vec());
        let right_indices = UInt32Array::from(right_rows.to_vec());
        let mut vectors = Vec::with_capacity(
            left.record_batch.num_columns() + right.record_batch.num_columns(),
        );

        for column in left.record_batch.columns() {
            vectors.push(
                take(column.as_ref(), &left_indices, None)
                    .map_err(|e| VeinError::internal(format!("join left materialization failed: {e}")))?,
            );
        }
        for column in right.record_batch.columns() {
            vectors.push(
                take(column.as_ref(), &right_indices, None)
                    .map_err(|e| VeinError::internal(format!("join right materialization failed: {e}")))?,
            );
        }

        let mut address_columns =
            Self::materialize_addresses(&left.address_columns, left_rows)?;
        address_columns.extend(Self::materialize_addresses(
            &right.address_columns,
            right_rows,
        )?);
        DataChunk::from_vectors_with_addresses(vectors, address_columns)
    }
}

#[async_trait::async_trait(?Send)]
impl Source<DataChunk> for HashJoinSource {
    async fn execute(&mut self, next: &mut dyn PipelineNode<DataChunk>) -> Result<()> {
        if !matches!(self.source.join_type, PhysicalJoinType::Inner) {
            return Err(VeinError::internal(
                "only INNER JOIN execution is implemented in the partition-local join source",
            ));
        }
        if self.source.left.partition_count != self.source.right.partition_count
            || self.source.left.partition_id != self.source.right.partition_id
        {
            return Err(VeinError::internal(
                "join inputs are not partition aligned",
            ));
        }
        if !self.source.is_equi_join() {
            return Err(VeinError::internal(
                "partition-local join requires aligned equi-join keys",
            ));
        }

        let (left_batches, left_bytes) = self
            .read_input(&self.source.left, &self.source.left_schema)
            .await?;
        let (right_batches, right_bytes) = self
            .read_input(&self.source.right, &self.source.right_schema)
            .await?;
        self.bytes_read = left_bytes.saturating_add(right_bytes);

        let left_columns = Self::key_columns(&self.source.left_schema, &self.source.left_keys)?;
        let right_columns = Self::key_columns(&self.source.right_schema, &self.source.right_keys)?;

        let mut table = HashJoinTable::new();
        for (batch_id, batch) in left_batches.iter().enumerate() {
            let chunk = Self::chunk_for_batch(batch)?;
            let keys = Self::extract_keys(&chunk, &left_columns, &self.source.left_keys)?;
            table.insert_batch(batch_id as u32, &keys)?;
        }

        for (probe_id, right_batch) in right_batches.iter().enumerate() {
            let probe_chunk = Self::chunk_for_batch(right_batch)?;
            let probe_keys =
                Self::extract_keys(&probe_chunk, &right_columns, &self.source.right_keys)?;
            let matches = table.probe_batch(probe_id as u32, &probe_keys);

            let mut grouped: HashMap<u32, Vec<(u32, u32)>> = HashMap::new();
            for item in matches {
                grouped
                    .entry(item.build_row.batch_id)
                    .or_default()
                    .push((item.build_row.row_index, item.probe_row.row_index));
            }

            for (left_batch_id, pairs) in grouped {
                let left_batch = left_batches
                    .get(left_batch_id as usize)
                    .ok_or_else(|| VeinError::internal("join build batch reference is invalid"))?;
                let mut left_rows = Vec::with_capacity(pairs.len());
                let mut right_rows = Vec::with_capacity(pairs.len());
                for (left_row, right_row) in pairs {
                    left_rows.push(left_row);
                    right_rows.push(right_row);
                }
                next.push(Self::materialize_output(
                    left_batch,
                    right_batch,
                    &left_rows,
                    &right_rows,
                )?)?;
            }
        }

        next.flush()?;
        Ok(())
    }

    fn bytes_read(&self) -> u64 {
        self.bytes_read
    }
}
