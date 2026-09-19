//! VFS-backed exchange source and frame decoding.
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use common::arrow::AsyncIpcDecoder;
use common::identifiers::PartitionId;
use common::protocol::{ExchangeBatch, ExchangeFragment, ExchangeKey, FieldSchema};
use common::Result;
use error::VeinError;
use exo::AddressBatch;

use crate::exchange::SharedExchangeMetadata;
use crate::pipeline::{DataChunk, PipelineNode, Source};

/// A decoded exchange batch with optional canonical-address sidecars.
#[derive(Debug, Clone)]
pub struct DecodedExchangeBatch {
    /// Arrow values transported by the exchange.
    pub record_batch: RecordBatch,
    /// Address metadata aligned with the Arrow columns.
    pub address_columns: Vec<Option<AddressBatch>>,
}

/// Reads independently framed exchange batches from one VFS exchange partition.
pub async fn read_exchange_batches(
    store: &vfs::VfsTableStore,
    path: &str,
    expected_schema: &[FieldSchema],
) -> Result<(Vec<DecodedExchangeBatch>, u64)> {
    let frames = store
        .read_exchange_frames(path)
        .await
        .map_err(|e| VeinError::internal(format!("failed to read exchange '{path}': {e}")))?;
    let mut batches = Vec::new();
    let mut bytes_read = 0u64;

    for frame in frames {
        bytes_read = bytes_read.saturating_add(frame.len() as u64);
        let exchange = ExchangeBatch::decode(&frame)
            .map_err(|error| VeinError::internal(format!("invalid exchange batch: {error}")))?;
        let mut decoder = AsyncIpcDecoder::new();
        let decoded = decoder.push_chunk(Bytes::from(exchange.arrow_ipc))?;
        if decoded.len() != 1 {
            return Err(VeinError::internal(format!(
                "exchange frame contains {} Arrow batches, expected exactly one",
                decoded.len()
            )));
        }
        let batch = decoded
            .into_iter()
            .next()
            .ok_or_else(|| VeinError::internal("exchange frame did not contain an Arrow batch"))?;
        validate_schema(batch.schema().as_ref(), expected_schema, path)?;
        let address_columns = decode_address_columns(
            exchange.address_columns,
            batch.num_columns(),
            batch.num_rows(),
            path,
        )?;
        batches.push(DecodedExchangeBatch {
            record_batch: batch,
            address_columns,
        });
    }
    Ok((batches, bytes_read))
}

/// Reads all immutable fragments assigned to one exchange partition.
pub async fn read_exchange_fragments(
    store: &vfs::VfsTableStore,
    fragments: &[ExchangeFragment],
    expected_schema: &[FieldSchema],
) -> Result<(Vec<DecodedExchangeBatch>, u64)> {
    let mut batches = Vec::new();
    let mut bytes_read = 0u64;
    for fragment in fragments {
        let (fragment_batches, fragment_bytes) =
            read_exchange_batches(store, &fragment.storage_path, expected_schema).await?;
        batches.extend(fragment_batches);
        bytes_read = bytes_read.saturating_add(fragment_bytes);
    }
    Ok((batches, bytes_read))
}

fn decode_address_columns(
    columns: Vec<Option<common::protocol::ExchangeAddressColumn>>,
    column_count: usize,
    row_count: usize,
    path: &str,
) -> Result<Vec<Option<AddressBatch>>> {
    if columns.len() != column_count {
        return Err(VeinError::internal(format!(
            "exchange '{path}' carries {} address columns, expected {column_count}",
            columns.len()
        )));
    }

    columns
        .into_iter()
        .map(|column| {
            let Some(column) = column else {
                return Ok(None);
            };
            if column.addresses.len() != row_count {
                return Err(VeinError::internal(format!(
                    "exchange '{path}' address sidecar has {} rows, expected {row_count}",
                    column.addresses.len()
                )));
            }
            let required_bitmap_len = row_count.saturating_add(7) / 8;
            if column.validity.len() != required_bitmap_len {
                return Err(VeinError::internal(format!(
                    "exchange '{path}' address validity bitmap has {} bytes, expected {required_bitmap_len}",
                    column.validity.len()
                )));
            }
            let validity = (0..row_count)
                .map(|row| (column.validity[row / 8] & (1 << (row % 8))) != 0)
                .collect::<Vec<_>>();
            AddressBatch::from_nullable(column.addresses, &validity)
                .map(Some)
                .map_err(|error| VeinError::internal(format!("invalid exchange address sidecar: {error}")))
        })
        .collect()
}

/// A source for a single exchange partition, useful for non-join operators.
pub struct VfsExchangeSource {
    store: Arc<vfs::VfsTableStore>,
    path: String,
    schema: Vec<FieldSchema>,
    exchange_key: Option<ExchangeKey>,
    partition_id: Option<PartitionId>,
    metadata: Option<SharedExchangeMetadata>,
    batches: Option<Vec<DecodedExchangeBatch>>,
    bytes_read: u64,
}

impl VfsExchangeSource {
    /// Creates an exchange source for one deterministic partition path.
    pub fn new(
        store: Arc<vfs::VfsTableStore>,
        path: impl Into<String>,
        schema: Vec<FieldSchema>,
    ) -> Self {
        Self {
            store,
            path: path.into(),
            schema,
            exchange_key: None,
            partition_id: None,
            metadata: None,
            batches: None,
            bytes_read: 0,
        }
    }

    /// Enables VCS-backed fragment discovery for this exchange source.
    pub fn with_exchange_metadata(
        mut self,
        key: ExchangeKey,
        partition_id: PartitionId,
        metadata: SharedExchangeMetadata,
    ) -> Self {
        self.exchange_key = Some(key);
        self.partition_id = Some(partition_id);
        self.metadata = Some(metadata);
        self
    }
}

#[async_trait::async_trait(?Send)]
impl Source<DataChunk> for VfsExchangeSource {
    async fn execute(&mut self, next: &mut dyn PipelineNode<DataChunk>) -> Result<()> {
        let (batches, bytes_read) = match (&self.metadata, self.exchange_key, self.partition_id) {
            (Some(metadata), Some(exchange_key), Some(partition_id)) => {
                let fragments = metadata.get_partition(exchange_key, partition_id)?;
                read_exchange_fragments(&self.store, &fragments, &self.schema).await?
            }
            _ => read_exchange_batches(&self.store, &self.path, &self.schema).await?,
        };
        self.bytes_read = bytes_read;
        self.batches = Some(batches);

        if let Some(batches) = self.batches.take() {
            for batch in batches {
                next.push(DataChunk::from_vectors_with_addresses(
                    batch.record_batch.columns().to_vec(),
                    batch.address_columns,
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

fn validate_schema(
    schema: &arrow::datatypes::Schema,
    expected: &[FieldSchema],
    path: &str,
) -> Result<()> {
    if expected.is_empty() {
        return Ok(());
    }
    if schema.fields().len() != expected.len() {
        return Err(VeinError::internal(format!(
            "exchange '{path}' schema has {} columns, expected {}",
            schema.fields().len(),
            expected.len()
        )));
    }
    for (actual, expected) in schema.fields().iter().zip(expected) {
        if actual.name() != expected.name.as_str() {
            return Err(VeinError::internal(format!(
                "exchange '{path}' column '{}' does not match expected '{}'",
                actual.name(),
                expected.name
            )));
        }
    }
    Ok(())
}
