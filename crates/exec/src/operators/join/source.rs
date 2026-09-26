//! Partition-local hash join source over VFS exchange partitions.
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::UInt32Array;
use arrow::compute::take;
use common::Result;
use common::arrow::IpcStreamEncoder;
use common::identifiers::PartitionId;
use common::protocol::{ExchangeAddressColumn, ExchangeBatch, SpillRun, SpillRunState};
use common::protocol::{
    ExchangeInput, FieldSchema, JoinKeyMode, JoinKeySpec, JoinSource as ProtocolJoinSource,
    PhysicalJoinType,
};
use error::VeinError;
use exo::AddressBatch;

use crate::exchange::SharedExchangeMetadata;
use crate::operators::join::{
    ArrowJoinKeyExtractor, HashJoinTable, JoinKeyBatch, JoinKeyExtractor, JoinKeyRow,
    JoinMemoryBudget, JoinMemoryReservation, JoinSpillIdentity, JoinSpillWriter,
    VfsJoinSpillWriter, new_attempt_spill_path, new_spill_path, try_extract_address_keys,
};
use crate::operators::source::exchange::{DecodedExchangeBatch, decode_exchange_frame};
use crate::pipeline::{DataChunk, PipelineNode, Source};

const SPILL_PARTITION_COUNT: usize = 32;

/// Executes one partition-local physical join.
pub struct HashJoinSource {
    source: ProtocolJoinSource,
    store: Arc<vfs::VfsTableStore>,
    metadata: Option<SharedExchangeMetadata>,
    spill_identity: Option<JoinSpillIdentity>,
    bytes_read: u64,
}

impl HashJoinSource {
    /// Creates a join source with an injected VFS store.
    pub fn with_store(source: ProtocolJoinSource, store: Arc<vfs::VfsTableStore>) -> Self {
        Self {
            source,
            store,
            metadata: None,
            spill_identity: None,
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
            spill_identity: None,
            bytes_read: 0,
        }
    }

    /// Associates the source with its executor task attempt for spill ownership.
    pub fn with_spill_identity(mut self, identity: JoinSpillIdentity) -> Self {
        self.spill_identity = Some(identity);
        self
    }

    fn input_paths(&self, input: &ExchangeInput) -> Result<Vec<String>> {
        if let Some(metadata) = &self.metadata {
            return Ok(metadata
                .get_partition(
                    input.exchange_key,
                    PartitionId::from_raw(input.partition_id as u16),
                )?
                .into_iter()
                .map(|fragment| fragment.storage_path)
                .collect());
        }
        Ok(vec![Self::partition_path(input)])
    }

    fn partition_path(input: &ExchangeInput) -> String {
        format!("{}/partition_{:05}", input.path_prefix, input.partition_id)
    }

    fn key_columns(schema: &[FieldSchema], keys: &[JoinKeySpec]) -> Result<Vec<usize>> {
        keys.iter()
            .map(|key| {
                schema
                    .iter()
                    .position(|field| {
                        field.field_id == key.field_id || field.name == key.column_name
                    })
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
                    .map_err(|error| {
                        VeinError::internal(format!("join address sidecar failed: {error}"))
                    })
            })
            .collect()
    }

    fn encode_selected_spill_frame(batch: &DecodedExchangeBatch, rows: &[u32]) -> Result<Vec<u8>> {
        let indices = UInt32Array::from(rows.to_vec());
        let columns = batch
            .record_batch
            .columns()
            .iter()
            .map(|column| {
                take(column.as_ref(), &indices, None).map_err(|error| {
                    VeinError::internal(format!("join spill materialization failed: {error}"))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let selected =
            arrow::record_batch::RecordBatch::try_new(batch.record_batch.schema(), columns)
                .map_err(|error| {
                    VeinError::internal(format!("join spill batch failed: {error}"))
                })?;
        let mut encoder = IpcStreamEncoder::new();
        encoder.write_batch(&selected)?;
        let address_columns = Self::materialize_addresses(&batch.address_columns, rows)?
            .into_iter()
            .map(|column| {
                column.map(|column| {
                    let (addresses, validity) = column.into_parts();
                    ExchangeAddressColumn {
                        addresses,
                        validity,
                    }
                })
            })
            .collect();
        ExchangeBatch {
            arrow_ipc: encoder.finish()?,
            address_columns,
        }
        .encode()
        .map_err(|error| VeinError::internal(format!("join spill encoding failed: {error}")))
    }

    fn spill_build_batch(
        writer: &dyn JoinSpillWriter,
        prefix: &str,
        partition_id: u32,
        identity: Option<JoinSpillIdentity>,
        batch: &DecodedExchangeBatch,
        keys: &JoinKeyBatch,
        paths: &mut [Option<String>],
        stats: &mut HashMap<String, (u64, u64, u64)>,
    ) -> Result<()> {
        let mut rows = (0..SPILL_PARTITION_COUNT)
            .map(|_| Vec::<u32>::new())
            .collect::<Vec<_>>();
        for row in keys.matching_rows() {
            rows[(row.hash as usize) % SPILL_PARTITION_COUNT].push(row.row_index);
        }

        for (subpartition, row_indices) in rows.iter().enumerate() {
            if row_indices.is_empty() {
                continue;
            }
            let path = paths[subpartition].get_or_insert_with(|| match identity {
                Some(identity) => new_attempt_spill_path(
                    &format!("{prefix}/subpartition_{subpartition:03}"),
                    partition_id,
                    identity,
                ),
                None => new_spill_path(
                    &format!("{prefix}/subpartition_{subpartition:03}"),
                    partition_id,
                ),
            });
            let frame = Self::encode_selected_spill_frame(batch, row_indices)?;
            writer.append_frame(path, &frame)?;
            let entry = stats.entry(path.clone()).or_default();
            entry.0 = entry.0.saturating_add(frame.len() as u64);
            entry.1 = entry.1.saturating_add(row_indices.len() as u64);
            entry.2 = entry.2.rotate_left(5) ^ Self::spill_checksum(&frame);
        }
        Ok(())
    }

    fn spill_checksum(frame: &[u8]) -> u64 {
        frame.iter().fold(0xcbf29ce484222325u64, |hash, byte| {
            (hash ^ *byte as u64).wrapping_mul(0x100000001b3)
        })
    }

    fn publish_spills(
        &self,
        paths: &[Option<String>],
        stats: &HashMap<String, (u64, u64, u64)>,
    ) -> Result<()> {
        let (Some(metadata), Some(identity)) = (&self.metadata, self.spill_identity) else {
            return Ok(());
        };
        let lease_until_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| VeinError::internal(format!("spill lease clock failed: {error}")))?
            .as_millis()
            .saturating_add(3_600_000) as u64;

        for (subpartition_id, path) in paths.iter().enumerate() {
            let Some(path) = path else {
                continue;
            };
            let (byte_len, row_count, checksum) = stats
                .get(path)
                .copied()
                .ok_or_else(|| VeinError::internal("join spill statistics are missing"))?;
            let spill = SpillRun {
                exchange_key: self.source.left.exchange_key,
                task_id: identity.task_id,
                attempt: identity.attempt,
                partition_id: self.source.left.partition_id,
                subpartition_id: subpartition_id as u32,
                storage_path: path.clone(),
                byte_len,
                row_count,
                checksum,
                state: SpillRunState::Open,
                lease_until_ms,
            };
            metadata.register_spill(spill)?;
            if let Err(error) = metadata.seal_spill(path.clone()) {
                let _ = metadata.abort_spill(path.clone());
                return Err(error);
            }
        }
        Ok(())
    }

    fn discover_spills(&self, fallback: &[Option<String>]) -> Result<Vec<Option<String>>> {
        let (Some(metadata), Some(identity)) = (&self.metadata, self.spill_identity) else {
            return Ok(fallback.to_vec());
        };
        let spills = metadata.get_spills(
            self.source.left.exchange_key,
            identity.task_id,
            identity.attempt,
        )?;
        let mut discovered = vec![None; SPILL_PARTITION_COUNT];
        for spill in spills {
            if spill.partition_id == self.source.left.partition_id
                && (spill.subpartition_id as usize) < discovered.len()
            {
                discovered[spill.subpartition_id as usize] = Some(spill.storage_path);
            }
        }
        Ok(discovered)
    }

    fn materialize_output(
        left: &DecodedExchangeBatch,
        right: &DecodedExchangeBatch,
        left_rows: &[u32],
        right_rows: &[u32],
    ) -> Result<DataChunk> {
        let left_indices = UInt32Array::from(left_rows.to_vec());
        let right_indices = UInt32Array::from(right_rows.to_vec());
        let mut vectors =
            Vec::with_capacity(left.record_batch.num_columns() + right.record_batch.num_columns());

        for column in left.record_batch.columns() {
            vectors.push(take(column.as_ref(), &left_indices, None).map_err(|e| {
                VeinError::internal(format!("join left materialization failed: {e}"))
            })?);
        }
        for column in right.record_batch.columns() {
            vectors.push(take(column.as_ref(), &right_indices, None).map_err(|e| {
                VeinError::internal(format!("join right materialization failed: {e}"))
            })?);
        }

        let mut address_columns = Self::materialize_addresses(&left.address_columns, left_rows)?;
        address_columns.extend(Self::materialize_addresses(
            &right.address_columns,
            right_rows,
        )?);
        DataChunk::from_vectors_with_addresses(vectors, address_columns)
    }

    fn emit_matches(
        next: &mut dyn PipelineNode<DataChunk>,
        left_batches: &[DecodedExchangeBatch],
        right_batch: &DecodedExchangeBatch,
        matches: Vec<crate::operators::join::JoinMatch>,
    ) -> Result<()> {
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
        Ok(())
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
            return Err(VeinError::internal("join inputs are not partition aligned"));
        }
        if !self.source.is_equi_join() {
            return Err(VeinError::internal(
                "partition-local join requires aligned equi-join keys",
            ));
        }

        let left_columns = Self::key_columns(&self.source.left_schema, &self.source.left_keys)?;
        let right_columns = Self::key_columns(&self.source.right_schema, &self.source.right_keys)?;
        let address_key_mode = self
            .source
            .left_keys
            .iter()
            .chain(self.source.right_keys.iter())
            .all(|key| !matches!(key.mode, JoinKeyMode::ValueNative));
        tracing::info!(
            target: "vein.audit",
            event = "join_started",
            input_query_id = %self.source.left.exchange_key.query_id,
            input_stage_id = %self.source.left.exchange_key.stage_id,
            partition_id = self.source.left.partition_id,
            join_type = ?self.source.join_type,
            key_mode = if address_key_mode { "address_candidate" } else { "value" },
            "join_started"
        );

        // Build state is retained for the current partition; the probe side is
        // decoded and processed one exchange frame at a time.
        let build_memory = JoinMemoryBudget::default();
        let mut left_batches = Vec::new();
        let mut left_batch_reservations: Vec<JoinMemoryReservation> = Vec::new();
        let mut table = HashJoinTable::with_budget(build_memory.clone());
        let spill_writer = VfsJoinSpillWriter::new(self.store.clone());
        let mut spill_paths = vec![None; SPILL_PARTITION_COUNT];
        let mut spill_stats = HashMap::new();
        let mut build_rows = 0u64;
        let mut probe_rows = 0u64;
        let mut matches_emitted = 0u64;
        let mut spill_frames = 0u64;
        let left_paths = self.input_paths(&self.source.left)?;
        for path in left_paths {
            let mut reader = self
                .store
                .open_exchange_frame_reader(&path)
                .await
                .map_err(|e| {
                    VeinError::internal(format!("failed to open exchange '{path}': {e}"))
                })?;
            while let Some(frame) = reader.next_frame().await.map_err(|e| {
                VeinError::internal(format!("failed to read exchange '{path}': {e}"))
            })? {
                self.bytes_read = self.bytes_read.saturating_add(frame.len() as u64);
                let batch = decode_exchange_frame(frame, &self.source.left_schema, &path)?;
                let batch_bytes = batch.record_batch.get_array_memory_size();
                let reservation = build_memory.reserve_owned(batch_bytes);
                let chunk = Self::chunk_for_batch(&batch)?;
                let batch_id = left_batches.len() as u32;
                let keys = Self::extract_keys(&chunk, &left_columns, &self.source.left_keys)?;
                build_rows = build_rows.saturating_add(keys.len() as u64);
                let table_result: Result<()> = match reservation {
                    Ok(reservation) => match table.insert_batch(batch_id, &keys) {
                        Ok(()) => {
                            left_batches.push(batch);
                            left_batch_reservations.push(reservation);
                            continue;
                        }
                        Err(error) => {
                            drop(reservation);
                            Err(error)
                        }
                    },
                    Err(error) => Err(error),
                };
                match table_result {
                    Ok(()) => continue,
                    Err(error @ VeinError::ResourceExhausted { .. }) => {
                        spill_frames = spill_frames.saturating_add(1);
                        Self::spill_build_batch(
                            &spill_writer,
                            &self.source.left.path_prefix,
                            self.source.left.partition_id,
                            self.spill_identity,
                            &batch,
                            &keys,
                            &mut spill_paths,
                            &mut spill_stats,
                        )?;
                        tracing::debug!(
                            error = %error,
                            "spilled join build frame into hash subpartitions"
                        );
                    }
                    Err(error) => return Err(error),
                }
            }
        }

        self.publish_spills(&spill_paths, &spill_stats)?;

        let active_spill_paths = self.discover_spills(&spill_paths)?;
        let mut probe_id = 0u32;
        let right_paths = self.input_paths(&self.source.right)?;
        for path in right_paths {
            let mut reader = self
                .store
                .open_exchange_frame_reader(&path)
                .await
                .map_err(|e| {
                    VeinError::internal(format!("failed to open exchange '{path}': {e}"))
                })?;
            while let Some(frame) = reader.next_frame().await.map_err(|e| {
                VeinError::internal(format!("failed to read exchange '{path}': {e}"))
            })? {
                self.bytes_read = self.bytes_read.saturating_add(frame.len() as u64);
                let right_batch = decode_exchange_frame(frame, &self.source.right_schema, &path)?;
                let probe_chunk = Self::chunk_for_batch(&right_batch)?;
                let probe_keys =
                    Self::extract_keys(&probe_chunk, &right_columns, &self.source.right_keys)?;
                probe_rows = probe_rows.saturating_add(probe_keys.len() as u64);
                let current_probe_id = probe_id;
                probe_id = probe_id.saturating_add(1);
                let matches = table.probe_batch(current_probe_id, &probe_keys);
                matches_emitted = matches_emitted.saturating_add(matches.len() as u64);
                Self::emit_matches(next, &left_batches, &right_batch, matches)?;

                let mut probe_partitions = (0..SPILL_PARTITION_COUNT)
                    .map(|_| Vec::<JoinKeyRow>::new())
                    .collect::<Vec<_>>();
                for row in probe_keys.rows() {
                    if row.key.is_some() {
                        probe_partitions[(row.hash as usize) % SPILL_PARTITION_COUNT]
                            .push(row.clone());
                    }
                }

                for (subpartition, rows) in probe_partitions.into_iter().enumerate() {
                    let Some(spill_path) = active_spill_paths[subpartition].as_ref() else {
                        continue;
                    };
                    if rows.is_empty() {
                        continue;
                    }
                    let probe_partition = JoinKeyBatch::new(rows);
                    let mut spill_reader = self
                        .store
                        .open_exchange_frame_reader(spill_path)
                        .await
                        .map_err(|e| {
                            VeinError::internal(format!(
                                "failed to open join spill '{spill_path}': {e}"
                            ))
                        })?;
                    while let Some(spill_frame) = spill_reader.next_frame().await.map_err(|e| {
                        VeinError::internal(format!(
                            "failed to read join spill '{spill_path}': {e}"
                        ))
                    })? {
                        let spilled_batch = decode_exchange_frame(
                            spill_frame,
                            &self.source.left_schema,
                            spill_path,
                        )?;
                        let spilled_chunk = Self::chunk_for_batch(&spilled_batch)?;
                        let spilled_keys = Self::extract_keys(
                            &spilled_chunk,
                            &left_columns,
                            &self.source.left_keys,
                        )?;
                        let mut spilled_table = HashJoinTable::new();
                        spilled_table.insert_batch(0, &spilled_keys)?;
                        let spilled_matches =
                            spilled_table.probe_batch(current_probe_id, &probe_partition);
                        matches_emitted =
                            matches_emitted.saturating_add(spilled_matches.len() as u64);
                        Self::emit_matches(
                            next,
                            std::slice::from_ref(&spilled_batch),
                            &right_batch,
                            spilled_matches,
                        )?;
                    }
                }
            }
        }

        next.flush()?;
        for spill_path in spill_paths.into_iter().flatten() {
            match spill_writer.delete_run(&spill_path) {
                Ok(()) => {
                    if let Some(metadata) = &self.metadata {
                        if let Err(error) = metadata.delete_spill(spill_path.clone()) {
                            tracing::warn!(
                                path = %spill_path,
                                error = %error,
                                "join spill metadata cleanup deferred for garbage collection"
                            );
                        }
                    }
                }
                Err(error) => {
                    tracing::warn!(
                        path = %spill_path,
                        error = %error,
                        "join spill cleanup deferred for garbage collection"
                    );
                }
            }
        }
        tracing::info!(
            target: "vein.audit",
            event = "join_completed",
            %self.source.left.exchange_key.query_id,
            %self.source.left.exchange_key.stage_id,
            partition_id = self.source.left.partition_id,
            key_mode = if address_key_mode { "address_candidate" } else { "value" },
            build_rows,
            probe_rows,
            matches = matches_emitted,
            exchange_bytes_read = self.bytes_read,
            spill_frames,
            "join_completed"
        );
        Ok(())
    }

    fn bytes_read(&self) -> u64 {
        self.bytes_read
    }
}
