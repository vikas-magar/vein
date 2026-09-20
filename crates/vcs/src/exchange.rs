//! Authoritative exchange metadata owned by VCS.
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use bytes::Bytes;
use common::identifiers::{NodeId, QueryId, StageId, TaskId};
use common::protocol::{
    ExchangeFragment, ExchangeKey, ExchangeManifest, ExchangeState, SpillRun, SpillRunState,
};
use error::{Result, VeinError};
use iceberg::io::FileIO;

/// VCS contract for exchange lifecycle and fragment discovery.
#[async_trait]
pub trait ExchangeCatalog: Send + Sync {
    /// Creates or returns an open exchange manifest.
    async fn create_exchange(
        &self,
        key: ExchangeKey,
        partition_count: u16,
        expected_tasks: Vec<TaskId>,
        schema_fingerprint: String,
    ) -> Result<ExchangeManifest>;

    /// Publishes one immutable fragment from a producer task.
    async fn publish_fragment(&self, key: ExchangeKey, fragment: ExchangeFragment) -> Result<()>;

    /// Atomically seals an exchange after all expected producers publish.
    async fn seal_exchange(&self, key: ExchangeKey) -> Result<ExchangeManifest>;

    /// Returns the sealed manifest for reducer consumption.
    async fn get_manifest(&self, key: ExchangeKey) -> Result<ExchangeManifest>;

    /// Returns the sealed fragments for one reducer partition.
    async fn get_partition(
        &self,
        key: ExchangeKey,
        partition_id: u32,
    ) -> Result<Vec<ExchangeFragment>>;

    /// Removes exchange metadata after retention and query cleanup.
    async fn delete_exchange(&self, key: ExchangeKey) -> Result<()>;

    /// Registers or replaces one join spill run.
    async fn register_spill(&self, spill: SpillRun) -> Result<()>;

    /// Discovers spill runs belonging to one task attempt.
    async fn get_spills(
        &self,
        key: ExchangeKey,
        task_id: TaskId,
        attempt: u32,
    ) -> Result<Vec<SpillRun>>;

    /// Transitions one spill run to sealed.
    async fn seal_spill(&self, storage_path: &str) -> Result<()>;

    /// Transitions one spill run to aborted.
    async fn abort_spill(&self, storage_path: &str) -> Result<()>;

    /// Deletes one spill descriptor after physical cleanup.
    async fn delete_spill(&self, storage_path: &str) -> Result<()>;
}

/// In-memory VCS exchange catalog used by the current VCS process.
///
/// The interface is deliberately independent of storage. A replicated or
/// durable implementation can replace it without changing executor contracts.
#[derive(Clone, Default)]
pub struct InMemoryExchangeCatalog {
    manifests: Arc<RwLock<HashMap<ExchangeKey, ExchangeManifest>>>,
    spills: Arc<RwLock<HashMap<String, SpillRun>>>,
}

impl InMemoryExchangeCatalog {
    fn read_manifest(&self, key: ExchangeKey) -> Result<ExchangeManifest> {
        self.manifests
            .read()
            .map_err(|_| VeinError::internal("exchange catalog read lock poisoned"))?
            .get(&key)
            .cloned()
            .ok_or_else(|| VeinError::not_found(format!("exchange {key:?}")))
    }

    fn validate_fragment(manifest: &ExchangeManifest, fragment: &ExchangeFragment) -> Result<()> {
        if manifest.state != ExchangeState::Open {
            return Err(VeinError::shuffle(format!(
                "exchange {:?} is not open",
                manifest.key
            )));
        }
        if fragment.partition_id >= manifest.partition_count as u32 {
            return Err(VeinError::shuffle(format!(
                "partition {} is outside exchange partition count {}",
                fragment.partition_id, manifest.partition_count
            )));
        }
        if !manifest.expected_tasks.contains(&fragment.task_id) {
            return Err(VeinError::shuffle(format!(
                "task {} is not registered for exchange {:?}",
                fragment.task_id, manifest.key
            )));
        }
        Ok(())
    }
}

#[async_trait]
impl ExchangeCatalog for InMemoryExchangeCatalog {
    async fn create_exchange(
        &self,
        key: ExchangeKey,
        partition_count: u16,
        expected_tasks: Vec<TaskId>,
        schema_fingerprint: String,
    ) -> Result<ExchangeManifest> {
        if partition_count == 0 {
            return Err(VeinError::shuffle(
                "exchange must have at least one partition",
            ));
        }

        let mut manifests = self
            .manifests
            .write()
            .map_err(|_| VeinError::internal("exchange catalog write lock poisoned"))?;

        if let Some(existing) = manifests.get(&key) {
            if existing.partition_count != partition_count
                || existing.expected_tasks != expected_tasks
                || existing.schema_fingerprint != schema_fingerprint
            {
                return Err(VeinError::shuffle(format!(
                    "exchange {:?} was created with incompatible metadata",
                    key
                )));
            }
            return Ok(existing.clone());
        }

        let manifest =
            ExchangeManifest::new(key, partition_count, expected_tasks, schema_fingerprint);
        manifests.insert(key, manifest.clone());
        Ok(manifest)
    }

    async fn publish_fragment(&self, key: ExchangeKey, fragment: ExchangeFragment) -> Result<()> {
        let mut manifests = self
            .manifests
            .write()
            .map_err(|_| VeinError::internal("exchange catalog write lock poisoned"))?;
        let manifest = manifests
            .get_mut(&key)
            .ok_or_else(|| VeinError::not_found(format!("exchange {key:?}")))?;
        Self::validate_fragment(manifest, &fragment)?;

        if let Some(existing) = manifest.fragments.iter_mut().find(|existing| {
            existing.task_id == fragment.task_id && existing.partition_id == fragment.partition_id
        }) {
            if fragment.attempt < existing.attempt {
                return Ok(());
            }
            *existing = fragment;
        } else {
            manifest.fragments.push(fragment);
        }
        if manifest.all_fragments_published() {
            manifest.state = ExchangeState::Sealed;
        }
        Ok(())
    }

    async fn seal_exchange(&self, key: ExchangeKey) -> Result<ExchangeManifest> {
        let mut manifests = self
            .manifests
            .write()
            .map_err(|_| VeinError::internal("exchange catalog write lock poisoned"))?;
        let manifest = manifests
            .get_mut(&key)
            .ok_or_else(|| VeinError::not_found(format!("exchange {key:?}")))?;

        match manifest.state {
            ExchangeState::Sealed => return Ok(manifest.clone()),
            ExchangeState::Aborted => {
                return Err(VeinError::shuffle(format!("exchange {:?} is aborted", key)));
            }
            ExchangeState::Open => {}
        }

        if !manifest.all_tasks_published() {
            return Err(VeinError::shuffle(format!(
                "exchange {:?} is missing producer fragments",
                key
            )));
        }

        manifest.state = ExchangeState::Sealed;
        Ok(manifest.clone())
    }

    async fn get_manifest(&self, key: ExchangeKey) -> Result<ExchangeManifest> {
        let manifest = self.read_manifest(key)?;
        if manifest.state != ExchangeState::Sealed {
            return Err(VeinError::shuffle(format!(
                "exchange {:?} is not sealed",
                key
            )));
        }
        Ok(manifest)
    }

    async fn get_partition(
        &self,
        key: ExchangeKey,
        partition_id: u32,
    ) -> Result<Vec<ExchangeFragment>> {
        let manifest = self.get_manifest(key).await?;
        if partition_id >= manifest.partition_count as u32 {
            return Err(VeinError::shuffle(format!(
                "partition {partition_id} is outside exchange partition count {}",
                manifest.partition_count
            )));
        }
        Ok(manifest
            .fragments_for_partition(partition_id)
            .cloned()
            .collect())
    }

    async fn delete_exchange(&self, key: ExchangeKey) -> Result<()> {
        let mut manifests = self
            .manifests
            .write()
            .map_err(|_| VeinError::internal("exchange catalog write lock poisoned"))?;
        manifests
            .remove(&key)
            .map(|_| ())
            .ok_or_else(|| VeinError::not_found(format!("exchange {key:?}")))
    }

    async fn register_spill(&self, spill: SpillRun) -> Result<()> {
        let mut spills = self
            .spills
            .write()
            .map_err(|_| VeinError::internal("spill catalog write lock poisoned"))?;
        if spill.storage_path.is_empty() || spill.state != SpillRunState::Open {
            return Err(VeinError::shuffle(
                "spill must be registered with an open state and path",
            ));
        }
        spills.insert(spill.storage_path.clone(), spill);
        Ok(())
    }

    async fn get_spills(
        &self,
        key: ExchangeKey,
        task_id: TaskId,
        attempt: u32,
    ) -> Result<Vec<SpillRun>> {
        let spills = self
            .spills
            .read()
            .map_err(|_| VeinError::internal("spill catalog read lock poisoned"))?;
        Ok(spills
            .values()
            .filter(|spill| {
                spill.exchange_key == key
                    && spill.task_id == task_id
                    && spill.attempt == attempt
                    && spill.state == SpillRunState::Sealed
            })
            .cloned()
            .collect())
    }

    async fn seal_spill(&self, storage_path: &str) -> Result<()> {
        let mut spills = self
            .spills
            .write()
            .map_err(|_| VeinError::internal("spill catalog write lock poisoned"))?;
        let spill = spills
            .get_mut(storage_path)
            .ok_or_else(|| VeinError::not_found(format!("spill {storage_path}")))?;
        if spill.state != SpillRunState::Open {
            return Err(VeinError::shuffle("spill is not open"));
        }
        spill.state = SpillRunState::Sealed;
        Ok(())
    }

    async fn abort_spill(&self, storage_path: &str) -> Result<()> {
        let mut spills = self
            .spills
            .write()
            .map_err(|_| VeinError::internal("spill catalog write lock poisoned"))?;
        let spill = spills
            .get_mut(storage_path)
            .ok_or_else(|| VeinError::not_found(format!("spill {storage_path}")))?;
        spill.state = SpillRunState::Aborted;
        Ok(())
    }

    async fn delete_spill(&self, storage_path: &str) -> Result<()> {
        let mut spills = self
            .spills
            .write()
            .map_err(|_| VeinError::internal("spill catalog write lock poisoned"))?;
        spills
            .remove(storage_path)
            .map(|_| ())
            .ok_or_else(|| VeinError::not_found(format!("spill {storage_path}")))
    }
}

/// File-backed exchange catalog used by the VCS authority.
///
/// The catalog stores a complete manifest snapshot under the configured
/// warehouse. VCS loads it once during startup and serializes every metadata
/// mutation while holding the catalog lock, which keeps retries and sealing
/// linearizable within the authority process.
#[derive(Clone)]
pub struct PersistentExchangeCatalog {
    file_io: FileIO,
    manifest_path: String,
    spill_path: String,
    manifests: Arc<tokio::sync::Mutex<HashMap<ExchangeKey, ExchangeManifest>>>,
    spills: Arc<tokio::sync::Mutex<HashMap<String, SpillRun>>>,
}

impl PersistentExchangeCatalog {
    /// Opens an exchange catalog, restoring the last committed manifest snapshot
    /// when the object exists.
    pub async fn open(file_io: FileIO, manifest_path: impl Into<String>) -> Result<Self> {
        let manifest_path = manifest_path.into();
        let spill_path = format!("{}.spills.json", manifest_path.trim_end_matches(".json"));
        let input = file_io
            .new_input(&manifest_path)
            .map_err(VeinError::Iceberg)?;
        let manifests = if input.exists().await.map_err(VeinError::Iceberg)? {
            let bytes = input.read().await.map_err(VeinError::Iceberg)?;
            let snapshot: Vec<ExchangeManifest> = serde_json::from_slice(&bytes).map_err(|e| {
                VeinError::serialization(format!("invalid exchange catalog snapshot: {e}"))
            })?;
            snapshot
                .into_iter()
                .map(|manifest| (manifest.key, manifest))
                .collect()
        } else {
            HashMap::new()
        };
        let spill_input = file_io.new_input(&spill_path).map_err(VeinError::Iceberg)?;
        let spills = if spill_input.exists().await.map_err(VeinError::Iceberg)? {
            let bytes = spill_input.read().await.map_err(VeinError::Iceberg)?;
            let snapshot: Vec<SpillRun> = serde_json::from_slice(&bytes).map_err(|e| {
                VeinError::serialization(format!("invalid spill catalog snapshot: {e}"))
            })?;
            snapshot
                .into_iter()
                .map(|spill| (spill.storage_path.clone(), spill))
                .collect()
        } else {
            HashMap::new()
        };

        Ok(Self {
            file_io,
            manifest_path,
            spill_path,
            manifests: Arc::new(tokio::sync::Mutex::new(manifests)),
            spills: Arc::new(tokio::sync::Mutex::new(spills)),
        })
    }

    async fn persist_locked(
        &self,
        manifests: &HashMap<ExchangeKey, ExchangeManifest>,
    ) -> Result<()> {
        let snapshot: Vec<ExchangeManifest> = manifests.values().cloned().collect();
        let bytes = serde_json::to_vec(&snapshot).map_err(|e| {
            VeinError::serialization(format!("failed to encode exchange catalog: {e}"))
        })?;
        let output = self
            .file_io
            .new_output(&self.manifest_path)
            .map_err(VeinError::Iceberg)?;
        output
            .write(Bytes::from(bytes))
            .await
            .map_err(VeinError::Iceberg)
    }

    async fn persist_spills_locked(&self, spills: &HashMap<String, SpillRun>) -> Result<()> {
        let snapshot: Vec<SpillRun> = spills.values().cloned().collect();
        let bytes = serde_json::to_vec(&snapshot).map_err(|e| {
            VeinError::serialization(format!("failed to encode spill catalog: {e}"))
        })?;
        let output = self
            .file_io
            .new_output(&self.spill_path)
            .map_err(VeinError::Iceberg)?;
        output
            .write(Bytes::from(bytes))
            .await
            .map_err(VeinError::Iceberg)
    }

    fn validate_fragment(manifest: &ExchangeManifest, fragment: &ExchangeFragment) -> Result<()> {
        if manifest.state != ExchangeState::Open {
            return Err(VeinError::shuffle(format!(
                "exchange {:?} is not open",
                manifest.key
            )));
        }
        if fragment.partition_id >= manifest.partition_count as u32 {
            return Err(VeinError::shuffle(format!(
                "partition {} is outside exchange partition count {}",
                fragment.partition_id, manifest.partition_count
            )));
        }
        if !manifest.expected_tasks.contains(&fragment.task_id) {
            return Err(VeinError::shuffle(format!(
                "task {} is not registered for exchange {:?}",
                fragment.task_id, manifest.key
            )));
        }
        Ok(())
    }
}

#[async_trait]
impl ExchangeCatalog for PersistentExchangeCatalog {
    async fn create_exchange(
        &self,
        key: ExchangeKey,
        partition_count: u16,
        expected_tasks: Vec<TaskId>,
        schema_fingerprint: String,
    ) -> Result<ExchangeManifest> {
        if partition_count == 0 {
            return Err(VeinError::shuffle(
                "exchange must have at least one partition",
            ));
        }

        let mut manifests = self.manifests.lock().await;
        if let Some(existing) = manifests.get(&key) {
            if existing.partition_count != partition_count
                || existing.expected_tasks != expected_tasks
                || existing.schema_fingerprint != schema_fingerprint
            {
                return Err(VeinError::shuffle(format!(
                    "exchange {:?} was created with incompatible metadata",
                    key
                )));
            }
            return Ok(existing.clone());
        }

        let previous = manifests.clone();
        let manifest =
            ExchangeManifest::new(key, partition_count, expected_tasks, schema_fingerprint);
        manifests.insert(key, manifest.clone());
        if let Err(error) = self.persist_locked(&manifests).await {
            *manifests = previous;
            return Err(error);
        }
        Ok(manifest)
    }

    async fn publish_fragment(&self, key: ExchangeKey, fragment: ExchangeFragment) -> Result<()> {
        let mut manifests = self.manifests.lock().await;
        let previous;
        {
            let manifest = manifests
                .get_mut(&key)
                .ok_or_else(|| VeinError::not_found(format!("exchange {key:?}")))?;
            Self::validate_fragment(manifest, &fragment)?;

            previous = manifest.clone();
            if let Some(existing) = manifest.fragments.iter_mut().find(|existing| {
                existing.task_id == fragment.task_id
                    && existing.partition_id == fragment.partition_id
            }) {
                if fragment.attempt < existing.attempt {
                    return Ok(());
                }
                *existing = fragment;
            } else {
                manifest.fragments.push(fragment);
            }
            if manifest.all_fragments_published() {
                manifest.state = ExchangeState::Sealed;
            }
        }

        if let Err(error) = self.persist_locked(&manifests).await {
            manifests.insert(key, previous);
            return Err(error);
        }
        Ok(())
    }

    async fn seal_exchange(&self, key: ExchangeKey) -> Result<ExchangeManifest> {
        let mut manifests = self.manifests.lock().await;
        let previous;
        let sealed;
        {
            let manifest = manifests
                .get_mut(&key)
                .ok_or_else(|| VeinError::not_found(format!("exchange {key:?}")))?;

            match manifest.state {
                ExchangeState::Sealed => return Ok(manifest.clone()),
                ExchangeState::Aborted => {
                    return Err(VeinError::shuffle(format!("exchange {:?} is aborted", key)));
                }
                ExchangeState::Open => {}
            }

            if !manifest.all_tasks_published() {
                return Err(VeinError::shuffle(format!(
                    "exchange {:?} is missing producer fragments",
                    key
                )));
            }

            previous = manifest.clone();
            manifest.state = ExchangeState::Sealed;
            sealed = manifest.clone();
        }

        if let Err(error) = self.persist_locked(&manifests).await {
            manifests.insert(key, previous);
            return Err(error);
        }
        Ok(sealed)
    }

    async fn get_manifest(&self, key: ExchangeKey) -> Result<ExchangeManifest> {
        let manifests = self.manifests.lock().await;
        let manifest = manifests
            .get(&key)
            .cloned()
            .ok_or_else(|| VeinError::not_found(format!("exchange {key:?}")))?;
        if manifest.state != ExchangeState::Sealed {
            return Err(VeinError::shuffle(format!(
                "exchange {:?} is not sealed",
                key
            )));
        }
        Ok(manifest)
    }

    async fn get_partition(
        &self,
        key: ExchangeKey,
        partition_id: u32,
    ) -> Result<Vec<ExchangeFragment>> {
        let manifest = self.get_manifest(key).await?;
        if partition_id >= manifest.partition_count as u32 {
            return Err(VeinError::shuffle(format!(
                "partition {partition_id} is outside exchange partition count {}",
                manifest.partition_count
            )));
        }
        Ok(manifest
            .fragments_for_partition(partition_id)
            .cloned()
            .collect())
    }

    async fn delete_exchange(&self, key: ExchangeKey) -> Result<()> {
        let mut manifests = self.manifests.lock().await;
        let previous = manifests.clone();
        manifests
            .remove(&key)
            .ok_or_else(|| VeinError::not_found(format!("exchange {key:?}")))?;
        if let Err(error) = self.persist_locked(&manifests).await {
            *manifests = previous;
            return Err(error);
        }
        Ok(())
    }

    async fn register_spill(&self, spill: SpillRun) -> Result<()> {
        if spill.storage_path.is_empty() || spill.state != SpillRunState::Open {
            return Err(VeinError::shuffle(
                "spill must be registered with an open state and path",
            ));
        }
        let mut spills = self.spills.lock().await;
        let previous = spills.clone();
        spills.insert(spill.storage_path.clone(), spill);
        if let Err(error) = self.persist_spills_locked(&spills).await {
            *spills = previous;
            return Err(error);
        }
        Ok(())
    }

    async fn get_spills(
        &self,
        key: ExchangeKey,
        task_id: TaskId,
        attempt: u32,
    ) -> Result<Vec<SpillRun>> {
        let spills = self.spills.lock().await;
        Ok(spills
            .values()
            .filter(|spill| {
                spill.exchange_key == key
                    && spill.task_id == task_id
                    && spill.attempt == attempt
                    && spill.state == SpillRunState::Sealed
            })
            .cloned()
            .collect())
    }

    async fn seal_spill(&self, storage_path: &str) -> Result<()> {
        let mut spills = self.spills.lock().await;
        let previous = spills.clone();
        let spill = spills
            .get_mut(storage_path)
            .ok_or_else(|| VeinError::not_found(format!("spill {storage_path}")))?;
        if spill.state != SpillRunState::Open {
            return Err(VeinError::shuffle("spill is not open"));
        }
        spill.state = SpillRunState::Sealed;
        if let Err(error) = self.persist_spills_locked(&spills).await {
            *spills = previous;
            return Err(error);
        }
        Ok(())
    }

    async fn abort_spill(&self, storage_path: &str) -> Result<()> {
        let mut spills = self.spills.lock().await;
        let previous = spills.clone();
        let spill = spills
            .get_mut(storage_path)
            .ok_or_else(|| VeinError::not_found(format!("spill {storage_path}")))?;
        spill.state = SpillRunState::Aborted;
        if let Err(error) = self.persist_spills_locked(&spills).await {
            *spills = previous;
            return Err(error);
        }
        Ok(())
    }

    async fn delete_spill(&self, storage_path: &str) -> Result<()> {
        let mut spills = self.spills.lock().await;
        let previous = spills.clone();
        spills
            .remove(storage_path)
            .ok_or_else(|| VeinError::not_found(format!("spill {storage_path}")))?;
        if let Err(error) = self.persist_spills_locked(&spills).await {
            *spills = previous;
            return Err(error);
        }
        Ok(())
    }
}

/// Creates an exchange fragment descriptor for a persisted VFS object.
pub fn fragment(
    partition_id: u32,
    task_id: TaskId,
    attempt: u32,
    storage_path: impl Into<String>,
    node_id: Option<NodeId>,
    byte_len: u64,
    row_count: u64,
    checksum: u64,
) -> ExchangeFragment {
    ExchangeFragment {
        partition_id,
        task_id,
        attempt,
        storage_path: storage_path.into(),
        node_id,
        byte_len,
        row_count,
        checksum,
    }
}

/// Convenience key constructor for VCS callers.
pub fn key(query_id: QueryId, stage_id: StageId) -> ExchangeKey {
    ExchangeKey { query_id, stage_id }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::identifiers::{QueryId, StageId, TaskId};

    #[tokio::test]
    async fn publishes_retries_and_seals_idempotently() {
        let catalog = InMemoryExchangeCatalog::default();
        let key = key(QueryId::from_raw(1), StageId::from_raw(2));
        let task = TaskId::from_raw(3);
        catalog
            .create_exchange(key, 2, vec![task], "schema-v1".to_string())
            .await
            .unwrap();

        catalog
            .publish_fragment(key, fragment(0, task, 0, "fragment-old", None, 10, 1, 7))
            .await
            .unwrap();
        catalog
            .publish_fragment(key, fragment(0, task, 1, "fragment-new", None, 12, 1, 8))
            .await
            .unwrap();

        let manifest = catalog.seal_exchange(key).await.unwrap();
        assert_eq!(manifest.state, ExchangeState::Sealed);
        assert_eq!(manifest.fragments.len(), 1);
        assert_eq!(manifest.fragments[0].storage_path, "fragment-new");

        let partition = catalog.get_partition(key, 0).await.unwrap();
        assert_eq!(partition.len(), 1);
        assert!(catalog.seal_exchange(key).await.is_ok());
    }

    #[tokio::test]
    async fn persistent_catalog_restores_sealed_manifest() {
        let directory = tempfile::tempdir().unwrap();
        let path = format!("file://{}/exchange.json", directory.path().display());
        let key = key(QueryId::from_raw(11), StageId::from_raw(12));
        let task = TaskId::from_raw(13);

        let catalog = PersistentExchangeCatalog::open(FileIO::new_with_fs(), path.clone())
            .await
            .unwrap();
        catalog
            .create_exchange(key, 1, vec![task], "schema-v2".to_string())
            .await
            .unwrap();
        catalog
            .publish_fragment(key, fragment(0, task, 0, "fragment", None, 64, 4, 99))
            .await
            .unwrap();
        catalog.seal_exchange(key).await.unwrap();

        let reopened = PersistentExchangeCatalog::open(FileIO::new_with_fs(), path)
            .await
            .unwrap();
        let manifest = reopened.get_manifest(key).await.unwrap();
        assert_eq!(manifest.state, ExchangeState::Sealed);
        assert_eq!(manifest.fragments[0].row_count, 4);
    }

    #[tokio::test]
    async fn spill_catalog_publishes_seals_discovers_and_deletes() {
        let catalog = InMemoryExchangeCatalog::default();
        let exchange_key = key(QueryId::from_raw(21), StageId::from_raw(22));
        let task_id = TaskId::from_raw(23);
        let spill = SpillRun {
            exchange_key,
            task_id,
            attempt: 1,
            partition_id: 0,
            subpartition_id: 4,
            storage_path: "spill/run-4".to_string(),
            byte_len: 128,
            row_count: 16,
            checksum: 99,
            state: SpillRunState::Open,
            lease_until_ms: 123,
        };

        catalog.register_spill(spill.clone()).await.unwrap();
        assert!(
            catalog
                .get_spills(exchange_key, task_id, 1)
                .await
                .unwrap()
                .is_empty()
        );

        catalog.seal_spill(&spill.storage_path).await.unwrap();
        let discovered = catalog.get_spills(exchange_key, task_id, 1).await.unwrap();
        assert_eq!(
            discovered,
            vec![SpillRun {
                state: SpillRunState::Sealed,
                ..spill.clone()
            }]
        );

        catalog.delete_spill(&spill.storage_path).await.unwrap();
        assert!(
            catalog
                .get_spills(exchange_key, task_id, 1)
                .await
                .unwrap()
                .is_empty()
        );
    }
}
