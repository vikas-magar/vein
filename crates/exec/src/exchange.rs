//! Executor-side exchange metadata abstraction.
use std::future::Future;
use std::sync::Arc;

use common::Result;
use common::identifiers::{PartitionId, TaskId};
use common::protocol::{ExchangeFragment, ExchangeKey, ExchangeManifest, SpillRun};
use common::transport::VcsExchangeClient;
use error::VeinError;

/// Synchronous metadata contract used by synchronous pipeline nodes.
pub trait ExchangeMetadata: Send + Sync {
    /// Creates or reopens the exchange manifest.
    fn create_exchange(
        &self,
        key: ExchangeKey,
        partition_count: u16,
        expected_tasks: Vec<TaskId>,
        schema_fingerprint: String,
    ) -> Result<ExchangeManifest>;

    /// Publishes one immutable producer fragment.
    fn publish_fragment(&self, key: ExchangeKey, fragment: ExchangeFragment) -> Result<()>;

    /// Returns the sealed fragments for one reducer partition.
    fn get_partition(
        &self,
        key: ExchangeKey,
        partition_id: PartitionId,
    ) -> Result<Vec<ExchangeFragment>>;

    /// Registers one open join spill run.
    fn register_spill(&self, spill: SpillRun) -> Result<()>;

    /// Returns sealed spill runs for one task attempt.
    fn get_spills(&self, key: ExchangeKey, task_id: TaskId, attempt: u32) -> Result<Vec<SpillRun>>;

    /// Marks one spill run sealed.
    fn seal_spill(&self, storage_path: String) -> Result<()>;

    /// Marks one spill run aborted.
    fn abort_spill(&self, storage_path: String) -> Result<()>;

    /// Deletes one spill descriptor after physical cleanup.
    fn delete_spill(&self, storage_path: String) -> Result<()>;
}

/// VCS-backed implementation for executor workers.
#[derive(Clone)]
pub struct VcsExchangeMetadata {
    client: VcsExchangeClient,
}

impl VcsExchangeMetadata {
    /// Creates an executor metadata adapter over a pooled VCS client.
    pub fn new(client: VcsExchangeClient) -> Self {
        Self { client }
    }

    fn run_blocking<T, F>(&self, operation: F) -> Result<T>
    where
        T: Send + 'static,
        F: Future<Output = Result<T>> + Send + 'static,
    {
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| {
                    VeinError::internal(format!("build VCS metadata runtime: {error}"))
                })?;
            runtime.block_on(operation)
        })
        .join()
        .map_err(|_| VeinError::internal("VCS metadata worker thread panicked"))?
    }
}

impl ExchangeMetadata for VcsExchangeMetadata {
    fn create_exchange(
        &self,
        key: ExchangeKey,
        partition_count: u16,
        expected_tasks: Vec<TaskId>,
        schema_fingerprint: String,
    ) -> Result<ExchangeManifest> {
        let client = self.client.clone();
        self.run_blocking(async move {
            client
                .create_exchange(key, partition_count, expected_tasks, schema_fingerprint)
                .await
        })
    }

    fn publish_fragment(&self, key: ExchangeKey, fragment: ExchangeFragment) -> Result<()> {
        let client = self.client.clone();
        self.run_blocking(async move { client.publish_fragment(key, fragment).await })
    }

    fn get_partition(
        &self,
        key: ExchangeKey,
        partition_id: PartitionId,
    ) -> Result<Vec<ExchangeFragment>> {
        let client = self.client.clone();
        self.run_blocking(async move { client.get_partition(key, partition_id).await })
    }

    fn register_spill(&self, spill: SpillRun) -> Result<()> {
        let client = self.client.clone();
        self.run_blocking(async move { client.register_spill(spill).await })
    }

    fn get_spills(&self, key: ExchangeKey, task_id: TaskId, attempt: u32) -> Result<Vec<SpillRun>> {
        let client = self.client.clone();
        self.run_blocking(async move { client.get_spills(key, task_id, attempt).await })
    }

    fn seal_spill(&self, storage_path: String) -> Result<()> {
        let client = self.client.clone();
        self.run_blocking(async move { client.seal_spill(storage_path).await })
    }

    fn abort_spill(&self, storage_path: String) -> Result<()> {
        let client = self.client.clone();
        self.run_blocking(async move { client.abort_spill(storage_path).await })
    }

    fn delete_spill(&self, storage_path: String) -> Result<()> {
        let client = self.client.clone();
        self.run_blocking(async move { client.delete_spill(storage_path).await })
    }
}

/// Shareable exchange metadata handle.
pub type SharedExchangeMetadata = Arc<dyn ExchangeMetadata>;
