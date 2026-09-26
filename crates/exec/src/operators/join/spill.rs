//! Storage-independent join spill contracts.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use common::Result;
use common::identifiers::TaskId;
use error::VeinError;

/// Identity of one executor task attempt that owns spill runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct JoinSpillIdentity {
    /// Executor task identity.
    pub task_id: TaskId,
    /// Retry attempt, starting at zero.
    pub attempt: u32,
}

/// Writes immutable encoded exchange frames for a bounded join spill run.
pub trait JoinSpillWriter: Send + Sync {
    /// Appends one already-encoded exchange frame to a spill run.
    fn append_frame(&self, path: &str, frame: &[u8]) -> Result<()>;

    /// Removes a successfully consumed spill run.
    fn delete_run(&self, path: &str) -> Result<()>;
}

/// VFS-backed implementation of the join spill writer.
#[derive(Clone)]
pub struct VfsJoinSpillWriter {
    store: Arc<vfs::VfsTableStore>,
}

impl VfsJoinSpillWriter {
    /// Creates a spill writer over an existing VFS store.
    pub fn new(store: Arc<vfs::VfsTableStore>) -> Self {
        Self { store }
    }
}

impl JoinSpillWriter for VfsJoinSpillWriter {
    fn append_frame(&self, path: &str, frame: &[u8]) -> Result<()> {
        self.store
            .append_exchange_frame_sync(path, frame.to_vec())
            .map_err(|error| {
                VeinError::internal(format!(
                    "failed to append join spill frame '{path}': {error}"
                ))
            })
    }

    fn delete_run(&self, path: &str) -> Result<()> {
        self.store.delete_exchange_path_sync(path).map_err(|error| {
            VeinError::internal(format!("failed to delete join spill run '{path}': {error}"))
        })
    }
}

/// Creates a unique spill-run path for one executor attempt.
pub fn new_spill_path(prefix: &str, partition_id: u32) -> String {
    static NEXT_RUN_ID: AtomicU64 = AtomicU64::new(1);
    let run_id = NEXT_RUN_ID.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}/_join_spill/{run_id:016x}/partition_{partition_id:05}")
}

/// Creates an attempt-addressable spill-run path.
pub fn new_attempt_spill_path(
    prefix: &str,
    partition_id: u32,
    identity: JoinSpillIdentity,
) -> String {
    format!(
        "{prefix}/_join_spill/task_{:032x}/attempt_{:08x}/partition_{partition_id:05}",
        identity.task_id.into_raw(),
        identity.attempt
    )
}

#[cfg(test)]
mod tests {
    use super::new_spill_path;

    #[test]
    fn spill_paths_are_unique() {
        let first = new_spill_path("exchange", 3);
        let second = new_spill_path("exchange", 3);
        assert_ne!(first, second);
        assert!(first.ends_with("partition_00003"));
    }
}
