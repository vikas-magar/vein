//! Query-scoped spill ownership, cancellation, and disk accounting.

use std::io::{self, SeekFrom, Write};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use common::cancellation::CancellationToken;
use common::Result;
use error::VeinError;

use super::storage::{SpillRunId, SpillStorage, SpillStream};
use super::SpillErrorResolver;

struct SpillManagerInner {
    storage: Arc<dyn SpillStorage>,
    cancellation: CancellationToken,
    disk_limit: u64,
    disk_used: AtomicU64,
    disk_peak: AtomicU64,
}

/// Shared query-level owner for temporary runs and their resource budget.
#[derive(Clone)]
pub(crate) struct SpillManager {
    inner: Arc<SpillManagerInner>,
}

impl std::fmt::Debug for SpillManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpillManager")
            .field("disk_limit", &self.inner.disk_limit)
            .field("disk_used", &self.disk_used())
            .field("disk_peak", &self.disk_peak())
            .finish_non_exhaustive()
    }
}

impl SpillManager {
    pub(crate) fn with_storage(
        cancellation: CancellationToken,
        disk_limit: u64,
        storage: Arc<dyn SpillStorage>,
    ) -> Self {
        Self {
            inner: Arc::new(SpillManagerInner {
                storage,
                cancellation,
                disk_limit,
                disk_used: AtomicU64::new(0),
                disk_peak: AtomicU64::new(0),
            }),
        }
    }

    pub(crate) fn create_run(&self) -> Result<PendingSpillRun> {
        self.check_cancelled()?;
        let (id, stream) = self.inner.storage.create()?;
        let state = Arc::new(SpillRunInner {
            manager: Arc::clone(&self.inner),
            id,
            bytes: AtomicU64::new(0),
            deleted: AtomicBool::new(false),
        });
        Ok(PendingSpillRun {
            state,
            stream: Some(stream),
        })
    }

    pub(crate) fn check_cancelled(&self) -> Result<()> {
        if self.inner.cancellation.is_cancelled() {
            Err(VeinError::cancelled())
        } else {
            Ok(())
        }
    }

    pub(crate) fn disk_used(&self) -> u64 {
        self.inner.disk_used.load(Ordering::Acquire)
    }

    pub(crate) fn disk_peak(&self) -> u64 {
        self.inner.disk_peak.load(Ordering::Acquire)
    }

    pub(crate) fn disk_limit(&self) -> u64 {
        self.inner.disk_limit
    }

    #[cfg(test)]
    pub(crate) fn exists(&self, run: &SpillRun) -> bool {
        self.inner.storage.exists(&run.state.id)
    }

    #[cfg(test)]
    pub(crate) fn local_directory(&self) -> Option<std::path::PathBuf> {
        self.inner.storage.root_path()
    }
}

/// An uncommitted run. Dropping it removes the partial file via its shared
/// run state, including when codec creation or a write fails.
pub(crate) struct PendingSpillRun {
    state: Arc<SpillRunInner>,
    stream: Option<Box<dyn SpillStream>>,
}

impl PendingSpillRun {
    pub(crate) fn writer(&mut self) -> Result<SpillWriteHandle> {
        let stream = self
            .stream
            .take()
            .ok_or_else(|| VeinError::internal("spill run writer was already taken"))?;
        Ok(SpillWriteHandle {
            state: Arc::clone(&self.state),
            stream,
            failure: Arc::new(Mutex::new(None)),
        })
    }

    pub(crate) fn commit(self, writer: SpillWriteHandle) -> Result<SpillRun> {
        if !Arc::ptr_eq(&self.state, &writer.state) {
            return Err(VeinError::internal("spill writer belongs to a different run"));
        }
        if let Some((requested, limit)) = writer
            .failure
            .lock()
            .map_err(|_| VeinError::internal("spill error state poisoned"))?
            .take()
        {
            return Err(VeinError::resource_exhausted(
                "query spill disk",
                usize::try_from(requested).unwrap_or(usize::MAX),
                usize::try_from(limit).unwrap_or(usize::MAX),
            ));
        }
        if self.state.manager.cancellation.is_cancelled() {
            return Err(VeinError::cancelled());
        }
        let mut writer = writer;
        writer
            .flush()
            .map_err(|error| VeinError::internal(format!("flush spill run: {error}")))?;
        drop(writer);
        Ok(SpillRun { state: self.state })
    }
}

/// Seekable spill output that charges file growth to the owning query.
pub(crate) struct SpillWriteHandle {
    state: Arc<SpillRunInner>,
    stream: Box<dyn SpillStream>,
    failure: Arc<Mutex<Option<(u64, u64)>>>,
}

impl SpillWriteHandle {
    pub(crate) fn error_resolver(&self) -> SpillErrorResolver {
        let failure = Arc::clone(&self.failure);
        let cancellation = self.state.manager.cancellation.clone();
        Arc::new(move || {
            if cancellation.is_cancelled() {
                return Some(VeinError::cancelled());
            }
            let (requested, limit) = failure.lock().ok().and_then(|failure| *failure)?;
            Some(VeinError::resource_exhausted(
                "query spill disk",
                usize::try_from(requested).unwrap_or(usize::MAX),
                usize::try_from(limit).unwrap_or(usize::MAX),
            ))
        })
    }

    fn remember_limit_error(&self, requested: u64, limit: u64) {
        if let Ok(mut failure) = self.failure.lock() {
            *failure = Some((requested, limit));
        }
    }
}

impl Write for SpillWriteHandle {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        if self.state.manager.cancellation.is_cancelled() {
            return Err(io::Error::new(io::ErrorKind::Interrupted, "query cancelled"));
        }
        let position = self.stream.stream_position()?;
        let proposed_end = position
            .checked_add(buffer.len() as u64)
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "spill run size overflow"))?;
        let current_size = self.state.bytes.load(Ordering::Acquire);
        let reserved = proposed_end.saturating_sub(current_size);
        if reserved > 0 && !self.state.manager.reserve_disk(reserved) {
            self.remember_limit_error(proposed_end, self.state.manager.disk_limit);
            return Err(io::Error::new(
                io::ErrorKind::Other,
                SpillDiskLimitExceeded {
                    requested: proposed_end,
                    limit: self.state.manager.disk_limit,
                },
            ));
        }

        match self.stream.write(buffer) {
            Ok(written) => {
                let actual_end = position.saturating_add(written as u64);
                let new_size = current_size.max(actual_end);
                let actual_growth = new_size - current_size;
                if reserved > actual_growth {
                    self.state.manager.release_disk(reserved - actual_growth);
                }
                if actual_growth > 0 {
                    self.state.bytes.store(new_size, Ordering::Release);
                }
                Ok(written)
            }
            Err(error) => {
                self.state.manager.release_disk(reserved);
                Err(error)
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        self.stream.flush()
    }
}

#[derive(Debug)]
pub(crate) struct SpillDiskLimitExceeded {
    pub(crate) requested: u64,
    pub(crate) limit: u64,
}

impl std::fmt::Display for SpillDiskLimitExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "query spill disk limit exceeded")
    }
}

impl std::error::Error for SpillDiskLimitExceeded {}

impl std::io::Seek for SpillWriteHandle {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        self.stream.seek(position)
    }
}

/// A committed run. Clones share one cleanup lease; the backend deletes the
/// run and releases its disk charge when the final reference is dropped.
#[derive(Clone)]
pub(crate) struct SpillRun {
    state: Arc<SpillRunInner>,
}

impl std::fmt::Debug for SpillRun {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpillRun")
            .field("bytes", &self.bytes())
            .finish_non_exhaustive()
    }
}

impl SpillRun {
    pub(crate) fn open(&self) -> Result<Box<dyn SpillStream>> {
        if self.state.manager.cancellation.is_cancelled() {
            return Err(VeinError::cancelled());
        }
        self.state.manager.storage.open(&self.state.id)
    }

    pub(crate) fn bytes(&self) -> u64 {
        self.state.bytes.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn exists(&self) -> bool {
        self.state.manager.storage.exists(&self.state.id)
    }
}

struct SpillRunInner {
    manager: Arc<SpillManagerInner>,
    id: SpillRunId,
    bytes: AtomicU64,
    deleted: AtomicBool,
}

impl Drop for SpillRunInner {
    fn drop(&mut self) {
        if self.deleted.swap(true, Ordering::AcqRel) {
            return;
        }
        match self.manager.storage.remove(&self.id) {
            Ok(()) => self
                .manager
                .release_disk(self.bytes.load(Ordering::Acquire)),
            Err(error) => tracing::warn!(
                target: "vein.audit",
                event = "spill_run_cleanup_failed",
                error = %error,
                "failed to remove spill run"
            ),
        }
    }
}

impl SpillManagerInner {
    fn reserve_disk(&self, bytes: u64) -> bool {
        let mut used = self.disk_used.load(Ordering::Acquire);
        loop {
            let Some(next) = used.checked_add(bytes) else {
                return false;
            };
            if next > self.disk_limit {
                return false;
            }
            match self.disk_used.compare_exchange_weak(
                used,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.disk_peak.fetch_max(next, Ordering::AcqRel);
                    return true;
                }
                Err(actual) => used = actual,
            }
        }
    }

    fn release_disk(&self, bytes: u64) {
        if bytes > 0 {
            self.disk_used.fetch_sub(bytes, Ordering::AcqRel);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spill::LocalSpillStorage;
    use std::io::Write;

    fn manager(limit: u64) -> SpillManager {
        SpillManager::with_storage(
            CancellationToken::new(),
            limit,
            Arc::new(LocalSpillStorage::new()),
        )
    }

    #[test]
    fn committed_run_accounts_bytes_until_its_last_handle_drops() {
        let manager = manager(16);
        let mut pending = manager.create_run().expect("create spill run");
        let mut writer = pending.writer().expect("open spill writer");
        writer.write_all(b"spill").expect("write spill bytes");
        let run = pending.commit(writer).expect("commit spill run");
        let retained = run.clone();

        assert_eq!(run.bytes(), 5);
        assert_eq!(manager.disk_used(), 5);
        assert_eq!(manager.disk_peak(), 5);
        assert!(manager.exists(&run));

        drop(run);
        assert_eq!(manager.disk_used(), 5, "the clone still owns the run");
        assert!(manager.exists(&retained));
        drop(retained);
        assert_eq!(manager.disk_used(), 0);
    }

    #[test]
    fn disk_quota_failure_is_reported_as_resource_exhausted_and_releases_partial_run() {
        let manager = manager(4);
        let mut pending = manager.create_run().expect("create spill run");
        let mut writer = pending.writer().expect("open spill writer");
        assert!(writer.write_all(b"12345").is_err());

        let error = pending
            .commit(writer)
            .expect_err("run over query spill quota must fail");
        assert!(matches!(
            error,
            VeinError::ResourceExhausted { requested: 5, limit: 4, .. }
        ));
        assert_eq!(manager.disk_used(), 0);
    }

    #[test]
    fn dropping_uncommitted_run_removes_partial_file_and_releases_accounting() {
        let manager = manager(16);
        let mut pending = manager.create_run().expect("create spill run");
        let directory = manager.local_directory().expect("lazy directory created");
        let mut writer = pending.writer().expect("open spill writer");
        writer.write_all(b"partial").expect("write partial run");
        drop(writer);
        assert_eq!(manager.disk_used(), 7);

        drop(pending);

        assert_eq!(manager.disk_used(), 0);
        assert_eq!(std::fs::read_dir(directory).unwrap().count(), 0);
    }
}
