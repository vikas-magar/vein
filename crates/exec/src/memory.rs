//! Query-scoped accounting for memory owned by blocking operators.
//!
//! Reservations are acquired before an operator grows an owned data structure
//! and released when that structure is dropped. Most source Arrow buffers are
//! externally owned and are not charged here. Spill-decoded arrays attach a
//! reservation lease to [`crate::pipeline::DataChunkView`] so their footprint
//! remains charged while zero-copy output views retain those arrays.

use std::fs::{self, File, OpenOptions};
use std::io::ErrorKind;
use std::path::PathBuf;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

use common::Result;
use common::cancellation::CancellationToken;
use error::VeinError;

#[derive(Debug)]
struct MemoryTrackerInner {
    limit: usize,
    used: AtomicUsize,
    peak: AtomicUsize,
    cancel: CancellationToken,
    spill_directory: OnceLock<std::result::Result<SpillDirectory, String>>,
    retained_arrays: Mutex<HashMap<usize, Weak<MemoryReservation>>>,
    retained_array_requests: AtomicUsize,
}

/// Owns the files written by one query's blocking operators.
///
/// The directory is created on the first spill and removed when the final
/// query tracker is dropped. File creation uses `create_new` so concurrent
/// operators cannot overwrite one another's runs.
#[derive(Debug)]
struct SpillDirectory {
    path: PathBuf,
    next_file: AtomicUsize,
}

impl SpillDirectory {
    fn create() -> std::io::Result<Self> {
        let root = std::env::temp_dir();
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        for attempt in 0..128u32 {
            let path = root.join(format!(
                "vein-query-{}-{timestamp}-{attempt}",
                std::process::id()
            ));
            match fs::create_dir(&path) {
                Ok(()) => {
                    return Ok(Self {
                        path,
                        next_file: AtomicUsize::new(0),
                    });
                }
                Err(error) if error.kind() == ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error),
            }
        }
        Err(std::io::Error::new(
            ErrorKind::AlreadyExists,
            "could not allocate a unique query spill directory",
        ))
    }

    fn create_file(&self) -> std::io::Result<(PathBuf, File)> {
        loop {
            let index = self.next_file.fetch_add(1, Ordering::Relaxed);
            let path = self.path.join(format!("run-{index}.arrow"));
            match OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&path)
            {
                Ok(file) => return Ok((path, file)),
                Err(error) if error.kind() == ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error),
            }
        }
    }
}

impl Drop for SpillDirectory {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_dir_all(&self.path) {
            if error.kind() != ErrorKind::NotFound {
                tracing::warn!(
                    target: "vein.audit",
                    event = "query_spill_cleanup_failed",
                    path = %self.path.display(),
                    error = %error,
                    "failed to remove query spill directory"
                );
            }
        }
    }
}

/// Shared byte budget for the blocking operators in one task execution.
///
/// Cloning a tracker shares its counters and configured limit. Each call to
/// [`reserve`](Self::reserve) returns an RAII reservation; dropping it releases
/// the corresponding bytes on success, error, or cancellation.
#[derive(Clone, Debug)]
pub struct QueryMemoryTracker {
    inner: Arc<MemoryTrackerInner>,
}

impl QueryMemoryTracker {
    /// Create a tracker with a byte limit and the execution's cancellation token.
    pub fn new(limit: usize, cancel: CancellationToken) -> Self {
        Self {
            inner: Arc::new(MemoryTrackerInner {
                limit,
                used: AtomicUsize::new(0),
                peak: AtomicUsize::new(0),
                cancel,
                spill_directory: OnceLock::new(),
                retained_arrays: Mutex::new(HashMap::new()),
                retained_array_requests: AtomicUsize::new(0),
            }),
        }
    }

    /// Create a tracker whose limit is the platform's maximum addressable size.
    pub fn unlimited(cancel: CancellationToken) -> Self {
        Self::new(usize::MAX, cancel)
    }

    /// Configured limit in bytes.
    pub fn limit(&self) -> usize {
        self.inner.limit
    }

    /// Bytes currently reserved by live operator state.
    pub fn used(&self) -> usize {
        self.inner.used.load(Ordering::Acquire)
    }

    /// Highest observed reservation total.
    pub fn peak(&self) -> usize {
        self.inner.peak.load(Ordering::Acquire)
    }

    /// Stop blocking-operator work promptly when the owning query is cancelled.
    pub fn check_cancelled(&self) -> Result<()> {
        if self.inner.cancel.is_cancelled() {
            Err(VeinError::cancelled())
        } else {
            Ok(())
        }
    }

    /// Share one reservation for an Arrow array retained by blocking operators.
    ///
    /// The weak registry deduplicates the same `ArrayRef` across composed views
    /// without extending its lifetime. The returned lease owns the reservation;
    /// it is released when the final retaining view drops it.
    pub(crate) fn lease_retained_array(
        &self,
        identity: usize,
        bytes: usize,
        resource: &'static str,
    ) -> Result<Arc<MemoryReservation>> {
        let mut retained = self
            .inner
            .retained_arrays
            .lock()
            .map_err(|_| VeinError::internal("retained array lease registry poisoned"))?;
        if let Some(lease) = retained.get(&identity).and_then(Weak::upgrade) {
            return Ok(lease);
        }

        let lease = Arc::new(self.reserve(bytes, resource)?);
        retained.insert(identity, Arc::downgrade(&lease));

        // Bound stale weak entries after arrays have been released. Pruning is
        // amortized so the hot path does not scan the registry for every batch.
        let request = self
            .inner
            .retained_array_requests
            .fetch_add(1, Ordering::Relaxed)
            .wrapping_add(1);
        if request % 256 == 0 {
            retained.retain(|_, lease| lease.strong_count() != 0);
        }
        Ok(lease)
    }

    pub(crate) fn identity(&self) -> usize {
        Arc::as_ptr(&self.inner) as usize
    }

    /// Create a unique read/write file in this query's lazily allocated spill directory.
    ///
    /// The directory and all files created through this method are removed when
    /// the last tracker clone is dropped.
    pub fn create_spill_file(&self) -> Result<(PathBuf, File)> {
        if self.inner.cancel.is_cancelled() {
            return Err(VeinError::cancelled());
        }

        let directory = self
            .inner
            .spill_directory
            .get_or_init(|| SpillDirectory::create().map_err(|error| error.to_string()))
            .as_ref()
            .map_err(|error| VeinError::internal(format!("create query spill directory: {error}")))?;
        directory
            .create_file()
            .map_err(|error| VeinError::internal(format!("create query spill file: {error}")))
    }

    #[cfg(test)]
    fn spill_directory_path(&self) -> Option<PathBuf> {
        self.inner
            .spill_directory
            .get()
            .and_then(|directory| directory.as_ref().ok())
            .map(|directory| directory.path.clone())
    }

    /// Reserve bytes before growing an operator-owned allocation.
    pub fn reserve(&self, bytes: usize, resource: &'static str) -> Result<MemoryReservation> {
        if self.inner.cancel.is_cancelled() {
            return Err(VeinError::cancelled());
        }

        let mut used = self.inner.used.load(Ordering::Acquire);
        loop {
            let Some(next) = used.checked_add(bytes) else {
                tracing::warn!(
                    target: "vein.audit",
                    event = "query_memory_limit_exceeded",
                    resource,
                    used_bytes = used,
                    requested_bytes = bytes,
                    limit_bytes = self.inner.limit,
                    "query memory reservation overflow"
                );
                return Err(VeinError::ResourceExhausted {
                    resource: resource.to_owned(),
                    requested: bytes,
                    limit: self.inner.limit,
                });
            };
            if next > self.inner.limit {
                tracing::warn!(
                    target: "vein.audit",
                    event = "query_memory_limit_exceeded",
                    resource,
                    used_bytes = used,
                    requested_bytes = bytes,
                    limit_bytes = self.inner.limit,
                    "query memory limit exceeded"
                );
                return Err(VeinError::ResourceExhausted {
                    resource: resource.to_owned(),
                    requested: bytes,
                    limit: self.inner.limit,
                });
            }
            match self.inner.used.compare_exchange_weak(
                used,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.inner.peak.fetch_max(next, Ordering::AcqRel);
                    return Ok(MemoryReservation {
                        tracker: self.clone(),
                        bytes,
                    });
                }
                Err(actual) => used = actual,
            }
        }
    }
}

/// RAII ownership of a portion of a query memory budget.
#[derive(Debug)]
pub struct MemoryReservation {
    tracker: QueryMemoryTracker,
    bytes: usize,
}

impl crate::pipeline::view::ViewMemoryLease for MemoryReservation {
    fn tracker_id(&self) -> usize {
        self.tracker.identity()
    }
}

impl MemoryReservation {
    /// Bytes held by this reservation.
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// Grow the reservation before growing its backing allocation.
    pub fn grow(&mut self, bytes: usize, resource: &'static str) -> Result<()> {
        let mut extra = self.tracker.reserve(bytes, resource)?;
        self.bytes = self.bytes.checked_add(extra.bytes).ok_or_else(|| {
            VeinError::internal("memory reservation size overflow after successful reserve")
        })?;
        extra.bytes = 0;
        Ok(())
    }

    /// Release bytes after the corresponding allocation has been removed.
    pub fn shrink(&mut self, bytes: usize) {
        let released = bytes.min(self.bytes);
        self.bytes -= released;
        self.tracker.inner.used.fetch_sub(released, Ordering::AcqRel);
    }

    /// Transfer another reservation into this owner.
    ///
    /// Reservations from the same tracker can be combined without changing the
    /// total. When the reservations belong to different trackers, first charge
    /// the retained allocation to this reservation's budget; dropping `other`
    /// then releases it from its former budget. This lets independently built
    /// operator state be merged without losing destination-budget enforcement.
    pub fn absorb(&mut self, mut other: Self) -> Result<()> {
        if !Arc::ptr_eq(&self.tracker.inner, &other.tracker.inner) {
            self.grow(other.bytes, "merged operator state")?;
            return Ok(());
        }
        self.bytes = self
            .bytes
            .checked_add(other.bytes)
            .ok_or_else(|| VeinError::internal("combined memory reservation overflow"))?;
        other.bytes = 0;
        Ok(())
    }
}

impl Drop for MemoryReservation {
    fn drop(&mut self) {
        self.tracker.inner.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reservation_honors_exact_limit_and_releases_on_drop() {
        let tracker = QueryMemoryTracker::new(16, CancellationToken::new());
        let reservation = tracker.reserve(16, "test state").expect("exact budget fits");
        assert_eq!(tracker.used(), 16);
        assert_eq!(tracker.peak(), 16);
        assert!(matches!(
            tracker.reserve(1, "test state"),
            Err(VeinError::ResourceExhausted { requested: 1, limit: 16, .. })
        ));
        drop(reservation);
        assert_eq!(tracker.used(), 0);
    }

    #[test]
    fn reservation_growth_and_shrink_update_shared_accounting() {
        let tracker = QueryMemoryTracker::new(32, CancellationToken::new());
        let mut reservation = tracker.reserve(8, "test state").expect("initial reserve");
        reservation.grow(12, "test state").expect("grow reservation");
        assert_eq!(reservation.bytes(), 20);
        assert_eq!(tracker.used(), 20);
        reservation.shrink(5);
        assert_eq!(reservation.bytes(), 15);
        assert_eq!(tracker.used(), 15);
        drop(reservation);
        assert_eq!(tracker.used(), 0);
    }

    #[test]
    fn absorb_transfers_usage_between_independent_trackers() {
        let source = QueryMemoryTracker::new(32, CancellationToken::new());
        let destination = QueryMemoryTracker::new(32, CancellationToken::new());
        let mut retained = destination.reserve(8, "destination state").unwrap();
        let incoming = source.reserve(12, "source state").unwrap();

        retained.absorb(incoming).expect("transfer incoming reservation");

        assert_eq!(source.used(), 0);
        assert_eq!(destination.used(), 20);
        assert_eq!(retained.bytes(), 20);
        drop(retained);
        assert_eq!(destination.used(), 0);
    }

    #[test]
    fn failed_absorb_releases_incoming_state_without_changing_destination() {
        let source = QueryMemoryTracker::new(32, CancellationToken::new());
        let destination = QueryMemoryTracker::new(16, CancellationToken::new());
        let mut retained = destination.reserve(8, "destination state").unwrap();
        let incoming = source.reserve(12, "source state").unwrap();

        assert!(matches!(
            retained.absorb(incoming),
            Err(VeinError::ResourceExhausted { .. })
        ));

        assert_eq!(source.used(), 0, "failed merge drops the incoming state");
        assert_eq!(destination.used(), 8, "failed merge preserves destination state");
        assert_eq!(retained.bytes(), 8);
    }

    #[test]
    fn canceled_execution_cannot_reserve_more_memory() {
        let cancel = CancellationToken::new();
        let tracker = QueryMemoryTracker::new(128, cancel.clone());
        let reservation = tracker.reserve(64, "test state").expect("initial reserve");
        cancel.cancel();
        assert!(matches!(tracker.reserve(1, "test state"), Err(VeinError::Cancelled)));
        drop(reservation);
        assert_eq!(tracker.used(), 0);
    }

    #[test]
    fn concurrent_reservations_never_cross_the_limit() {
        let tracker = QueryMemoryTracker::new(64, CancellationToken::new());
        let threads = (0..8)
            .map(|_| {
                let tracker = tracker.clone();
                std::thread::spawn(move || tracker.reserve(16, "parallel state").ok())
            })
            .collect::<Vec<_>>();
        let reservations = threads
            .into_iter()
            .filter_map(|thread| thread.join().expect("reservation worker"))
            .collect::<Vec<_>>();
        assert_eq!(reservations.len(), 4);
        assert_eq!(tracker.used(), 64);
        drop(reservations);
        assert_eq!(tracker.used(), 0);
        assert!(tracker.peak() <= tracker.limit());
    }

    #[test]
    fn retained_array_leases_are_shared_and_released_after_the_last_view() {
        let tracker = QueryMemoryTracker::new(32, CancellationToken::new());
        let first = tracker
            .lease_retained_array(7, 16, "retained test array")
            .expect("reserve retained array");
        let second = tracker
            .lease_retained_array(7, 16, "retained test array")
            .expect("share retained array reservation");

        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(tracker.used(), 16, "one array identity is charged once");
        drop(first);
        assert_eq!(tracker.used(), 16, "the second view still retains its lease");
        drop(second);
        assert_eq!(tracker.used(), 0, "the final view releases its lease");

        let replacement = tracker
            .lease_retained_array(7, 8, "replacement test array")
            .expect("reused identity gets a fresh reservation");
        assert_eq!(tracker.used(), 8);
        drop(replacement);
        assert_eq!(tracker.used(), 0);
    }

    #[test]
    fn query_spill_files_are_unique_and_cleaned_with_tracker() {
        let tracker = QueryMemoryTracker::unlimited(CancellationToken::new());
        let (first_path, mut first) = tracker.create_spill_file().expect("create first run");
        let (second_path, second) = tracker.create_spill_file().expect("create second run");
        assert_ne!(first_path, second_path);
        assert_eq!(first_path.parent(), second_path.parent());
        let directory = tracker
            .spill_directory_path()
            .expect("spill directory is initialized")
            .to_owned();
        use std::io::Write;
        first.write_all(b"run").expect("write run marker");
        assert!(first_path.exists());
        drop(first);
        drop(second);
        drop(tracker);
        assert!(!directory.exists(), "query spill directory is removed on drop");
    }

    #[test]
    fn canceled_query_cannot_create_spill_directory_or_file() {
        let cancel = CancellationToken::new();
        let tracker = QueryMemoryTracker::unlimited(cancel.clone());
        cancel.cancel();

        assert!(matches!(tracker.create_spill_file(), Err(VeinError::Cancelled)));
        assert!(tracker.spill_directory_path().is_none());
    }
}
