//! Query-scoped accounting for memory owned by blocking operators.
//!
//! Reservations are acquired before an operator grows an owned data structure
//! and released when that structure is dropped. Source Arrow arrays retained by
//! blocking views receive one shared RAII lease per active array identity;
//! spill-decoded arrays keep that lease as long as zero-copy output views retain
//! them. The estimate uses Arrow's reported array footprint and does not include
//! transient scan buffers or allocations made inside a decoder before it returns.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use common::Result;
use common::cancellation::CancellationToken;
use error::VeinError;
use crate::spill::{SpillManager, SpillStorage};

pub(crate) mod batch;

/// Per-execution budgets shared by all blocking operators.
///
/// These limits cover memory reservations and bytes written to temporary spill
/// runs. They do not include unrelated scan buffers or allocations internal to
/// Arrow before an array's reported footprint is available.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecutionResourceLimits {
    /// Maximum bytes reserved by blocking operator state.
    pub memory_bytes: usize,
    /// Maximum bytes retained across query spill runs.
    pub spill_disk_bytes: u64,
}

impl ExecutionResourceLimits {
    /// Configure memory and spill-disk budgets for one query execution.
    pub const fn new(memory_bytes: usize, spill_disk_bytes: u64) -> Self {
        Self {
            memory_bytes,
            spill_disk_bytes,
        }
    }
}

impl Default for ExecutionResourceLimits {
    fn default() -> Self {
        Self::new(usize::MAX, u64::MAX)
    }
}

#[derive(Debug)]
struct MemoryTrackerInner {
    limit: usize,
    used: AtomicUsize,
    peak: AtomicUsize,
    cancel: CancellationToken,
    spill: SpillManager,
    retained_arrays: Mutex<HashMap<usize, Weak<MemoryReservation>>>,
    retained_array_requests: AtomicUsize,
}

/// Operator-owned resource counters shared with the execution trace.
///
/// These counters report explicit engine reservations and spill-run bytes. They
/// are not a process allocator or RSS measurement.
#[derive(Debug, Default)]
pub(crate) struct OperatorResourceMetrics {
    memory_used: AtomicUsize,
    memory_peak: AtomicUsize,
    spill_used: AtomicU64,
    spill_peak: AtomicU64,
    spill_written: AtomicU64,
}

impl OperatorResourceMetrics {
    fn acquire_memory(&self, bytes: usize) {
        let used = self.memory_used.fetch_add(bytes, Ordering::AcqRel).saturating_add(bytes);
        self.memory_peak.fetch_max(used, Ordering::AcqRel);
    }

    fn release_memory(&self, bytes: usize) {
        self.memory_used.fetch_sub(bytes, Ordering::AcqRel);
    }

    pub(crate) fn memory_used(&self) -> usize {
        self.memory_used.load(Ordering::Acquire)
    }

    pub(crate) fn memory_peak(&self) -> usize {
        self.memory_peak.load(Ordering::Acquire)
    }

    pub(crate) fn spill_used(&self) -> u64 {
        self.spill_used.load(Ordering::Acquire)
    }

    pub(crate) fn spill_peak(&self) -> u64 {
        self.spill_peak.load(Ordering::Acquire)
    }

    pub(crate) fn spill_written(&self) -> u64 {
        self.spill_written.load(Ordering::Acquire)
    }

    pub(crate) fn acquire_spill(&self, bytes: u64) {
        let used = self.spill_used.fetch_add(bytes, Ordering::AcqRel).saturating_add(bytes);
        self.spill_peak.fetch_max(used, Ordering::AcqRel);
        self.spill_written.fetch_add(bytes, Ordering::Relaxed);
    }

    pub(crate) fn release_spill(&self, bytes: u64) {
        self.spill_used.fetch_sub(bytes, Ordering::AcqRel);
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
    attribution: Option<Arc<OperatorResourceMetrics>>,
}

impl QueryMemoryTracker {
    /// Create a tracker with a byte limit and the execution's cancellation token.
    pub fn new(limit: usize, cancel: CancellationToken) -> Self {
        Self::with_resource_limits(ExecutionResourceLimits::new(limit, u64::MAX), cancel)
    }

    /// Create a tracker with query memory and spill-disk budgets.
    pub fn with_resource_limits(
        limits: ExecutionResourceLimits,
        cancel: CancellationToken,
    ) -> Self {
        Self::with_spill_storage(
            limits,
            cancel,
            Arc::new(crate::spill::LocalSpillStorage::new()),
        )
    }

    /// Create a tracker backed by a caller-provided, query-local spill store.
    pub fn with_spill_storage(
        limits: ExecutionResourceLimits,
        cancel: CancellationToken,
        storage: Arc<dyn SpillStorage>,
    ) -> Self {
        Self {
            inner: Arc::new(MemoryTrackerInner {
                limit: limits.memory_bytes,
                used: AtomicUsize::new(0),
                peak: AtomicUsize::new(0),
                cancel: cancel.clone(),
                spill: SpillManager::with_storage(cancel, limits.spill_disk_bytes, storage),
                retained_arrays: Mutex::new(HashMap::new()),
                retained_array_requests: AtomicUsize::new(0),
            }),
            attribution: None,
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

    /// Whether this tracker uses the platform maximum as its memory limit.
    ///
    /// Operators that support spilling can use worker-local state only when
    /// their reservations cannot trigger the bounded-memory spill path.
    pub fn is_unlimited(&self) -> bool {
        self.inner.limit == usize::MAX
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
        if request.is_multiple_of(256) {
            retained.retain(|_, lease| lease.strong_count() != 0);
        }
        Ok(lease)
    }

    pub(crate) fn identity(&self) -> usize {
        Arc::as_ptr(&self.inner) as usize
    }

    /// Shared temporary-run lifecycle for all blocking operators in this query.
    pub(crate) fn spill_manager(&self) -> SpillManager {
        self.inner.spill.with_attribution(self.attribution.clone())
    }

    /// Return a cheap tracker view that charges reservations to one plan node
    /// while still enforcing the shared query-wide budget.
    pub(crate) fn for_operator(
        &self,
        attribution: Arc<OperatorResourceMetrics>,
    ) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            attribution: Some(attribution),
        }
    }

    /// Current temporary storage charged to this query.
    pub fn spill_bytes_used(&self) -> u64 {
        self.inner.spill.disk_used()
    }

    /// Peak temporary storage charged to this query.
    pub fn spill_bytes_peak(&self) -> u64 {
        self.inner.spill.disk_peak()
    }

    /// Configured query spill-disk limit in bytes.
    pub fn spill_bytes_limit(&self) -> u64 {
        self.inner.spill.disk_limit()
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
                return Err(VeinError::resource_exhausted(
                    resource,
                    bytes as u128,
                    self.inner.limit as u128,
                ));
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
                return Err(VeinError::resource_exhausted(
                    resource,
                    bytes as u128,
                    self.inner.limit as u128,
                ));
            }
            match self.inner.used.compare_exchange_weak(
                used,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.inner.peak.fetch_max(next, Ordering::AcqRel);
                    if let Some(attribution) = &self.attribution {
                        attribution.acquire_memory(bytes);
                    }
                    if tracing::enabled!(target: "vein.audit", tracing::Level::DEBUG) {
                        tracing::debug!(
                            target: "vein.audit",
                            event = "memory_reservation_acquired",
                            resource,
                            reserved_bytes = bytes,
                            memory_used_bytes = next,
                            memory_peak_bytes = self.inner.peak.load(Ordering::Relaxed),
                            memory_limit_bytes = self.inner.limit,
                            "memory_reservation_acquired"
                        );
                    }
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
        let used = self.tracker.inner.used.fetch_sub(released, Ordering::AcqRel) - released;
        if let Some(attribution) = &self.tracker.attribution {
            attribution.release_memory(released);
        }
        if tracing::enabled!(target: "vein.audit", tracing::Level::DEBUG) {
            tracing::debug!(
                target: "vein.audit",
                event = "memory_reservation_released",
                released_bytes = released,
                memory_used_bytes = used,
                memory_peak_bytes = self.tracker.peak(),
                memory_limit_bytes = self.tracker.limit(),
                "memory_reservation_released"
            );
        }
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
        let same_attribution = match (&self.tracker.attribution, &other.tracker.attribution) {
            (Some(left), Some(right)) => Arc::ptr_eq(left, right),
            (None, None) => true,
            _ => false,
        };
        if !same_attribution {
            if let Some(attribution) = &self.tracker.attribution {
                attribution.acquire_memory(other.bytes);
            }
            if let Some(attribution) = &other.tracker.attribution {
                attribution.release_memory(other.bytes);
            }
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
        let released = self.bytes;
        let used = self
            .tracker
            .inner
            .used
            .fetch_sub(released, Ordering::AcqRel)
            .saturating_sub(released);
        if let Some(attribution) = &self.tracker.attribution {
            attribution.release_memory(released);
        }
        if tracing::enabled!(target: "vein.audit", tracing::Level::DEBUG) {
            tracing::debug!(
                target: "vein.audit",
                event = "memory_reservation_released",
                released_bytes = released,
                memory_used_bytes = used,
                memory_peak_bytes = self.tracker.peak(),
                memory_limit_bytes = self.tracker.limit(),
                "memory_reservation_released"
            );
        }
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
    fn query_spill_runs_are_unique_accounted_and_cleaned_with_tracker() {
        let tracker = QueryMemoryTracker::unlimited(CancellationToken::new());
        let manager = tracker.spill_manager();
        let mut first_pending = manager.create_run().expect("create first run");
        let mut second_pending = manager.create_run().expect("create second run");
        let directory = manager.local_directory().expect("local spill directory initialized");
        use std::io::Write;
        let mut writer = first_pending.writer().expect("first writer");
        writer.write_all(b"run").expect("write run marker");
        let first = first_pending.commit(writer).expect("commit first run");
        let second_writer = second_pending.writer().expect("second writer");
        let second = second_pending.commit(second_writer).expect("commit empty run");
        assert_eq!(tracker.spill_bytes_used(), 3);
        assert_eq!(tracker.spill_bytes_peak(), 3);
        assert!(manager.exists(&first));
        drop(first);
        drop(second);
        assert_eq!(tracker.spill_bytes_used(), 0);
        drop(tracker);
        drop(manager);
        assert!(!directory.exists(), "query spill directory is removed on drop");
    }

    #[test]
    fn canceled_query_cannot_create_spill_directory_or_file() {
        let cancel = CancellationToken::new();
        let tracker = QueryMemoryTracker::unlimited(cancel.clone());
        cancel.cancel();

        assert!(matches!(tracker.spill_manager().create_run(), Err(VeinError::Cancelled)));
        assert!(tracker.spill_manager().local_directory().is_none());
    }

    #[test]
    fn configured_spill_limit_is_enforced_and_partial_runs_are_cleaned() {
        use std::io::Write;

        let tracker = QueryMemoryTracker::with_resource_limits(
            ExecutionResourceLimits::new(64, 4),
            CancellationToken::new(),
        );
        let manager = tracker.spill_manager();
        let mut pending = manager.create_run().expect("create bounded spill run");
        let directory = manager.local_directory().expect("spill directory created");
        let writer = pending.writer().expect("create writer");
        let mut writer = writer;

        assert!(writer.write_all(b"12345").is_err());
        assert!(matches!(
            pending.commit(writer),
            Err(VeinError::ResourceExhausted { limit: 4, .. })
        ));
        assert_eq!(tracker.spill_bytes_used(), 0, "failed run releases its reservation");

        drop(tracker);
        drop(manager);
        assert!(!directory.exists(), "failed query spill directory is removed");
    }
}
