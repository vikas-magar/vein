//! Worker-local probe work counters. These never retain input values.

/// Counts operations, rather than inferring work from output cardinality.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ProbeMetrics {
    pub(crate) hash_ns: u64,
    pub(crate) bucket_ns: u64,
    pub(crate) lookup_ns: u64,
    pub(crate) hash_windows: u64,
    pub(crate) hash_rows: u64,
    pub(crate) bucket_lookups: u64,
    pub(crate) hash_rejections: u64,
    pub(crate) key_comparisons: u64,
    pub(crate) key_rejections: u64,
    pub(crate) control_polls: u64,
}

impl ProbeMetrics {
    /// Combines completed worker counters without synchronizing the probe loop.
    pub(crate) fn accumulate(&mut self, other: Self) {
        self.hash_ns = self.hash_ns.saturating_add(other.hash_ns);
        self.bucket_ns = self.bucket_ns.saturating_add(other.bucket_ns);
        self.lookup_ns = self.lookup_ns.saturating_add(other.lookup_ns);
        self.hash_windows = self.hash_windows.saturating_add(other.hash_windows);
        self.hash_rows = self.hash_rows.saturating_add(other.hash_rows);
        self.bucket_lookups = self.bucket_lookups.saturating_add(other.bucket_lookups);
        self.hash_rejections = self.hash_rejections.saturating_add(other.hash_rejections);
        self.key_comparisons = self.key_comparisons.saturating_add(other.key_comparisons);
        self.key_rejections = self.key_rejections.saturating_add(other.key_rejections);
        self.control_polls = self.control_polls.saturating_add(other.control_polls);
    }
}
