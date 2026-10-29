use super::*;

/// Prepared hashes and inline scalar comparison scratch, never Arrow payload buffers.
pub(super) struct ProbeRow {
    pub(super) key: Option<(u64, Option<i64>)>,
    pub(super) head: u32,
}

impl<T, V> HashProbe<T, V>
where T: std::ops::Deref<Target=HashTable>, V: std::ops::Deref<Target=DataChunkView> {
    pub(super) fn load_window(&mut self) -> Result<bool> {
        if self.next_window >= self.view.num_rows() { return Ok(false); }
        self.rows.clear();
        self.window_start = self.next_window;
        self.position = 0;
        let end = self.view.num_rows().min(self.window_start + self.rows.capacity());
        let started = Instant::now();
        for row in self.window_start..end {
            self.rows.push(ProbeRow {
                key: self.keys.hash_and_single_i64(row, &self.view)?,
                head: NONE,
            });
        }
        self.metrics.hash_ns = self.metrics.hash_ns.saturating_add(started.elapsed().as_nanos() as u64);
        self.metrics.hash_rows = self.metrics.hash_rows.saturating_add(self.rows.len() as u64);
        let started = Instant::now();
        for row in &mut self.rows {
            if let Some((hash, _)) = row.key {
                row.head = self.table.buckets[bucket_of(hash, self.table.buckets.len())];
                self.metrics.bucket_lookups += 1;
            }
        }
        self.metrics.bucket_ns = self.metrics.bucket_ns.saturating_add(started.elapsed().as_nanos() as u64);
        self.metrics.hash_windows += 1;
        self.next_window = end;
        Ok(true)
    }
}
