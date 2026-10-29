use super::*;

impl<T, V> HashProbe<T, V>
where T: std::ops::Deref<Target=HashTable>, V: std::ops::Deref<Target=DataChunkView> {
    /// Hashes one bounded window, then resumes its duplicate chains between output batches.
    /// Existence joins finish unresolved rows in that window before hashing the next window.
    pub(crate) fn next_batch(
        &mut self, output: &mut Vec<JoinMatch>, limit: usize, mut finished: impl FnMut() -> bool,
    ) -> Result<bool> {
        let started = Instant::now();
        let phase_ns = self.metrics.hash_ns.saturating_add(self.metrics.bucket_ns);
        let result = self.scan_candidates(output, limit, &mut finished);
        let preparation_ns = self.metrics.hash_ns.saturating_add(self.metrics.bucket_ns).saturating_sub(phase_ns);
        self.metrics.lookup_ns = self.metrics.lookup_ns.saturating_add(
            (started.elapsed().as_nanos() as u64).saturating_sub(preparation_ns));
        result
    }

    fn scan_candidates(
        &mut self, output: &mut Vec<JoinMatch>, limit: usize, finished: &mut impl FnMut() -> bool,
    ) -> Result<bool> {
        output.clear();
        let mut visited = 0;
        let mut budget = WorkBudget::for_batch(limit);
        while output.len() < limit {
            if self.poll_control(&mut budget, finished)? { return Ok(false); }
            if self.rows.is_empty() || !self.existence && self.position == self.rows.len() {
                if !self.load_window()? { break; }
                visited = 0;
            }
            if self.existence && visited == self.rows.len() {
                if !output.is_empty() { break; }
                self.rows.clear();
                continue;
            }
            let index = self.position;
            let row = self.window_start + index;
            if self.existence {
                visited += 1;
                self.position = (self.position + 1) % self.rows.len();
                if self.accepted[row] {
                    self.rows[index].head = NONE;
                    continue;
                }
            }
            let mut head = self.rows[index].head;
            if let Some((hash, inline_key)) = self.rows[index].key {
                while head != NONE {
                    if self.poll_control(&mut budget, finished)? { return Ok(false); }
                    self.examined = self.examined.saturating_add(1);
                    let entry_index = head as usize;
                    let entry = self.table.entries[entry_index];
                    head = entry.next;
                    let hash_matches = self.table.hashes[entry_index] == hash;
                    if hash_matches { self.metrics.key_comparisons += 1; }
                    else { self.metrics.hash_rejections += 1; }
                    let same = hash_matches && if self.inline_i64 {
                        self.table.keys_by_chunk[entry.chunk as usize].single_i64_value(
                            &self.table.chunks[entry.chunk as usize], entry.row as usize)? == inline_key
                    } else {
                        self.table.keys_by_chunk[entry.chunk as usize].equals(entry.row as usize,
                            &self.table.chunks[entry.chunk as usize], &self.keys, row, &self.view)?
                    };
                    if hash_matches && !same { self.metrics.key_rejections += 1; }
                    if same {
                        output.push((row as u32, (entry.chunk, entry.row)));
                        break;
                    }
                }
            }
            self.rows[index].head = head;
            if !self.existence && head == NONE { self.position += 1; }
        }
        Ok(!output.is_empty())
    }
}
