use super::*;

impl<'a> HashProbe<'a> {
    pub(crate) fn new(table: &'a HashTable, view: &'a DataChunkView, existence: bool) -> Result<Self> {
        u32::try_from(view.num_rows()).map_err(|_| VeinError::internal("join probe row-index overflow"))?;
        let (keys, key_access_us) = ChunkKeys::resolve_view(&table.probe_keys, view)?;
        let bytes = view.num_rows().checked_mul(size_of::<bool>())
            .and_then(|bytes| bytes.checked_add(keys.metadata_bytes()))
            .ok_or_else(|| VeinError::internal("join probe workspace overflow"))?;
        let mut memory = table.tracker.reserve(bytes, "join probe cursor")?;
        let mut accepted = Vec::new();
        accepted.try_reserve_exact(view.num_rows()).map_err(allocation_error)?;
        accepted.resize(view.num_rows(), false);
        let actual = accepted.capacity() * size_of::<bool>() + keys.metadata_bytes();
        if actual > bytes { memory.grow(actual - bytes, "join probe cursor")?; }
        // Leave at least half the available workspace for candidate output.
        let available = table.tracker.limit().saturating_sub(table.tracker.used());
        let count = view.num_rows().min(control::MAX_WORK_BETWEEN_POLLS)
            .min(available / (2 * size_of::<ProbeRow>())).max(1);
        memory.grow(count * size_of::<ProbeRow>(), "join probe hash window")?;
        let mut rows = Vec::new();
        rows.try_reserve_exact(count).map_err(allocation_error)?;
        if rows.capacity() > count {
            memory.grow((rows.capacity() - count) * size_of::<ProbeRow>(), "join probe hash window")?;
        }
        let inline_i64 = keys.is_single_i64() && table.keys_by_chunk.first().is_some_and(ChunkKeys::is_single_i64);
        Ok(Self {
            table, view, keys, position: 0, next_window: 0, window_start: 0, rows,
            accepted, existence, inline_i64, memory, key_access_us,
            examined: 0, metrics: ProbeMetrics::default(),
        })
    }

    pub(crate) fn reserve_candidates(&mut self, matches: &mut Vec<JoinMatch>) -> Result<usize> {
        let available = self.table.tracker.limit().saturating_sub(self.table.tracker.used());
        let batch_rows = self.table.chunks.iter().map(DataChunkView::num_rows)
            .max().unwrap_or(1).max(self.view.num_rows());
        let limit = batch_rows.min(available / size_of::<JoinMatch>()).max(1);
        self.memory.grow(limit * size_of::<JoinMatch>(), "join probe candidates")?;
        matches.try_reserve_exact(limit).map_err(allocation_error)?;
        if matches.capacity() > limit {
            self.memory.grow((matches.capacity() - limit) * size_of::<JoinMatch>(), "join probe candidates")?;
        }
        Ok(limit)
    }
}
