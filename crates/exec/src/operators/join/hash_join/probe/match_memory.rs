//! Compact, query-budgeted match state for right and full joins.
use super::*;

impl HashJoin {
    pub(super) fn record_build_matches(&mut self, matches: &[JoinMatch]) -> Result<()> {
        self.ensure_build_match_bitmap()?;
        for (_, (chunk, row)) in matches {
            let slot = self.build_slot(*chunk, *row)?;
            self.matched_build[slot / u64::BITS as usize] |= 1 << (slot % u64::BITS as usize);
        }
        Ok(())
    }

    pub(super) fn build_row_matched(&self, chunk: u32, row: u32) -> Result<bool> {
        let slot = self.build_slot(chunk, row)?;
        Ok(self.matched_build.get(slot / u64::BITS as usize)
            .is_some_and(|word| word & (1 << (slot % u64::BITS as usize)) != 0))
    }

    pub(super) fn ensure_build_match_bitmap(&mut self) -> Result<()> {
        if !self.matched_build.is_empty() || self.build_row_count == 0 { return Ok(()); }
        let word_count = self.build_row_count.div_ceil(u64::BITS as usize);
        let bytes = word_count.checked_mul(std::mem::size_of::<u64>())
            .ok_or_else(|| VeinError::internal("outer join bitmap size overflow"))?;
        let tracker = self.table.memory_tracker();
        let mut reservation = tracker.reserve(bytes, "outer join match bitmap")?;
        let mut words = Vec::new();
        words.try_reserve_exact(word_count)
            .map_err(|error| VeinError::internal(format!("cannot reserve outer join bitmap: {error}")))?;
        let actual = words.capacity().checked_mul(std::mem::size_of::<u64>())
            .ok_or_else(|| VeinError::internal("outer join bitmap capacity overflow"))?;
        if actual > bytes { reservation.grow(actual - bytes, "outer join match bitmap")?; }
        words.resize(word_count, 0);
        self.matched_build = words;
        self.matched_build_memory = Some(reservation);
        Ok(())
    }

    pub(super) fn combine_build_matches(&mut self, other: &mut Self) -> Result<()> {
        if self.matched_build.is_empty() {
            self.matched_build = std::mem::take(&mut other.matched_build);
            self.matched_build_memory = other.matched_build_memory.take();
            return Ok(());
        }
        if self.matched_build.len() != other.matched_build.len() {
            return Err(VeinError::internal("outer join bitmap sizes differ during worker combine"));
        }
        for (target, source) in self.matched_build.iter_mut().zip(&other.matched_build) {
            *target |= *source;
        }
        Ok(())
    }

    fn build_slot(&self, chunk: u32, row: u32) -> Result<usize> {
        let offset = self.build_chunk_offsets.get(chunk as usize)
            .ok_or_else(|| VeinError::internal("outer join match references an unknown build chunk"))?;
        let slot = offset.checked_add(row as usize)
            .ok_or_else(|| VeinError::internal("outer join match slot overflow"))?;
        if slot >= self.build_row_count {
            return Err(VeinError::internal("outer join match references an unknown build row"));
        }
        Ok(slot)
    }
}
