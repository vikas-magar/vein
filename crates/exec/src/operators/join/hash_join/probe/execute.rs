//! Bounded candidate production and join-kind semantics.
use super::*;
use super::super::super::hash_table::probe::HashProbe;

impl HashJoin {
    pub(super) fn probe(&mut self, chunk: DataChunkView) -> Result<()> {
        if chunk.is_empty() || self.next.finished() { return Ok(()); }
        u32::try_from(chunk.num_rows())
            .map_err(|_| VeinError::internal("hash join probe batch exceeds the row-index range"))?;
        self.probed_rows = self.probed_rows.saturating_add(chunk.num_rows() as u64);
        if self.table.is_empty() {
            if matches!(self.join_type, JoinType::Left | JoinType::Full | JoinType::Single | JoinType::Anti | JoinType::NullAwareAnti) {
                self.forward_rows(Some(&chunk), (0..chunk.num_rows()).map(|row| OutputRow {
                    probe: Some(row as u32),
                    build: None,
                }))?;
            }
            return Ok(());
        }
        if self.join_type == JoinType::NullAwareAnti && self.table.has_null_keys() { return Ok(()); }
        if self.join_type.tracks_build_matches() {
            self.ensure_build_match_bitmap()?;
        }
        let table = Arc::clone(&self.table);
        let existence = matches!(self.join_type, JoinType::Semi | JoinType::Anti | JoinType::NullAwareAnti);
        let started = Instant::now();
        let mut cursor = HashProbe::new(&table, &chunk, existence)?;
        self.probe_key_materialization.accumulate(cursor.report());
        self.probe_key_materialize_us = self.probe_key_materialize_us.saturating_add(cursor.key_access_us);
        let mut candidates = Vec::new();
        let limit = cursor.reserve_candidates(&mut candidates)?;
        self.probe_us = self.probe_us.saturating_add(started.elapsed().as_micros() as u64);
        loop {
            let started = Instant::now();
            let has_candidates = cursor.next_batch(&mut candidates, limit, || self.next.finished())?;
            self.probe_us = self.probe_us.saturating_add(started.elapsed().as_micros() as u64);
            if !has_candidates { break; }
            self.max_candidate_batch_rows = self.max_candidate_batch_rows.max(candidates.len());
            if self.join_type.build_existence() {
                self.retain_unmatched_build_candidates(&mut candidates)?;
            }
            if self.residual_transform.is_some() {
                let elapsed = self.filter_residual_matches(&chunk, &mut candidates)?;
                self.residual_us = self.residual_us.saturating_add(elapsed);
            }
            for &(probe, _) in &candidates {
                if cursor.accept(probe as usize) && self.join_type == JoinType::Single {
                    return Err(VeinError::stage("join", "scalar subquery returned more than one row"));
                }
            }
            if self.join_type.tracks_build_matches() {
                self.record_build_matches(&candidates)?;
            }
            self.matched_rows = self.matched_rows.saturating_add(candidates.len() as u64);
            if !existence && !self.join_type.build_existence() {
                self.forward_rows(Some(&chunk), candidates.iter().map(|&(probe, build)| OutputRow {
                    probe: Some(probe),
                    build: Some(build),
                }))?;
            }
            if self.next.finished() { break; }
        }
        self.candidate_rows_examined = self.candidate_rows_examined.saturating_add(cursor.examined);
        if self.next.finished() { return Ok(()); }
        if !existence && !matches!(self.join_type, JoinType::Left | JoinType::Full | JoinType::Single) { return Ok(()); }
        let mut remaining = Vec::new();
        for row in 0..chunk.num_rows() {
            let matched = cursor.matched(row);
            let keep = match self.join_type {
                JoinType::Semi => matched,
                JoinType::Anti => !matched,
                JoinType::NullAwareAnti => !matched && (table.is_empty() || !cursor.is_null(row)?),
                JoinType::Left | JoinType::Full | JoinType::Single => !matched,
                _ => false,
            };
            if keep {
                remaining.push(OutputRow { probe: Some(row as u32), build: None });
                if remaining.len() == limit {
                    self.forward_rows(Some(&chunk), remaining.drain(..))?;
                    if self.next.finished() { break; }
                }
            }
        }
        self.forward_rows(Some(&chunk), remaining)?;
        Ok(())
    }
}
