//! Residual evaluation over a bounded, multi-source candidate view.
use super::*;

impl HashJoin {
    pub(super) async fn filter_residual_matches(
        &mut self, probe: &DataChunkView, matches: &mut Vec<JoinMatch>,
    ) -> Result<u64> {
        if matches.is_empty() { return Ok(0); }
        let output = self.residual_output.as_ref().cloned().ok_or_else(|| {
            VeinError::internal("join residual transform has no output collector")
        })?;
        self.residual_rows.clear();
        self.residual_rows.extend(matches.iter().map(|&(probe, build)| {
            OutputRow {
                probe: Some(probe),
                build: Some(build),
            }
        }));
        let candidate = self.residual_composer.as_mut()
            .ok_or_else(|| VeinError::internal("join residual composer is missing"))?
            .compose(&self.table, Some(probe), &self.probe_types, &self.residual_rows)?;
        output.clear()?;
        let started = Instant::now();
        self.residual_transform.as_mut()
            .ok_or_else(|| VeinError::internal("join residual transform is missing"))?
            .push(candidate).await?;
        let elapsed = started.elapsed().as_micros() as u64;
        let filtered = output.take()?;
        if let Some(filtered) = filtered {
            let mut previous = None;
            let mut retained = 0;
            for position in 0..filtered.selection().size() {
                let row = filtered.selection().get_index(position);
                if previous.is_some_and(|previous| row <= previous) {
                    return Err(VeinError::internal("join residual must preserve candidate order"));
                }
                let candidate = matches.get(row).copied()
                    .ok_or_else(|| VeinError::internal("join residual selection is outside candidates"))?;
                matches[retained] = candidate;
                retained += 1;
                previous = Some(row);
            }
            matches.truncate(retained);
        } else { matches.clear(); }
        Ok(elapsed)
    }
}
