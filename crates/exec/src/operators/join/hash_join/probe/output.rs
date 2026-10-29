//! Bounded reference-backed output emission.
use super::*;

impl HashJoin {
    /// Preserves row order while emitting probe-sized, multi-source views.
    pub(super) async fn forward_rows<I>(
        &mut self,
        probe: Option<&DataChunkView>,
        rows: I,
    ) -> Result<()>
    where
        I: IntoIterator<Item = OutputRow> + Send,
        I::IntoIter: Send,
    {
        let batch_limit = probe
            .map_or(self.build_batch_rows, |view| {
                view.num_rows().max(self.build_batch_rows)
            })
            .max(1);
        for row in rows {
            if self.next.finished() { break; }
            self.output_batch.push(row);
            if self.output_batch.len() == batch_limit {
                self.push_output_batch(probe).await?;
            }
        }
        if !self.output_batch.is_empty() {
            self.push_output_batch(probe).await?;
        }
        Ok(())
    }

    pub(super) async fn push_output_batch(&mut self, probe: Option<&DataChunkView>) -> Result<()> {
        let mut rows = std::mem::take(&mut self.output_batch);
        let compose_started = Instant::now();
        let composed = self
            .output_composer
            .compose(&self.table, probe, &self.probe_types, &rows);
        self.view_compose_us = self
            .view_compose_us
            .saturating_add(compose_started.elapsed().as_micros() as u64);
        let result = async {
            let view = composed?;
            if view.is_empty() {
                return Ok(());
            }
            let row_count = view.num_rows();
            let downstream_started = Instant::now();
            let pushed = self.next.push(view).await;
            self.downstream_push_us = self
                .downstream_push_us
                .saturating_add(downstream_started.elapsed().as_micros() as u64);
            pushed?;
            self.output_batches = self.output_batches.saturating_add(1);
            self.output_rows = self.output_rows.saturating_add(row_count as u64);
            self.max_output_batch_rows = self.max_output_batch_rows.max(row_count);
            Ok(())
        }.await;
        rows.clear();
        self.output_batch = rows;
        result
    }
}
