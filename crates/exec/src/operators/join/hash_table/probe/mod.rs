//! Resumable batch hashing and bounded candidate production over retained Arrow arrays.
use super::{bucket_of, key_access::ChunkKeys, HashTable, NONE};
use crate::memory::MemoryReservation;
use crate::pipeline::{DataChunkView, MaterializationReport};
use common::Result;
use error::VeinError;
use std::mem::size_of;
use common::time::Instant;

pub(crate) mod control;
pub(crate) mod metrics;
mod setup;
mod candidates;
mod window;
#[cfg(test)]
mod tests;

use control::WorkBudget;
use metrics::ProbeMetrics;
use window::ProbeRow;

pub(crate) type JoinMatch = (u32, (u32, u32));

/// A local cursor over an immutable table. Only hashes and row indexes enter scratch storage.
pub(crate) struct HashProbe<'a> {
    table: &'a HashTable,
    view: &'a DataChunkView,
    keys: ChunkKeys,
    position: usize,
    next_window: usize,
    window_start: usize,
    rows: Vec<ProbeRow>,
    accepted: Vec<bool>,
    existence: bool,
    inline_i64: bool,
    memory: MemoryReservation,
    pub(crate) key_access_us: u64,
    pub(crate) examined: u64,
    pub(crate) metrics: ProbeMetrics,
}

impl HashProbe<'_> {
    pub(crate) fn report(&self) -> MaterializationReport { MaterializationReport::default() }
    pub(crate) fn matched(&self, row: usize) -> bool { self.accepted[row] }
    pub(crate) fn accept(&mut self, row: usize) -> bool {
        std::mem::replace(&mut self.accepted[row], true)
    }
    pub(crate) fn is_null(&self, row: usize) -> Result<bool> { self.keys.any_null(self.view, row) }

    #[inline]
    fn poll_control(&mut self, budget: &mut WorkBudget, finished: &mut impl FnMut() -> bool) -> Result<bool> {
        if !budget.should_poll() { return Ok(false); }
        self.metrics.control_polls += 1;
        self.table.tracker.check_cancelled()?;
        Ok(finished())
    }
}

fn allocation_error(error: std::collections::TryReserveError) -> VeinError {
    VeinError::internal(format!("cannot reserve join probe workspace: {error}"))
}

impl HashTable {
    pub(crate) fn memory_tracker(&self) -> &crate::memory::QueryMemoryTracker { &self.tracker }
    pub(crate) fn build_rows(&self) -> impl Iterator<Item=(u32, u32)> + '_ {
        self.entries.iter().map(|entry| (entry.chunk, entry.row)).chain(self.null_keyed.iter().copied())
    }
}
