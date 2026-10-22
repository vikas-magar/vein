//! Resumable key probing. Candidate storage is bounded by a probe batch, never its join product.
use super::{bucket_of, key_access::ChunkKeys, HashTable, NONE};
use crate::memory::MemoryReservation;
use crate::pipeline::{DataChunkView, MaterializationReport};
use common::Result;
use error::VeinError;
use std::mem::size_of;
#[cfg(test)]
mod tests;

pub(crate) type JoinMatch = (u32, (u32, u32));

/// A local cursor over an immutable table; Arrow key values remain reference-backed.
pub(crate) struct HashProbe<'a> {
    table: &'a HashTable,
    view: &'a DataChunkView,
    keys: ChunkKeys,
    position: usize,
    head: Option<u32>,
    heads: Vec<Option<u32>>,
    cached_key: Option<(u64, Option<i64>)>,
    accepted: Vec<bool>,
    existence: bool,
    inline_i64: bool,
    memory: MemoryReservation,
    pub(crate) key_access_us: u64,
    pub(crate) examined: u64,
}

impl<'a> HashProbe<'a> {
    pub(crate) fn new(table: &'a HashTable, view: &'a DataChunkView, existence: bool) -> Result<Self> {
        u32::try_from(view.num_rows()).map_err(|_| VeinError::internal("join probe row-index overflow"))?;
        let (keys, key_access_us) = ChunkKeys::resolve_view(&table.probe_keys, view)?;
        let bytes = view.num_rows().checked_mul(size_of::<bool>() + if existence { size_of::<Option<u32>>() } else { 0 })
            .and_then(|bytes| bytes.checked_add(keys.metadata_bytes()))
            .ok_or_else(|| VeinError::internal("join probe workspace overflow"))?;
        let mut memory = table.tracker.reserve(bytes, "join probe cursor")?;
        let mut accepted = Vec::new();
        accepted.try_reserve_exact(view.num_rows()).map_err(allocation_error)?;
        accepted.resize(view.num_rows(), false);
        let mut heads = Vec::new();
        if existence {
            heads.try_reserve_exact(view.num_rows()).map_err(allocation_error)?;
            heads.resize(view.num_rows(), None);
        }
        let actual = accepted.capacity() * size_of::<bool>() + heads.capacity() * size_of::<Option<u32>>() + keys.metadata_bytes();
        if actual > bytes { memory.grow(actual - bytes, "join probe cursor")?; }
        let inline_i64 = keys.is_single_i64() && table.keys_by_chunk.first().is_some_and(ChunkKeys::is_single_i64);
        Ok(Self {
            table,
            view,
            keys,
            position: 0,
            head: None,
            heads,
            cached_key: None,
            accepted,
            existence,
            inline_i64,
            memory,
            key_access_us,
            examined: 0,
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

    pub(crate) fn report(&self) -> MaterializationReport { MaterializationReport::default() }
    pub(crate) fn matched(&self, row: usize) -> bool { self.accepted[row] }
    pub(crate) fn accept(&mut self, row: usize) -> bool {
        std::mem::replace(&mut self.accepted[row], true)
    }
    pub(crate) fn is_null(&self, row: usize) -> Result<bool> { self.keys.any_null(self.view, row) }

    /// Existence probes visit each unresolved row once per round. A residual is evaluated
    /// on the round before another duplicate candidate is examined for that row.
    pub(crate) fn next_batch(
        &mut self, output: &mut Vec<JoinMatch>, limit: usize, mut finished: impl FnMut() -> bool,
    ) -> Result<bool> {
        output.clear();
        let count = self.view.num_rows();
        let mut visited = 0;
        while self.position < count && output.len() < limit {
            if finished() { return Ok(false); }
            self.table.tracker.check_cancelled()?;
            let row = self.position;
            if self.existence {
                if visited == count { break; }
                visited += 1;
                self.position = (self.position + 1) % count;
                if self.accepted[row] || self.heads[row] == Some(NONE) {
                    self.heads[row] = Some(NONE);
                    continue;
                }
            }
            let key = if self.existence { self.keys.hash_and_single_i64(row, self.view)? } else if let Some(key) = self.cached_key { Some(key) } else {
                self.cached_key = self.keys.hash_and_single_i64(row, self.view)?;
                self.cached_key
            };
            let Some((hash, inline_key)) = key else {
                if self.existence { self.heads[row] = Some(NONE); } else {
                    self.position += 1;
                    self.head = None;
                }
                continue;
            };
            let mut head = (if self.existence { self.heads[row] } else { self.head })
                .unwrap_or_else(|| self.table.buckets[bucket_of(hash, self.table.buckets.len())]);
            while head != NONE {
                if finished() { return Ok(false); }
                self.table.tracker.check_cancelled()?;
                self.examined = self.examined.saturating_add(1);
                let index = head as usize;
                let entry = self.table.entries[index];
                head = entry.next;
                let same = self.table.hashes[index] == hash && if self.inline_i64 {
                    self.table.keys_by_chunk[entry.chunk as usize].single_i64_value(
                        &self.table.chunks[entry.chunk as usize], entry.row as usize)? == inline_key
                } else {
                    self.table.keys_by_chunk[entry.chunk as usize].equals(entry.row as usize,
                                                                          &self.table.chunks[entry.chunk as usize], &self.keys, row, self.view)?
                };
                if same {
                    output.push((row as u32, (entry.chunk, entry.row)));
                    break;
                }
            }
            if self.existence { self.heads[row] = Some(head); } else if head == NONE {
                self.position += 1;
                self.head = None;
                self.cached_key = None;
            } else { self.head = Some(head); }
        }
        Ok(!output.is_empty())
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
