use std::time::Instant;

use common::Result;
use error::VeinError;
use hashbrown::HashTable;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct GroupIndexEntry {
    pub(super) hash: u64,
    pub(super) slot: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ProbeResolution {
    Match(usize),
    Retry,
    Insert(usize),
}

#[derive(Clone, Copy, Default)]
pub(super) struct GroupIndexMetrics {
    pub(super) candidate_checks: u64,
    pub(super) reserve_ns: u64,
    pub(super) capacity_growth_events: u64,
    pub(super) entries_at_capacity_growth: u64,
    pub(super) capacity: usize,
    /// Time through hashbrown entry resolution, including the caller's key resolver.
    pub(super) entries_ns: u64,
}

/// Safe SwissTable-backed scalar probe baseline.
///
/// Arrow key arrays stay borrowed by the caller. This index retains only the
/// full hash and the aggregate's group slot; key equality is delegated to the
/// caller and runs only for entries with a matching hash fingerprint.
pub(super) struct GroupIndex {
    entries: HashTable<GroupIndexEntry>,
}

impl Default for GroupIndex {
    fn default() -> Self {
        Self {
            entries: HashTable::new(),
        }
    }
}

impl GroupIndex {
    pub(super) fn find(
        &self,
        hash: u64,
        mut matches: impl FnMut(GroupIndexEntry) -> bool,
    ) -> Option<GroupIndexEntry> {
        self.entries
            .find(hash, |entry| entry.hash == hash && matches(*entry))
            .copied()
    }

    /// Resolves candidates sequentially through hashbrown's SwissTable.
    ///
    /// This is deliberately a scalar baseline for comparison with a future
    /// batch probe implementation. The callback returns Retry for hash
    /// collisions whose borrowed Arrow keys differ, allowing the table to
    /// continue searching its probe sequence.
    pub(super) fn resolve_rows(
        &mut self,
        hashes: &[u64],
        group_slots: &mut Vec<usize>,
        mut resolve: impl FnMut(usize, Option<usize>) -> Result<ProbeResolution>,
    ) -> Result<GroupIndexMetrics> {
        group_slots.clear();
        group_slots.reserve(hashes.len());
        let mut observed_capacity = self.entries.capacity();
        let mut candidate_checks = 0u64;
        let mut capacity_growth_events = 0u64;
        let mut entries_at_capacity_growth = 0u64;

        let entries_started = Instant::now();
        for (row, &hash) in hashes.iter().enumerate() {
            let mut resolved_slot = None;
            let mut resolution_error = None;
            let entry = self.entries.entry(
                hash,
                |entry| {
                    candidate_checks = candidate_checks.saturating_add(1);
                    if resolution_error.is_some() || entry.hash != hash {
                        return resolution_error.is_some();
                    }

                    match resolve(row, Some(entry.slot)) {
                        Ok(ProbeResolution::Match(slot)) => {
                            resolved_slot = Some(slot);
                            true
                        }
                        Ok(ProbeResolution::Retry) => false,
                        Ok(ProbeResolution::Insert(_)) => {
                            resolution_error = Some(VeinError::internal(
                                "aggregate resolver tried to insert into an occupied bucket",
                            ));
                            true
                        }
                        Err(error) => {
                            resolution_error = Some(error);
                            true
                        }
                    }
                },
                |entry| entry.hash,
            );

            if let Some(error) = resolution_error {
                return Err(error);
            }

            match entry {
                hashbrown::hash_table::Entry::Occupied(_) => {
                    group_slots.push(
                        resolved_slot.expect("an occupied aggregate entry resolves its group slot"),
                    );
                }
                hashbrown::hash_table::Entry::Vacant(vacant) => match resolve(row, None)? {
                    ProbeResolution::Insert(slot) => {
                        vacant.insert(GroupIndexEntry { hash, slot });
                        let capacity = self.entries.capacity();
                        if capacity > observed_capacity {
                            capacity_growth_events = capacity_growth_events.saturating_add(1);
                            entries_at_capacity_growth = entries_at_capacity_growth
                                .saturating_add(self.entries.len().saturating_sub(1) as u64);
                            observed_capacity = capacity;
                        }
                        group_slots.push(slot);
                    }
                    ProbeResolution::Match(_) | ProbeResolution::Retry => {
                        return Err(VeinError::internal(
                            "aggregate resolver did not insert a missing group",
                        ));
                    }
                },
            }
        }

        let capacity = self.entries.capacity();
        Ok(GroupIndexMetrics {
            candidate_checks,
            reserve_ns: 0,
            capacity_growth_events,
            entries_at_capacity_growth,
            capacity,
            entries_ns: entries_started.elapsed().as_nanos() as u64,
        })
    }

    pub(super) fn insert_unique(&mut self, entry: GroupIndexEntry) -> Result<()> {
        self.reserve(1)?;
        self.insert_reserved(entry);
        Ok(())
    }

    /// Reserves table space using an upper bound on newly discovered groups.
    pub(super) fn reserve(&mut self, additional: usize) -> Result<u64> {
        let started = Instant::now();
        self.entries
            .try_reserve(additional, |existing| existing.hash)
            .map_err(|error| {
                VeinError::internal(format!("cannot grow aggregate group index: {error}"))
            })?;
        Ok(started.elapsed().as_nanos() as u64)
    }

    /// Inserts after `reserve` has secured capacity for the current batch.
    fn insert_reserved(&mut self, entry: GroupIndexEntry) {
        self.entries
            .insert_unique(entry.hash, entry, |existing| existing.hash);
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }
}

#[cfg(test)]
mod tests {
    use super::{GroupIndex, GroupIndexEntry, ProbeResolution};
    #[test]
    fn scalar_resolution_reuses_same_batch_groups_and_resolves_hash_collisions() {
        let hashes = [7, 7, 7];
        let keys = [10, 20, 20];
        let mut index = GroupIndex::default();
        let mut slots = Vec::new();
        let mut retained = Vec::<i64>::new();

        let metrics = index
            .resolve_rows(&hashes, &mut slots, |row, candidate| match candidate {
                Some(slot) if retained[slot] == keys[row] => Ok(ProbeResolution::Match(slot)),
                Some(_) => Ok(ProbeResolution::Retry),
                None => {
                    let slot = retained.len();
                    retained.push(keys[row]);
                    Ok(ProbeResolution::Insert(slot))
                }
            })
            .unwrap();

        assert_eq!(slots, [0, 1, 1]);
        assert_eq!(index.len(), 2);
        assert_eq!(metrics.capacity_growth_events, 1);
        assert_eq!(metrics.entries_at_capacity_growth, 0);
        assert!(metrics.capacity >= index.len());
        let first_capacity = metrics.capacity;

        let metrics = index
            .resolve_rows(&hashes, &mut slots, |row, candidate| match candidate {
                Some(slot) if retained[slot] == keys[row] => Ok(ProbeResolution::Match(slot)),
                Some(_) => Ok(ProbeResolution::Retry),
                None => unreachable!("all keys were inserted by the first batch"),
            })
            .unwrap();
        assert_eq!(metrics.capacity_growth_events, 0);
        assert_eq!(metrics.capacity, first_capacity);
        assert_eq!(metrics.capacity, index.entries.capacity());
    }

    #[test]
    fn repeated_rows_only_grow_for_the_distinct_group() {
        let hashes = vec![7; 4096];
        let mut index = GroupIndex::default();
        let mut slots = Vec::new();

        let metrics = index
            .resolve_rows(&hashes, &mut slots, |_, candidate| {
                Ok(candidate.map_or(ProbeResolution::Insert(0), ProbeResolution::Match))
            })
            .unwrap();

        assert_eq!(slots.len(), hashes.len());
        assert!(slots.iter().all(|&slot| slot == 0));
        assert_eq!(index.len(), 1);
        assert!(metrics.capacity < hashes.len() / 4);
        assert_eq!(metrics.capacity_growth_events, 1);
        assert_eq!(metrics.entries_at_capacity_growth, 0);

        let metrics = index
            .resolve_rows(&hashes, &mut slots, |_, candidate| {
                Ok(candidate.map_or(ProbeResolution::Insert(0), ProbeResolution::Match))
            })
            .unwrap();

        assert!(slots.iter().all(|&slot| slot == 0));
        assert_eq!(index.len(), 1);
        assert_eq!(metrics.capacity_growth_events, 0);
        assert!(metrics.capacity < hashes.len() / 4);
    }

    #[test]
    fn later_batches_find_groups_inserted_by_earlier_batches() {
        let mut index = GroupIndex::default();
        let mut slots = Vec::new();

        index
            .resolve_rows(&[11], &mut slots, |_, candidate| {
                Ok(candidate.map_or(ProbeResolution::Insert(4), ProbeResolution::Match))
            })
            .unwrap();
        assert_eq!(slots, [4]);

        index
            .resolve_rows(&[11], &mut slots, |_, candidate| {
                Ok(candidate.map_or(ProbeResolution::Insert(9), ProbeResolution::Match))
            })
            .unwrap();
        assert_eq!(slots, [4]);
        assert_eq!(index.len(), 1);
    }

    #[test]
    fn explicit_insert_is_findable() {
        let mut index = GroupIndex::default();
        index
            .insert_unique(GroupIndexEntry { hash: 23, slot: 6 })
            .unwrap();
        assert_eq!(index.find(23, |_| true).unwrap().slot, 6);
    }

    #[test]
    fn reserve_allocates_capacity_without_inserting_entries() {
        let mut index = GroupIndex::default();

        index.reserve(4096).expect("reserve group index");

        assert_eq!(index.entries.len(), 0);
        assert!(index.entries.capacity() >= 4096);
    }
}
