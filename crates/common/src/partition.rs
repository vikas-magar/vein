use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::identifiers::PartitionId;
use error::{Result, VeinError};

/// A set of partition IDs with range-based operations.
///
/// Used by shuffle stages to track which partitions are assigned
/// to a given task or reducer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionSet {
    partitions: BTreeSet<PartitionId>,
}

impl PartitionSet {
    /// Create a set from an inclusive range of partition IDs.
    pub fn from_range(from: PartitionId, to_inclusive: PartitionId) -> Self {
        let mut partitions = BTreeSet::new();
        for i in from.into_raw()..=to_inclusive.into_raw() {
            partitions.insert(PartitionId::from_raw(i));
        }
        Self { partitions }
    }

    /// Create a set containing a single partition.
    pub fn single(partition: PartitionId) -> Self {
        let mut set = BTreeSet::new();
        set.insert(partition);
        Self { partitions: set }
    }

    /// Create an empty set.
    pub fn empty() -> Self {
        Self {
            partitions: BTreeSet::new(),
        }
    }

    /// Return the number of partitions in the set.
    pub fn len(&self) -> usize {
        self.partitions.len()
    }

    /// Whether the set is empty.
    pub fn is_empty(&self) -> bool {
        self.partitions.is_empty()
    }

    /// Check if a partition is in this set.
    pub fn contains(&self, partition: &PartitionId) -> bool {
        self.partitions.contains(partition)
    }

    /// Insert a partition into the set.
    pub fn insert(&mut self, partition: PartitionId) {
        self.partitions.insert(partition);
    }

    /// Remove and return the smallest partition.
    pub fn pop_first(&mut self) -> Option<PartitionId> {
        self.partitions.pop_first()
    }

    /// Iterate over all partitions in order.
    pub fn iter(&self) -> impl Iterator<Item = &PartitionId> {
        self.partitions.iter()
    }

    /// Partition this set into `n` roughly equal subsets (greedy round-robin).
    /// Returns an error if `n == 0`.
    pub fn split(&self, n: usize) -> Result<Vec<PartitionSet>> {
        if n == 0 {
            return Err(VeinError::internal("cannot split PartitionSet into 0 subsets"));
        }
        let mut result: Vec<PartitionSet> = (0..n).map(|_| PartitionSet::empty()).collect();
        for (i, p) in self.partitions.iter().enumerate() {
            result[i % n].partitions.insert(*p);
        }
        Ok(result)
    }

    /// Convert into the underlying sorted partition ID list.
    pub fn into_ids(self) -> Vec<PartitionId> {
        self.partitions.into_iter().collect()
    }

    /// Return a sorted list of partition IDs.
    pub fn ids(&self) -> Vec<PartitionId> {
        self.partitions.iter().copied().collect()
    }
}

impl FromIterator<PartitionId> for PartitionSet {
    fn from_iter<I: IntoIterator<Item = PartitionId>>(iter: I) -> Self {
        Self {
            partitions: iter.into_iter().collect(),
        }
    }
}

#[cfg(test)]
#[allow(missing_docs)]
mod tests {
    use super::*;

    #[test]
    fn test_empty() {
        let set = PartitionSet::empty();
        assert!(set.is_empty());
        assert_eq!(set.len(), 0);
    }

    #[test]
    fn test_single() {
        let set = PartitionSet::single(PartitionId::from_raw(3));
        assert_eq!(set.len(), 1);
        assert!(set.contains(&PartitionId::from_raw(3)));
    }

    #[test]
    fn test_range() {
        let a = PartitionId::from_raw(0);
        let b = PartitionId::from_raw(4);
        let set = PartitionSet::from_range(a, b);
        assert_eq!(set.len(), 5);
        assert!(set.contains(&PartitionId::from_raw(2)));
    }

    #[test]
    fn test_insert() {
        let mut set = PartitionSet::empty();
        set.insert(PartitionId::from_raw(7));
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn test_split_even() {
        let a = PartitionId::from_raw(0);
        let b = PartitionId::from_raw(7);
        let set = PartitionSet::from_range(a, b);
        let subsets = set.split(4).unwrap();
        assert_eq!(subsets.len(), 4);
        // 8 partitions into 4 = 2 each
        assert_eq!(subsets[0].len(), 2);
        assert_eq!(subsets[1].len(), 2);
        assert_eq!(subsets[2].len(), 2);
        assert_eq!(subsets[3].len(), 2);
    }

    #[test]
    fn test_split_uneven() {
        let a = PartitionId::from_raw(0);
        let b = PartitionId::from_raw(4);
        let set = PartitionSet::from_range(a, b);
        // 5 partitions into 3 = 2, 2, 1
        let subsets = set.split(3).unwrap();
        // greedy round-robin distributes the extra
        assert_eq!(subsets[0].len(), 2);
        assert_eq!(subsets[1].len(), 2);
        assert_eq!(subsets[2].len(), 1);
    }

    #[test]
    fn test_split_zero_error() {
        let set = PartitionSet::single(PartitionId::from_raw(0));
        assert!(set.split(0).is_err());
    }

    #[test]
    fn test_iter_ids() {
        let mut set = PartitionSet::empty();
        set.insert(PartitionId::from_raw(3));
        set.insert(PartitionId::from_raw(1));
        set.insert(PartitionId::from_raw(2));
        assert_eq!(set.ids(), vec![
            PartitionId::from_raw(1),
            PartitionId::from_raw(2),
            PartitionId::from_raw(3),
        ]);
    }
}
