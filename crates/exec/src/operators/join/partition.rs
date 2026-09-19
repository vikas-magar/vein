//! Deterministic hash partitioning for distributed join exchange.

use super::key::{JoinKeyBatch, JoinKeyRow};
use common::Result;
use error::VeinError;

/// Routes join-key rows to stable exchange partitions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct JoinPartitioner {
    partition_count: u16,
}

impl JoinPartitioner {
    /// Creates a partitioner with a fixed output partition count.
    pub fn new(partition_count: u16) -> Result<Self> {
        if partition_count == 0 {
            return Err(VeinError::internal(
                "join partition count must be greater than zero",
            ));
        }
        Ok(Self { partition_count })
    }

    /// Number of output partitions.
    pub fn partition_count(&self) -> u16 {
        self.partition_count
    }

    /// Computes the destination partition for a stable key hash.
    pub fn partition_for_hash(&self, hash: u64) -> u16 {
        (hash % self.partition_count as u64) as u16
    }

    /// Partitions all rows, including null-key rows for outer-join semantics.
    pub fn partition(&self, batch: &JoinKeyBatch) -> Vec<Vec<JoinKeyRow>> {
        let mut partitions = (0..self.partition_count)
            .map(|_| Vec::new())
            .collect::<Vec<_>>();

        for row in batch.rows() {
            let partition = self.partition_for_hash(row.hash) as usize;
            partitions[partition].push(row.clone());
        }

        partitions
    }
}

#[cfg(test)]
mod tests {
    use super::JoinPartitioner;
    use crate::operators::join::{JoinKey, JoinKeyBatch, JoinKeyPart, JoinKeyRow};

    #[test]
    fn routes_equal_hashes_to_the_same_partition() {
        let key = JoinKey::new(vec![JoinKeyPart::Canonical(42)]);
        let batch = JoinKeyBatch::new(vec![
            JoinKeyRow {
                row_index: 1,
                hash: key.stable_hash(),
                key: Some(key.clone()),
            },
            JoinKeyRow {
                row_index: 2,
                hash: key.stable_hash(),
                key: Some(key),
            },
        ]);
        let partitions = JoinPartitioner::new(4)
            .expect("valid partition count")
            .partition(&batch);

        let occupied = partitions
            .iter()
            .enumerate()
            .filter(|(_, rows)| !rows.is_empty())
            .collect::<Vec<_>>();
        assert_eq!(occupied.len(), 1);
        assert_eq!(occupied[0].1.len(), 2);
    }

    #[test]
    fn retains_null_rows_for_outer_join_handling() {
        let batch = JoinKeyBatch::new(vec![JoinKeyRow {
            row_index: 7,
            hash: 0,
            key: None,
        }]);
        let partitions = JoinPartitioner::new(2)
            .expect("valid partition count")
            .partition(&batch);

        assert_eq!(partitions[0][0].row_index, 7);
        assert!(partitions[0][0].key.is_none());
    }

    #[test]
    fn rejects_zero_partitions() {
        assert!(JoinPartitioner::new(0).is_err());
    }
}
