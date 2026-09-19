//! Partition-local hash join state.
//!
//! This layer is deliberately independent of storage format and Arrow
//! materialization. EXO and generic Arrow extractors feed the same table.

use std::collections::HashMap;

use super::key::{JoinKey, JoinKeyBatch};
use common::Result;
use error::VeinError;

/// Identifies a row in an input batch stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct JoinRowRef {
    /// Input batch identifier.
    pub batch_id: u32,
    /// Physical row index within that batch.
    pub row_index: u32,
}

/// A candidate match emitted by the probe phase.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct JoinMatch {
    /// Row from the build side.
    pub build_row: JoinRowRef,
    /// Row from the probe side.
    pub probe_row: JoinRowRef,
}

/// A partition-local hash table for exact equi-join matching.
#[derive(Debug, Default)]
pub struct HashJoinTable {
    buckets: HashMap<u64, Vec<(JoinKey, JoinRowRef)>>,
    build_rows: u64,
}

impl HashJoinTable {
    /// Creates an empty hash table.
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of non-null rows inserted into the table.
    pub fn build_rows(&self) -> u64 {
        self.build_rows
    }

    /// Inserts all non-null rows from one extracted batch.
    pub fn insert_batch(&mut self, batch_id: u32, batch: &JoinKeyBatch) -> Result<()> {
        for row in batch.matching_rows() {
            let key = row
                .key
                .as_ref()
                .ok_or_else(|| VeinError::internal("matching row has no join key"))?;
            self.buckets
                .entry(row.hash)
                .or_default()
                .push((key.clone(), JoinRowRef {
                    batch_id,
                    row_index: row.row_index,
                }));
            self.build_rows = self.build_rows.saturating_add(1);
        }
        Ok(())
    }

    /// Probes the table and returns exact matches, including duplicate keys.
    pub fn probe_batch(&self, batch_id: u32, batch: &JoinKeyBatch) -> Vec<JoinMatch> {
        let mut matches = Vec::new();
        for row in batch.matching_rows() {
            let Some(key) = row.key.as_ref() else {
                continue;
            };
            let Some(candidates) = self.buckets.get(&row.hash) else {
                continue;
            };
            for (candidate, build_row) in candidates {
                if candidate == key {
                    matches.push(JoinMatch {
                        build_row: *build_row,
                        probe_row: JoinRowRef {
                            batch_id,
                            row_index: row.row_index,
                        },
                    });
                }
            }
        }
        matches
    }
}

#[cfg(test)]
mod tests {
    use super::{HashJoinTable, JoinRowRef};
    use crate::operators::join::{JoinKey, JoinKeyBatch, JoinKeyPart, JoinKeyRow};

    fn batch(rows: Vec<JoinKeyRow>) -> JoinKeyBatch {
        JoinKeyBatch::new(rows)
    }

    #[test]
    fn preserves_duplicate_build_rows() {
        let key = JoinKey::new(vec![JoinKeyPart::Canonical(7)]);
        let mut table = HashJoinTable::new();
        table
            .insert_batch(
                1,
                &batch(vec![
                    JoinKeyRow {
                        row_index: 2,
                        hash: key.stable_hash(),
                        key: Some(key.clone()),
                    },
                    JoinKeyRow {
                        row_index: 4,
                        hash: key.stable_hash(),
                        key: Some(key.clone()),
                    },
                ]),
            )
            .expect("build succeeds");

        let matches = table.probe_batch(
            9,
            &batch(vec![JoinKeyRow {
                row_index: 5,
                hash: key.stable_hash(),
                key: Some(key),
            }]),
        );

        assert_eq!(matches.len(), 2);
        assert_eq!(matches[0].build_row, JoinRowRef { batch_id: 1, row_index: 2 });
        assert_eq!(matches[1].build_row, JoinRowRef { batch_id: 1, row_index: 4 });
        assert!(matches.iter().all(|item| item.probe_row == JoinRowRef {
            batch_id: 9,
            row_index: 5
        }));
    }

    #[test]
    fn null_keys_do_not_match() {
        let mut table = HashJoinTable::new();
        table
            .insert_batch(
                1,
                &batch(vec![JoinKeyRow {
                    row_index: 0,
                    hash: 0,
                    key: None,
                }]),
            )
            .expect("build succeeds");

        assert!(table
            .probe_batch(
                2,
                &batch(vec![JoinKeyRow {
                    row_index: 0,
                    hash: 0,
                    key: None,
                }])
            )
            .is_empty());
        assert_eq!(table.build_rows(), 0);
    }

    #[test]
    fn address_hash_candidates_require_exact_verification() {
        let left = JoinKey::new(vec![JoinKeyPart::CanonicalWithVerification {
            address: 42,
            value: b"left".to_vec(),
        }]);
        let right = JoinKey::new(vec![JoinKeyPart::CanonicalWithVerification {
            address: 42,
            value: b"right".to_vec(),
        }]);
        assert_eq!(left.stable_hash(), right.stable_hash());

        let mut table = HashJoinTable::new();
        table
            .insert_batch(
                1,
                &batch(vec![JoinKeyRow {
                    row_index: 0,
                    hash: left.stable_hash(),
                    key: Some(left),
                }]),
            )
            .expect("build succeeds");

        let matches = table.probe_batch(
            2,
            &batch(vec![JoinKeyRow {
                row_index: 0,
                hash: right.stable_hash(),
                key: Some(right),
            }]),
        );
        assert!(matches.is_empty());
    }

    #[test]
    fn full_key_equality_verifies_hash_candidates() {
        let left = JoinKey::new(vec![JoinKeyPart::Bytes(b"left".to_vec())]);
        let right = JoinKey::new(vec![JoinKeyPart::Bytes(b"right".to_vec())]);
        let mut table = HashJoinTable::new();
        table
            .insert_batch(
                1,
                &batch(vec![JoinKeyRow {
                    row_index: 0,
                    hash: 42,
                    key: Some(left),
                }]),
            )
            .expect("build succeeds");

        let matches = table.probe_batch(
            2,
            &batch(vec![JoinKeyRow {
                row_index: 0,
                hash: 42,
                key: Some(right),
            }]),
        );

        assert!(matches.is_empty());
    }
}
