//! Physical data-distribution contracts used by shuffle-aware operators.

use common::protocol::JoinKeySpec;

/// A concrete partitioning property of an execution stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Distribution {
    /// The source does not guarantee a useful partitioning property.
    Unknown,
    /// All rows are available in one logical partition.
    SinglePartition,
    /// Rows are hash-partitioned by the listed keys.
    Hash(HashPartitioning),
}

/// Hash partitioning metadata shared by both sides of a partition-local join.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HashPartitioning {
    /// Keys used by the stable partition hash.
    pub keys: Vec<JoinKeySpec>,
    /// Number of output partitions.
    pub partition_count: u16,
}

impl HashPartitioning {
    /// Creates a hash partitioning description.
    pub fn new(keys: Vec<JoinKeySpec>, partition_count: u16) -> Self {
        Self {
            keys,
            partition_count,
        }
    }

    /// Returns whether this partitioning has a valid execution shape.
    pub fn is_valid(&self) -> bool {
        self.partition_count > 0 && !self.keys.is_empty()
    }
}

/// A distribution requirement imposed by a physical operator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DistributionRequirement {
    /// Any input distribution is acceptable.
    Any,
    /// The input must be a single logical partition.
    SinglePartition,
    /// The input must be hash-partitioned by these keys.
    Hash(HashPartitioning),
}

impl Distribution {
    /// Returns whether this distribution satisfies a physical requirement.
    pub fn satisfies(&self, requirement: &DistributionRequirement) -> bool {
        match requirement {
            DistributionRequirement::Any => true,
            DistributionRequirement::SinglePartition => {
                matches!(self, Self::SinglePartition)
            }
            DistributionRequirement::Hash(required) => {
                matches!(self, Self::Hash(actual) if actual == required)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Distribution, DistributionRequirement, HashPartitioning};
    use common::protocol::{JoinKeyMode, JoinKeySpec};

    fn key(name: &str) -> JoinKeySpec {
        JoinKeySpec {
            field_id: 1,
            column_name: name.to_string(),
            mode: JoinKeyMode::Auto,
        }
    }

    #[test]
    fn matching_hash_partitioning_satisfies_join_requirement() {
        let partitioning = HashPartitioning::new(vec![key("id")], 4);
        assert!(Distribution::Hash(partitioning.clone())
            .satisfies(&DistributionRequirement::Hash(partitioning)));
    }

    #[test]
    fn different_partition_counts_do_not_satisfy() {
        let actual = HashPartitioning::new(vec![key("id")], 2);
        let required = HashPartitioning::new(vec![key("id")], 4);
        assert!(!Distribution::Hash(actual).satisfies(&DistributionRequirement::Hash(required)));
    }

    #[test]
    fn invalid_partitioning_is_rejected() {
        assert!(!HashPartitioning::new(Vec::new(), 4).is_valid());
        assert!(!HashPartitioning::new(vec![key("id")], 0).is_valid());
    }
}
