use super::super::{InputProperties, OrderingRequirement, PartitioningGuarantee, PartitioningRequirement};
use common::protocol::SortKey;
use common::{Result, VeinError};
use std::sync::Arc;

/// Proven output properties available to subsequent stage construction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedPipelineProperties {
    /// Ordered key prefix; absent means no proven ordering.
    pub ordering: Option<Arc<[SortKey]>>,
    /// Proven partitioning; unknown does not satisfy a stronger requirement.
    pub partitioning: PartitioningGuarantee,
}

impl ResolvedPipelineProperties {
    pub(super) fn unknown() -> Self {
        Self { ordering: None, partitioning: PartitioningGuarantee::Unknown }
    }
    pub(super) fn satisfies(&self, required: &InputProperties) -> Result<()> {
        let ordered = match &required.ordering {
            OrderingRequirement::Any => true,
            OrderingRequirement::Ordered(keys) => self.ordering.as_ref().is_some_and(|actual| {
                actual.starts_with(keys.as_ref())
            }),
        };
        let partitioned = match (&required.partitioning, &self.partitioning) {
            (PartitioningRequirement::Any, _) => true,
            (PartitioningRequirement::Single, PartitioningGuarantee::Single) => true,
            (PartitioningRequirement::Hash(required), PartitioningGuarantee::Hash(actual)) => required == actual,
            _ => false,
        };
        if !ordered || !partitioned {
            return Err(VeinError::planning("pipeline input ordering or partitioning requirement is not guaranteed by its producer", None));
        }
        Ok(())
    }
}
