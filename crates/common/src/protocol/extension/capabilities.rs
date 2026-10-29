use std::sync::Arc;

use crate::protocol::SortKey;
use crate::{Result, VeinError};

use super::{OperatorMetadata, OperatorRole};

/// Ordering an operator requires from one child stream.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OrderingRequirement {
    /// Accepts any input ordering.
    Any,
    /// Requires these keys in lexicographic order.
    Ordered(Arc<[SortKey]>),
}

/// Ordering the operator guarantees for its output.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OrderingGuarantee {
    /// Makes no ordering promise.
    Unknown,
    /// Preserves the ordering of the indexed input.
    PreservesInput(usize),
    /// Produces output ordered by these keys.
    Ordered(Arc<[SortKey]>),
}

/// Partitioning required from one child stream.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PartitioningRequirement {
    /// Accepts any partitioning.
    Any,
    /// Requires one unpartitioned stream.
    Single,
    /// Requires hash partitions over the listed input columns.
    Hash(Arc<[u32]>),
}

/// Partitioning guaranteed for the operator's output.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PartitioningGuarantee {
    /// Makes no partitioning promise.
    Unknown,
    /// Produces one stream.
    Single,
    /// Produces hash partitions over the listed columns.
    Hash(Arc<[u32]>),
}

/// Required properties for one child input port.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InputProperties {
    /// Required ordering for the child stream.
    pub ordering: OrderingRequirement,
    /// Required partitioning for the child stream.
    pub partitioning: PartitioningRequirement,
}

impl InputProperties {
    /// Builds unconstrained input requirements.
    pub fn any() -> Self {
        Self { ordering: OrderingRequirement::Any, partitioning: PartitioningRequirement::Any }
    }
}

/// Scheduling and data-flow guarantees declared by an operator factory.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperatorCapabilities {
    /// Requirements by child port; empty means every child accepts any properties.
    pub inputs: Arc<[InputProperties]>,
    /// Ordering guaranteed when this operator has an output stream.
    pub output_ordering: OrderingGuarantee,
    /// Partitioning guaranteed when this operator has an output stream.
    pub output_partitioning: PartitioningGuarantee,
    /// Safe concurrency behavior for independent pipeline instances.
    pub parallelism: ParallelismCapability,
}

/// Concurrency behavior supported by a registered operator implementation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ParallelismCapability {
    /// Only one instance may execute at a time.
    Serial,
    /// Instances run independently and need no state combination.
    Independent,
    /// Instances return local state that the factory can safely combine.
    CombineLocalState,
}

impl OperatorCapabilities {
    /// Creates a capability declaration with immutable per-port requirements.
    pub fn new(
        inputs: Vec<InputProperties>,
        output_ordering: OrderingGuarantee,
        output_partitioning: PartitioningGuarantee,
        parallelism: ParallelismCapability,
    ) -> Self {
        Self { inputs: inputs.into(), output_ordering, output_partitioning, parallelism }
    }

    /// Validates capability declarations against a concrete graph node.
    pub fn validate_for(&self, metadata: &OperatorMetadata) -> Result<()> {
        let child_count = metadata.children.len();
        let inputs_match = self.inputs.is_empty() || self.inputs.len() == child_count;
        let role_matches = match metadata.role {
            OperatorRole::Source => child_count == 0 && self.inputs.is_empty(),
            OperatorRole::Transform | OperatorRole::Sink => child_count > 0 && inputs_match,
        };
        if !role_matches {
            return Err(VeinError::invalid_input(
                "operator capabilities do not match its input ports or output schema",
            ));
        }
        for (properties, child) in self.inputs.iter().zip(metadata.children.iter()) {
            if !ordering_fits(&properties.ordering, child.schema.fields().len())
                || !partitioning_fits(&properties.partitioning, child.schema.fields().len())
            {
                return Err(VeinError::invalid_input(
                    "operator input capability references a missing child column",
                ));
            }
        }
        let Some(output) = &metadata.output_schema else {
            if self.output_ordering != OrderingGuarantee::Unknown
                || self.output_partitioning != PartitioningGuarantee::Unknown
            {
                return Err(VeinError::invalid_input("sink capabilities cannot declare output properties"));
            }
            return Ok(());
        };
        let ordering_fits = match &self.output_ordering {
            OrderingGuarantee::Unknown => true,
            OrderingGuarantee::PreservesInput(index) => *index < child_count,
            OrderingGuarantee::Ordered(keys) => {
                !keys.is_empty() && keys.iter().all(|key| (key.column as usize) < output.fields().len())
            }
        };
        let partitioning_fits = match &self.output_partitioning {
            PartitioningGuarantee::Unknown | PartitioningGuarantee::Single => true,
            PartitioningGuarantee::Hash(columns) => columns_fit(columns, output.fields().len()),
        };
        if !ordering_fits || !partitioning_fits {
            return Err(VeinError::invalid_input("operator output capability references a missing column"));
        }
        Ok(())
    }
}

fn ordering_fits(requirement: &OrderingRequirement, columns: usize) -> bool {
    match requirement {
        OrderingRequirement::Any => true,
        OrderingRequirement::Ordered(keys) => {
            !keys.is_empty() && keys.iter().all(|key| (key.column as usize) < columns)
        }
    }
}

fn partitioning_fits(requirement: &PartitioningRequirement, columns: usize) -> bool {
    match requirement {
        PartitioningRequirement::Any | PartitioningRequirement::Single => true,
        PartitioningRequirement::Hash(keys) => columns_fit(keys, columns),
    }
}

fn columns_fit(columns: &[u32], schema_columns: usize) -> bool {
    !columns.is_empty() && columns.iter().all(|column| (*column as usize) < schema_columns)
}

