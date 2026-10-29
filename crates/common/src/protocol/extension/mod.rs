//! Shared extension identities, capabilities and portable physical descriptors.

mod capabilities;
mod definition;
mod descriptor;
mod port;

pub use capabilities::{InputProperties, OperatorCapabilities, OrderingGuarantee, OrderingRequirement,
                       ParallelismCapability, PartitioningGuarantee, PartitioningRequirement};
pub use definition::{AnyOperatorDefinition, OperatorChild, OperatorDefinition, OperatorMetadata,
                     OperatorNodeId, OperatorRole, OperatorTypeId};
pub use descriptor::{ExtensionInput, ExtensionInputMode, PhysicalExtension};
pub use port::OperatorInputPort;

#[cfg(test)]
mod tests;
