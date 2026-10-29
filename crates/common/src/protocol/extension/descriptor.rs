use super::{OperatorRole, OperatorTypeId};
use crate::protocol::PhysicalPlanNode;
use crate::{Result, VeinError};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use types::{VeinSchema, VeinSchemaRef};

/// How a child participates in an extension's pipeline.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ExtensionInputMode {
    /// Batches may flow while the downstream pipeline executes.
    Streaming,
    /// The child must finish before the extension's output pipeline can run.
    Blocking,
}

/// Required schema and scheduling relationship for one child port.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExtensionInput {
    /// Schema accepted at this input port.
    pub schema: VeinSchemaRef,
    /// Declared pipeline dependency for this port.
    pub mode: ExtensionInputMode,
}

/// Portable configuration stored in the authoritative physical tree.
///
/// Only configuration metadata is encoded here. Columnar data stays in views;
/// factory-created mutable state belongs to an individual query execution.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PhysicalExtension {
    /// Registered implementation identity.
    pub factory_id: OperatorTypeId,
    /// Configuration format version interpreted by the registered codec.
    pub configuration_version: u32,
    /// Immutable codec payload; cloning a plan shares this owner.
    pub configuration: Arc<[u8]>,
    /// Source, transform or terminal sink behavior.
    pub role: OperatorRole,
    /// Child requirements in input-port order.
    pub inputs: Arc<[ExtensionInput]>,
}

impl PhysicalExtension {
    /// Validates child coverage and schemas before construction or scheduling.
    pub fn validate_inputs(&self, children: &[Arc<PhysicalPlanNode>]) -> Result<()> {
        if self.inputs.len() != children.len() {
            return Err(VeinError::planning("extension child count differs from its input contracts", None));
        }
        let valid_role = match self.role {
            OperatorRole::Source => children.is_empty(),
            OperatorRole::Transform | OperatorRole::Sink => !children.is_empty(),
        };
        if !valid_role {
            return Err(VeinError::planning("extension role does not match its child count", None));
        }
        for (port, (contract, child)) in self.inputs.iter().zip(children).enumerate() {
            if !schemas_match(&contract.schema, &child.output_schema) {
                return Err(VeinError::planning(format!("extension input port {port} schema differs from its child"), None));
            }
        }
        Ok(())
    }
}

/// Compares identities, names, types and nullability at a canonical child edge.
pub(crate) fn schemas_match(left: &VeinSchema, right: &VeinSchema) -> bool {
    left.fields == right.fields
}

