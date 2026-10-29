use super::*;
use crate::protocol::extension::{OperatorRole, PhysicalExtension};
use std::sync::Arc;
pub(super) fn validate(descriptor: &PhysicalExtension, inputs: &[Arc<PhysicalPlanNode>], path: &str, root: bool) -> Result<()> {
    descriptor.validate_inputs(inputs)?;
    if (descriptor.role == OperatorRole::Sink) != root {
        return Err(invalid(path, "extension sinks must be terminal and other extensions cannot be plan roots"));
    }
    for (port, child) in inputs.iter().enumerate() {
        validate_node(child, &format!("{path}.input[{port}]"), false)?;
    }
    Ok(())
}
