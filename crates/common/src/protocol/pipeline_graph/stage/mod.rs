//! Phase-specific bindings into the authoritative physical tree.

use crate::protocol::extension::{ExtensionInputMode, OperatorInputPort, OperatorRole};
use crate::protocol::{PhysicalPlanNode, PhysicalPlanOperator};
use crate::{Result, VeinError};
use std::sync::Arc;
use types::VeinSchemaRef;

mod construction;

/// Work performed by a canonical operator in one pipeline.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum PipelinePhase {
    /// Produces batches from a scan or completed blocking operator.
    Source,
    /// Consumes one streaming child and produces output views.
    Transform(OperatorInputPort),
    /// Consumes one child into shared state or the query destination.
    Sink(OperatorInputPort),
    /// Finalizes a terminal operator after all blocking inputs finish.
    Finalize,
}

impl PipelinePhase {
    /// Canonical child port consumed by this phase, when it accepts input.
    pub const fn input_port(self) -> Option<OperatorInputPort> {
        match self {
            Self::Transform(port) | Self::Sink(port) => Some(port),
            _ => None
        }
    }

    /// Streaming role of this phase, distinct from the whole operator's role.
    pub const fn role(self) -> OperatorRole {
        match self {
            Self::Source => OperatorRole::Source,
            Self::Transform(_) => OperatorRole::Transform,
            Self::Sink(_) | Self::Finalize => OperatorRole::Sink,
        }
    }
}

/// Immutable stage binding that shares canonical nodes and schema owners.
#[derive(Clone, Debug)]
pub struct PipelineStage {
    node: Arc<PhysicalPlanNode>,
    phase: PipelinePhase,
    input_schema: Option<VeinSchemaRef>,
    output_schema: Option<VeinSchemaRef>,
}

impl PipelineStage {
    pub(super) fn new(node: &Arc<PhysicalPlanNode>, phase: PipelinePhase) -> Result<Self> {
        let input_schema = phase.input_port().map(|port| {
            node.operator.inputs().nth(port.index()).map(|child| Arc::clone(&child.output_schema))
                .ok_or_else(|| VeinError::planning("pipeline stage references a missing child port", None))
        }).transpose()?;
        let output_schema = matches!(phase, PipelinePhase::Source | PipelinePhase::Transform(_))
            .then(|| Arc::clone(&node.output_schema));
        Ok(Self { node: Arc::clone(node), phase, input_schema, output_schema })
    }

    /// Canonical operator owning semantics and immutable configuration.
    pub fn node(&self) -> &Arc<PhysicalPlanNode> { &self.node }
    /// Input/output phase performed by this stage.
    pub fn phase(&self) -> PipelinePhase { self.phase }
    /// Exact canonical child schema consumed by this phase.
    pub fn input_schema(&self) -> Option<&VeinSchemaRef> { self.input_schema.as_ref() }
    /// Canonical schema emitted by this phase; sinks have no output.
    pub fn output_schema(&self) -> Option<&VeinSchemaRef> { self.output_schema.as_ref() }
}

/// Resolves the single streaming port without assuming it is child zero.
pub(crate) fn streaming_port(node: &PhysicalPlanNode) -> Result<OperatorInputPort> {
    match &node.operator {
        PhysicalPlanOperator::Transform { .. } | PhysicalPlanOperator::Limit { .. }
        | PhysicalPlanOperator::Join { .. } | PhysicalPlanOperator::Sink { .. } =>
            Ok(OperatorInputPort::new(0)),
        PhysicalPlanOperator::Extension { descriptor, .. } => {
            let mut ports = descriptor.inputs.iter().enumerate()
                .filter(|(_, input)| input.mode == ExtensionInputMode::Streaming);
            let port = ports.next().map(|(port, _)| OperatorInputPort::new(port))
                .ok_or_else(|| VeinError::planning("pipeline operator has no streaming input", None))?;
            if ports.next().is_some() {
                return Err(VeinError::planning("pipeline operator has multiple streaming inputs", None));
            }
            Ok(port)
        }
        _ => Err(VeinError::planning("blocking operator cannot be a streaming pipeline stage", None)),
    }
}
