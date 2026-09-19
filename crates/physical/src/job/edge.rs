//! Directed shuffle dependency edge between execution stages.

use common::identifiers::StageId;

use crate::distribution::Distribution;

/// Input port on a multi-input physical operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StageInputPort {
    /// The default or only input.
    Main,
    /// The left input of a binary operator.
    Left,
    /// The right input of a binary operator.
    Right,
}

/// A directed edge representing a shuffle dependency between stages.
#[derive(Debug, Clone)]
pub struct StageEdge {
    /// The producing (upstream) stage.
    pub from_stage: StageId,
    /// The consuming (downstream) stage.
    pub to_stage: StageId,
    /// The input port on the consuming stage.
    pub input_port: StageInputPort,
    /// Distribution guaranteed by the producing stage.
    pub distribution: Distribution,
}

impl StageEdge {
    /// Creates a new [`StageEdge`].
    pub fn new(from_stage: StageId, to_stage: StageId) -> Self {
        Self::routed(from_stage, to_stage, StageInputPort::Main, Distribution::Unknown)
    }

    /// Creates an edge with explicit input-port and distribution metadata.
    pub fn routed(
        from_stage: StageId,
        to_stage: StageId,
        input_port: StageInputPort,
        distribution: Distribution,
    ) -> Self {
        Self {
            from_stage,
            to_stage,
            input_port,
            distribution,
        }
    }
}
