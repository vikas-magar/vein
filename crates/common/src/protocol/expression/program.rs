//! Ordered scalar operations applied to one reference-backed input batch.

use serde::{Deserialize, Serialize};

use super::PhysicalExpr;

/// One lazy operation in an expression pipeline.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ExpressionStep {
    /// Restrict active rows using SQL filter semantics (only TRUE survives).
    Filter(PhysicalExpr),
    /// Replace the visible columns; direct column expressions retain their
    /// Arrow buffers and computed expressions add result arrays to the view.
    Projection(Vec<PhysicalExpr>),
}

/// Immutable scalar work attached to a physical pipeline node.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ExpressionProgram {
    /// Ordered transformations; each step sees the view emitted by its parent.
    pub steps: Vec<ExpressionStep>,
}

impl ExpressionProgram {
    /// An identity pipeline with no scalar transformations.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Whether this pipeline performs any filtering or projection.
    pub fn is_empty(&self) -> bool {
        self.steps.is_empty()
    }
}
