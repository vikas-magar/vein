#[derive(Debug, thiserror::Error)]
pub enum PlanError {
    #[error("Unsupported feature: {0}")]
    UnsupportedFeature(String),

    #[error("Invalid pipe operator '{op}' at position {pos}: {reason}")]
    InvalidPipe { op: String, pos: usize, reason: String },

    #[error("Source not found: {0}")]
    SourceNotFound(String),

    #[error("Internal error: {0}")]
    Internal(String),
}
