//! Cost gate for adding a materialization barrier to a streaming input.

/// Available planning work for one input; absent metrics remain explicit.
#[derive(Debug, Clone, Copy)]
pub struct CheckpointWork {
    /// Base input rows that the producer must process.
    pub input_rows: Option<u64>,
    /// Estimated rows retained for replay and synopsis computation.
    pub output_rows: Option<f64>,
}

/// Controls whether feedback justifies interrupting streaming execution.
pub trait CheckpointCostModel: Send + Sync {
    /// Declines checkpoints dominated by another input's measurement cost.
    fn allows(&self, candidate: CheckpointWork, sibling: CheckpointWork) -> bool;
}

/// Measures an input only when neither its read work nor retained population
/// exceeds its sibling. It never turns missing metrics into a cheap zero cost.
#[derive(Debug, Default)]
pub struct InputDominanceCost;

impl CheckpointCostModel for InputDominanceCost {
    fn allows(&self, candidate: CheckpointWork, sibling: CheckpointWork) -> bool {
        if candidate.input_rows == Some(0) { return true; }
        candidate.input_rows.zip(sibling.input_rows).is_some_and(|(a, b)| a <= b)
            && candidate.output_rows.zip(sibling.output_rows).is_some_and(|(a, b)| {
                a.is_finite() && b.is_finite() && a >= 0.0 && b >= 0.0 && a <= b
            })
    }
}
