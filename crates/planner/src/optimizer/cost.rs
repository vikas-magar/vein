use crate::plan::LogicalPlan;
use error::Result;
use std::collections::HashMap;

/// Statistical metadata for a column.
#[derive(Debug, Clone, Default)]
pub struct ColumnStatistics {
    /// Number of null values.
    pub null_count: Option<usize>,
    /// Number of distinct values.
    pub distinct_count: Option<usize>,
    /// Minimum value (as a string for simplicity right now).
    pub min_value: Option<String>,
    /// Maximum value (as a string).
    pub max_value: Option<String>,
}

/// Statistics collected for a table or intermediate plan node.
#[derive(Debug, Clone, Default)]
pub struct Statistics {
    /// Estimated row count.
    pub row_count: Option<usize>,
    /// Estimated byte size.
    pub total_byte_size: Option<usize>,
    /// Maps column index to its statistics.
    pub column_stats: HashMap<usize, ColumnStatistics>,
}

/// A trait for models that predict the execution cost of a LogicalPlan.
///
/// This is heavily abstracted so that we can eventually plug in Machine Learning
/// (ML) models or Reinforcement Learning models that learn costs based on historical telemetry.
pub trait CostModel: Send + Sync {
    /// Predicts the computational cost (e.g. CPU, I/O, memory) of a specific LogicalPlan node.
    fn compute_cost(&self, plan: &LogicalPlan, stats: &Statistics) -> Result<f64>;
}
