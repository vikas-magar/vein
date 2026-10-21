use catalog::AnalyzedTableStatistics;
use serde::{Deserialize, Serialize};

/// Committed files and optional analyzed statistics for a managed table.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableManifest {
    /// Monotonic logical revision assigned by the table writer.
    pub revision: u64,
    /// Root-relative paths to the committed Parquet data files.
    pub files: Vec<String>,
    /// Statistics computed from the exact source snapshot, when available.
    #[serde(default)]
    pub statistics: Option<AnalyzedTableStatistics>,
}

/// Analyzed statistics stored for an external table without a managed manifest.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExternalStatisticsSidecar {
    /// Logical table name to which the statistics belong.
    pub table_name: String,
    /// Statistics and source snapshot used to validate freshness.
    pub statistics: AnalyzedTableStatistics,
}
