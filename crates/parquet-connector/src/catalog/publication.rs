use catalog::AnalyzedTableStatistics;
use error::Result;

/// Persistence boundary for publishing Parquet table analysis results.
///
/// The catalog validates the current source snapshot before calling this
/// adapter. Managed-table writers must recheck their manifest revision while
/// publishing so a concurrent commit cannot be overwritten.
pub trait ParquetStatisticsPublisher: Send + Sync {
    /// Atomically attaches statistics to a managed table manifest.
    fn publish_managed(&self, table: &str, statistics: &AnalyzedTableStatistics) -> Result<()>;

    /// Publishes statistics for an external table without a managed manifest.
    fn publish_external(&self, table: &str, statistics: &AnalyzedTableStatistics) -> Result<()>;
}
