use catalog::AnalyzedTableStatistics;
use common::Result;

use crate::catalog::ParquetStatisticsPublisher;

use super::ParquetWriteStorage;

impl ParquetStatisticsPublisher for ParquetWriteStorage {
    fn publish_managed(&self, table: &str, statistics: &AnalyzedTableStatistics) -> Result<()> {
        self.publish_managed_statistics(table, statistics)
    }

    fn publish_external(&self, table: &str, statistics: &AnalyzedTableStatistics) -> Result<()> {
        self.publish_external_statistics(table, statistics)
    }
}
