//! Format-neutral scan-provider contract.

use async_trait::async_trait;
use catalog::DataFileFormatId;
use common::Result;
use common::protocol::{DataSource, FileScanTask};

use types::ColumnId;

use crate::pipeline::PipelineNode;

use super::{ScanPlanningRequest, ScanRequest};

/// Executes a file scan for one registered physical data format.
///
/// Implementations own format-specific metadata interpretation and decoding.
/// They push reference-backed views to the next pipeline node.
#[async_trait]
pub trait ScanProvider: Send + Sync {
    /// Returns the open identifier used to register this provider.
    fn format_id(&self) -> &DataFileFormatId;

    /// Plans provider-defined scan units; the default schedules one unit per file.
    fn plan_work_units(&self, request: ScanPlanningRequest<'_>) -> Result<Vec<DataSource>> {
        Ok(super::split_scan_by_file(&DataSource::File(request.scan.clone())))
    }

    /// Reports whether runtime filters can be pushed into this source format.
    fn supports_runtime_filter_pushdown(&self, _scan: &FileScanTask, _columns: &[ColumnId]) -> bool {
        false
    }

    /// Reads the request and streams views into the execution pipeline.
    async fn execute(&self, request: ScanRequest<'_>, next: &mut dyn PipelineNode) -> Result<()>;
}
