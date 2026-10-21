use catalog::MetadataStore;
use common::Result;
use common::protocol::FileScanTask;
use error::VeinError;

use crate::runtime::filter::RuntimeFilterSet;

/// Borrowed context for a provider to produce independent scan work units.
pub struct ScanPlanningRequest<'a> {
    /// Logical file task to divide into provider-owned units.
    pub scan: &'a FileScanTask,
    /// Shared typed metadata cache available to the planner.
    pub metadata_store: &'a MetadataStore,
    /// Maximum number of concurrent workers available to the query.
    pub max_parallelism: usize,
    /// Runtime filters available for safe metadata pruning.
    pub runtime_filters: Option<&'a RuntimeFilterSet>,
}

/// Checks file-scan invariants shared by partition planning and source setup.
pub(crate) fn validate_file_scan(scan: &FileScanTask) -> Result<()> {
    if scan.files.is_empty() {
        return Err(VeinError::planning("file scan contains no files", None));
    }
    if !scan.storage_metadata.is_empty() && scan.storage_metadata.len() != scan.files.len() {
        return Err(VeinError::planning(
            "scan storage snapshots must align with the scan file list",
            None,
        ));
    }
    if scan.scan_unit_range.is_some() && scan.files.len() != 1 {
        return Err(VeinError::planning(
            "a scan-unit range must address exactly one file",
            None,
        ));
    }
    Ok(())
}
