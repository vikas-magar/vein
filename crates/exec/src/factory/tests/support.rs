use std::sync::Arc;

use crate::factory::scan_partition::partition_source;
use crate::operators::source::scan::ScanProviderRegistry;
use crate::runtime::filter::RuntimeFilterSet;
use catalog::MetadataStore;
use common::Result;
use common::identifiers::TaskId;
use common::protocol::{DataFileFormatId, DataSource, FileScanTask};
pub(super) fn file_scan(
    format: &str,
    files: Vec<String>,
    metadata: Vec<storage::StorageFileMetadata>,
) -> FileScanTask {
    FileScanTask {
        files,
        storage_metadata: metadata,
        data_file_format: DataFileFormatId::new(format),
        format_options: Vec::new(),
        byte_range: None,
        scan_unit_range: None,
        projected: Arc::new(types::VeinSchema::empty()),
        column_encoding_hints: Vec::new(),
        pushdown: None,
        task_id: TaskId::new(),
        catalog_snapshot: None,
        runtime_statistics: None,
    }
}

pub(super) fn partition(
    scan: &FileScanTask,
    providers: &ScanProviderRegistry,
    metadata: &MetadataStore,
    parallelism: usize,
    filters: Option<&RuntimeFilterSet>,
) -> Result<Vec<DataSource>> {
    partition_source(
        &DataSource::File(scan.clone()),
        providers,
        metadata,
        parallelism,
        filters,
    )
}
