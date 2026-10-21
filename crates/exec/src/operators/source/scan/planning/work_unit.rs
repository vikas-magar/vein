use common::identifiers::TaskId;
use common::protocol::{DataSource, FileScanTask};

/// Creates one file-local unit while preserving aligned storage identity metadata.
pub fn file_scan_work_unit(
    scan: &FileScanTask,
    file_index: usize,
    scan_unit_range: Option<std::ops::Range<usize>>,
) -> DataSource {
    let mut unit = scan.clone();
    unit.files = vec![scan.files[file_index].clone()];
    if scan.storage_metadata.len() == scan.files.len()
        && let Some(metadata) = scan.storage_metadata.get(file_index)
    {
        unit.storage_metadata = vec![metadata.clone()];
    }
    unit.scan_unit_range = scan_unit_range;
    unit.task_id = TaskId::new();
    DataSource::File(unit)
}

/// Creates one independently schedulable source per file.
pub(crate) fn split_scan_by_file(source: &DataSource) -> Vec<DataSource> {
    let DataSource::File(scan) = source else {
        return vec![source.clone()];
    };
    if scan.files.len() <= 1 {
        return vec![source.clone()];
    }
    (0..scan.files.len())
        .map(|index| file_scan_work_unit(scan, index, scan.scan_unit_range.clone()))
        .collect()
}
