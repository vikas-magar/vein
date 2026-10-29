use std::path::PathBuf;

use common::{Result, protocol::FileScanTask};
use error::VeinError;
use storage::StorageLocation;

use crate::operators::source::scan::ScanFile;

/// Object identities are authoritative for non-local namespaces. Native scans
/// retain canonical filesystem paths and the existing common-root resolution.
pub(super) fn resolve(scan: &FileScanTask) -> Result<(StorageLocation, Vec<ScanFile>)> {
    let non_local = scan.storage_metadata.iter()
        .find(|metadata| metadata.identity.location().as_local_path().is_none());
    if let Some(first) = non_local {
        let location = first.identity.location();
        if scan.storage_metadata.len() != scan.files.len()
            || scan.storage_metadata.iter().any(|metadata| metadata.identity.location() != location)
        {
            return Err(VeinError::invalid_input("non-local scan requires aligned identities in one storage namespace"));
        }
        let files = scan.storage_metadata.iter().map(|metadata| ScanFile {
            relative: metadata.identity.path().as_str().to_owned(),
            absolute: PathBuf::new(),
            storage_metadata: Some(metadata.clone()),
            scan_unit_range: scan.scan_unit_range.clone(),
        }).collect();
        return Ok((location.clone(), files));
    }
    if scan.files.iter().any(|file| file.contains("://")) {
        return Err(VeinError::invalid_input("URI scan requires complete catalog storage identities"));
    }
    let (root, relative) = local_scan_paths(&scan.files)?;
    let files = relative.into_iter().enumerate().map(|(index, relative)| ScanFile {
        absolute: root.join(&relative),
        relative,
        storage_metadata: scan.storage_metadata.get(index).cloned(),
        scan_unit_range: scan.scan_unit_range.clone(),
    }).collect();
    Ok((StorageLocation::local(root), files))
}

fn local_scan_paths(files: &[String]) -> Result<(PathBuf, Vec<String>)> {
    let paths = files.iter().map(|file| {
        std::fs::canonicalize(file)
            .map_err(|error| VeinError::io(format!("resolve local scan file '{file}'"), error))
    }).collect::<Result<Vec<_>>>()?;
    let first = paths.first().ok_or_else(|| VeinError::internal("file scan contains no files"))?;
    let root = first.ancestors().skip(1)
        .find(|candidate| paths.iter().all(|path| path.starts_with(candidate)))
        .ok_or_else(|| VeinError::internal("local scan files have no common directory"))?.to_path_buf();
    let relative = paths.iter().map(|path| {
        path.strip_prefix(&root).map(|path| path.to_string_lossy().into_owned())
            .map_err(|error| VeinError::internal(format!("local scan path is outside its root: {error}")))
    }).collect::<Result<Vec<_>>>()?;
    Ok((root, relative))
}
