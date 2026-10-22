use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::operators::source::ScanSource;
use crate::operators::source::scan::{
    ScanFile, ScanSourceConfig, ScanWorkerState, validate_file_scan,
};
use crate::pipeline::Source;
use crate::runtime::filter::RuntimeFilterSet;
use common::Result;
use common::protocol::DataSource;
use common::statistics::QueryStatisticsContext;
use error::VeinError;
use storage::{ReadStorage, StorageLocation};

use super::{DefaultExecutorBuilder, STORAGE_HANDLE_CACHE_CAPACITY};

impl DefaultExecutorBuilder {
    pub(super) fn build_source_inner(
        &self,
        source: &DataSource,
        runtime_filters: &RuntimeFilterSet,
        query_statistics: Option<Arc<QueryStatisticsContext>>,
        worker_state: Arc<ScanWorkerState>,
    ) -> Result<Box<dyn Source>> {
        match source {
            DataSource::Memory => Err(VeinError::internal(
                "a DataSource::Memory task carries no payload to materialize",
            )),
            DataSource::Intermediate(_) => Err(VeinError::internal(
                "intermediate sources must be resolved by the query task executor",
            )),
            DataSource::File(scan) => {
                validate_file_scan(scan)?;
                let (root, relative) = local_scan_paths(&scan.files)?;
                let files = relative
                    .into_iter()
                    .enumerate()
                    .map(|(index, relative)| ScanFile {
                        absolute: root.join(&relative),
                        relative,
                        storage_metadata: scan.storage_metadata.get(index).cloned(),
                        scan_unit_range: scan.scan_unit_range.clone(),
                    })
                    .collect();
                let storage = self.local_storage(&root)?;
                let provider = self.scan_providers.resolve(&scan.data_file_format)?;
                let source = ScanSource::new_with_worker_state(ScanSourceConfig {
                    storage,
                    files,
                    projected: scan.projected.clone(),
                    column_encoding_hints: scan.column_encoding_hints.clone(),
                    pushdown: scan.pushdown.clone(),
                    runtime_filters: runtime_filters.clone(),
                    metadata_store: self.metadata_store.clone(),
                    registry: self.registry.clone(),
                    expression_kernels: Arc::clone(&self.expression_kernels),
                    runtime_statistics: self.runtime_statistics.clone(),
                    statistics_scan: scan.runtime_statistics.clone(),
                }, provider, worker_state);
                let source = match query_statistics {
                    Some(context) => source.with_query_statistics_context(context),
                    None => source,
                };
                Ok(Box::new(source))
            }
            DataSource::Stream(_) => Err(VeinError::internal(
                "this local executor supports only memory and file sources",
            )),
        }
    }

    fn local_storage(&self, root: &Path) -> Result<Arc<dyn ReadStorage>> {
        let factory = self.read_storage_factory.as_ref().ok_or_else(|| {
            VeinError::unsupported("file scans require a configured read-storage factory", None)
        })?;
        let mut storages = self
            .storages
            .lock()
            .map_err(|_| VeinError::internal("read-storage cache mutex poisoned"))?;
        if let Some(storage) = storages.get(root) {
            return Ok(Arc::clone(storage));
        }
        if storages.len() >= STORAGE_HANDLE_CACHE_CAPACITY {
            storages.clear();
        }
        let storage = factory.open(&StorageLocation::local(root.to_path_buf()))?;
        storages.insert(root.to_path_buf(), Arc::clone(&storage));
        Ok(storage)
    }
}

fn local_scan_paths(files: &[String]) -> Result<(PathBuf, Vec<String>)> {
    let paths = files
        .iter()
        .map(|file| {
            std::fs::canonicalize(file)
                .map_err(|error| VeinError::io(format!("resolve local scan file '{file}'"), error))
        })
        .collect::<Result<Vec<_>>>()?;
    let first = paths
        .first()
        .ok_or_else(|| VeinError::internal("file scan contains no files"))?;
    let root = first
        .ancestors()
        .skip(1)
        .find(|candidate| paths.iter().all(|path| path.starts_with(candidate)))
        .ok_or_else(|| VeinError::internal("local scan files have no common directory"))?
        .to_path_buf();
    let relative = paths
        .iter()
        .map(|path| {
            path.strip_prefix(&root)
                .map(|path| path.to_string_lossy().into_owned())
                .map_err(|error| {
                    VeinError::internal(format!("local scan path is outside its root: {error}"))
                })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((root, relative))
}
