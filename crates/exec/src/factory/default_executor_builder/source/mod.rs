mod paths;

#[cfg(test)]
mod tests;

use std::sync::Arc;

use crate::operators::source::ScanSource;
use crate::operators::source::scan::{ScanSourceConfig, ScanWorkerState, validate_file_scan};
use crate::pipeline::Source;
use crate::runtime::filter::RuntimeFilterSet;
use common::{Result, protocol::DataSource, statistics::QueryStatisticsContext};
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
            DataSource::Memory => Err(VeinError::internal("a DataSource::Memory task carries no payload to materialize")),
            DataSource::Intermediate(_) => Err(VeinError::internal("intermediate sources must be resolved by the query task executor")),
            DataSource::File(scan) => {
                validate_file_scan(scan)?;
                let (location, files) = paths::resolve(scan)?;
                let storage = self.storage_for(&location)?;
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
            DataSource::Stream(_) => Err(VeinError::internal("this local executor supports only memory and file sources")),
        }
    }

    fn storage_for(&self, location: &StorageLocation) -> Result<Arc<dyn ReadStorage>> {
        let factory = self.read_storage_factory.as_ref().ok_or_else(|| {
            VeinError::unsupported("file scans require a configured read-storage factory", None)
        })?;
        let mut storages = self.storages.lock()
            .map_err(|_| VeinError::internal("read-storage cache mutex poisoned"))?;
        if let Some(storage) = storages.get(location) {
            return Ok(Arc::clone(storage));
        }
        if storages.len() >= STORAGE_HANDLE_CACHE_CAPACITY { storages.clear(); }
        let storage = factory.open(location)?;
        storages.insert(location.clone(), Arc::clone(&storage));
        Ok(storage)
    }
}
