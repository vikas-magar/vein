//! Default mapping from protocol tasks to pipeline sources, transforms, and sinks.

mod config;
mod executor;
mod source;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::expression::eval::ExpressionKernelProvider;
use crate::operators::sink::WriteStorage;
use crate::operators::source::scan::ScanProviderRegistry;
use crate::runtime::filter::{RuntimeFilterDecisionPolicy, RuntimeFilterFactory};
use catalog::MetadataStore;
use common::statistics::RuntimeStatisticsStore;
use functions::FunctionRegistry;
use storage::{ReadStorage, ReadStorageFactory, StorageLocation};

/// Maximum number of root-scoped read handles retained by one executor.
const STORAGE_HANDLE_CACHE_CAPACITY: usize = 64;

/// Builds concrete pipeline stages while delegating file work to registered providers.
pub struct DefaultExecutorBuilder {
    registry: Arc<FunctionRegistry>,
    expression_kernels: Arc<dyn ExpressionKernelProvider>,
    storage: Option<Arc<dyn WriteStorage>>,
    metadata_store: MetadataStore,
    runtime_filter_factory: Arc<dyn RuntimeFilterFactory>,
    runtime_filter_policy: Arc<dyn RuntimeFilterDecisionPolicy>,
    runtime_statistics: RuntimeStatisticsStore,
    scan_providers: ScanProviderRegistry,
    read_storage_factory: Option<Arc<dyn ReadStorageFactory>>,
    storages: Mutex<HashMap<StorageLocation, Arc<dyn ReadStorage>>>,
}
