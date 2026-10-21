use std::path::Path;
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use futures::future::BoxFuture;
use parquet::errors::Result;
use ::storage::ReadStorage;

mod native;
mod storage;

static CONFIGURED_BACKEND: OnceLock<ParquetIoBackend> = OnceLock::new();

pub(super) trait ParquetRangeSource: Send + Sync {
    fn read_range(&self, range: std::ops::Range<u64>) -> BoxFuture<'_, Result<Bytes>>;
    fn representation(&self) -> &'static str;
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum ParquetIoBackend {
    #[default]
    Auto,
    Storage,
    Native,
}

impl ParquetIoBackend {
    pub(super) fn parse(value: &str) -> Option<Self> {
        if value.eq_ignore_ascii_case("auto") {
            Some(Self::Auto)
        } else if value.eq_ignore_ascii_case("storage") {
            Some(Self::Storage)
        } else if value.eq_ignore_ascii_case("native") {
            Some(Self::Native)
        } else {
            None
        }
    }

    pub(crate) fn configured() -> Self {
        *CONFIGURED_BACKEND.get_or_init(Self::load_from_environment)
    }

    fn load_from_environment() -> Self {
        let backend = match std::env::var("VEIN_PARQUET_IO_BACKEND") {
            Ok(value) => match Self::parse(&value) {
                Some(backend) => backend,
                None => {
                    tracing::warn!(
                        target: "vein.audit",
                        event = "parquet_io_backend_invalid",
                        configured = %value,
                        fallback = "auto",
                        "unknown Parquet I/O backend; using the default"
                    );
                    Self::default()
                }
            },
            Err(_) => Self::default(),
        };
        tracing::info!(
            target: "vein.audit",
            event = "parquet_io_backend_selected",
            backend = backend.name(),
            "selected Parquet range-read backend for this process"
        );
        backend
    }

    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Storage => "storage",
            Self::Native => "native",
        }
    }

    pub(super) async fn open_source(
        self,
        storage: &Arc<dyn ReadStorage>,
        relative_path: &str,
        absolute_path: &Path,
        expected_size_bytes: u64,
    ) -> Result<Arc<dyn ParquetRangeSource>> {
        let source: Arc<dyn ParquetRangeSource> = match self {
            Self::Auto => match native::NativeRangeSource::open(absolute_path, expected_size_bytes).await {
                Ok(source) => Arc::new(source),
                Err(error) => {
                    tracing::debug!(
                        target: "vein.audit",
                        event = "parquet_local_range_source_unavailable",
                        source_id = %common::logging::LogLocation::from(absolute_path),
                        error = %error,
                        fallback = "storage",
                        "using the configured storage range reader"
                    );
                    Arc::new(storage::StorageRangeSource::new(Arc::clone(storage), relative_path))
                }
            },
            Self::Storage => Arc::new(storage::StorageRangeSource::new(
                Arc::clone(storage),
                relative_path,
            )),
            Self::Native => Arc::new(
                native::NativeRangeSource::open(absolute_path, expected_size_bytes).await?,
            ),
        };
        tracing::debug!(
            target: "vein.audit",
            event = "parquet_range_source_selected",
            configured_backend = self.name(),
            representation = source.representation(),
            source_id = %common::logging::LogLocation::from(relative_path),
            "Parquet range source selected"
        );
        Ok(source)
    }
}

#[cfg(test)]
#[path = "backend_tests.rs"]
mod tests;
