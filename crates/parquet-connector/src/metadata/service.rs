use std::future::Future;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use catalog::{MetadataKey, MetadataKind, MetadataStore};
use error::{Result, VeinError};
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};
use parquet::file::reader::ChunkReader;
use storage::StorageFileMetadata;

/// Cache kind shared by Parquet catalog discovery, planning, and scans.
pub const PARQUET_FOOTER: MetadataKind = MetadataKind::new("parquet.footer");

/// Owns Parquet footer parsing, Arrow schema derivation, and shared cache access.
#[derive(Clone, Debug)]
pub struct ParquetMetadataService {
    store: MetadataStore,
}

impl ParquetMetadataService {
    /// Creates a service backed by the engine's shared metadata store.
    pub fn new(store: MetadataStore) -> Self {
        Self { store }
    }

    /// Returns the shared cache handle for generic catalog and scan contracts.
    pub fn store(&self) -> MetadataStore {
        self.store.clone()
    }

    /// Loads and caches optional page indexes from a seekable Parquet source.
    pub fn load_footer_from_seekable<R: ChunkReader>(
        &self,
        source: &StorageFileMetadata,
        reader: R,
        source_label: &str,
    ) -> Result<(Arc<ParquetMetaData>, bool)> {
        self.get_or_parse_footer(source, || {
            ParquetMetaDataReader::new()
                .with_page_index_policy(PageIndexPolicy::Optional)
                .parse_and_finish(&reader)
                .map(Arc::new)
                .map_err(|error| {
                    VeinError::internal(format!(
                        "read Parquet metadata from '{source_label}': {error}"
                    ))
                })
        })
    }

    /// Loads footer metadata through a Parquet async reader and the shared cache.
    pub async fn load_footer_from_reader<R: AsyncFileReader + Send>(
        &self,
        source: &StorageFileMetadata,
        reader: &mut R,
    ) -> Result<(Arc<ParquetMetaData>, bool)> {
        self.get_or_load_footer(source, || async {
            reader
                .get_metadata(None)
                .await
                .map_err(|error| VeinError::internal(format!("read Parquet metadata: {error}")))
        })
            .await
    }

    /// Derives the Arrow schema from a cached Parquet footer.
    pub fn arrow_schema(&self, footer: &Arc<ParquetMetaData>, source_label: &str) -> Result<SchemaRef> {
        ArrowReaderMetadata::try_new(Arc::clone(footer), ArrowReaderOptions::new())
            .map(|reader| reader.schema().clone())
            .map_err(|error| {
                VeinError::internal(format!(
                    "derive Arrow schema from '{source_label}': {error}"
                ))
            })
    }

    /// Returns a footer only when the exact source version has a cache key.
    pub fn cached_footer(&self, source: &StorageFileMetadata) -> Option<Arc<ParquetMetaData>> {
        let key = MetadataKey::from_storage(PARQUET_FOOTER, source)?;
        self.store.get(&key)
    }

    fn get_or_parse_footer(
        &self,
        source: &StorageFileMetadata,
        parse: impl FnOnce() -> Result<Arc<ParquetMetaData>>,
    ) -> Result<(Arc<ParquetMetaData>, bool)> {
        let Some(key) = MetadataKey::from_storage(PARQUET_FOOTER, source) else {
            tracing::debug!(
                target: "vein.audit",
                event = "parquet_footer_cache_bypass",
                reason = "source_version_unavailable",
                "parquet footer cache bypassed"
            );
            return parse().map(|footer| (footer, false));
        };
        let (footer, cache_hit) = self.store.get_or_parse(key, parse)?;
        self.trace_cache_access(cache_hit);
        Ok((footer, cache_hit))
    }

    async fn get_or_load_footer<F, Fut>(
        &self,
        source: &StorageFileMetadata,
        load: F,
    ) -> Result<(Arc<ParquetMetaData>, bool)>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output=Result<Arc<ParquetMetaData>>>,
    {
        if let Some(footer) = self.cached_footer(source) {
            self.trace_cache_access(true);
            return Ok((footer, true));
        }
        let footer = load().await?;
        if let Some(key) = MetadataKey::from_storage(PARQUET_FOOTER, source) {
            self.store.insert(key, Arc::clone(&footer));
        }
        self.trace_cache_access(false);
        Ok((footer, false))
    }

    fn trace_cache_access(&self, cache_hit: bool) {
        tracing::debug!(
            target: "vein.audit",
            event = "parquet_footer_cache_access",
            cache_hit,
            "parquet footer cache access"
        );
    }
}
