use std::path::PathBuf;

use parquet_connector::catalog::ParquetCatalog;
use storage::StorageLocation;
use storage::opendal::OpenDalReadStorageFactory;

pub(crate) fn parquet_catalog(root: impl Into<PathBuf>) -> ParquetCatalog {
    let factory = OpenDalReadStorageFactory;
    ParquetCatalog::new(StorageLocation::local(root), &factory)
        .expect("open local Parquet test catalog")
}
