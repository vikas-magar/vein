use std::sync::Arc;

use iceberg::io::{
    FileIO,
    FileIOBuilder,
    // Exact property key constants from iceberg-rust s3.rs config
    S3_ACCESS_KEY_ID,
    S3_ENDPOINT,
    S3_PATH_STYLE_ACCESS,
    S3_REGION,
    S3_SECRET_ACCESS_KEY,
};
use iceberg_storage_opendal::OpenDalStorageFactory;

use error::Result;

/// Storage backend configuration for the VCS.
///
/// Both variants work on macOS (dev) and Linux (prod).
/// `LocalFs` needs zero network; `S3` works with AWS S3, MinIO, Ceph.
#[derive(Debug, Clone)]
pub enum StorageConfig {
    /// Local filesystem. Uses iceberg's built-in `LocalFsStorageFactory`.
    /// Paths must use the `file://` scheme, e.g. `file:///data/warehouse`.
    LocalFs,

    /// S3-compatible object store via OpenDAL + reqwest.
    /// Compatible with: AWS S3, MinIO, Ceph RGW, Cloudflare R2.
    S3 {
        /// Full endpoint URL, e.g. `http://localhost:9000` for MinIO.
        /// For AWS S3, omit this (leave as None) and region alone is enough.
        endpoint: Option<String>,
        /// AWS/MinIO region, e.g. `us-east-1`.
        region: String,
        /// AWS Access Key ID.
        access_key_id: String,
        /// AWS Secret Access Key.
        secret_access_key: String,
        /// Set to `true` for MinIO and path-style S3-compatible stores.
        /// Set to `false` (default) for AWS S3 virtual-host-style.
        path_style_access: bool,
    },
}

/// Build a `FileIO` from the given storage config.
///
/// The returned `FileIO` is lazy — the actual connection is established
/// on the first IO call, not here.
pub fn build_file_io(config: &StorageConfig) -> Result<FileIO> {
    match config {
        StorageConfig::LocalFs => {
            // Built into the `iceberg` crate — no opendal, no reqwest.
            Ok(FileIO::new_with_fs())
        }

        StorageConfig::S3 {
            endpoint,
            region,
            access_key_id,
            secret_access_key,
            path_style_access,
        } => {
            // OpenDalStorageFactory::S3 reads props from the StorageConfig
            // map using the exact property keys from iceberg-rust's s3.rs.
            let mut builder = FileIOBuilder::new(Arc::new(OpenDalStorageFactory::S3 {
                customized_credential_load: None,
            }))
            .with_prop(S3_ACCESS_KEY_ID, access_key_id)
            .with_prop(S3_SECRET_ACCESS_KEY, secret_access_key)
            .with_prop(S3_REGION, region)
            // path-style-access=true → needed for MinIO, path-style endpoints
            .with_prop(
                S3_PATH_STYLE_ACCESS,
                if *path_style_access { "true" } else { "false" },
            );

            if let Some(ep) = endpoint {
                builder = builder.with_prop(S3_ENDPOINT, ep);
            }

            Ok(builder.build())
        }
    }
}
