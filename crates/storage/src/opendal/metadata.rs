use std::sync::Arc;
use error::Result;

use crate::{
    StorageFileMetadata, StorageObjectIdentity, StorageObjectPath, StorageObjectVersion,
    StorageTimestamp,
};

use super::{OpenDalReadStorage, storage_error};

pub(super) async fn stat(
    storage: &OpenDalReadStorage,
    path: &StorageObjectPath,
) -> Result<StorageFileMetadata> {
    let context = format!("stat storage object {}", path.as_str());
    let metadata = storage
        .operator
        .stat(path.as_str())
        .await
        .map_err(|error| storage_error(error, context))?;

    Ok(StorageFileMetadata {
        identity: StorageObjectIdentity::new(storage.location.clone(), path.clone()),
        size_bytes: metadata.content_length(),
        version: StorageObjectVersion {
            etag: metadata.etag().map(Arc::<str>::from),
            provider_version: metadata.version().map(Arc::<str>::from),
            last_modified: metadata
                .last_modified()
                .map(|time| StorageTimestamp::from_unix_nanos(time.into_inner().as_nanosecond())),
        },
    })
}
