use std::sync::Arc;

use error::Result;
use futures::TryStreamExt;

use crate::StorageObjectPath;

use super::{OpenDalReadStorage, storage_error};

pub(super) fn list<'a>(
    storage: &'a OpenDalReadStorage,
    prefix: &'a StorageObjectPath,
) -> impl futures::Stream<Item=Result<StorageObjectPath>> + Send + 'a {
    let operator = storage.operator.clone();
    let prefix = prefix.clone();
    let filter_prefix = prefix.clone();
    let context: Arc<str> = Arc::from(format!("list storage objects under {}", prefix.as_str()));
    let open_context = Arc::clone(&context);

    futures::stream::once(async move {
        operator
            .lister_with(prefix.as_str())
            .recursive(true)
            .await
            .map_err(|error| storage_error(error, open_context.to_string()))
    })
        .map_ok(move |lister| {
            let item_context = Arc::clone(&context);
            lister.map_err(move |error| storage_error(error, item_context.to_string()))
        })
        .try_flatten()
        .try_filter_map(move |entry| {
            let path = StorageObjectPath::new(entry.path());
            let is_file = !entry.metadata().is_dir();
            futures::future::ready(Ok(
                (is_file && is_within_prefix(&path, &filter_prefix)).then_some(path),
            ))
        })
}

fn is_within_prefix(path: &StorageObjectPath, prefix: &StorageObjectPath) -> bool {
    let prefix = prefix.as_str().trim_end_matches('/');
    let path = path.as_str();
    prefix.is_empty()
        || path == prefix
        || path
            .strip_prefix(prefix)
            .is_some_and(|remainder| remainder.starts_with('/'))
}
