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
        .try_filter_map(|entry| {
            futures::future::ready(Ok((!entry.metadata().is_dir())
                .then(|| StorageObjectPath::new(entry.path()))))
        })
}
