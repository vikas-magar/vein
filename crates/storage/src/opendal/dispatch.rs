//! Preserve the engine's Send contracts without moving browser-local futures.

use error::Result;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use std::future::Future;

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn future<T: Send + 'static>(value: impl Future<Output=Result<T>> + Send + 'static) -> BoxFuture<'static, Result<T>> {
    Box::pin(value)
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn stream<T: Send + 'static>(value: impl futures::Stream<Item=Result<T>> + Send + 'static) -> BoxStream<'static, Result<T>> {
    Box::pin(value)
}

#[cfg(target_arch = "wasm32")]
pub(super) fn future<T: Send + 'static>(value: impl Future<Output=Result<T>> + 'static) -> BoxFuture<'static, Result<T>> {
    use futures::{FutureExt, channel::oneshot};
    let mut value = Box::pin(value);
    // Synchronous browser file operations finish inline without task dispatch.
    if let Some(result) = value.as_mut().now_or_never() {
        return Box::pin(futures::future::ready(result));
    }
    let (sender, receiver) = oneshot::channel();
    wasm_bindgen_futures::spawn_local(async move { let _ = sender.send(value.await); });
    Box::pin(async move {
        receiver.await.map_err(|_| error::VeinError::internal("browser storage operation was cancelled"))?
    })
}

#[cfg(target_arch = "wasm32")]
pub(super) fn stream<T: Send + 'static>(value: impl futures::Stream<Item=Result<T>> + 'static) -> BoxStream<'static, Result<T>> {
    use futures::{SinkExt, StreamExt, channel::mpsc};
    let (mut sender, receiver) = mpsc::channel(1);
    wasm_bindgen_futures::spawn_local(async move {
        futures::pin_mut!(value);
        while let Some(item) = value.next().await {
            if sender.send(item).await.is_err() { break; }
        }
    });
    Box::pin(receiver)
}
