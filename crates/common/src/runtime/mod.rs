//! Host-aware dispatch for detached asynchronous work.

use std::future::Future;

#[cfg(test)]
mod tests;

/// Schedules a task on the active host runtime.
///
/// Native engines use Tokio's configured runtime. Browser builds use the
/// JavaScript event loop, which works without WebAssembly threads.
pub fn spawn(task: impl Future<Output = ()> + Send + 'static) {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_futures::spawn_local(task);

    #[cfg(not(target_arch = "wasm32"))]
    drop(tokio::spawn(task));
}

/// Runs synchronous work on the host's supported execution facility.
/// Browser calls already execute inside a dedicated Worker and need no threads.
pub async fn run_blocking<T: Send + 'static>(
    work: impl FnOnce() -> crate::Result<T> + Send + 'static,
) -> crate::Result<T> {
    #[cfg(target_arch = "wasm32")]
    { work() }

    #[cfg(not(target_arch = "wasm32"))]
    {
        tokio::task::spawn_blocking(work).await.map_err(|error| {
            crate::VeinError::internal(format!("host blocking task failed: {error}"))
        })?
    }
}
