//! Host event-loop adaptation, independent of engine composition.
mod pending;
#[cfg(target_arch = "wasm32")]
mod browser;
#[cfg(target_arch = "wasm32")]
pub use browser::BrowserHost;
