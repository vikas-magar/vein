#![deny(missing_docs)]

//! Browser bindings for Vein's local query engine.

#[cfg(target_arch = "wasm32")]
mod runtime;

#[cfg(any(target_arch = "wasm32", test))]
mod scheduling;

mod object_path;
#[cfg(any(target_arch = "wasm32", test))]
mod ipc;
#[cfg(any(target_arch = "wasm32", test))]
mod inspection;
#[cfg(any(target_arch = "wasm32", test))]
mod file_storage;

/// Browser engine and JavaScript-facing query API.
#[cfg(target_arch = "wasm32")]
pub mod browser;

/// Indicates that browser bindings require a WebAssembly target.
#[cfg(not(target_arch = "wasm32"))]
pub fn browser_bindings_require_wasm() -> &'static str {
    "vein-wasm browser bindings require wasm32-unknown-unknown"
}
