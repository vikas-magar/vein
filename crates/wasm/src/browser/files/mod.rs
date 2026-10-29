//! Worker-local JavaScript file ownership and a Rust range-reader bridge.

use opendal::{Error, ErrorKind, Result};
use wasm_bindgen::prelude::*;

use crate::file_storage::FileRangeReader;

#[wasm_bindgen(module = "/src/browser/files/registry.mjs")]
extern "C" {
    #[wasm_bindgen(catch, js_name = createRegistry)]
    fn create_registry() -> std::result::Result<u32, JsValue>;
    #[wasm_bindgen(catch, js_name = registerFile)]
    fn register_file(id: u32, path: &str, file: &web_sys::File) -> std::result::Result<(), JsValue>;
    #[wasm_bindgen(catch, js_name = readInto)]
    fn read_into(id: u32, path: &str, offset: f64, output: &mut [u8]) -> std::result::Result<(), JsValue>;
    #[wasm_bindgen(catch, js_name = takeStatistics)]
    fn take_statistics(id: u32) -> std::result::Result<JsValue, JsValue>;
    #[wasm_bindgen(js_name = dropRegistry)]
    fn drop_registry(id: u32);
}

/// Contains only a registry identifier, so engine handles remain Send + Sync.
/// The actual File objects never leave their owning Worker's JavaScript registry.
#[derive(Debug)]
pub(crate) struct BrowserFiles {
    id: u32,
}

impl BrowserFiles {
    pub(crate) fn new() -> std::result::Result<Self, JsValue> { Ok(Self { id: create_registry()? }) }
    pub(crate) fn namespace(&self) -> String { self.id.to_string() }
    pub(crate) fn register(&self, path: &str, file: &web_sys::File) -> std::result::Result<(), JsValue> {
        register_file(self.id, path, file)
    }
    pub(crate) fn take_statistics(&self) -> std::result::Result<JsValue, JsValue> { take_statistics(self.id) }
}

impl FileRangeReader for BrowserFiles {
    fn read_into(&self, path: &str, offset: u64, output: &mut [u8]) -> Result<()> {
        read_into(self.id, path, offset as f64, output).map_err(|cause| {
            let message = cause.dyn_ref::<js_sys::Error>().map(|error| String::from(error.message()))
                .or_else(|| cause.as_string()).unwrap_or_else(|| "browser file read failed".into());
            Error::new(ErrorKind::Unexpected, message).with_context("path", path)
        })
    }
}

impl Drop for BrowserFiles {
    fn drop(&mut self) { drop_registry(self.id); }
}
