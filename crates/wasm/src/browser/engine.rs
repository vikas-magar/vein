use std::sync::Arc;

use engine::local::LocalQueryEngine;
use storage::StorageObjectPath;
use wasm_bindgen::prelude::*;

use crate::file_storage::{FileIndex, FileMetadata};
use crate::object_path::valid_object_path;
use super::files::BrowserFiles;

/// Query engine backed by lazily read browser files, without a dataset preload.
#[wasm_bindgen]
pub struct BrowserEngine {
    files: FileIndex,
    registry: Arc<BrowserFiles>,
    pub(super) engine: Option<LocalQueryEngine>,
}

#[wasm_bindgen]
impl BrowserEngine {
    /// Version of the acknowledged, packet-oriented query transport contract.
    #[wasm_bindgen(js_name = resultProtocol)]
    pub fn result_protocol(&self) -> u32 { 1 }

    /// Creates a file registry. Register selected files before executing queries.
    #[wasm_bindgen(constructor)]
    pub fn new() -> Result<BrowserEngine, JsValue> {
        super::panic_hook::install();
        Ok(Self { files: Arc::default(), registry: Arc::new(BrowserFiles::new()?), engine: None })
    }

    /// Registers a root-relative file and its metadata without reading its payload.
    #[wasm_bindgen(js_name = registerFile)]
    pub fn register_file(&mut self, path: String, file: web_sys::File) -> Result<(), JsValue> {
        if self.engine.is_some() { return Err(JsValue::from_str("files cannot be registered after query execution starts")); }
        if !valid_object_path(&StorageObjectPath::new(path.as_str())) {
            return Err(JsValue::from_str("file path must be root-relative and contain no '..' segments"));
        }
        if self.files.contains_key(&path) { return Err(JsValue::from_str("duplicate file path")); }
        let size = file.size();
        if !size.is_finite() || size < 0.0 || size.fract() != 0.0 || size > 9_007_199_254_740_991.0 {
            return Err(JsValue::from_str("file size exceeds the browser's exact integer range"));
        }
        let metadata = FileMetadata { size: size as u64, version: format!("{}:{size}", file.last_modified()) };
        self.registry.register(&path, &file)?;
        Arc::make_mut(&mut self.files).insert(path, metadata);
        Ok(())
    }

    /// Number of registered files, independent of how many bytes have been read.
    #[wasm_bindgen(js_name = fileCount)]
    pub fn file_count(&self) -> usize { self.files.len() }

    /// Returns and resets counters for browser file reads, excluding OS cache behavior.
    #[wasm_bindgen(js_name = takeIoStatistics)]
    pub fn take_io_statistics(&self) -> Result<JsValue, JsValue> { self.registry.take_statistics() }
}

impl BrowserEngine {
    pub(super) fn query_engine(&mut self) -> Result<&LocalQueryEngine, JsValue> {
        if self.engine.is_none() {
            if self.files.is_empty() { return Err(JsValue::from_str("select a Parquet data directory first")); }
            let engine = crate::runtime::build_engine(Arc::clone(&self.registry), Arc::clone(&self.files))
                .map_err(|error| JsValue::from_str(&error.to_string()))?;
            self.engine = Some(engine);
        }
        self.engine.as_ref().ok_or_else(|| JsValue::from_str("browser engine initialization failed"))
    }
}
