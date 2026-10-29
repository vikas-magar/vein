use wasm_bindgen::prelude::*;
use super::BrowserEngine;

#[wasm_bindgen]
impl BrowserEngine {
    /// Inspect syntax, semantic validity, prefix schema and real function signatures.
    /// Only catalog metadata may be read; query execution and optimization are not invoked.
    pub async fn inspect(&mut self, query: &str, prefix: &str) -> Result<String, JsValue> {
        let output = crate::inspection::inspect(self.query_engine()?, query, prefix).await;
        serde_json::to_string(&output).map_err(|error| JsValue::from_str(&error.to_string()))
    }
}
