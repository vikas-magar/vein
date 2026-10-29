use std::sync::Arc;
use arrow::array::RecordBatch;
use common::VeinError;
use engine::local::LocalQuery;
use futures::StreamExt;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;
use super::BrowserEngine;

#[wasm_bindgen]
impl BrowserEngine {
    /// Emits bounded, independent Arrow IPC packets. The callback Promise acknowledges each packet.
    /// This bounds transport buffering; execution retains the existing engine scheduling contract.
    pub async fn query(&mut self, query: &str, consume: js_sys::Function, packet_limit: u32) -> Result<(), JsValue> {
        let prepared = self.query_engine()?.prepare(query).await.map_err(js_error)?;
        let schema = output_schema(&prepared).map_err(js_error)?;
        let mut stream = prepared.into_stream();
        let mut emitted = false;
        while let Some(view) = stream.next().await {
            let view = view.map_err(js_error)?;
            let (batch, _) = exec::pipeline::arrow_adapter::to_record_batch_with_report(&view, Arc::clone(&schema))
                .map_err(js_error)?;
            deliver(&batch, &consume, packet_limit as usize).await?;
            emitted = true;
        }
        if !emitted { deliver(&RecordBatch::new_empty(schema), &consume, packet_limit as usize).await?; }
        Ok(())
    }
}

async fn deliver(batch: &RecordBatch, consume: &js_sys::Function, limit: usize) -> Result<(), JsValue> {
    let mut offset = 0;
    let mut encoder = crate::ipc::PacketEncoder::new(limit);
    loop {
        let (bytes, rows) = encoder.encode(batch, offset).map_err(js_error)?;
        let packet = js_sys::Uint8Array::from(bytes.as_slice());
        drop(bytes);
        let acknowledgement = consume.call1(&JsValue::UNDEFINED, &packet)?;
        drop(packet);
        JsFuture::from(js_sys::Promise::resolve(&acknowledgement)).await?;
        offset += rows;
        if offset >= batch.num_rows() { return Ok(()); }
    }
}

fn output_schema(query: &LocalQuery) -> common::Result<arrow::datatypes::SchemaRef> {
    query.physical_plan(0).map(|plan| Arc::new(plan.root.output_schema.to_arrow()))
        .ok_or_else(|| VeinError::planning("query has no output task", None))
}
fn js_error(error: VeinError) -> JsValue { JsValue::from_str(&error.to_string()) }
