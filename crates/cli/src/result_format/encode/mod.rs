//! Reuse Arrow's extensible encoders for each retained source array.
use std::collections::{HashMap, hash_map::Entry};
use std::io::Write;
use std::sync::Arc;
use std::time::Instant;

use arrow::datatypes::SchemaRef;
use arrow::json::writer::{EncoderOptions, NullableEncoder, make_encoder};
use common::{Result, VeinError};
use exec::pipeline::DataChunkView;

/// Bound buffered JSON between rows, independently of result cardinality.
const FLUSH_BYTES: usize = 64 * 1024;

#[derive(Default)]
pub(super) struct EncodingMetrics {
    pub source_encoders: usize,
    pub bytes: usize,
    pub write_ns: u128,
}

pub(super) fn write_view<W: Write>(
    view: &DataChunkView,
    schema: &SchemaRef,
    keys: &[Vec<u8>],
    buffer: &mut Vec<u8>,
    output: &mut W,
) -> Result<EncodingMetrics> {
    if view.num_columns() != schema.fields().len() || keys.len() != view.num_columns() {
        return Err(VeinError::internal("JSON result view and schema widths differ"));
    }
    let options = EncoderOptions::default().with_explicit_nulls(true);
    let mut metrics = EncodingMetrics::default();
    let mut columns: Vec<HashMap<usize, NullableEncoder<'_>>> =
        (0..view.num_columns()).map(|_| HashMap::new()).collect();
    buffer.clear();
    for row in 0..view.num_rows() {
        buffer.push(b'{');
        for (column, cache) in columns.iter_mut().enumerate() {
            if column > 0 { buffer.push(b','); }
            buffer.extend_from_slice(&keys[column]);
            let Some((array, index)) = view.resolve(column, row)? else {
                buffer.extend_from_slice(b"null");
                continue;
            };
            // Identity is borrowed for this call only. Sources remain retained by
            // the view, so addresses cannot be recycled while cached encoders live.
            let identity = Arc::as_ptr(array) as *const () as usize;
            let encoder = match cache.entry(identity) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => entry.insert(
                    make_encoder(&schema.fields()[column], array.as_ref(), &options)?,
                ),
            };
            if encoder.is_null(index) {
                buffer.extend_from_slice(b"null");
            } else {
                encoder.encode(index, buffer);
            }
        }
        buffer.extend_from_slice(b"}\n");
        if buffer.len() >= FLUSH_BYTES { flush(buffer, output, &mut metrics)?; }
    }
    flush(buffer, output, &mut metrics)?;
    metrics.source_encoders = columns.iter().map(HashMap::len).sum();
    Ok(metrics)
}

fn flush<W: Write>(buffer: &mut Vec<u8>, output: &mut W, metrics: &mut EncodingMetrics) -> Result<()> {
    if buffer.is_empty() { return Ok(()); }
    let started = Instant::now();
    output.write_all(buffer)
        .map_err(|error| VeinError::internal(format!("write JSON result rows: {error}")))?;
    metrics.write_ns += started.elapsed().as_nanos();
    metrics.bytes += buffer.len();
    buffer.clear();
    Ok(())
}
