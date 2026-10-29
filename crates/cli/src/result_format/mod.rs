//! Streaming JSON output over reference-backed view rows.
mod encode;
#[cfg(test)]
mod tests;

use arrow::datatypes::SchemaRef;
use common::{Result, VeinError};
use exec::pipeline::DataChunkView;
use std::io::Write;
use std::sync::Arc;
use common::time::Instant;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum OutputFormat {
    Pretty,
    JsonLines,
}

impl OutputFormat {
    pub(super) fn from_env() -> Result<Self> {
        match std::env::var("VEIN_OUTPUT_FORMAT").as_deref() {
            Ok("pretty") | Err(std::env::VarError::NotPresent) => Ok(Self::Pretty),
            Ok("jsonl") => Ok(Self::JsonLines),
            Ok(value) => Err(VeinError::invalid_input(format!(
                "unsupported VEIN_OUTPUT_FORMAT '{value}'; expected 'pretty' or 'jsonl'"
            ))),
            Err(std::env::VarError::NotUnicode(_)) => Err(VeinError::invalid_input(
                "VEIN_OUTPUT_FORMAT must be valid UTF-8",
            )),
        }
    }

    pub(super) fn is_json_lines(self) -> bool {
        matches!(self, Self::JsonLines)
    }
}

/// Serializes view references without gathering values or constructing row batches.
pub(super) struct JsonRowsWriter<W: Write> {
    output: W,
    input_schema: Option<SchemaRef>,
    keys: Vec<Vec<u8>>,
    buffer: Vec<u8>,
}

impl<W: Write> JsonRowsWriter<W> {
    pub(super) fn new(output: W) -> Self {
        Self {
            output,
            input_schema: None,
            keys: Vec::new(),
            buffer: Vec::new(),
        }
    }

    pub(super) fn write_view_batch(&mut self, result: &DataChunkView) -> Result<()> {
        let schema = result
            .schema()
            .ok_or_else(|| VeinError::internal("result view is missing its schema"))?;
        self.ensure_schema(schema)?;
        let started = Instant::now();
        let metrics = encode::write_view(
            result,
            schema,
            &self.keys,
            &mut self.buffer,
            &mut self.output,
        )?;
        tracing::debug!(target: "vein.output", event = "result_json_encoded",
            rows = result.num_rows(), columns = result.num_columns(),
            source_encoders = metrics.source_encoders, gathered_payload_bytes = 0,
            encoded_bytes = metrics.bytes,
            encode_us = (started.elapsed().as_nanos().saturating_sub(metrics.write_ns) / 1_000) as u64,
            output_write_us = (metrics.write_ns / 1_000) as u64,
            "serialized retained source values with cached Arrow encoders");
        Ok(())
    }

    pub(super) fn finish(&mut self) -> Result<()> {
        self.output
            .flush()
            .map_err(|error| VeinError::internal(format!("flush JSON result output: {error}")))
    }

    fn ensure_schema(&mut self, input: &SchemaRef) -> Result<()> {
        if let Some(expected) = &self.input_schema {
            if expected.as_ref() != input.as_ref() {
                return Err(VeinError::internal(
                    "result schema changed while writing JSON Lines",
                ));
            }
            return Ok(());
        }
        let fields = input
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                Ok(serde_json::json!({
                    "name": format!("vein_col_{index}"),
                    "data_type": plan::VeinDataType::arrow_field_type_name(field)?,
                    "storage_type": format!("{:?}", field.data_type()),
                    "nullable": field.is_nullable(),
                }))
            })
            .collect::<Result<Vec<_>>>()?;
        serde_json::to_writer(
            &mut self.output,
            &serde_json::json!({"kind": "schema", "fields": fields}),
        )
        .map_err(|error| VeinError::internal(format!("serialize JSON result schema: {error}")))?;
        self.output
            .write_all(b"\n")
            .map_err(|error| VeinError::internal(format!("write JSON result schema: {error}")))?;
        self.keys = (0..input.fields().len())
            .map(|index| format!("\"vein_col_{index}\":").into_bytes())
            .collect();
        self.input_schema = Some(Arc::clone(input));
        Ok(())
    }
}
