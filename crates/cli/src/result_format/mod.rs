use std::io::Write;
use std::sync::Arc;

use arrow::array::{Array, new_null_array};
use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::json::writer::{LineDelimitedWriter, WriterBuilder};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use common::{Result, VeinError};
use exec::pipeline::DataChunkView;

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

/// Serializes reference-backed rows without gathering their Arrow payloads.
pub(super) struct JsonRowsWriter<W: Write> {
    writer: LineDelimitedWriter<W>,
    input_schema: Option<SchemaRef>,
    output_schema: Option<SchemaRef>,
    header_written: bool,
}

impl<W: Write> JsonRowsWriter<W> {
    pub(super) fn new(output: W) -> Self {
        Self {
            writer: WriterBuilder::new()
                .with_explicit_nulls(true)
                .build(output),
            input_schema: None,
            output_schema: None,
            header_written: false,
        }
    }

    pub(super) fn write_view_batch(&mut self, result: &DataChunkView) -> Result<()> {
        let input_schema = result
            .schema()
            .ok_or_else(|| VeinError::internal("result view is missing its schema"))?;
        let schema = self.ensure_schema(input_schema)?;
        let mut start = 0;
        while start < result.num_rows() {
            let (batch, run_len) = contiguous_run_batch(result, Arc::clone(&schema), start)?;
            self.writer.write(&batch)?;
            start += run_len;
        }
        Ok(())
    }

    pub(super) fn finish(&mut self) -> Result<()> {
        self.writer.finish()?;
        self.writer
            .get_mut()
            .flush()
            .map_err(|error| VeinError::internal(format!("flush JSON result output: {error}")))?;
        Ok(())
    }

    fn ensure_schema(&mut self, input: &SchemaRef) -> Result<SchemaRef> {
        if let Some(expected) = &self.input_schema {
            if expected.as_ref() != input.as_ref() {
                return Err(VeinError::internal(
                    "result schema changed while writing JSON Lines",
                ));
            }
        } else {
            let fields = input
                .fields()
                .iter()
                .enumerate()
                .map(|(index, field)| {
                    Field::new(
                        format!("vein_col_{index}"),
                        field.data_type().clone(),
                        field.is_nullable(),
                    )
                })
                .collect::<Vec<_>>();
            if !self.header_written {
                self.write_schema_header(input)?;
                self.header_written = true;
            }
            self.input_schema = Some(Arc::clone(input));
            self.output_schema = Some(Arc::new(Schema::new(fields)));
        }
        Ok(Arc::clone(
            self.output_schema
                .as_ref()
                .expect("output schema is initialized with input schema"),
        ))
    }

    fn write_schema_header(&mut self, schema: &SchemaRef) -> Result<()> {
        let fields = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                serde_json::json!({
                    "name": format!("vein_col_{index}"),
                    "data_type": format!("{:?}", field.data_type()),
                    "nullable": field.is_nullable(),
                })
            })
            .collect::<Vec<_>>();
        serde_json::to_writer(
            self.writer.get_mut(),
            &serde_json::json!({"kind": "schema", "fields": fields}),
        )
            .map_err(|error| VeinError::internal(format!("serialize JSON result schema: {error}")))?;
        self.writer
            .get_mut()
            .write_all(b"\n")
            .map_err(|error| VeinError::internal(format!("write JSON result schema: {error}")))?;
        Ok(())
    }
}

fn contiguous_run_batch(
    view: &DataChunkView,
    schema: SchemaRef,
    start: usize,
) -> Result<(RecordBatch, usize)> {
    if view.num_columns() != schema.fields().len() || start >= view.num_rows() {
        return Err(VeinError::internal("invalid JSON result view range or schema width"));
    }
    let bases = (0..view.num_columns())
        .map(|column| {
            view.resolve(column, start)
                .map(|row| row.map(|(array, source_row)| (Arc::clone(array), source_row)))
        })
        .collect::<Result<Vec<_>>>()?;
    let mut run_len = 1;
    'rows: for logical_row in (start + 1)..view.num_rows() {
        for (column, base) in bases.iter().enumerate() {
            let current = view.resolve(column, logical_row)?;
            let continues = match (base, current) {
                (Some((source, source_row)), Some((array, row))) => {
                    Arc::ptr_eq(source, array) && source_row.checked_add(run_len) == Some(row)
                }
                (None, None) => true,
                _ => false,
            };
            if !continues {
                break 'rows;
            }
        }
        run_len += 1;
    }

    let mut columns = Vec::with_capacity(view.num_columns());
    for (column, base) in bases.iter().enumerate() {
        let field = schema
            .fields()
            .get(column)
            .ok_or_else(|| VeinError::internal("JSON result schema is narrower than its view"))?;
        let array = match base {
            Some((array, source_row)) => array.slice(*source_row, run_len),
            None => new_null_array(field.data_type(), run_len),
        };
        columns.push(array);
    }
    if columns.len() != schema.fields().len() {
        return Err(VeinError::internal("JSON result schema is wider than its view"));
    }
    RecordBatch::try_new_with_options(
        schema,
        columns,
        &RecordBatchOptions::new().with_row_count(Some(run_len)),
    )
        .map(|batch| (batch, run_len))
        .map_err(|error| VeinError::internal(format!("build JSON result batch: {error}")))
}

#[cfg(test)]
mod tests;
