//! Compiles Parquet-backed linear query plans into the vector program.
use common::Result;
use common::identifiers::TaskId;
use common::protocol::{DataSource, FieldSchema, FileFormat, FileScanTask, MicroProgram};
use error::VeinError;
use plan::LogicalPlan;
use plan::schema::VeinSchema;

use super::expr::BytecodeCompiler;

/// Compiles Parquet scans with filters and projections into an executable pipeline.
pub struct PipelineCompiler;

impl PipelineCompiler {
    /// Compiles a linear logical plan into its Parquet source and vector program.
    pub fn compile(plan: &LogicalPlan) -> Result<(DataSource, MicroProgram, VeinSchema)> {
        let (source, input_schema) = Self::collect_leaf_source(plan)?;
        let register_count = u8::try_from(input_schema.fields.len())
            .map_err(|_| VeinError::planning("input schema exceeds VM register capacity", None))?;
        let mut compiler = BytecodeCompiler::new(register_count);
        let output_schema = Self::compile_node(plan, &input_schema, &mut compiler)?;
        Ok((source, compiler.finish(), output_schema))
    }

    fn collect_leaf_source(plan: &LogicalPlan) -> Result<(DataSource, VeinSchema)> {
        match plan {
            LogicalPlan::Scan {
                table,
                projection,
                schema,
                ..
            } => {
                if table.format.as_ref().is_some_and(|format| format != &plan::TableFormat::Parquet) {
                    return Err(VeinError::planning(
                        format!("only Parquet tables are supported: '{}'", table.name),
                        None,
                    ));
                }
                let files = if !table.files.is_empty() {
                    table.files.clone()
                } else if let Some(location) = &table.location {
                    vec![location.clone()]
                } else {
                    vec![table.name.clone()]
                };
                let columns = projection
                    .clone()
                    .unwrap_or_else(|| schema.fields.iter().map(|field| field.id).collect());
                Self::file_source(files, &columns, schema)
            }
            LogicalPlan::ExternalScan { url, options, files, schema } => {
                if options.iter().any(|(key, value)| {
                    key.eq_ignore_ascii_case("format") && !value.eq_ignore_ascii_case("parquet")
                }) {
                    return Err(VeinError::planning("only Parquet external sources are supported", None));
                }
                let paths = if files.is_empty() { vec![url.clone()] } else { files.clone() };
                Self::file_source(paths, &schema.fields.iter().map(|field| field.id).collect::<Vec<_>>(), schema)
            }
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::Write { input, .. } => Self::collect_leaf_source(input),
            other => Err(VeinError::planning(
                format!("unsupported source node in Parquet pipeline: {other:?}"),
                None,
            )),
        }
    }

    fn file_source(
        files: Vec<String>,
        columns: &[plan::schema::ColumnId],
        schema: &VeinSchema,
    ) -> Result<(DataSource, VeinSchema)> {
        if files.is_empty() {
            return Err(VeinError::planning("Parquet scan has no input files", None));
        }
        let projected_schema = schema
            .fields
            .iter()
            .filter(|field| columns.contains(&field.id))
            .map(|field| FieldSchema {
                field_id: field.id.0 as i32,
                name: field.name.clone(),
                data_type: format!("{:?}", field.data_type),
            })
            .collect();
        Ok((
            DataSource::File(FileScanTask {
                files,
                format: FileFormat::Parquet,
                byte_range: None,
                column_ids: columns.iter().map(|id| id.0 as i32).collect(),
                projected_schema,
                task_id: TaskId::new(),
                catalog_snapshot: None,
            }),
            schema.clone(),
        ))
    }

    fn compile_node(
        plan: &LogicalPlan,
        input_schema: &VeinSchema,
        compiler: &mut BytecodeCompiler,
    ) -> Result<VeinSchema> {
        match plan {
            LogicalPlan::Scan { filters, schema, .. } => {
                let schema = (**schema).clone();
                for filter in filters {
                    compiler.compile_filter(filter, &schema)?;
                }
                Ok(schema)
            }
            LogicalPlan::ExternalScan { schema, .. } => Ok((**schema).clone()),
            LogicalPlan::Write { input, .. } => Self::compile_node(input, input_schema, compiler),
            LogicalPlan::Filter { input, predicate } => {
                let schema = Self::compile_node(input, input_schema, compiler)?;
                compiler.compile_filter(predicate, &schema)?;
                Ok(schema)
            }
            LogicalPlan::Project { input, exprs, schema } => {
                let input_schema = Self::compile_node(input, input_schema, compiler)?;
                compiler.compile_projection(exprs, &input_schema)?;
                Ok((**schema).clone())
            }
            other => Err(VeinError::planning(
                format!("unsupported operator in Parquet pipeline: {other:?}"),
                None,
            )),
        }
    }
}
