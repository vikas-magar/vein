//! Compiles Parquet-backed linear query plans into the vector program.
use std::collections::HashSet;
use std::sync::Arc;

use common::Result;
use common::identifiers::TaskId;
use common::protocol::{DataSource, FileFormat, FileScanTask, MicroProgram};
use error::VeinError;
use plan::schema::VeinSchema;
use plan::{LogicalExpr, LogicalPlan};

use super::expr::BytecodeCompiler;

/// Compiles Parquet scans with filters and projections into an executable pipeline.
pub struct PipelineCompiler;

impl PipelineCompiler {
    /// Compiles a linear logical plan into its Parquet source and vector program.
    pub fn compile(plan: &LogicalPlan) -> Result<(DataSource, MicroProgram, VeinSchema)> {
        let (source, scan_schema) = Self::collect_leaf_source(plan)?;
        let register_count = u8::try_from(scan_schema.fields.len())
            .map_err(|_| VeinError::planning("input schema exceeds VM register capacity", None))?;
        let mut compiler = BytecodeCompiler::new(register_count);
        let output_schema = Self::compile_node(plan, scan_schema, &mut compiler)?;
        Ok((source, compiler.finish(), output_schema))
    }

    fn collect_leaf_source(plan: &LogicalPlan) -> Result<(DataSource, VeinSchema)> {
        match plan {
            LogicalPlan::Scan {
                table,
                projection,
                filters,
                schema,
            } => {
                if table
                    .format
                    .as_ref()
                    .is_some_and(|format| format != &plan::TableFormat::Parquet)
                {
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
                let scan_schema =
                    Self::projected_scan_schema(schema, projection.as_deref(), filters)?;
                Self::file_source(files, scan_schema)
            }
            LogicalPlan::ExternalScan {
                url,
                options,
                files,
                schema,
            } => {
                if options.iter().any(|(key, value)| {
                    key.eq_ignore_ascii_case("format") && !value.eq_ignore_ascii_case("parquet")
                }) {
                    return Err(VeinError::planning(
                        "only Parquet external sources are supported",
                        None,
                    ));
                }
                let paths = if files.is_empty() {
                    vec![url.clone()]
                } else {
                    files.clone()
                };
                Self::file_source(paths, (**schema).clone())
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

    fn file_source(files: Vec<String>, schema: VeinSchema) -> Result<(DataSource, VeinSchema)> {
        if files.is_empty() {
            return Err(VeinError::planning("Parquet scan has no input files", None));
        }
        // One handle to the projected schema: splitting this scan into
        // per-file tasks then copies a refcount instead of the schema.
        let projected = Arc::new(schema.clone());
        Ok((
            DataSource::File(FileScanTask {
                files,
                format: FileFormat::Parquet,
                byte_range: None,
                projected,
                task_id: TaskId::new(),
                catalog_snapshot: None,
            }),
            schema,
        ))
    }

    fn projected_scan_schema(
        schema: &VeinSchema,
        projection: Option<&[plan::schema::ColumnId]>,
        filters: &[LogicalExpr],
    ) -> Result<VeinSchema> {
        let mut required = projection
            .map(|columns| columns.iter().copied().collect::<HashSet<_>>())
            .unwrap_or_else(|| schema.fields.iter().map(|field| field.id).collect());
        for filter in filters {
            Self::collect_column_ids(filter, &mut required);
        }
        if required.is_empty() {
            let first = schema.fields.first().ok_or_else(|| {
                VeinError::planning("cannot scan a Parquet schema with no columns", None)
            })?;
            required.insert(first.id);
        }

        let fields = schema
            .fields
            .iter()
            .filter(|field| required.contains(&field.id))
            .cloned()
            .collect::<Vec<_>>();
        if fields.len() != required.len() {
            return Err(VeinError::planning(
                "scan projection references a column that is absent from the scan schema",
                None,
            ));
        }

        Ok(VeinSchema { fields })
    }

    fn collect_column_ids(expression: &LogicalExpr, output: &mut HashSet<plan::schema::ColumnId>) {
        match expression {
            LogicalExpr::Column(column) | LogicalExpr::OuterColumn(column) => {
                output.insert(column.id);
            }
            _ => {
                for child in expression.children() {
                    Self::collect_column_ids(child, output);
                }
            }
        }
    }

    fn compile_node(
        plan: &LogicalPlan,
        input_schema: VeinSchema,
        compiler: &mut BytecodeCompiler,
    ) -> Result<VeinSchema> {
        match plan {
            LogicalPlan::Scan { filters, .. } => {
                for filter in filters {
                    compiler.compile_filter(filter, &input_schema)?;
                }
                Ok(input_schema)
            }
            LogicalPlan::ExternalScan { .. } => Ok(input_schema),
            LogicalPlan::Write { input, .. } => Self::compile_node(input, input_schema, compiler),
            LogicalPlan::Filter { input, predicate } => {
                let schema = Self::compile_node(input, input_schema, compiler)?;
                compiler.compile_filter(predicate, &schema)?;
                Ok(schema)
            }
            LogicalPlan::Project {
                input,
                exprs,
                schema,
            } => {
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
