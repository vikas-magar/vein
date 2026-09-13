//! Plan-level compiler lowering linear logical operators into a [`MicroProgram`].
//!
//! Provides [`PipelineCompiler`], which collects unbroken linear sequences of
//! relational nodes ([`LogicalPlan::Scan`], [`LogicalPlan::Filter`], [`LogicalPlan::Project`])
//! and compiles them into a unified [`DataSource`] and [`MicroProgram`].

use common::identifiers::TaskId;
use common::protocol::{DataSource, FieldSchema, FileFormat, FileScanTask, MicroProgram};
use common::Result;
use error::VeinError;
use plan::schema::VeinSchema;
use plan::LogicalPlan;

use super::expr::BytecodeCompiler;

/// Compiles unbroken linear logical plan pipelines into a leaf [`DataSource`] and a [`MicroProgram`].
pub struct PipelineCompiler;

impl PipelineCompiler {
    /// Compiles a linear [`LogicalPlan`] pipeline starting from `plan`.
    pub fn compile(plan: &LogicalPlan) -> Result<(DataSource, MicroProgram, VeinSchema)> {
        // 1. Traverse down to the leaf to determine the input schema and DataSource
        let (source, leaf_schema) = Self::collect_leaf_source(plan)?;
        let input_cols = leaf_schema.fields.len() as u8;

        // 2. Initialize bytecode compiler with the leaf's physical column count
        let mut compiler = BytecodeCompiler::new(input_cols);

        // 3. Lower nodes in execution order (source-to-sink)
        let out_schema = Self::compile_node(plan, &leaf_schema, &mut compiler)?;

        let program = compiler.finish();
        Ok((source, program, out_schema))
    }

    /// Recursively locates the leaf data source and schema.
    fn collect_leaf_source(plan: &LogicalPlan) -> Result<(DataSource, VeinSchema)> {
        match plan {
            LogicalPlan::Scan {
                table_name,
                projection,
                schema,
                ..
            } => {
                let column_ids = projection
                    .clone()
                    .unwrap_or_else(|| schema.fields.iter().map(|f| f.id).collect());
                let projected_schema = schema
                    .fields
                    .iter()
                    .map(|f| FieldSchema {
                        field_id: f.id.0 as i32,
                        name: f.name.clone(),
                        data_type: format!("{:?}", f.data_type),
                    })
                    .collect();

                let (files, format) = if table_name.ends_with(".exo") || table_name.contains("part_") {
                    (vec![table_name.clone()], FileFormat::Exo)
                } else {
                    (vec![format!("{table_name}.parquet")], FileFormat::Parquet)
                };

                let scan_task = FileScanTask {
                    files,
                    format,
                    byte_range: None,
                    column_ids: column_ids.into_iter().map(|id| id.0 as i32).collect(),
                    projected_schema,
                    task_id: TaskId::new(),
                    shuffle_target_path: None,
                };
                Ok((DataSource::File(scan_task), (**schema).clone()))
            }
            LogicalPlan::ExternalScan {
                url,
                options,
                schema,
            } => {
                let format = options
                    .iter()
                    .find(|(k, _)| k.eq_ignore_ascii_case("format"))
                    .map(|(_, v)| match v.to_lowercase().as_str() {
                        "exo" => FileFormat::Exo,
                        "csv" => FileFormat::Csv,
                        "json" => FileFormat::Json,
                        _ => FileFormat::Parquet,
                    })
                    .unwrap_or_else(|| {
                        if url.ends_with(".exo") || url.contains("part_") {
                            FileFormat::Exo
                        } else {
                            FileFormat::Parquet
                        }
                    });

                let scan_task = FileScanTask {
                    files: vec![url.clone()],
                    format,
                    byte_range: None,
                    column_ids: schema.fields.iter().map(|f| f.id.0 as i32).collect(),
                    projected_schema: schema
                        .fields
                        .iter()
                        .map(|f| FieldSchema {
                            field_id: f.id.0 as i32,
                            name: f.name.clone(),
                            data_type: format!("{:?}", f.data_type),
                        })
                        .collect(),
                    task_id: TaskId::new(),
                    shuffle_target_path: None,
                };
                Ok((DataSource::File(scan_task), (**schema).clone()))
            }
            LogicalPlan::Filter { input, .. } => Self::collect_leaf_source(input),
            LogicalPlan::Project { input, .. } => Self::collect_leaf_source(input),
            other => Err(VeinError::planning(
                format!("Non-linear node encountered in pipeline compiler: {other:?}"),
                None,
            )),
        }
    }

    /// Compiles a logical plan node in source-first order.
    fn compile_node(
        plan: &LogicalPlan,
        current_schema: &VeinSchema,
        compiler: &mut BytecodeCompiler,
    ) -> Result<VeinSchema> {
        match plan {
            LogicalPlan::Scan { filters, schema, .. } => {
                for filter in filters {
                    compiler.compile_filter(filter, schema)?;
                }
                Ok((**schema).clone())
            }
            LogicalPlan::ExternalScan { schema, .. } => Ok((**schema).clone()),
            LogicalPlan::Filter { input, predicate } => {
                let upstream_schema = Self::compile_node(input, current_schema, compiler)?;
                compiler.compile_filter(predicate, &upstream_schema)?;
                Ok(upstream_schema)
            }
            LogicalPlan::Project {
                input,
                exprs,
                schema,
            } => {
                let upstream_schema = Self::compile_node(input, current_schema, compiler)?;
                compiler.compile_projection(exprs, &upstream_schema)?;
                Ok((**schema).clone())
            }
            other => Err(VeinError::planning(
                format!("Unsupported node in linear pipeline compilation: {other:?}"),
                None,
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parser::ast::expr::BinaryOp;
    use plan::expr::literal::LogicalLiteral;
    use plan::expr::logical::LogicalExpr;
    use plan::schema::{ColumnId, ResolvedColumn, VeinDataType};

    #[test]
    fn test_compile_linear_pipeline() {
        let schema = Box::new(VeinSchema::new(vec![
            ResolvedColumn::new(ColumnId(1), "x", None, VeinDataType::Int64),
            ResolvedColumn::new(ColumnId(2), "y", None, VeinDataType::Int64),
        ]));

        let scan = LogicalPlan::Scan {
            table_name: "test_table".to_string(),
            projection: None,
            filters: Vec::new(),
            schema: schema.clone(),
        };

        let filter = LogicalPlan::Filter {
            input: Box::new(scan),
            predicate: LogicalExpr::BinaryOp {
                left: Box::new(LogicalExpr::Column(ResolvedColumn::new(
                    ColumnId(1),
                    "x",
                    None,
                    VeinDataType::Int64,
                ))),
                op: BinaryOp::Gt,
                right: Box::new(LogicalExpr::Literal(LogicalLiteral::Int64(10))),
                return_type: VeinDataType::Boolean,
            },
        };

        let (source, program, out_schema) = PipelineCompiler::compile(&filter).unwrap();
        assert!(matches!(source, DataSource::File(_)));
        assert_eq!(program.instructions.len(), 3);
        assert_eq!(out_schema.fields.len(), 2);
    }

    #[test]
    fn test_compile_exo_scan_format() {
        let schema = Box::new(VeinSchema::new(vec![
            ResolvedColumn::new(ColumnId(1), "col_a", None, VeinDataType::Utf8),
        ]));

        // 1. Table with part_ prefix should select FileFormat::Exo
        let scan_exo = LogicalPlan::Scan {
            table_name: "tbl_10/part_000002".to_string(),
            projection: None,
            filters: Vec::new(),
            schema: schema.clone(),
        };
        let (source_exo, _, _) = PipelineCompiler::compile(&scan_exo).unwrap();
        match source_exo {
            DataSource::File(task) => {
                assert_eq!(task.format, FileFormat::Exo);
                assert_eq!(task.files, vec!["tbl_10/part_000002"]);
            }
            _ => panic!("Expected DataSource::File"),
        }

        // 2. ExternalScan with format option "exo"
        let ext_scan = LogicalPlan::ExternalScan {
            url: "s3://bucket/data/orders.exo".to_string(),
            options: vec![("format".to_string(), "exo".to_string())],
            schema: schema.clone(),
        };
        let (source_ext, _, _) = PipelineCompiler::compile(&ext_scan).unwrap();
        match source_ext {
            DataSource::File(task) => {
                assert_eq!(task.format, FileFormat::Exo);
                assert_eq!(task.files, vec!["s3://bucket/data/orders.exo"]);
            }
            _ => panic!("Expected DataSource::File"),
        }
    }
}
