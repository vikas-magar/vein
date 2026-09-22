//! Plan-level compiler lowering linear logical operators into a [`MicroProgram`].
//!
//! Provides [`PipelineCompiler`], which collects unbroken linear sequences of
//! relational nodes ([`LogicalPlan::Scan`], [`LogicalPlan::Filter`], [`LogicalPlan::Project`])
//! and compiles them into a unified [`DataSource`] and [`MicroProgram`].

use common::Result;
use common::identifiers::TaskId;
use common::protocol::{
    DataSource, FieldSchema, FileFormat, FileScanTask, MicroProgram, ScanPredicate,
    ScanPredicateValue,
};
use error::VeinError;
use parser::ast::expr::BinaryOp;
use plan::LogicalPlan;
use plan::expr::literal::LogicalLiteral;
use plan::expr::logical::LogicalExpr;
use plan::schema::VeinSchema;

use super::expr::BytecodeCompiler;

/// Compiles unbroken linear logical plan pipelines into a leaf [`DataSource`] and a [`MicroProgram`].
pub struct PipelineCompiler;

impl PipelineCompiler {
    /// Compiles a linear [`LogicalPlan`] pipeline starting from `plan`.
    pub fn compile(plan: &LogicalPlan) -> Result<(DataSource, MicroProgram, VeinSchema)> {
        // 1. Traverse down to the leaf to determine the input schema and DataSource
        let (mut source, leaf_schema) = Self::collect_leaf_source(plan)?;
        if let DataSource::File(scan) = &mut source {
            scan.scan_predicates = Self::extract_scan_predicates(plan);
        }
        let input_cols = leaf_schema.fields.len() as u8;

        // 2. Initialize bytecode compiler with the leaf's physical column count
        let mut compiler = BytecodeCompiler::new(input_cols);

        // 3. Lower nodes in execution order (source-to-sink)
        let out_schema = Self::compile_node(plan, &leaf_schema, &mut compiler)?;

        let program = compiler.finish();
        Ok((source, program, out_schema))
    }

    /// Extracts conservative storage predicates from the scan/filter chain.
    ///
    /// Only predicates directly attached to a scan are eligible. Filters above a
    /// projection or expression are left to the Micro-IR residual filter.
    fn extract_scan_predicates(plan: &LogicalPlan) -> Vec<ScanPredicate> {
        // Address predicates are a physical capability of EXO. Other sources
        // retain the expression for residual execution until they expose their
        // own zone-map contract.
        if !Self::scan_supports_address_pruning(plan) {
            return Vec::new();
        }
        match plan {
            LogicalPlan::Scan { filters, .. } => {
                let mut predicates = Vec::new();
                for filter in filters {
                    Self::collect_scan_predicates(filter, &mut predicates);
                }
                predicates
            }
            LogicalPlan::Filter { input, predicate } => {
                let mut predicates = Self::extract_scan_predicates(input);
                if Self::is_scan_filter_chain(input) {
                    Self::collect_scan_predicates(predicate, &mut predicates);
                }
                predicates
            }
            LogicalPlan::Project { input, .. } | LogicalPlan::Write { input, .. } => {
                Self::extract_scan_predicates(input)
            }
            _ => Vec::new(),
        }
    }

    fn is_scan_filter_chain(plan: &LogicalPlan) -> bool {
        matches!(plan, LogicalPlan::Scan { .. } | LogicalPlan::Filter { .. })
    }

    fn scan_supports_address_pruning(plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::Scan { table, .. } => {
                matches!(table.format.as_ref(), Some(plan::TableFormat::Exo))
            }
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::Write { input, .. } => Self::scan_supports_address_pruning(input),
            _ => false,
        }
    }

    fn collect_scan_predicates(expr: &LogicalExpr, output: &mut Vec<ScanPredicate>) {
        if let LogicalExpr::BinaryOp {
            left,
            op: BinaryOp::And,
            right,
            ..
        } = expr
        {
            Self::collect_scan_predicates(left, output);
            Self::collect_scan_predicates(right, output);
        } else if let Some(predicate) = Self::compile_scan_predicate(expr) {
            output.push(predicate);
        }
    }

    fn compile_scan_predicate(expr: &LogicalExpr) -> Option<ScanPredicate> {
        match expr {
            LogicalExpr::IsNull { expr } => {
                Self::column_name(expr).map(|column| ScanPredicate::AddressNull {
                    column,
                    is_null: true,
                })
            }
            LogicalExpr::IsNotNull { expr } => {
                Self::column_name(expr).map(|column| ScanPredicate::AddressNull {
                    column,
                    is_null: false,
                })
            }
            LogicalExpr::InList {
                expr,
                list,
                negated: false,
            } => {
                let column = Self::column_name(expr)?;
                let values = list
                    .iter()
                    .map(Self::literal_to_scan_value)
                    .collect::<Option<Vec<_>>>()?;
                Some(ScanPredicate::AddressIn { column, values })
            }
            LogicalExpr::BinaryOp {
                left, op, right, ..
            } => {
                let (column, operator, literal) =
                    match (Self::column_name(left), Self::literal_to_scan_value(right)) {
                        (Some(column), Some(value)) => (column, Self::operator_name(op)?, value),
                        (None, _) => {
                            let column = Self::column_name(right)?;
                            let value = Self::literal_to_scan_value(left)?;
                            (column, Self::reverse_operator_name(op)?, value)
                        }
                        _ => return None,
                    };
                Some(ScanPredicate::Address {
                    column,
                    operator: operator.to_string(),
                    value: literal,
                })
            }
            _ => None,
        }
    }

    fn column_name(expr: &LogicalExpr) -> Option<String> {
        match expr {
            LogicalExpr::Column(column) => Some(column.name.clone()),
            _ => None,
        }
    }

    fn literal_to_scan_value(expr: &LogicalExpr) -> Option<ScanPredicateValue> {
        match expr {
            LogicalExpr::Literal(LogicalLiteral::Boolean(value)) => {
                Some(ScanPredicateValue::Boolean(*value))
            }
            LogicalExpr::Literal(LogicalLiteral::Int64(value)) => {
                Some(ScanPredicateValue::Int64(*value))
            }
            LogicalExpr::Literal(LogicalLiteral::Float64(value)) => {
                Some(ScanPredicateValue::Float64(*value))
            }
            LogicalExpr::Literal(LogicalLiteral::String(value)) => {
                Some(ScanPredicateValue::String(value.clone()))
            }
            LogicalExpr::Literal(LogicalLiteral::Date(value)) => {
                Some(ScanPredicateValue::Date(*value))
            }
            LogicalExpr::Literal(LogicalLiteral::Timestamp(value)) => {
                Some(ScanPredicateValue::Timestamp(*value))
            }
            _ => None,
        }
    }

    fn operator_name(op: &BinaryOp) -> Option<&'static str> {
        match op {
            BinaryOp::Eq => Some("eq"),
            BinaryOp::Neq => Some("neq"),
            BinaryOp::Lt => Some("lt"),
            BinaryOp::Leq => Some("lte"),
            BinaryOp::Gt => Some("gt"),
            BinaryOp::Geq => Some("gte"),
            _ => None,
        }
    }

    fn reverse_operator_name(op: &BinaryOp) -> Option<&'static str> {
        match op {
            BinaryOp::Eq => Some("eq"),
            BinaryOp::Neq => Some("neq"),
            BinaryOp::Lt => Some("gt"),
            BinaryOp::Leq => Some("gte"),
            BinaryOp::Gt => Some("lt"),
            BinaryOp::Geq => Some("lte"),
            _ => None,
        }
    }

    /// Infers a source format only when the catalog did not provide one.
    ///
    /// EXO is never used as an implicit fallback: native EXO requires an explicit
    /// catalog format or an unambiguous native marker in the source directory.
    fn infer_source_format(files: &[String]) -> Result<FileFormat> {
        let mut detected = None;

        for file in files {
            let path = std::path::Path::new(file);
            let candidate = if file.starts_with("vfs://")
                || path.join("_schema.json").is_file()
                || path
                    .read_dir()
                    .map(|entries| {
                        entries.flatten().any(|entry| {
                            entry
                                .file_name()
                                .to_string_lossy()
                                .ends_with(".exo.addr")
                        })
                    })
                    .unwrap_or(false)
            {
                Some(FileFormat::Exo)
            } else if file.to_ascii_lowercase().ends_with(".parquet")
                || path
                    .read_dir()
                    .map(|entries| {
                        entries.flatten().any(|entry| {
                            entry
                                .path()
                                .extension()
                                .is_some_and(|extension| extension.eq_ignore_ascii_case("parquet"))
                        })
                    })
                    .unwrap_or(false)
            {
                Some(FileFormat::Parquet)
            } else if file.to_ascii_lowercase().ends_with(".csv") {
                Some(FileFormat::Csv)
            } else if file.to_ascii_lowercase().ends_with(".json") {
                Some(FileFormat::Json)
            } else {
                None
            };

            if let Some(candidate) = candidate {
                if let Some(previous) = &detected {
                    if previous != &candidate {
                        return Err(VeinError::planning(
                            format!("source paths contain mixed formats: {files:?}"),
                            None,
                        ));
                    }
                } else {
                    detected = Some(candidate);
                }
            }
        }

        detected.ok_or_else(|| {
            VeinError::planning(
                format!(
                    "source format is missing from catalog metadata and cannot be inferred from paths: {files:?}"
                ),
                None,
            )
        })
    }

    /// Recursively locates the leaf data source and schema.
    fn collect_leaf_source(plan: &LogicalPlan) -> Result<(DataSource, VeinSchema)> {
        match plan {
            LogicalPlan::Scan {
                table,
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

                let files = if !table.files.is_empty() {
                    table.files.clone()
                } else if let Some(loc) = &table.location {
                    vec![loc.clone()]
                } else {
                    vec![table.name.clone()]
                };

                let format = match &table.format {
                    Some(plan::TableFormat::Exo) => FileFormat::Exo,
                    Some(plan::TableFormat::Parquet) => FileFormat::Parquet,
                    Some(plan::TableFormat::Csv) => FileFormat::Csv,
                    Some(plan::TableFormat::Json) => FileFormat::Json,
                    Some(plan::TableFormat::Custom(s)) => match s.to_lowercase().as_str() {
                        "exo" => FileFormat::Exo,
                        "csv" => FileFormat::Csv,
                        "json" => FileFormat::Json,
                        _ => FileFormat::Parquet,
                    },
                    _ => Self::infer_source_format(&files)?,
                };

                let scan_task = FileScanTask {
                    files,
                    format,
                    byte_range: None,
                    column_ids: column_ids.into_iter().map(|id| id.0 as i32).collect(),
                    projected_schema,
                    scan_predicates: Vec::new(),
                    task_id: TaskId::new(),
                    catalog_snapshot: table.catalog_snapshot.clone(),
                    shuffle_target_path: None,
                };
                Ok((DataSource::File(scan_task), (**schema).clone()))
            }
            LogicalPlan::ExternalScan {
                url,
                options,
                files,
                schema,
            } => {
                let scan_files = if !files.is_empty() {
                    files.clone()
                } else {
                    vec![url.clone()]
                };

                let format = if let Some((_, v)) = options
                    .iter()
                    .find(|(k, _)| k.eq_ignore_ascii_case("format"))
                {
                    match v.to_lowercase().as_str() {
                        "exo" => FileFormat::Exo,
                        "csv" => FileFormat::Csv,
                        "json" => FileFormat::Json,
                        _ => FileFormat::Parquet,
                    }
                } else if scan_files.iter().any(|f| f.ends_with(".parquet"))
                    || url.ends_with(".parquet")
                {
                    FileFormat::Parquet
                } else if scan_files.iter().any(|f| f.ends_with(".csv")) || url.ends_with(".csv") {
                    FileFormat::Csv
                } else if scan_files.iter().any(|f| f.ends_with(".json")) || url.ends_with(".json")
                {
                    FileFormat::Json
                } else {
                    return Err(VeinError::planning(
                        format!("external source format is missing for '{url}'"),
                        None,
                    ));
                };

                let scan_task = FileScanTask {
                    files: scan_files,
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
                    scan_predicates: Vec::new(),
                    task_id: TaskId::new(),
                    catalog_snapshot: None,
                    shuffle_target_path: None,
                };
                Ok((DataSource::File(scan_task), (**schema).clone()))
            }
            LogicalPlan::Filter { input, .. } => Self::collect_leaf_source(input),
            LogicalPlan::Project { input, .. } => Self::collect_leaf_source(input),
            LogicalPlan::Write { input, .. } => Self::collect_leaf_source(input),
            other => Err(VeinError::planning(
                format!("Unsupported source node in linear pipeline compilation: {other:?}"),
                None,
            )),
        }
    }

    /// Compiles a logical plan into executable vector instructions
    fn compile_node(
        plan: &LogicalPlan,
        current_schema: &VeinSchema,
        compiler: &mut BytecodeCompiler,
    ) -> Result<VeinSchema> {
        match plan {
            LogicalPlan::Scan {
                filters, schema, ..
            } => {
                let upstream_schema = (**schema).clone();
                for filter in filters {
                    compiler.compile_filter(filter, &upstream_schema)?;
                }
                Ok(upstream_schema)
            }
            LogicalPlan::ExternalScan { schema, .. } => Ok((**schema).clone()),
            LogicalPlan::Write { input, .. } => Self::compile_node(input, current_schema, compiler),
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
    fn test_missing_format_infers_parquet_from_source_file() {
        let format = PipelineCompiler::infer_source_format(&[
            "./data/input/part-00000.parquet".to_string(),
        ])
        .expect("Parquet source format should be inferred");
        assert_eq!(format, FileFormat::Parquet);
    }

    #[test]
    fn test_missing_format_does_not_default_to_exo() {
        let error = PipelineCompiler::infer_source_format(&["./data/input".to_string()])
            .expect_err("ambiguous source format must fail planning");
        assert!(error.to_string().contains("source format is missing"));
    }

    #[test]
    fn test_compile_linear_pipeline() {
        let schema = Box::new(VeinSchema::new(vec![
            ResolvedColumn::new(ColumnId(1), "x", None, VeinDataType::Int64),
            ResolvedColumn::new(ColumnId(2), "y", None, VeinDataType::Int64),
        ]));

        let table = std::sync::Arc::new(
            plan::TableMetadata::new("test_table", std::sync::Arc::new(*schema.clone()))
                .with_format(plan::TableFormat::Parquet),
        );

        let scan = LogicalPlan::Scan {
            table,
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
    fn test_compile_extracts_conjunctive_scan_predicates() {
        let schema = Box::new(VeinSchema::new(vec![
            ResolvedColumn::new(ColumnId(1), "id", None, VeinDataType::Int64),
            ResolvedColumn::new(ColumnId(2), "tenant_id", None, VeinDataType::Int64),
        ]));
        let table = std::sync::Arc::new(
            plan::TableMetadata::new("events", std::sync::Arc::new(*schema.clone()))
                .with_format(plan::TableFormat::Exo),
        );
        let scan = LogicalPlan::Scan {
            table,
            projection: None,
            filters: Vec::new(),
            schema: schema.clone(),
        };
        let predicate = LogicalExpr::BinaryOp {
            left: Box::new(LogicalExpr::BinaryOp {
                left: Box::new(LogicalExpr::Column(ResolvedColumn::new(
                    ColumnId(1),
                    "id",
                    None,
                    VeinDataType::Int64,
                ))),
                op: BinaryOp::Gt,
                right: Box::new(LogicalExpr::Literal(LogicalLiteral::Int64(100))),
                return_type: VeinDataType::Boolean,
            }),
            op: BinaryOp::And,
            right: Box::new(LogicalExpr::BinaryOp {
                left: Box::new(LogicalExpr::Column(ResolvedColumn::new(
                    ColumnId(2),
                    "tenant_id",
                    None,
                    VeinDataType::Int64,
                ))),
                op: BinaryOp::Eq,
                right: Box::new(LogicalExpr::Literal(LogicalLiteral::Int64(5))),
                return_type: VeinDataType::Boolean,
            }),
            return_type: VeinDataType::Boolean,
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(scan),
            predicate,
        };

        let (source, _, _) = PipelineCompiler::compile(&plan).unwrap();
        match source {
            DataSource::File(task) => {
                assert_eq!(task.scan_predicates.len(), 2);
                assert!(matches!(
                    &task.scan_predicates[0],
                    ScanPredicate::Address { column, operator, .. }
                        if column == "id" && operator == "gt"
                ));
                assert!(matches!(
                    &task.scan_predicates[1],
                    ScanPredicate::Address { column, operator, .. }
                        if column == "tenant_id" && operator == "eq"
                ));
            }
            _ => panic!("Expected DataSource::File"),
        }
    }

    #[test]
    fn test_compile_exo_scan_format() {
        let schema = Box::new(VeinSchema::new(vec![ResolvedColumn::new(
            ColumnId(1),
            "col_a",
            None,
            VeinDataType::Utf8,
        )]));

        // 1. Table with Exo format metadata
        let table = std::sync::Arc::new(
            plan::TableMetadata::new("tbl_10/part_000002", std::sync::Arc::new(*schema.clone()))
                .with_format(plan::TableFormat::Exo)
                .with_files(vec!["tbl_10/part_000002".to_string()]),
        );
        let scan_exo = LogicalPlan::Scan {
            table,
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
            files: vec!["s3://bucket/data/orders.exo".to_string()],
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
