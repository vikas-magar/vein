//! Compiles Parquet-backed linear query plans into the vector program.
use std::collections::HashSet;
use std::sync::Arc;

use common::Result;
use common::identifiers::TaskId;
use common::protocol::{
    AggOpCode, AggregatePlan, DataSource, FileFormat, FileScanTask, LimitSpec, MicroProgram, RegId,
    ScanPredicate,
};
use error::VeinError;
use plan::schema::VeinSchema;
use plan::{LogicalExpr, LogicalPlan};

use super::expr::BytecodeCompiler;

/// Compiles Parquet scans with filters and projections into an executable pipeline.
pub struct PipelineCompiler;

impl PipelineCompiler {
    /// The row window the plan applies, if the executor can apply it where it
    /// places one: above the whole pipeline program.
    ///
    /// A limit above a projection, an alias or a write is the same window as the
    /// limit below it — all three are row-wise — so those are walked through.
    /// A limit *below* a filter is not: "the first 5 rows, then filter" reads a
    /// different set than "filter, then the first 5 rows", and the program runs
    /// as a single node. That is refused rather than silently reinterpreted.
    pub fn window(plan: &LogicalPlan) -> Result<Option<LimitSpec>> {
        Ok(match plan {
            LogicalPlan::Limit { offset, count, .. } => Some(LimitSpec {
                offset: *offset,
                count: *count,
            }),
            LogicalPlan::Project { input, .. }
            | LogicalPlan::SubqueryAlias { input, .. }
            | LogicalPlan::Write { input, .. } => Self::window(input)?,
            LogicalPlan::Filter { input, .. } => {
                if Self::window(input)?.is_some() {
                    return Err(VeinError::planning(
                        "a LIMIT below a WHERE is not supported: the engine applies a limit \
                         after the filter, so it would keep a different set of rows than the \
                         plan describes. Put the WHERE before the LIMIT.",
                        None,
                    ));
                }
                None
            }
            _ => None,
        })
    }

    /// The aggregate a plan describes, for the executor.
    ///
    /// An aggregate is a pipeline breaker the executor drives, not a program the
    /// VM runs, so the executor needs the key and argument columns in one piece.
    ///
    /// Only a limit may sit above it. Anything else above an aggregate would
    /// have to run *after* the groups exist, and the program runs before: a
    /// projection compiled against the group schema would read registers that
    /// hold the aggregate's inputs, not its outputs, and quietly return the
    /// wrong columns. That is refused rather than miscompiled.
    pub fn aggregate(plan: &LogicalPlan) -> Result<Option<AggregatePlan>> {
        match plan {
            LogicalPlan::Limit { input, .. } => Self::aggregate(input),
            LogicalPlan::Aggregate {
                grouping_sets,
                aggs,
                schema,
                ..
            } => {
                let [keys] = grouping_sets.as_slice() else {
                    return Err(VeinError::planning(
                        "multiple grouping sets are not supported yet",
                        None,
                    ));
                };
                Ok(Some(AggregatePlan {
                    groups: keys.len() as u32,
                    aggs: aggs
                        .iter()
                        .map(|agg| match agg {
                            LogicalExpr::FunctionCall { name, .. } => agg_op(name),
                            other => Err(VeinError::planning(
                                format!("an aggregate must be a function call, found {other:?}"),
                                None,
                            )),
                        })
                        .collect::<Result<_>>()?,
                    types: schema.fields.iter().map(|f| f.data_type.clone()).collect(),
                }))
            }
            // A `DISTINCT` is the same operator with every output column as a key
            // and nothing to fold, which is why it needs no plan of its own.
            LogicalPlan::Distinct { schema, .. } => Ok(Some(AggregatePlan {
                groups: schema.fields.len() as u32,
                aggs: Vec::new(),
                types: schema.fields.iter().map(|f| f.data_type.clone()).collect(),
            })),
            LogicalPlan::Filter { input, .. } | LogicalPlan::Project { input, .. } => {
                if Self::aggregate(input)?.is_some() {
                    Err(VeinError::planning(
                        "an operator above an aggregate is not supported yet: the aggregate's groups \
                         are built after the vector program runs, so anything above it would have to \
                         run after that too",
                        None,
                    ))
                } else {
                    Ok(None)
                }
            }
            _ => Ok(None),
        }
    }

    /// Compiles a linear logical plan into its Parquet source and vector program.
    pub fn compile(plan: &LogicalPlan) -> Result<(DataSource, MicroProgram, VeinSchema)> {
        let (source, scan_schema) = Self::collect_leaf_source(plan)?;
        // `RegId` is wide enough for any schema, so the scan no longer has a
        // column-count ceiling (`02-execution-and-vir-lld.md` §2).
        let register_count = scan_schema.fields.len() as RegId;
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
                let pushdown = Self::scan_predicate(filters, schema)?;
                Self::file_source(files, scan_schema, pushdown)
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
                Self::file_source(paths, (**schema).clone(), None)
            }
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::SubqueryAlias { input, .. }
            | LogicalPlan::Write { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::Distinct { input, .. } => Self::collect_leaf_source(input),
            other => Err(VeinError::planning(
                format!("unsupported source node in Parquet pipeline: {other:?}"),
                None,
            )),
        }
    }

    fn file_source(
        files: Vec<String>,
        schema: VeinSchema,
        pushdown: Option<Arc<ScanPredicate>>,
    ) -> Result<(DataSource, VeinSchema)> {
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
                pushdown,
                task_id: TaskId::new(),
                catalog_snapshot: None,
            }),
            schema,
        ))
    }

    /// Compiles the scan's filters into the predicate a source evaluates before
    /// materializing rows, over only the columns those filters read.
    ///
    /// The predicate is compiled against its own narrow input schema — input
    /// column `i` lands in VM register `i` — so the reader mounts a batch of just
    /// those columns and never touches the wider projected schema.
    fn scan_predicate(
        filters: &[LogicalExpr],
        schema: &VeinSchema,
    ) -> Result<Option<Arc<ScanPredicate>>> {
        if filters.is_empty() {
            return Ok(None);
        }
        let mut required = HashSet::new();
        for filter in filters {
            Self::collect_column_ids(filter, &mut required);
        }
        let input = VeinSchema {
            fields: schema
                .fields
                .iter()
                .filter(|field| required.contains(&field.id))
                .cloned()
                .collect(),
        };
        if input.fields.len() != required.len() {
            return Err(VeinError::planning(
                "scan predicate references a column that is absent from the scan schema",
                None,
            ));
        }
        let register_count = input.fields.len() as RegId;
        let mut compiler = BytecodeCompiler::new(register_count);
        let result = compiler.compile_predicate(filters, &input)?;
        Ok(Some(Arc::new(ScanPredicate {
            input: Arc::new(input),
            program: Arc::new(compiler.finish()),
            result,
        })))
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
            // A limit changes no column, only how many rows come out: the window
            // is applied by the executor, so the program sees straight through.
            LogicalPlan::Limit { input, .. } => Self::compile_node(input, input_schema, compiler),
            // An alias compiles its input but reports *its own* schema: the
            // columns are the same, and it is the alias's field ids and
            // qualifiers that everything above it was resolved against.
            LogicalPlan::SubqueryAlias { input, schema, .. } => {
                Self::compile_node(input, input_schema, compiler)?;
                Ok((**schema).clone())
            }
            // The program's *output* registers become the aggregate's input
            // columns: the keys first, then one argument column per aggregate,
            // which is what lets `sum(price * quantity)` be an ordinary register
            // instead of an expression the executor would have to interpret.
            LogicalPlan::Aggregate {
                input,
                grouping_sets,
                aggs,
                schema,
            } => {
                let input_schema = Self::compile_node(input, input_schema, compiler)?;
                let [keys] = grouping_sets.as_slice() else {
                    return Err(VeinError::planning(
                        "multiple grouping sets are not supported yet",
                        None,
                    ));
                };
                let mut projected = keys.clone();
                for agg in aggs {
                    let LogicalExpr::FunctionCall { name, args, .. } = agg else {
                        return Err(VeinError::planning(
                            format!("an aggregate must be a function call, found {agg:?}"),
                            None,
                        ));
                    };
                    agg_op(name)?;
                    let [argument] = args.as_slice() else {
                        return Err(VeinError::planning(
                            format!("{name} takes exactly one argument in this engine"),
                            None,
                        ));
                    };
                    projected.push(argument.clone());
                }
                compiler.compile_projection(&projected, &input_schema)?;
                Ok((**schema).clone())
            }
            // Distinct is a group by every column with nothing to fold.
            LogicalPlan::Distinct { input, schema } => {
                let input_schema = Self::compile_node(input, input_schema, compiler)?;
                let projected: Vec<LogicalExpr> = input_schema
                    .fields
                    .iter()
                    .cloned()
                    .map(LogicalExpr::Column)
                    .collect();
                compiler.compile_projection(&projected, &input_schema)?;
                Ok((**schema).clone())
            }
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

/// The accumulator a registered aggregate function folds with.
///
/// Refusing an unknown name here is what keeps the executor from having to
/// interpret a function it was never told how to fold.
fn agg_op(name: &str) -> Result<AggOpCode> {
    Ok(match name.to_ascii_uppercase().as_str() {
        "COUNT" => AggOpCode::Count,
        "SUM" => AggOpCode::Sum,
        "MIN" => AggOpCode::Min,
        "MAX" => AggOpCode::Max,
        "AVG" => AggOpCode::Avg,
        other => {
            return Err(VeinError::planning(
                format!("aggregate {other} is not implemented"),
                None,
            ))
        }
    })
}
