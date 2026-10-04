//! Compiles Parquet-backed linear query plans into the vector program.
use std::collections::HashSet;
use std::sync::Arc;

use common::Result;
use common::identifiers::TaskId;
use common::protocol::{
    AggOpCode, AggregatePlan, DataSource, FileFormat, FileScanTask, LimitSpec, MicroProgram, RegId,
    ScanPredicate, SortKey as PhysicalSortKey, SortPlan,
};
use error::VeinError;
use plan::schema::VeinSchema;
use plan::{LogicalExpr, LogicalPlan, SortKey as LogicalSortKey};

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
    /// Filters above it are compiled separately to run after the groups exist.
    /// Projections remain unsupported until the physical plan carries their
    /// output schema through the post-aggregate pipeline.
    pub fn aggregate(plan: &LogicalPlan) -> Result<Option<AggregatePlan>> {
        match plan {
            LogicalPlan::Limit { input, .. } | LogicalPlan::Sort { input, .. } => {
                Self::aggregate(input)
            }
            LogicalPlan::Aggregate {
                grouping_sets,
                aggs,
                schema,
                ..
            } => {
                let (keys, grouping_sets) = grouping_layout(grouping_sets)?;
                let (aggs, aggregate_input_types): (Vec<_>, Vec<_>) = aggs
                    .iter()
                    .map(|agg| {
                        let (name, argument) = aggregate_argument(agg)?;
                        Ok((agg_op(name)?, argument.data_type()))
                    })
                    .collect::<Result<Vec<_>>>()?
                    .into_iter()
                    .unzip();
                Ok(Some(AggregatePlan {
                    groups: keys.len() as u32,
                    grouping_sets: Some(grouping_sets),
                    aggs,
                    aggregate_input_types,
                    types: schema.fields.iter().map(|f| f.data_type.clone()).collect(),
                }))
            }
            // A `DISTINCT` is the same operator with every output column as a key
            // and nothing to fold, which is why it needs no plan of its own.
            LogicalPlan::Distinct { schema, .. } => Ok(Some(AggregatePlan {
                groups: schema.fields.len() as u32,
                grouping_sets: Some(vec![(0..schema.fields.len() as u32).collect()]),
                aggs: Vec::new(),
                aggregate_input_types: Vec::new(),
                types: schema.fields.iter().map(|f| f.data_type.clone()).collect(),
            })),
            LogicalPlan::Filter { input, .. } => Self::aggregate(input),
            LogicalPlan::Project { input, .. } => {
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

    /// Builds the row ordering for a root `ORDER`, optionally under a `LIMIT`.
    ///
    /// The current task output already contains each sort key, so this metadata
    /// can point directly at its column without adding a second expression path.
    pub fn sort(plan: &LogicalPlan) -> Result<Option<SortPlan>> {
        let Some((sort_keys, schema)) = root_sort(plan)? else {
            return Ok(None);
        };
        let mut computed_index = 0usize;
        let keys = sort_keys.iter()
            .map(|key| {
                let position = match &key.expr {
                    LogicalExpr::Column(column) => schema
                        .position_of_id(column.id)
                        .or_else(|| schema.fields.iter().position(|field| field.name == column.name))
                        .ok_or_else(|| {
                            VeinError::planning(
                                format!("ORDER BY column '{}' is absent from its output schema", column.name),
                                None,
                            )
                        })?,
                    _ => {
                        let position = schema.fields.len() + computed_index;
                        computed_index += 1;
                        position
                    }
                };
                let column = u32::try_from(position).map_err(|_| {
                    VeinError::planning("ORDER BY has too many output columns", None)
                })?;
                Ok(PhysicalSortKey {
                    column,
                    ascending: key.asc,
                    nulls_first: key.nulls_first,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        if keys.is_empty() {
            return Err(VeinError::planning("ORDER requires at least one sort key", None));
        }
        Ok(Some(SortPlan { keys }))
    }

    /// Evaluates computed ORDER expressions once and appends them as temporary
    /// columns after the visible pipeline output.
    pub fn sort_key_program(plan: &LogicalPlan) -> Result<Option<MicroProgram>> {
        let Some((sort_keys, schema)) = root_sort(plan)? else {
            return Ok(None);
        };
        if sort_keys
            .iter()
            .all(|key| matches!(&key.expr, LogicalExpr::Column(_)))
        {
            return Ok(None);
        }

        let mut compiler = BytecodeCompiler::new(schema.fields.len() as RegId);
        let mut output_registers = (0..schema.fields.len())
            .map(|index| index as RegId)
            .collect::<Vec<_>>();
        for key in sort_keys {
            if !matches!(&key.expr, LogicalExpr::Column(_)) {
                output_registers.push(compiler.compile_expr(&key.expr, schema)?);
            }
        }
        compiler.compile_projection_registers(output_registers);
        Ok(Some(compiler.finish()))
    }

    /// Compiles a linear logical plan into its Parquet source and vector program.
    pub fn compile(plan: &LogicalPlan) -> Result<(DataSource, MicroProgram, VeinSchema)> {
        let (source, scan_schema) = Self::collect_leaf_source(plan)?;
        // `RegId` is wide enough for any schema, so the scan no longer has a
        // column-count ceiling (`02-execution-and-vir-lld.md` §2).
        let register_count = scan_schema.fields.len() as RegId;
        let mut compiler = BytecodeCompiler::new(register_count);
        compiler.set_input_schema(&scan_schema);
        let output_schema = Self::compile_node(plan, scan_schema, &mut compiler)?;
        Ok((source, compiler.finish(), output_schema))
    }

    /// Compiles row filters above an aggregate against the aggregate's output schema.
    pub fn post_aggregate_program(plan: &LogicalPlan) -> Result<Option<MicroProgram>> {
        let Some(schema) = aggregate_output_schema(plan) else {
            return Ok(None);
        };
        let mut compiler = BytecodeCompiler::new(schema.fields.len() as RegId);
        compile_post_aggregate_node(plan, &mut compiler)?;
        let program = compiler.finish();
        Ok((!program.instructions.is_empty()).then_some(program))
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
            | LogicalPlan::Sort { input, .. }
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
            LogicalPlan::Sort { input, schema, .. } => {
                Self::compile_node(input, input_schema, compiler)?;
                Ok((**schema).clone())
            }
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
                let (keys, _) = grouping_layout(grouping_sets)?;
                let mut projected = keys;
                for agg in aggs {
                    let (name, argument) = aggregate_argument(agg)?;
                    agg_op(name)?;
                    projected.push(argument.clone());
                }
                compiler.compile_projection_with_schema(&projected, &input_schema, schema)?;
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
                compiler.compile_projection_with_schema(&projected, &input_schema, schema)?;
                Ok((**schema).clone())
            }
            LogicalPlan::Filter { input, predicate } => {
                let schema = Self::compile_node(input, input_schema, compiler)?;
                if Self::aggregate(input)?.is_none() {
                    compiler.compile_filter(predicate, &schema)?;
                }
                Ok(schema)
            }
            LogicalPlan::Project {
                input,
                exprs,
                schema,
            } => {
                let input_schema = Self::compile_node(input, input_schema, compiler)?;
                compiler.compile_projection_with_schema(exprs, &input_schema, schema)?;
                Ok((**schema).clone())
            }
            other => Err(VeinError::planning(
                format!("unsupported operator in Parquet pipeline: {other:?}"),
                None,
            )),
        }
    }
}

/// Returns the output schema at the aggregate boundary, walking row-wise nodes above it.
fn aggregate_output_schema(plan: &LogicalPlan) -> Option<VeinSchema> {
    match plan {
        LogicalPlan::Aggregate { schema, .. } | LogicalPlan::Distinct { schema, .. } => {
            Some((**schema).clone())
        }
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::SubqueryAlias { input, .. }
        | LogicalPlan::Write { input, .. } => aggregate_output_schema(input),
        _ => None,
    }
}

/// Compiles supported operators above the aggregate, in execution order.
fn compile_post_aggregate_node(
    plan: &LogicalPlan,
    compiler: &mut BytecodeCompiler,
) -> Result<VeinSchema> {
    match plan {
        LogicalPlan::Aggregate { schema, .. } | LogicalPlan::Distinct { schema, .. } => {
            Ok((**schema).clone())
        }
        LogicalPlan::Filter { input, predicate } => {
            let schema = compile_post_aggregate_node(input, compiler)?;
            compiler.compile_filter(predicate, &schema)?;
            Ok(schema)
        }
        LogicalPlan::Limit { input, .. } | LogicalPlan::Write { input, .. } => {
            compile_post_aggregate_node(input, compiler)
        }
        LogicalPlan::Sort { input, .. } => compile_post_aggregate_node(input, compiler),
        LogicalPlan::SubqueryAlias { input, schema, .. } => {
            compile_post_aggregate_node(input, compiler)?;
            Ok((**schema).clone())
        }
        LogicalPlan::Project { .. } => Err(VeinError::planning(
            "projection above an aggregate is not supported yet",
            None,
        )),
        other => Err(VeinError::planning(
            format!("unsupported operator above an aggregate: {other:?}"),
            None,
        )),
    }
}

fn contains_sort(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Sort { .. } => true,
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::SubqueryAlias { input, .. }
        | LogicalPlan::Write { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Distinct { input, .. } => contains_sort(input),
        _ => false,
    }
}

fn root_sort(plan: &LogicalPlan) -> Result<Option<(&[LogicalSortKey], &VeinSchema)>> {
    match plan {
        LogicalPlan::Limit { input, .. } => root_sort(input),
        LogicalPlan::Sort {
            input,
            sort_keys,
            schema,
        } => {
            if contains_limit(input) {
                return Err(VeinError::planning(
                    "LIMIT before ORDER is not supported; move LIMIT after ORDER",
                    None,
                ));
            }
            if contains_sort(input) {
                return Err(VeinError::planning(
                    "multiple ORDER operators in one pipeline are not supported yet",
                    None,
                ));
            }
            Ok(Some((sort_keys, schema)))
        }
        other if contains_sort(other) => Err(VeinError::planning(
            "ORDER is currently supported at the end of a pipeline, optionally followed by LIMIT",
            None,
        )),
        _ => Ok(None),
    }
}

fn contains_limit(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Limit { .. } => true,
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::SubqueryAlias { input, .. }
        | LogicalPlan::Write { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Distinct { input, .. } => contains_limit(input),
        _ => false,
    }
}

/// Flattens the grouping columns into one VM projection and records each set as
/// positions into that projection. The output schema uses this same first-seen
/// union of keys.
fn grouping_layout(grouping_sets: &[Vec<LogicalExpr>]) -> Result<(Vec<LogicalExpr>, Vec<Vec<u32>>)> {
    if grouping_sets.is_empty() {
        return Err(VeinError::planning("an aggregate must contain a grouping set", None));
    }

    let mut keys = Vec::new();
    let mut positions = Vec::with_capacity(grouping_sets.len());
    for grouping_set in grouping_sets {
        let mut set_positions = Vec::with_capacity(grouping_set.len());
        for expression in grouping_set {
            let position = match keys.iter().position(|key| key == expression) {
                Some(position) => position,
                None => {
                    keys.push(expression.clone());
                    keys.len() - 1
                }
            };
            let position = u32::try_from(position).map_err(|_| {
                VeinError::planning("aggregate has too many grouping columns", None)
            })?;
            if !set_positions.contains(&position) {
                set_positions.push(position);
            }
        }
        positions.push(set_positions);
    }
    Ok((keys, positions))
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
            ));
        }
    })
}

/// Returns the single input argument for an aggregate expression.
fn aggregate_argument(agg: &LogicalExpr) -> Result<(&str, &LogicalExpr)> {
    let LogicalExpr::FunctionCall { name, args, .. } = agg else {
        return Err(VeinError::planning(
            format!("an aggregate must be a function call, found {agg:?}"),
            None,
        ));
    };
    let [argument] = args.as_slice() else {
        return Err(VeinError::planning(
            format!("{name} takes exactly one argument in this engine"),
            None,
        ));
    };
    Ok((name, argument))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common::protocol::{SortKey as PhysicalSortKey, SortPlan};
    use plan::schema::{ColumnId, ResolvedColumn, VeinDataType, VeinSchema};
    use plan::{LogicalExpr, LogicalLiteral, LogicalPlan, SortKey as LogicalSortKey};

    use super::PipelineCompiler;

    fn schema() -> (VeinSchema, Arc<ResolvedColumn>) {
        let first = Arc::new(ResolvedColumn::new(
            ColumnId(1),
            "name",
            None,
            VeinDataType::Utf8,
        ));
        let second = Arc::new(ResolvedColumn::new(
            ColumnId(2),
            "price",
            None,
            VeinDataType::Int64,
        ));
        (VeinSchema::new(vec![first, second.clone()]), second)
    }

    fn external_scan(schema: &VeinSchema) -> LogicalPlan {
        LogicalPlan::ExternalScan {
            url: "memory://sort-test".to_owned(),
            options: Vec::new(),
            files: Vec::new(),
            schema: Box::new(schema.clone()),
        }
    }

    #[test]
    fn compiles_sort_columns_from_the_output_schema() {
        let (schema, column) = schema();
        let plan = LogicalPlan::Sort {
            input: Box::new(external_scan(&schema)),
            sort_keys: vec![LogicalSortKey {
                expr: LogicalExpr::Column(column),
                asc: false,
                nulls_first: true,
            }],
            schema: Box::new(schema),
        };

        assert_eq!(
            PipelineCompiler::sort(&plan).expect("sort metadata"),
            Some(SortPlan {
                keys: vec![PhysicalSortKey {
                    column: 1,
                    ascending: false,
                    nulls_first: true,
                }],
            })
        );
    }

    #[test]
    fn compiles_computed_sort_keys_as_temporary_vm_columns() {
        let (schema, column) = schema();
        let expression = LogicalExpr::BinaryOp {
            left: Box::new(LogicalExpr::Column(column)),
            op: parser::ast::expr::BinaryOp::Plus,
            right: Box::new(LogicalExpr::Literal(LogicalLiteral::Int64(1))),
            return_type: VeinDataType::Int64,
        };
        let plan = LogicalPlan::Sort {
            input: Box::new(external_scan(&schema)),
            sort_keys: vec![LogicalSortKey {
                expr: expression,
                asc: false,
                nulls_first: false,
            }],
            schema: Box::new(schema),
        };

        assert_eq!(
            PipelineCompiler::sort(&plan)
                .expect("sort metadata")
                .expect("sort exists")
                .keys[0]
                .column,
            2,
        );
        let program = PipelineCompiler::sort_key_program(&plan)
            .expect("sort key program")
            .expect("computed key needs a program");
        assert!(program
            .instructions
            .iter()
            .any(|instruction| matches!(instruction, common::protocol::MicroOp::Add { .. })));
        assert!(matches!(
            program.instructions.last(),
            Some(common::protocol::MicroOp::ProjectRegisters { out_regs })
                if out_regs.len() == 3
        ));
    }

    #[test]
    fn rejects_limit_below_sort_instead_of_reordering_it() {
        let (schema, column) = schema();
        let input = LogicalPlan::Limit {
            input: Box::new(external_scan(&schema)),
            offset: 0,
            count: 1,
            schema: Box::new(schema.clone()),
        };
        let plan = LogicalPlan::Sort {
            input: Box::new(input),
            sort_keys: vec![LogicalSortKey {
                expr: LogicalExpr::Column(column),
                asc: true,
                nulls_first: false,
            }],
            schema: Box::new(schema),
        };

        let error = PipelineCompiler::sort(&plan).expect_err("pre-sort LIMIT is unsupported");
        assert!(error.to_string().contains("LIMIT before ORDER"));
    }
}
