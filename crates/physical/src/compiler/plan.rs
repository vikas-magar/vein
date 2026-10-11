//! Compiles Parquet-backed linear query plans into the vector program.
use std::collections::HashSet;
use std::sync::Arc;

use common::Result;
use common::identifiers::TaskId;
use common::protocol::{
    AggOpCode, AggregatePlan, DataSource, FileFormat, FileScanTask, JoinOutput, JoinResidualPlan,
    LimitSpec, MicroProgram, RegId, ScanPredicate, SortKey as PhysicalSortKey, SortPlan,
};
use error::VeinError;
use plan::schema::VeinSchema;
use plan::{LogicalExpr, LogicalPlan, SortKey as LogicalSortKey};

use super::expr::BytecodeCompiler;

/// Compiles Parquet scans with filters and projections into an executable pipeline.
pub struct PipelineCompiler;

impl PipelineCompiler {
    /// Compiles the non-key conjuncts of a hash join condition against only the
    /// columns they reference. The probe key is retained as a row-count carrier
    /// when a residual is constant-only.
    pub fn compile_join_residual(
        predicates: &[LogicalExpr],
        probe: &VeinSchema,
        build: &VeinSchema,
        probe_key: u32,
    ) -> Result<Option<JoinResidualPlan>> {
        if predicates.is_empty() {
            return Ok(None);
        }

        let mut required = HashSet::new();
        for predicate in predicates {
            Self::collect_column_ids(predicate, &mut required);
        }

        let mut input_schema = VeinSchema::empty();
        let mut input_columns = Vec::new();
        for (schema, is_probe) in [(probe, true), (build, false)] {
            for field in &schema.fields {
                if required.remove(&field.id) {
                    input_schema.fields.push(Arc::clone(field));
                    let position = schema.position_of_id(field.id).ok_or_else(|| {
                        VeinError::planning("join residual column is absent from its input", None)
                    })?;
                    let position = u32::try_from(position).map_err(|_| {
                        VeinError::planning("join input has too many columns", None)
                    })?;
                    input_columns.push(if is_probe {
                        JoinOutput::Probe(position)
                    } else {
                        JoinOutput::Build(position)
                    });
                }
            }
        }
        if !required.is_empty() {
            return Err(VeinError::planning(
                "join residual predicate references a column absent from both inputs",
                None,
            ));
        }
        if input_schema.fields.is_empty() {
            let key = probe.fields.get(probe_key as usize).ok_or_else(|| {
                VeinError::planning("join residual has no row-count input column", None)
            })?;
            input_schema.fields.push(Arc::clone(key));
            input_columns.push(JoinOutput::Probe(probe_key));
        }

        let register_count = RegId::try_from(input_schema.fields.len())
            .map_err(|_| VeinError::planning("join residual has too many input columns", None))?;
        let mut compiler = BytecodeCompiler::new(register_count);
        compiler.set_input_schema(&input_schema);
        for predicate in predicates {
            compiler.compile_filter(predicate, &input_schema)?;
        }
        Ok(Some(JoinResidualPlan {
            input_columns,
            program: Arc::new(compiler.finish()),
        }))
    }

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
                    return Err(VeinError::unsupported(
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
    /// Filters and projections above it are compiled separately to run after
    /// the groups exist.
    pub fn aggregate(plan: &LogicalPlan) -> Result<Option<AggregatePlan>> {
        match plan {
            LogicalPlan::Limit { input, .. } | LogicalPlan::Sort { input, .. } => {
                Self::aggregate(input)
            }
            LogicalPlan::Aggregate {
                input,
                grouping_sets,
                aggs,
                schema,
                ..
            } => {
                let estimated_groups = estimate_single_key_groups(input, grouping_sets);
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
                    estimated_groups,
                    grouping_sets: Some(grouping_sets),
                    emit_empty_group_on_empty_input: true,
                    aggs,
                    aggregate_input_types,
                    types: schema.fields.iter().map(|f| f.data_type.clone()).collect(),
                }))
            }
            // The runtime keeps DISTINCT explicit in the physical tree while
            // reusing the aggregate engine's all-keys, no-fold execution plan.
            LogicalPlan::Distinct { schema, .. } => {
                let groups = u32::try_from(schema.fields.len()).map_err(|_| {
                    VeinError::planning("DISTINCT input has too many columns", None)
                })?;
                Ok(Some(AggregatePlan {
                    groups,
                    estimated_groups: None,
                    grouping_sets: Some(vec![(0..groups).collect()]),
                    emit_empty_group_on_empty_input: false,
                    aggs: Vec::new(),
                    aggregate_input_types: Vec::new(),
                    types: schema.fields.iter().map(|f| f.data_type.clone()).collect(),
                }))
            }
            LogicalPlan::Filter { input, .. } => Self::aggregate(input),
            LogicalPlan::Project { input, .. } => Self::aggregate(input),
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
        Ok(Some(compile_sort_plan(sort_keys, schema)?))
    }

    /// Compiles the sort operator at this node without imposing root-pipeline
    /// restrictions on operators below it. Recursive inputs retain the exact
    /// logical order, including a limit that precedes a sort.
    pub fn recursive_sort(plan: &LogicalPlan) -> Result<Option<(SortPlan, Option<MicroProgram>)>> {
        let LogicalPlan::Sort {
            sort_keys, schema, ..
        } = plan
        else {
            return Ok(None);
        };
        Ok(Some((
            compile_sort_plan(sort_keys, schema)?,
            compile_sort_key_program(sort_keys, schema)?,
        )))
    }

    /// Evaluates computed ORDER expressions once and appends them as temporary
    /// columns after the visible pipeline output.
    pub fn sort_key_program(plan: &LogicalPlan) -> Result<Option<MicroProgram>> {
        let Some((sort_keys, schema)) = root_sort(plan)? else {
            return Ok(None);
        };
        compile_sort_key_program(sort_keys, schema)
    }

    /// Compiles a linear logical plan into its Parquet source and vector program.
    pub fn compile(
        plan: &LogicalPlan,
    ) -> Result<(DataSource, MicroProgram, VeinSchema, VeinSchema)> {
        let mut dictionary_group_ids = Vec::new();
        let (mut source, scan_schema) = Self::collect_leaf_source(plan, &mut dictionary_group_ids)?;
        if let DataSource::File(scan) = &mut source {
            if scan.pushdown.is_none() {
                scan.dictionary_columns = scan_schema
                    .fields
                    .iter()
                    .enumerate()
                    .filter(|(_, field)| {
                        field.data_type == plan::schema::VeinDataType::Utf8
                            && dictionary_group_ids.contains(&field.id)
                    })
                    .filter_map(|(index, _)| u32::try_from(index).ok())
                    .collect();
            }
        }
        // `RegId` is wide enough for any schema, so the scan no longer has a
        // column-count ceiling (`02-execution-and-vir-lld.md` §2).
        let register_count = scan_schema.fields.len() as RegId;
        let mut compiler = BytecodeCompiler::new(register_count);
        compiler.set_input_schema(&scan_schema);
        let output_schema = Self::compile_node(plan, scan_schema.clone(), &mut compiler)?;
        Ok((source, compiler.finish(), scan_schema, output_schema))
    }

    /// Compiles the VM portion of one operator against its recursively produced
    /// input. Unlike `compile`, this does not search for a Parquet leaf: the
    /// physical child supplies the batches at runtime.
    pub fn compile_recursive_transform(
        plan: &LogicalPlan,
        input_schema: &VeinSchema,
    ) -> Result<(MicroProgram, VeinSchema)> {
        Self::compile_recursive_transforms(&[plan], input_schema)
    }

    /// Compiles adjacent VM-expressible operators into one program over a
    /// recursively produced input. The nodes must be ordered from the child
    /// toward the root so each operator sees the preceding node's output schema.
    pub fn compile_recursive_transforms(
        plans: &[&LogicalPlan],
        input_schema: &VeinSchema,
    ) -> Result<(MicroProgram, VeinSchema)> {
        let mut compiler = BytecodeCompiler::new(input_schema.fields.len() as RegId);
        compiler.set_input_schema(input_schema);
        let mut output_schema = input_schema.clone();
        for plan in plans {
            // Recursive lowering already compiled the child into a canonical
            // physical node. Compile only each operator against its immediate
            // input schema; walking its logical child again can lose columns
            // that a preceding operator has intentionally narrowed.
            output_schema = Self::compile_recursive_transform_node(plan, &output_schema, &mut compiler)
                .map_err(|error| {
                    let operator = match *plan {
                        LogicalPlan::Filter { .. } => "filter",
                        LogicalPlan::Project { .. } => "projection",
                        LogicalPlan::Aggregate { .. } => "aggregate",
                        LogicalPlan::Distinct { .. } => "distinct",
                        LogicalPlan::SubqueryAlias { .. } => "subquery alias",
                        _ => "transform",
                    };
                    let columns = output_schema
                        .fields
                        .iter()
                        .map(|field| format!("{}#{}", field.name, field.id.0))
                        .collect::<Vec<_>>()
                        .join(", ");
                    VeinError::context(
                        format!(
                            "failed to compile recursive {operator} against input schema [{columns}]"
                        ),
                        error,
                    )
                })?;
        }
        Ok((compiler.finish(), output_schema))
    }

    fn compile_recursive_transform_node(
        plan: &LogicalPlan,
        input_schema: &VeinSchema,
        compiler: &mut BytecodeCompiler,
    ) -> Result<VeinSchema> {
        match plan {
            LogicalPlan::Filter {
                input: _,
                predicate,
            } => {
                // Recursive input plans execute this transform immediately
                // after its child. For a filter above an aggregate, the child
                // schema is the aggregate output, so compile the predicate here
                // instead of relying on the linear task's post-aggregate VM.
                compiler.compile_filter(predicate, input_schema)?;
                Ok(input_schema.clone())
            }
            LogicalPlan::Project { exprs, schema, .. } => {
                compiler.compile_projection_with_schema(exprs, input_schema, schema)?;
                Ok((**schema).clone())
            }
            LogicalPlan::Aggregate {
                grouping_sets,
                aggs,
                schema,
                ..
            } => {
                let (keys, _) = grouping_layout(grouping_sets)?;
                let mut projected = keys;
                for agg in aggs {
                    let (name, argument) = aggregate_argument(agg)?;
                    agg_op(name)?;
                    projected.push(argument.clone());
                }
                compiler.compile_projection_with_schema(&projected, input_schema, schema)?;
                Ok((**schema).clone())
            }
            LogicalPlan::Distinct { schema, .. } => {
                let projected = input_schema
                    .fields
                    .iter()
                    .cloned()
                    .map(LogicalExpr::Column)
                    .collect::<Vec<_>>();
                compiler.compile_projection_with_schema(&projected, input_schema, schema)?;
                Ok((**schema).clone())
            }
            LogicalPlan::Join { schema, .. }
            | LogicalPlan::Scan { schema, .. }
            | LogicalPlan::ExternalScan { schema, .. } => Ok((**schema).clone()),
            LogicalPlan::Limit { .. } | LogicalPlan::Sort { .. } => Err(VeinError::unsupported(
                "recursive LIMIT and ORDER require dedicated physical input nodes",
                None,
            )),
            LogicalPlan::SubqueryAlias { schema, .. } => {
                if schema.fields.len() != input_schema.fields.len() {
                    return Err(VeinError::planning(
                        "subquery alias changes the number of columns in a recursive join input",
                        None,
                    ));
                }
                let projection = input_schema
                    .fields
                    .iter()
                    .cloned()
                    .map(LogicalExpr::Column)
                    .collect::<Vec<_>>();
                compiler.compile_projection_with_schema(&projection, input_schema, schema)?;
                Ok((**schema).clone())
            }
            other => Err(VeinError::unsupported(
                format!(
                    "unsupported recursive join transform '{}'; add transform lowering for this operator",
                    other.operator_name()
                ),
                None,
            )),
        }
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

    fn collect_leaf_source(
        plan: &LogicalPlan,
        dictionary_group_ids: &mut Vec<plan::schema::ColumnId>,
    ) -> Result<(DataSource, VeinSchema)> {
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
                    return Err(VeinError::unsupported(
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
                    return Err(VeinError::unsupported(
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
            LogicalPlan::Aggregate {
                input,
                grouping_sets,
                ..
            } => {
                let (keys, _) = grouping_layout(grouping_sets)?;
                for key in keys {
                    if let LogicalExpr::Column(column) = key {
                        if !dictionary_group_ids.contains(&column.id) {
                            dictionary_group_ids.push(column.id);
                        }
                    }
                }
                Self::collect_leaf_source(input, dictionary_group_ids)
            }
            LogicalPlan::Filter { input, .. } => {
                // A filter below an aggregate may evaluate the key before the
                // aggregate consumes it. Keep that scan on decoded Arrow arrays.
                dictionary_group_ids.clear();
                Self::collect_leaf_source(input, dictionary_group_ids)
            }
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::SubqueryAlias { input, .. }
            | LogicalPlan::Write { input, .. }
            | LogicalPlan::Distinct { input, .. } => {
                Self::collect_leaf_source(input, dictionary_group_ids)
            }
            other => Err(VeinError::unsupported(
                format!(
                    "unsupported source operator '{}' in Parquet pipeline; provide a supported scan source",
                    other.operator_name()
                ),
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
                dictionary_columns: Vec::new(),
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
            // The recursive physical input builder executes the join and
            // supplies its output batch; this method compiles only operators
            // above that boundary.
            LogicalPlan::Join { schema, .. } => Ok((**schema).clone()),
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
                // Aggregate state is built after this input program runs. A
                // projection above that boundary belongs to the post-aggregate
                // program, not the program that produces grouping keys and
                // aggregate arguments.
                if Self::aggregate(input)?.is_none() {
                    compiler.compile_projection_with_schema(exprs, &input_schema, schema)?;
                }
                Ok((**schema).clone())
            }
            other => Err(VeinError::unsupported(
                format!(
                    "unsupported operator '{}' in Parquet pipeline; add pipeline lowering for this operator",
                    other.operator_name()
                ),
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
        | LogicalPlan::Sort { input, .. }
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
        LogicalPlan::Project {
            input,
            exprs,
            schema,
        } => {
            let input_schema = compile_post_aggregate_node(input, compiler)?;
            compiler.compile_projection_with_schema(exprs, &input_schema, schema)?;
            Ok((**schema).clone())
        }
        other => Err(VeinError::unsupported(
            format!(
                "unsupported operator '{}' above an aggregate; add post-aggregate lowering for this operator",
                other.operator_name()
            ),
            None,
        )),
    }
}

fn compile_sort_plan(sort_keys: &[LogicalSortKey], schema: &VeinSchema) -> Result<SortPlan> {
    let mut computed_index = 0usize;
    let keys = sort_keys
        .iter()
        .map(|key| {
            let position = match &key.expr {
                LogicalExpr::Column(column) => schema
                    .position_of_id(column.id)
                    .or_else(|| {
                        schema
                            .fields
                            .iter()
                            .position(|field| field.name == column.name)
                    })
                    .ok_or_else(|| {
                        VeinError::planning(
                            format!(
                                "ORDER BY column '{}' is absent from its output schema",
                                column.name
                            ),
                            None,
                        )
                    })?,
                _ => {
                    let position = schema.fields.len() + computed_index;
                    computed_index += 1;
                    position
                }
            };
            let column = u32::try_from(position)
                .map_err(|_| VeinError::planning("ORDER BY has too many output columns", None))?;
            Ok(PhysicalSortKey {
                column,
                ascending: key.asc,
                nulls_first: key.nulls_first,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    if keys.is_empty() {
        return Err(VeinError::planning(
            "ORDER requires at least one sort key",
            None,
        ));
    }
    Ok(SortPlan { keys })
}

fn compile_sort_key_program(
    sort_keys: &[LogicalSortKey],
    schema: &VeinSchema,
) -> Result<Option<MicroProgram>> {
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
                return Err(VeinError::unsupported(
                    "LIMIT before ORDER is not supported; move LIMIT after ORDER",
                    None,
                ));
            }
            if contains_sort(input) {
                return Err(VeinError::unsupported(
                    "multiple ORDER operators in one pipeline are not supported yet",
                    None,
                ));
            }
            Ok(Some((sort_keys, schema)))
        }
        other if contains_sort(other) => Err(VeinError::unsupported(
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
fn grouping_layout(
    grouping_sets: &[Vec<LogicalExpr>],
) -> Result<(Vec<LogicalExpr>, Vec<Vec<u32>>)> {
    if grouping_sets.is_empty() {
        return Err(VeinError::planning(
            "an aggregate must contain a grouping set",
            None,
        ));
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
        "SINGLE_VALUE" => AggOpCode::SingleValue,
        other => {
            return Err(VeinError::unsupported(
                format!("aggregate {other} is not implemented; use a supported aggregate function"),
                None,
            ));
        }
    })
}

/// Returns the single input argument for an aggregate expression.
fn aggregate_argument(agg: &LogicalExpr) -> Result<(&str, &LogicalExpr)> {
    let LogicalExpr::FunctionCall { name, args, .. } = agg else {
        return Err(VeinError::planning(
            format!(
                "aggregate expression must be a function call, found expression kind {}",
                agg.kind_name()
            ),
            None,
        ));
    };
    let [argument] = args.as_slice() else {
        return Err(VeinError::unsupported(
            format!("{name} currently requires exactly one argument"),
            None,
        ));
    };
    Ok((name, argument))
}

/// Uses a catalog NDV only when it describes the unfiltered single-key input
/// directly. Estimates for computed, filtered, or multi-key groups are too
/// uncertain to justify a hash-table allocation.
fn estimate_single_key_groups(
    input: &LogicalPlan,
    grouping_sets: &[Vec<LogicalExpr>],
) -> Option<u64> {
    let [grouping_set] = grouping_sets else {
        return None;
    };
    let [LogicalExpr::Column(column)] = grouping_set.as_slice() else {
        return None;
    };
    let LogicalPlan::Scan { table, filters, .. } = input else {
        return None;
    };
    if !filters.is_empty() {
        return None;
    }

    let statistics = table.statistics.as_ref()?;
    let row_count = statistics.num_rows?;
    let distinct_count = statistics
        .column_statistics
        .get(&column.name)
        .and_then(|column_statistics| column_statistics.distinct_count);
    Some(distinct_count.map_or(row_count, |count| count.min(row_count)))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use catalog::{ColumnStatistics, TableMetadata, TableStatistics};
    use common::protocol::{JoinOutput, MicroOp, SortKey as PhysicalSortKey, SortPlan};
    use plan::schema::{ColumnId, ResolvedColumn, VeinDataType, VeinSchema};
    use plan::{LogicalExpr, LogicalLiteral, LogicalPlan, SortKey as LogicalSortKey};

    use crate::compiler::BytecodeCompiler;
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

    fn unsupported_explain_plan() -> LogicalPlan {
        let schema = VeinSchema::empty();
        let scan = LogicalPlan::Scan {
            table: Arc::new(
                TableMetadata::new("private_table_marker", Arc::new(schema.clone()))
                    .with_format(plan::TableFormat::Parquet),
            ),
            projection: None,
            filters: Vec::new(),
            schema: Box::new(schema.clone()),
        };
        LogicalPlan::Explain {
            input: Box::new(scan),
            schema: Box::new(schema),
        }
    }

    fn assert_concise_operator_error(error: &error::VeinError, context: &str) {
        let message = error.to_string();
        assert_eq!(error.category(), error::ErrorCategory::Unsupported);
        assert!(message.contains("Explain"), "{message}");
        assert!(message.contains(context), "{message}");
        assert!(
            !message.contains("private_table_marker"),
            "nested scan metadata leaked into the error: {message}"
        );
        assert!(!message.contains("Scan {"), "full plan was formatted: {message}");
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
    fn compiles_only_the_project_above_an_already_built_join_filter() {
        let visible = Arc::new(ResolvedColumn::new(
            ColumnId(10),
            "s_name",
            None,
            VeinDataType::Utf8,
        ));
        let hidden_filter_column = Arc::new(ResolvedColumn::new(
            ColumnId(11),
            "ps_supplycost",
            None,
            VeinDataType::Int64,
        ));
        let join_schema = Box::new(VeinSchema::new(vec![
            Arc::clone(&visible),
            Arc::clone(&hidden_filter_column),
        ]));
        let leaf = external_scan(&join_schema);
        let join = LogicalPlan::Join {
            join_type: parser::ast::pipe_op::JoinType::Inner,
            left: Box::new(leaf.clone()),
            right: Box::new(leaf),
            on: None,
            using: Vec::new(),
            schema: join_schema,
        };
        let filtered = LogicalPlan::Filter {
            input: Box::new(join),
            predicate: LogicalExpr::BinaryOp {
                left: Box::new(LogicalExpr::Column(hidden_filter_column.clone())),
                op: parser::ast::expr::BinaryOp::Eq,
                right: Box::new(LogicalExpr::Literal(LogicalLiteral::Int64(7))),
                return_type: VeinDataType::Boolean,
            },
        };
        let plan = LogicalPlan::Project {
            input: Box::new(filtered),
            exprs: vec![LogicalExpr::Column(Arc::clone(&visible))],
            schema: Box::new(VeinSchema::new(vec![Arc::clone(&visible)])),
        };
        let built_child_schema = VeinSchema::new(vec![visible, hidden_filter_column]);

        let (program, output_schema) =
            PipelineCompiler::compile_recursive_transform(&plan, &built_child_schema)
                .expect("the project must bind against the already-built filtered join schema");

        assert_eq!(output_schema.fields.len(), 1);
        assert!(
            !program.instructions.iter().any(|instruction| matches!(
                instruction,
                common::protocol::MicroOp::FilterMask { .. }
            )),
            "the child filter must not be compiled a second time"
        );
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
    fn compiles_recursive_sort_after_a_limit_without_reordering_the_nodes() {
        let (schema, column) = schema();
        let limited = LogicalPlan::Limit {
            input: Box::new(external_scan(&schema)),
            offset: 1,
            count: 2,
            schema: Box::new(schema.clone()),
        };
        let sort = LogicalPlan::Sort {
            input: Box::new(limited),
            sort_keys: vec![LogicalSortKey {
                expr: LogicalExpr::Column(column),
                asc: true,
                nulls_first: false,
            }],
            schema: Box::new(schema),
        };

        let (plan, key_program) = PipelineCompiler::recursive_sort(&sort)
            .expect("recursive sort planning")
            .expect("sort node should compile");

        assert_eq!(plan.keys[0].column, 1);
        assert!(plan.keys[0].ascending);
        assert!(key_program.is_none());
        assert!(
            PipelineCompiler::sort(&sort).is_err(),
            "root ORDER keeps its existing sort-after-limit validation"
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
        assert!(
            program
                .instructions
                .iter()
                .any(|instruction| matches!(instruction, common::protocol::MicroOp::Add { .. }))
        );
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

    #[test]
    fn requests_dictionary_encoding_for_direct_utf8_aggregate_keys() {
        let (input_schema, _) = schema();
        let key = input_schema.fields[0].clone();
        let aggregate = LogicalPlan::Aggregate {
            input: Box::new(external_scan(&input_schema)),
            grouping_sets: vec![vec![LogicalExpr::Column(key.clone())]],
            aggs: Vec::new(),
            schema: Box::new(VeinSchema::new(vec![key])),
        };

        let (source, _, _, _) = PipelineCompiler::compile(&aggregate).expect("compile aggregate");
        let common::protocol::DataSource::File(scan) = source else {
            panic!("aggregate scan should use Parquet");
        };

        assert_eq!(scan.dictionary_columns, vec![0]);
    }

    #[test]
    fn does_not_request_dictionary_encoding_for_computed_group_keys() {
        let (input_schema, price) = schema();
        let computed_key = LogicalExpr::BinaryOp {
            left: Box::new(LogicalExpr::Column(price)),
            op: parser::ast::expr::BinaryOp::Plus,
            right: Box::new(LogicalExpr::Literal(LogicalLiteral::Int64(1))),
            return_type: VeinDataType::Int64,
        };
        let output = Arc::new(ResolvedColumn::new(
            ColumnId(3),
            "adjusted_price",
            None,
            VeinDataType::Int64,
        ));
        let aggregate = LogicalPlan::Aggregate {
            input: Box::new(external_scan(&input_schema)),
            grouping_sets: vec![vec![computed_key]],
            aggs: Vec::new(),
            schema: Box::new(VeinSchema::new(vec![output])),
        };

        let (source, _, _, _) = PipelineCompiler::compile(&aggregate).expect("compile aggregate");
        let common::protocol::DataSource::File(scan) = source else {
            panic!("aggregate scan should use Parquet");
        };

        assert!(scan.dictionary_columns.is_empty());
    }

    #[test]
    fn aggregate_uses_single_key_ndv_as_a_capped_capacity_hint() {
        let (input_schema, key) = schema();
        let mut column_statistics = std::collections::HashMap::new();
        column_statistics.insert(
            key.name.clone(),
            ColumnStatistics {
                distinct_count: Some(120),
                ..ColumnStatistics::default()
            },
        );
        let statistics = TableStatistics {
            num_rows: Some(100),
            column_statistics,
            ..TableStatistics::default()
        };
        let table = Arc::new(
            TableMetadata::new("orders", Arc::new(input_schema.clone()))
                .with_statistics(statistics),
        );
        let input = LogicalPlan::Scan {
            table,
            projection: None,
            filters: Vec::new(),
            schema: Box::new(input_schema),
        };
        let aggregate = LogicalPlan::Aggregate {
            input: Box::new(input),
            grouping_sets: vec![vec![LogicalExpr::Column(key)]],
            aggs: Vec::new(),
            schema: Box::new(VeinSchema::empty()),
        };

        let plan = PipelineCompiler::aggregate(&aggregate)
            .expect("plan aggregate")
            .expect("aggregate plan");
        assert_eq!(plan.estimated_groups, Some(100));
    }

    #[test]
    fn aggregate_capacity_hint_requires_unfiltered_input_and_row_count() {
        let (input_schema, key) = schema();
        let mut column_statistics = std::collections::HashMap::new();
        column_statistics.insert(
            key.name.clone(),
            ColumnStatistics {
                distinct_count: Some(80),
                ..ColumnStatistics::default()
            },
        );
        let table = Arc::new(
            TableMetadata::new("orders", Arc::new(input_schema.clone())).with_statistics(
                TableStatistics {
                    num_rows: Some(100),
                    column_statistics,
                    ..TableStatistics::default()
                },
            ),
        );
        let aggregate_for = |filters| LogicalPlan::Aggregate {
            input: Box::new(LogicalPlan::Scan {
                table: Arc::clone(&table),
                projection: None,
                filters,
                schema: Box::new(input_schema.clone()),
            }),
            grouping_sets: vec![vec![LogicalExpr::Column(Arc::clone(&key))]],
            aggs: Vec::new(),
            schema: Box::new(VeinSchema::empty()),
        };

        let filtered = PipelineCompiler::aggregate(&aggregate_for(vec![LogicalExpr::Column(
            Arc::clone(&key),
        )]))
            .expect("plan filtered aggregate")
            .expect("aggregate plan");
        assert_eq!(filtered.estimated_groups, None);

        let mut no_ndv = (*table).clone();
        no_ndv
            .statistics
            .as_mut()
            .unwrap()
            .column_statistics
            .clear();
        let aggregate_without_ndv = LogicalPlan::Aggregate {
            input: Box::new(LogicalPlan::Scan {
                table: Arc::new(no_ndv),
                projection: None,
                filters: Vec::new(),
                schema: Box::new(input_schema.clone()),
            }),
            grouping_sets: vec![vec![LogicalExpr::Column(Arc::clone(&key))]],
            aggs: Vec::new(),
            schema: Box::new(VeinSchema::empty()),
        };
        let row_count_hint = PipelineCompiler::aggregate(&aggregate_without_ndv)
            .expect("plan aggregate without NDV")
            .expect("aggregate plan");
        assert_eq!(row_count_hint.estimated_groups, Some(100));

        let mut no_row_count = (*table).clone();
        no_row_count.statistics.as_mut().unwrap().num_rows = None;
        let no_row_count = Arc::new(no_row_count);
        let input = LogicalPlan::Scan {
            table: no_row_count,
            projection: None,
            filters: Vec::new(),
            schema: Box::new(input_schema),
        };
        let aggregate = LogicalPlan::Aggregate {
            input: Box::new(input),
            grouping_sets: vec![vec![LogicalExpr::Column(key)]],
            aggs: Vec::new(),
            schema: Box::new(VeinSchema::empty()),
        };
        let unestimated = PipelineCompiler::aggregate(&aggregate)
            .expect("plan unestimated aggregate")
            .expect("aggregate plan");
        assert_eq!(unestimated.estimated_groups, None);
    }

    #[test]
    fn zero_column_distinct_plan_does_not_seed_empty_input_group() {
        let schema = VeinSchema::empty();
        let distinct = LogicalPlan::Distinct {
            input: Box::new(external_scan(&schema)),
            schema: Box::new(schema),
        };

        let plan = PipelineCompiler::aggregate(&distinct)
            .expect("plan distinct")
            .expect("distinct uses aggregate state");

        assert_eq!(plan.groups, 0);
        assert!(!plan.emit_empty_group_on_empty_input);
    }

    #[test]
    fn compiles_project_above_aggregate_after_grouping_with_having_order_and_limit() {
        let (input_schema, value) = schema();
        let group = Arc::clone(&input_schema.fields[0]);
        let group_output = Arc::new(ResolvedColumn::new(
            ColumnId(10),
            "group_alias",
            None,
            VeinDataType::Utf8,
        ));
        let count_output = Arc::new(ResolvedColumn::new(
            ColumnId(11),
            "row_count",
            None,
            VeinDataType::Int64,
        ));
        let aggregate_schema = Box::new(VeinSchema::new(vec![
            Arc::clone(&group_output),
            Arc::clone(&count_output),
        ]));
        let aggregate = LogicalPlan::Aggregate {
            input: Box::new(external_scan(&input_schema)),
            grouping_sets: vec![vec![LogicalExpr::Column(group)]],
            aggs: vec![LogicalExpr::FunctionCall {
                name: "count".to_owned(),
                args: vec![LogicalExpr::Column(value)],
                is_aggregate: true,
                distinct: false,
                return_type: VeinDataType::Int64,
            }],
            schema: aggregate_schema,
        };
        let aliased_group = Arc::new(ResolvedColumn::new(
            ColumnId(20),
            "group_name",
            None,
            VeinDataType::Utf8,
        ));
        let adjusted_count = Arc::new(ResolvedColumn::new(
            ColumnId(21),
            "adjusted_count",
            None,
            VeinDataType::Int64,
        ));
        let projection_schema = Box::new(VeinSchema::new(vec![
            Arc::clone(&aliased_group),
            Arc::clone(&adjusted_count),
        ]));
        let projection = LogicalPlan::Project {
            input: Box::new(aggregate),
            exprs: vec![
                LogicalExpr::Column(group_output),
                LogicalExpr::BinaryOp {
                    left: Box::new(LogicalExpr::Column(Arc::clone(&count_output))),
                    op: parser::ast::expr::BinaryOp::Plus,
                    right: Box::new(LogicalExpr::Literal(LogicalLiteral::Int64(1))),
                    return_type: VeinDataType::Int64,
                },
            ],
            schema: projection_schema.clone(),
        };
        let having = LogicalPlan::Filter {
            input: Box::new(projection),
            predicate: LogicalExpr::BinaryOp {
                left: Box::new(LogicalExpr::Column(Arc::clone(&adjusted_count))),
                op: parser::ast::expr::BinaryOp::Gt,
                right: Box::new(LogicalExpr::Literal(LogicalLiteral::Int64(1))),
                return_type: VeinDataType::Boolean,
            },
        };
        let ordered = LogicalPlan::Sort {
            input: Box::new(having),
            sort_keys: vec![LogicalSortKey {
                expr: LogicalExpr::Column(adjusted_count),
                asc: false,
                nulls_first: false,
            }],
            schema: projection_schema.clone(),
        };
        let limited = LogicalPlan::Limit {
            input: Box::new(ordered),
            offset: 0,
            count: 10,
            schema: projection_schema,
        };

        let aggregate_plan = PipelineCompiler::aggregate(&limited)
            .expect("aggregate under post-aggregate operators should plan")
            .expect("aggregate plan");
        assert_eq!(
            aggregate_plan.types,
            vec![VeinDataType::Utf8, VeinDataType::Int64]
        );

        let (_, input_program, _, output_schema) =
            PipelineCompiler::compile(&limited).expect("compile aggregate input program");
        assert_eq!(output_schema.fields[1].name, "adjusted_count");
        assert!(
            !input_program
                .instructions
                .iter()
                .any(|instruction| matches!(instruction, MicroOp::Add { .. })),
            "computed projection must not execute before aggregate groups are built"
        );

        let post_aggregate = PipelineCompiler::post_aggregate_program(&limited)
            .expect("compile post-aggregate operators")
            .expect("project and HAVING require a post-aggregate program");
        assert!(
            post_aggregate
                .instructions
                .iter()
                .any(|instruction| matches!(instruction, MicroOp::Add { .. }))
        );
        assert!(
            post_aggregate
                .instructions
                .iter()
                .any(|instruction| matches!(instruction, MicroOp::FilterMask { .. }))
        );
        assert!(post_aggregate.instructions.iter().any(|instruction| matches!(instruction, MicroOp::ProjectRegisters { out_regs } if out_regs.len() == 2)));
    }

    #[test]
    fn compiles_join_residual_against_only_referenced_build_columns() {
        let probe_key = Arc::new(ResolvedColumn::new(
            ColumnId(1),
            "probe_key",
            None,
            VeinDataType::Int64,
        ));
        let build_key = Arc::new(ResolvedColumn::new(
            ColumnId(2),
            "build_key",
            None,
            VeinDataType::Int64,
        ));
        let build_comment = Arc::new(ResolvedColumn::new(
            ColumnId(3),
            "comment",
            None,
            VeinDataType::Utf8,
        ));
        let probe = VeinSchema::new(vec![Arc::clone(&probe_key)]);
        let build = VeinSchema::new(vec![Arc::clone(&build_key), Arc::clone(&build_comment)]);
        let predicate = LogicalExpr::Like {
            expr: Box::new(LogicalExpr::Column(build_comment)),
            pattern: Box::new(LogicalExpr::Literal(LogicalLiteral::String(
                "%special%requests%".into(),
            ))),
            negated: true,
        };

        let residual = PipelineCompiler::compile_join_residual(&[predicate], &probe, &build, 0)
            .expect("join residual should compile")
            .expect("predicate should produce a residual plan");

        assert_eq!(residual.input_columns, vec![JoinOutput::Build(1)]);
        assert!(
            residual
                .program
                .instructions
                .iter()
                .any(|op| matches!(op, MicroOp::Like { negated: true, .. }))
        );
        assert!(
            residual
                .program
                .instructions
                .iter()
                .any(|op| matches!(op, MicroOp::FilterMask { .. }))
        );
    }

    #[test]
    fn compiles_expression_equality_as_a_join_residual_without_copying_unreferenced_columns() {
        let probe_key = Arc::new(ResolvedColumn::new(
            ColumnId(1),
            "probe_key",
            None,
            VeinDataType::Int64,
        ));
        let probe_value = Arc::new(ResolvedColumn::new(
            ColumnId(2),
            "probe_value",
            None,
            VeinDataType::Int64,
        ));
        let build_key = Arc::new(ResolvedColumn::new(
            ColumnId(3),
            "build_key",
            None,
            VeinDataType::Int64,
        ));
        let build_value = Arc::new(ResolvedColumn::new(
            ColumnId(4),
            "build_value",
            None,
            VeinDataType::Int64,
        ));
        let probe = VeinSchema::new(vec![Arc::clone(&probe_key), Arc::clone(&probe_value)]);
        let build = VeinSchema::new(vec![Arc::clone(&build_key), Arc::clone(&build_value)]);
        let add_one = |column| LogicalExpr::BinaryOp {
            left: Box::new(LogicalExpr::Column(column)),
            op: parser::ast::expr::BinaryOp::Plus,
            right: Box::new(LogicalExpr::Literal(LogicalLiteral::Int64(1))),
            return_type: VeinDataType::Int64,
        };
        let predicate = LogicalExpr::BinaryOp {
            left: Box::new(add_one(probe_value)),
            op: parser::ast::expr::BinaryOp::Eq,
            right: Box::new(add_one(build_value)),
            return_type: VeinDataType::Boolean,
        };

        let residual = PipelineCompiler::compile_join_residual(&[predicate], &probe, &build, 0)
            .expect("expression-valued residual should compile")
            .expect("predicate should produce a residual plan");

        assert_eq!(
            residual.input_columns,
            vec![JoinOutput::Probe(1), JoinOutput::Build(1)]
        );
        assert_eq!(
            residual
                .program
                .instructions
                .iter()
                .filter(|instruction| matches!(instruction, MicroOp::Add { .. }))
                .count(),
            2
        );
        assert!(residual
            .program
            .instructions
            .iter()
            .any(|instruction| matches!(instruction, MicroOp::CmpEq { .. })));
        assert!(residual
            .program
            .instructions
            .iter()
            .any(|instruction| matches!(instruction, MicroOp::FilterMask { .. })));
    }

    #[test]
    fn missing_recursive_filter_column_reports_operator_identity_and_input_schema() {
        let input_column = Arc::new(ResolvedColumn::new(
            ColumnId(1),
            "p_partkey",
            None,
            VeinDataType::Int64,
        ));
        let input_schema = VeinSchema::new(vec![input_column]);
        let missing_column = Arc::new(ResolvedColumn::new(
            ColumnId(39),
            "ps_supplycost",
            None,
            VeinDataType::Int64,
        ));
        let filter = LogicalPlan::Filter {
            input: Box::new(LogicalPlan::Scan {
                table: Arc::new(
                    TableMetadata::new("partsupp", Arc::new(VeinSchema::empty()))
                        .with_format(plan::TableFormat::Parquet),
                ),
                projection: None,
                filters: Vec::new(),
                schema: Box::new(VeinSchema::empty()),
            }),
            predicate: LogicalExpr::Column(missing_column),
        };

        let error = PipelineCompiler::compile_recursive_transform(&filter, &input_schema)
            .expect_err("filter column omitted from recursive input should fail during lowering");
        let message = error.to_string();
        assert_eq!(error.category(), error::ErrorCategory::Planning);
        assert!(message.contains("recursive filter"), "{message}");
        assert!(message.contains("ps_supplycost#39"), "{message}");
        assert!(message.contains("input schema [p_partkey#1]"), "{message}");
    }

    #[test]
    fn unsupported_source_error_names_operator_without_dumping_nested_scan() {
        let error = PipelineCompiler::collect_leaf_source(
            &unsupported_explain_plan(),
            &mut Vec::new(),
        )
            .expect_err("EXPLAIN cannot be used as a Parquet source");

        assert_concise_operator_error(&error, "source operator");
        assert!(
            error.to_string().contains("supported scan source"),
            "diagnostic should give a next action: {error}"
        );
    }

    #[test]
    fn unsupported_recursive_transform_error_names_operator_concisely() {
        let error = PipelineCompiler::compile_recursive_transform(
            &unsupported_explain_plan(),
            &VeinSchema::empty(),
        )
            .expect_err("EXPLAIN has no recursive join transform");

        assert_concise_operator_error(&error, "recursive join transform");
    }

    #[test]
    fn unsupported_pipeline_operator_error_names_operator_concisely() {
        let mut compiler = BytecodeCompiler::new(0);
        let error = PipelineCompiler::compile_node(
            &unsupported_explain_plan(),
            VeinSchema::empty(),
            &mut compiler,
        )
            .expect_err("EXPLAIN has no Parquet pipeline lowering");

        assert_concise_operator_error(&error, "pipeline");
    }

    #[test]
    fn unsupported_post_aggregate_operator_error_names_operator_concisely() {
        let mut compiler = BytecodeCompiler::new(0);
        let error = super::compile_post_aggregate_node(&unsupported_explain_plan(), &mut compiler)
            .expect_err("EXPLAIN has no post-aggregate lowering");

        assert_concise_operator_error(&error, "post-aggregate");
    }
}
