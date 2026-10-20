//! Compiles Parquet-backed linear query plans into the vector program.
use std::collections::HashSet;
use std::sync::Arc;

use catalog::{StatisticsComparison, StatisticsPredicate};
use common::Result;
use common::identifiers::TaskId;
use common::protocol::{
    AggOpCode, AggregatePlan, DataSource, FileFormat, FileScanTask, JoinOutput, JoinResidualPlan,
    LimitSpec, MicroProgram, RegId, ScanPredicate, ScanPredicateStage, SortKey as PhysicalSortKey,
    SortPlan,
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
                        let (name, argument, distinct) = aggregate_argument(agg)?;
                        Ok((agg_op(name, distinct)?, argument.data_type()))
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
        if let DataSource::File(scan) = &mut source
            && scan.pushdown.is_none() {
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
            output_schema = Self::compile_recursive_transform_node(
                plan,
                &output_schema,
                &mut compiler,
            )
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
                    let (name, argument, distinct) = aggregate_argument(agg)?;
                    agg_op(name, distinct)?;
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
            LogicalPlan::Intermediate { source_id, schema, .. } => Ok((
                DataSource::Intermediate(common::identifiers::IntermediateSourceId::from_raw(*source_id)),
                (**schema).clone(),
            )),
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
                let statistics =
                    optimizer::estimation::ScanStatistics::new(schema, table.statistics.as_ref());
                let pushdown = Self::scan_predicate_with_estimator(filters, schema, |predicate| {
                    use optimizer::estimation::{SelectivityEstimator, StatisticsSelectivity};
                    let estimate = StatisticsSelectivity
                        .estimate(predicate, &statistics)
                        .true_fraction
                        .value();
                    tracing::debug!(
                        target: "vein.optimizer",
                        event = "scan_filter_selectivity_estimate",
                        table = %table.name,
                        predicate_kind = predicate.kind_name(),
                        estimated_pass_fraction = ?estimate,
                        estimate_used_for_ordering = estimate.is_some(),
                        table_rows = ?table.statistics.as_ref().and_then(|statistics| statistics.num_rows),
                        statistics_column_count = table.statistics
                            .as_ref()
                            .map_or(0, |statistics| statistics.column_statistics.len()),
                        source_snapshot_pinned = table.catalog_snapshot.is_some(),
                        "scan filter selectivity estimated"
                    );
                    estimate
                })?;
                // Full row coverage does not require reading every table
                // column. The synopsis records only columns it observed, while
                // the pinned row count validates complete source-row coverage.
                let row_coverage_candidate = pushdown.is_none();
                let runtime_statistics = table.catalog_snapshot.as_ref().map(|snapshot| {
                    common::protocol::RuntimeStatisticsScan {
                        scan_id: TaskId::new().into_raw(),
                        table_key: common::statistics::runtime::table_statistics_key(snapshot),
                        expected_rows: table
                            .statistics
                            .as_ref()
                            .and_then(|statistics| statistics.num_rows),
                        row_coverage_candidate,
                        partition_index: 0,
                        partition_count: 1,
                    }
                });
                Self::file_source(files, scan_schema, pushdown, runtime_statistics)
            }
            LogicalPlan::ExternalScan {
                url,
                options,
                files,
                schema,
                ..
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
                Self::file_source(paths, (**schema).clone(), None, None)
            }
            LogicalPlan::Aggregate {
                input,
                grouping_sets,
                ..
            } => {
                let (keys, _) = grouping_layout(grouping_sets)?;
                for key in keys {
                    if let LogicalExpr::Column(column) = key
                        && !dictionary_group_ids.contains(&column.id) {
                            dictionary_group_ids.push(column.id);
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
        runtime_statistics: Option<common::protocol::RuntimeStatisticsScan>,
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
                scan_unit_range: None,
                projected,
                dictionary_columns: Vec::new(),
                pushdown,
                task_id: TaskId::new(),
                catalog_snapshot: None,
                runtime_statistics,
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
    #[cfg(test)]
    fn scan_predicate(
        filters: &[LogicalExpr],
        schema: &VeinSchema,
    ) -> Result<Option<Arc<ScanPredicate>>> {
        Self::scan_predicate_with_estimator(filters, schema, |_| None)
    }

    fn scan_predicate_with_estimator(
        filters: &[LogicalExpr],
        schema: &VeinSchema,
        estimate: impl Fn(&LogicalExpr) -> Option<f64>,
    ) -> Result<Option<Arc<ScanPredicate>>> {
        if filters.is_empty() {
            return Ok(None);
        }
        fn flatten_conjuncts(expression: &LogicalExpr, output: &mut Vec<LogicalExpr>) {
            match expression {
                LogicalExpr::BinaryOp {
                    left,
                    op: parser::ast::expr::BinaryOp::And,
                    right,
                    ..
                } => {
                    flatten_conjuncts(left, output);
                    flatten_conjuncts(right, output);
                }
                _ => output.push(expression.clone()),
            }
        }

        let mut conjuncts = Vec::new();
        for filter in filters {
            flatten_conjuncts(filter, &mut conjuncts);
        }

        let stage_filters = super::scan_stages::group_predicates(&conjuncts, estimate);

        let mut stages = Vec::with_capacity(stage_filters.len());
        for (stage_index, predicates) in stage_filters.into_iter().enumerate() {
            let mut required = HashSet::new();
            for predicate in &predicates {
                Self::collect_column_ids(predicate, &mut required);
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
            let result = compiler.compile_predicate(&predicates, &input)?;
            let program = Arc::new(compiler.finish());
            let reorder_safe = predicates.iter().all(super::scan_stages::reorder_safe);
            let input_columns = input
                .fields
                .iter()
                .map(|field| field.name.as_str())
                .collect::<Vec<_>>();
            tracing::debug!(
                target: "vein.optimizer",
                event = "scan_predicate_stage_compiled",
                stage_index,
                predicate_count = predicates.len(),
                input_columns = ?input_columns,
                input_column_count = input.fields.len(),
                register_count,
                instruction_count = program.instructions.len(),
                reorder_safe,
                "scan_predicate_stage_compiled"
            );
            stages.push(ScanPredicateStage {
                predicate_count: predicates.len(),
                reorder_safe,
                input: Arc::new(input),
                program,
                result,
            });
        }

        tracing::debug!(
            target: "vein.optimizer",
            event = "scan_predicate_compiled",
            predicate_count = conjuncts.len(),
            stage_count = stages.len(),
            stage_predicate_counts = ?stages.iter().map(|stage| stage.predicate_count).collect::<Vec<_>>(),
            "scan_predicate_compiled"
        );
        Ok(Some(Arc::new(ScanPredicate {
            predicate_count: conjuncts.len(),
            stages,
            statistics: conjuncts.iter().map(Self::statistics_predicate).collect(),
        })))
    }

    pub(super) fn scan_predicate_correlation_class(
        predicate: &StatisticsPredicate,
    ) -> Option<bool> {
        match predicate {
            StatisticsPredicate::Unknown => None,
            StatisticsPredicate::ColumnComparison { .. } => Some(true),
            StatisticsPredicate::And(children) | StatisticsPredicate::Or(children) => {
                let mut contains_column_comparison = false;
                for child in children {
                    contains_column_comparison |= Self::scan_predicate_correlation_class(child)?;
                }
                Some(contains_column_comparison)
            }
            _ => Some(false),
        }
    }

    /// Lowers the subset of a logical predicate whose truth can be bounded by
    /// exact per-column statistics. Unknown expressions are retained as an
    /// explicit fail-open node, so combining them with safe conjuncts remains
    /// correct while still allowing independent bounds to prune.
    pub(super) fn statistics_predicate(expression: &LogicalExpr) -> StatisticsPredicate {
        use StatisticsPredicate as Predicate;

        match expression {
            LogicalExpr::BinaryOp {
                left, op, right, ..
            } => {
                use parser::ast::expr::BinaryOp as Op;
                match op {
                    Op::And => Predicate::And(vec![
                        Self::statistics_predicate(left),
                        Self::statistics_predicate(right),
                    ]),
                    Op::Or => Predicate::Or(vec![
                        Self::statistics_predicate(left),
                        Self::statistics_predicate(right),
                    ]),
                    Op::Eq | Op::Neq | Op::Lt | Op::Leq | Op::Gt | Op::Geq => {
                        match (left.as_ref(), right.as_ref()) {
                            (LogicalExpr::Column(column), LogicalExpr::Literal(literal)) => {
                                Predicate::Comparison {
                                    column: column.name.clone(),
                                    comparison: statistics_comparison(*op),
                                    literal: literal.clone(),
                                }
                            }
                            (LogicalExpr::Literal(literal), LogicalExpr::Column(column)) => {
                                Predicate::Comparison {
                                    column: column.name.clone(),
                                    comparison: statistics_comparison(reverse_comparison(*op)),
                                    literal: literal.clone(),
                                }
                            }
                            (LogicalExpr::Column(left), LogicalExpr::Column(right)) => {
                                Predicate::ColumnComparison {
                                    left_column: left.name.clone(),
                                    comparison: statistics_comparison(*op),
                                    right_column: right.name.clone(),
                                }
                            }
                            _ => Predicate::Unknown,
                        }
                    }
                    _ => Predicate::Unknown,
                }
            }
            LogicalExpr::IsNull { expr } => match expr.as_ref() {
                LogicalExpr::Column(column) => Predicate::IsNull {
                    column: column.name.clone(),
                },
                _ => Predicate::Unknown,
            },
            LogicalExpr::IsNotNull { expr } => match expr.as_ref() {
                LogicalExpr::Column(column) => Predicate::IsNotNull {
                    column: column.name.clone(),
                },
                _ => Predicate::Unknown,
            },
            LogicalExpr::InList {
                expr,
                list,
                negated: false,
            } => {
                let LogicalExpr::Column(column) = expr.as_ref() else {
                    return Predicate::Unknown;
                };
                if list.is_empty() {
                    return Predicate::Unknown;
                }
                let equalities = list
                    .iter()
                    .map(|value| {
                        let LogicalExpr::Literal(literal) = value else {
                            return None;
                        };
                        Some(Predicate::Comparison {
                            column: column.name.clone(),
                            comparison: StatisticsComparison::Equal,
                            literal: literal.clone(),
                        })
                    })
                    .collect::<Option<Vec<_>>>();
                equalities.map_or(Predicate::Unknown, Predicate::Or)
            }
            _ => Predicate::Unknown,
        }
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

    pub(super) fn collect_column_ids(
        expression: &LogicalExpr,
        output: &mut HashSet<plan::schema::ColumnId>,
    ) {
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
            LogicalPlan::Intermediate { .. } => Ok(input_schema),
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
                    let (name, argument, distinct) = aggregate_argument(agg)?;
                    agg_op(name, distinct)?;
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

fn statistics_comparison(operator: parser::ast::expr::BinaryOp) -> StatisticsComparison {
    use parser::ast::expr::BinaryOp as Op;
    match operator {
        Op::Eq => StatisticsComparison::Equal,
        Op::Neq => StatisticsComparison::NotEqual,
        Op::Lt => StatisticsComparison::Less,
        Op::Leq => StatisticsComparison::LessOrEqual,
        Op::Gt => StatisticsComparison::Greater,
        Op::Geq => StatisticsComparison::GreaterOrEqual,
        _ => unreachable!("only ordered comparison operators are lowered"),
    }
}

fn reverse_comparison(operator: parser::ast::expr::BinaryOp) -> parser::ast::expr::BinaryOp {
    use parser::ast::expr::BinaryOp as Op;
    match operator {
        Op::Lt => Op::Gt,
        Op::Leq => Op::Geq,
        Op::Gt => Op::Lt,
        Op::Geq => Op::Leq,
        other => other,
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
fn agg_op(name: &str, distinct: bool) -> Result<AggOpCode> {
    if distinct {
        if name.eq_ignore_ascii_case("COUNT") {
            return Ok(AggOpCode::CountDistinct);
        }
        return Err(VeinError::unsupported(
            format!("DISTINCT is currently supported only for COUNT, not {name}"),
            None,
        ));
    }
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
fn aggregate_argument(agg: &LogicalExpr) -> Result<(&str, &LogicalExpr, bool)> {
    let LogicalExpr::FunctionCall { name, args, distinct, .. } = agg else {
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
    Ok((name, argument, *distinct))
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
mod tests;
