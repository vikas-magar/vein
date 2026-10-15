use std::sync::Arc;

use catalog::{
    ColumnStatistics, StatisticsComparison, StatisticsPredicate, TableMetadata,
    TableStatistics,
};
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

#[test]
fn statistics_lowering_preserves_column_to_column_comparisons() {
    let left = Arc::new(ResolvedColumn::new(
        ColumnId(10),
        "l_shipdate",
        None,
        VeinDataType::Date32,
    ));
    let right = Arc::new(ResolvedColumn::new(
        ColumnId(11),
        "l_commitdate",
        None,
        VeinDataType::Date32,
    ));
    let expression = LogicalExpr::BinaryOp {
        left: Box::new(LogicalExpr::Column(left)),
        op: parser::ast::expr::BinaryOp::Lt,
        right: Box::new(LogicalExpr::Column(right)),
        return_type: VeinDataType::Boolean,
    };

    assert_eq!(
        PipelineCompiler::statistics_predicate(&expression),
        StatisticsPredicate::ColumnComparison {
            left_column: "l_shipdate".into(),
            comparison: StatisticsComparison::Less,
            right_column: "l_commitdate".into(),
        }
    );
}

#[test]
fn compiled_scan_predicate_reports_filter_count_and_narrow_input_columns() {
    let (schema, price) = schema();
    let predicate = || LogicalExpr::BinaryOp {
        left: Box::new(LogicalExpr::Column(Arc::clone(&price))),
        op: parser::ast::expr::BinaryOp::Gt,
        right: Box::new(LogicalExpr::Literal(LogicalLiteral::Int64(0))),
        return_type: VeinDataType::Boolean,
    };

    let compiled = PipelineCompiler::scan_predicate(&[predicate(), predicate()], &schema)
        .expect("compile scan predicate")
        .expect("two scan filters produce a predicate");

    assert_eq!(compiled.predicate_count, 2);
    assert_eq!(compiled.stages.len(), 1);
    assert_eq!(compiled.stages[0].predicate_count, 2);
    assert_eq!(compiled.stages[0].input.fields.len(), 1);
    assert_eq!(compiled.stages[0].input.fields[0].name, "price");
    assert!(!compiled.stages[0].program.instructions.is_empty());
}

#[test]
fn scan_predicate_stages_literal_filters_before_column_correlations() {
    let left = Arc::new(ResolvedColumn::new(
        ColumnId(100),
        "left_key",
        None,
        VeinDataType::Int64,
    ));
    let right = Arc::new(ResolvedColumn::new(
        ColumnId(101),
        "right_key",
        None,
        VeinDataType::Int64,
    ));
    let amount = Arc::new(ResolvedColumn::new(
        ColumnId(102),
        "amount",
        None,
        VeinDataType::Int64,
    ));
    let label = Arc::new(ResolvedColumn::new(
        ColumnId(103),
        "label",
        None,
        VeinDataType::Utf8,
    ));
    let schema = VeinSchema::new(vec![
        Arc::clone(&left),
        Arc::clone(&right),
        Arc::clone(&amount),
        Arc::clone(&label),
    ]);
    let correlated = LogicalExpr::BinaryOp {
        left: Box::new(LogicalExpr::Column(Arc::clone(&left))),
        op: parser::ast::expr::BinaryOp::Lt,
        right: Box::new(LogicalExpr::Column(Arc::clone(&right))),
        return_type: VeinDataType::Boolean,
    };
    let literal = LogicalExpr::BinaryOp {
        left: Box::new(LogicalExpr::Column(Arc::clone(&amount))),
        op: parser::ast::expr::BinaryOp::Gt,
        right: Box::new(LogicalExpr::Literal(LogicalLiteral::Int64(0))),
        return_type: VeinDataType::Boolean,
    };

    let compiled = PipelineCompiler::scan_predicate(&[correlated.clone(), literal], &schema)
        .expect("compile staged scan predicates")
        .expect("scan predicates produce a plan");

    assert_eq!(compiled.predicate_count, 2);
    assert_eq!(compiled.stages.len(), 2);
    assert_eq!(compiled.stages[0].input.fields.len(), 1);
    assert_eq!(compiled.stages[0].input.fields[0].name, "amount");
    assert_eq!(compiled.stages[1].input.fields.len(), 2);
    assert_eq!(compiled.stages[1].input.fields[0].name, "left_key");
    assert_eq!(compiled.stages[1].input.fields[1].name, "right_key");

    let unknown = LogicalExpr::Like {
        expr: Box::new(LogicalExpr::Column(label)),
        pattern: Box::new(LogicalExpr::Literal(LogicalLiteral::String("%urgent%".into()))),
        negated: false,
    };
    let conservative = PipelineCompiler::scan_predicate(&[correlated, unknown], &schema)
        .expect("compile conservative predicate order")
        .expect("scan predicates produce a plan");
    assert_eq!(conservative.stages.len(), 1);
    assert_eq!(conservative.stages[0].predicate_count, 2);
}

#[test]
fn scan_statistics_preserve_safe_ranges_and_fail_open_for_like() {
    let date = Arc::new(ResolvedColumn::new(
        ColumnId(3),
        "shipdate",
        None,
        VeinDataType::Date32,
    ));
    let name = Arc::new(ResolvedColumn::new(
        ColumnId(4),
        "name",
        None,
        VeinDataType::Utf8,
    ));
    let schema = VeinSchema::new(vec![date.clone(), name.clone()]);
    let lower = LogicalExpr::BinaryOp {
        left: Box::new(LogicalExpr::Column(Arc::clone(&date))),
        op: parser::ast::expr::BinaryOp::Geq,
        right: Box::new(LogicalExpr::Literal(LogicalLiteral::Date(100))),
        return_type: VeinDataType::Boolean,
    };
    let upper = LogicalExpr::BinaryOp {
        left: Box::new(LogicalExpr::Column(Arc::clone(&date))),
        op: parser::ast::expr::BinaryOp::Lt,
        right: Box::new(LogicalExpr::Literal(LogicalLiteral::Date(200))),
        return_type: VeinDataType::Boolean,
    };
    let like = LogicalExpr::Like {
        expr: Box::new(LogicalExpr::Column(name)),
        pattern: Box::new(LogicalExpr::Literal(LogicalLiteral::String("%urgent%".into()))),
        negated: false,
    };
    let range = LogicalExpr::BinaryOp {
        left: Box::new(lower),
        op: parser::ast::expr::BinaryOp::And,
        right: Box::new(upper),
        return_type: VeinDataType::Boolean,
    };

    let compiled = PipelineCompiler::scan_predicate(&[range, like], &schema)
        .expect("compile row predicate")
        .expect("scan filters produce a predicate");

    assert_eq!(
        compiled.statistics,
        vec![
            StatisticsPredicate::Comparison {
                column: "shipdate".into(),
                comparison: StatisticsComparison::GreaterOrEqual,
                literal: LogicalLiteral::Date(100),
            },
            StatisticsPredicate::Comparison {
                column: "shipdate".into(),
                comparison: StatisticsComparison::Less,
                literal: LogicalLiteral::Date(200),
            },
            StatisticsPredicate::Unknown,
        ]
    );
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