use super::*;
use plan::ColumnGroupStatistics;

#[test]
fn grouping_ndv_is_bounded_by_rows_and_includes_a_null_group() {
    let plan = scan(1, Some(100), Some(10), Some(20));
    let key = LogicalExpr::Column(plan.schema().fields[0].clone());
    assert_eq!(rows(&aggregate(plan.clone(), vec![vec![key.clone()]])), Some(11.0));
    assert_eq!(rows(&aggregate(plan, vec![vec![key.clone(), key]])), Some(11.0));
    let plan = scan(1, Some(0), Some(0), Some(0));
    let key = LogicalExpr::Column(plan.schema().fields[0].clone());
    assert_eq!(rows(&aggregate(plan.clone(), vec![vec![key]])), Some(0.0));
    assert_eq!(rows(&aggregate(plan, vec![vec![]])), Some(1.0));
}

#[test]
fn grouping_sets_sum_cardinalities_and_missing_ndv_uses_a_model_upper_bound() {
    let plan = scan(1, Some(100), Some(10), Some(0));
    let key = LogicalExpr::Column(plan.schema().fields[0].clone());
    assert_eq!(rows(&aggregate(plan, vec![vec![key], vec![]])), Some(11.0));
    let plan = scan(1, Some(100), None, Some(0));
    let key = LogicalExpr::Column(plan.schema().fields[0].clone());
    let estimate = root(&aggregate(plan, vec![vec![key]])).rows;
    assert_eq!(estimate.value(), Some(100.0));
    assert_eq!(estimate.source, EstimateSource::Aggregate);
    assert_eq!(estimate.confidence, EstimateConfidence::Model);
}

#[test]
fn scalar_aggregate_has_one_row_even_with_unknown_input() {
    assert_eq!(rows(&aggregate(scan(1, None, None, None), vec![vec![]])), Some(1.0));
}

#[test]
fn projected_and_aliased_column_ids_retain_lineage() {
    let plan = scan(7, Some(100), Some(10), Some(0));
    let expr = LogicalExpr::Column(plan.schema().fields[0].clone());
    let projected = LogicalPlan::Project {
        input: Box::new(plan),
        exprs: vec![expr],
        schema: Box::new(VeinSchema { fields: vec![column(90, "renamed", false)] }),
    };
    let aliased = LogicalPlan::SubqueryAlias {
        input: Box::new(projected),
        alias: "arbitrary".into(),
        schema: Box::new(VeinSchema { fields: vec![column(600, "different", false)] }),
    };
    let c = aliased.schema().fields[0].clone();
    assert_eq!(rows(&filter(aliased, predicate(&c, BinaryOp::Eq, LogicalLiteral::Int64(2)))), Some(10.0));
}

#[test]
fn equality_filter_updates_ndv_for_downstream_grouping() {
    let plan = scan(1, Some(100), Some(10), Some(0));
    let c = plan.schema().fields[0].clone();
    let plan = filter(plan, predicate(&c, BinaryOp::Eq, LogicalLiteral::Int64(2)));
    assert_eq!(rows(&aggregate(plan, vec![vec![LogicalExpr::Column(c)]])), Some(1.0));
}

#[test]
fn null_filter_retains_the_single_null_group() {
    let plan = scan(1, Some(100), Some(10), Some(20));
    let c = plan.schema().fields[0].clone();
    let plan = filter(plan, LogicalExpr::IsNull { expr: Box::new(LogicalExpr::Column(c.clone())) });
    assert_eq!(rows(&aggregate(plan, vec![vec![LogicalExpr::Column(c)]])), Some(1.0));
}

#[test]
fn limit_offset_distinct_sample_and_union_all_use_shared_estimates() {
    let plan = scan(1, Some(100), Some(10), Some(0));
    let schema = Box::new(plan.schema().clone());
    assert_eq!(rows(&LogicalPlan::Distinct { input: Box::new(plan.clone()), schema: schema.clone() }), Some(10.0));
    assert_eq!(rows(&LogicalPlan::Limit {
        input: Box::new(plan.clone()),
        offset: 95,
        count: 10,
        schema: schema.clone()
    }), Some(5.0));
    assert_eq!(rows(&LogicalPlan::Sample {
        input: Box::new(plan.clone()),
        method: "bernoulli".into(),
        percent: 25.0,
        schema: schema.clone()
    }), Some(25.0));
    assert_eq!(rows(&LogicalPlan::SetOp {
        left: Box::new(plan.clone()),
        right: Box::new(plan),
        kind: parser::ast::query::SetOpKind::Union,
        all: true,
        schema
    }), Some(200.0));
}

#[test]
fn unsupported_set_semantics_and_invalid_sampling_are_unknown() {
    let plan = scan(1, Some(100), Some(10), Some(0));
    let schema = Box::new(plan.schema().clone());
    for percent in [-1.0, f64::NAN, 101.0] {
        assert_eq!(rows(&LogicalPlan::Sample {
            input: Box::new(plan.clone()),
            method: "bernoulli".into(),
            percent,
            schema: schema.clone()
        }), None);
    }
    assert_eq!(rows(&LogicalPlan::SetOp {
        left: Box::new(plan.clone()),
        right: Box::new(plan),
        kind: parser::ast::query::SetOpKind::Union,
        all: false,
        schema
    }), None);
}

#[test]
fn unknown_input_with_zero_limit_has_zero_rows() {
    let plan = scan(1, None, None, None);
    let schema = Box::new(plan.schema().clone());
    assert_eq!(rows(&LogicalPlan::Limit { input: Box::new(plan), count: 0, offset: 0, schema }), Some(0.0));
}

fn scan_with_joint_statistics() -> LogicalPlan {
    let schema = VeinSchema {
        fields: vec![column(1, "left_key", false), column(2, "right_key", false)],
    };
    let statistics = TableStatistics {
        num_rows: Some(100),
        column_statistics: [
            ("left_key".into(), ColumnStatistics {
                distinct_count: Some(10),
                null_count: Some(0),
                nan_count: Some(0),
                ..Default::default()
            }),
            ("right_key".into(), ColumnStatistics {
                distinct_count: Some(10),
                null_count: Some(0),
                nan_count: Some(0),
                ..Default::default()
            }),
        ].into(),
        column_groups: vec![ColumnGroupStatistics {
            columns: vec!["left_key".into(), "right_key".into()],
            distinct_count: Some(15),
            distinct_count_relative_error: Some(0.02),
            distinct_sketch: None,
        }],
        ..Default::default()
    };
    let table = Arc::new(TableMetadata::new("paired", Arc::new(schema.clone()))
        .with_statistics(statistics));
    LogicalPlan::Scan {
        table,
        projection: None,
        filters: vec![],
        schema: Box::new(schema),
    }
}

#[test]
fn joint_ndv_is_used_for_grouping_and_survives_projection_lineage() {
    let source = scan_with_joint_statistics();
    let keys = source.schema().fields.iter().rev()
        .cloned().map(LogicalExpr::Column).collect::<Vec<_>>();
    assert_eq!(rows(&aggregate(source.clone(), vec![keys])), Some(15.0));

    let projected_schema = Box::new(VeinSchema {
        fields: vec![column(50, "first", false), column(51, "second", false)],
    });
    // New output identities verify that the optimizer maps group synopses
    // through expression lineage rather than relying on table column names.
    let projected = LogicalPlan::Project {
        exprs: source.schema().fields.iter().cloned().map(LogicalExpr::Column).collect(),
        input: Box::new(source),
        schema: projected_schema.clone(),
    };
    let remapped_keys = projected_schema.fields.iter().rev()
        .cloned().map(LogicalExpr::Column).collect();
    assert_eq!(rows(&aggregate(projected, vec![remapped_keys])), Some(15.0));
}
