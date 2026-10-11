use super::*;

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
fn grouping_sets_sum_cardinalities_and_unknown_keys_stay_unknown() {
    let plan = scan(1, Some(100), Some(10), Some(0));
    let key = LogicalExpr::Column(plan.schema().fields[0].clone());
    assert_eq!(rows(&aggregate(plan, vec![vec![key], vec![]])), Some(11.0));
    let plan = scan(1, Some(100), None, Some(0));
    let key = LogicalExpr::Column(plan.schema().fields[0].clone());
    assert_eq!(rows(&aggregate(plan, vec![vec![key]])), None);
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
