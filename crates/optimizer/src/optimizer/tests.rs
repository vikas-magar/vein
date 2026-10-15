use super::*;
use plan::{LogicalExpr, LogicalLiteral, VeinDataType};

fn lit(value: i64) -> LogicalExpr {
    LogicalExpr::Literal(LogicalLiteral::Int64(value))
}

fn project(exprs: Vec<LogicalExpr>) -> LogicalPlan {
    let schema = Box::new(plan::VeinSchema { fields: vec![] });
    let table = std::sync::Arc::new(plan::TableMetadata::new(
        "t",
        std::sync::Arc::new(*schema.clone()),
    ));
    LogicalPlan::Project {
        input: Box::new(LogicalPlan::Scan {
            table,
            projection: None,
            filters: vec![],
            schema: schema.clone(),
        }),
        exprs,
        schema,
    }
}

fn plus(left: LogicalExpr, right: LogicalExpr) -> LogicalExpr {
    LogicalExpr::BinaryOp {
        left: Box::new(left),
        op: parser::ast::expr::BinaryOp::Plus,
        right: Box::new(right),
        return_type: VeinDataType::Int64,
    }
}

#[test]
fn default_pipeline_folds_constants() {
    let pipeline = OptimizerPipeline::default();
    let plan = project(vec![plus(plus(lit(10), lit(5)), lit(2))]);

    let optimized = pipeline.optimize(plan).unwrap();
    let LogicalPlan::Project { exprs, .. } = optimized else {
        panic!("expected a Project node");
    };
    assert_eq!(exprs, vec![lit(17)]);
}

#[test]
fn already_optimized_plan_converges_unchanged() {
    let pipeline = OptimizerPipeline::default();
    let plan = project(vec![lit(17)]);

    let optimized = pipeline.optimize(plan).unwrap();
    let LogicalPlan::Project { exprs, .. } = optimized else {
        panic!("expected a Project node");
    };
    assert_eq!(exprs, vec![lit(17)]);
}

#[test]
fn filter_visibility_counts_scan_filters_and_residual_filters() {
    let LogicalPlan::Project { input, .. } = project(vec![]) else {
        unreachable!();
    };
    let LogicalPlan::Scan { table, schema, .. } = *input else {
        unreachable!();
    };
    let plan = LogicalPlan::Filter {
        input: Box::new(LogicalPlan::Scan {
            table,
            projection: None,
            filters: vec![lit(1)],
            schema,
        }),
        predicate: lit(2),
    };

    assert_eq!(
        FilterPlacementVisibility::from_plan(&plan),
        FilterPlacementVisibility {
            scan_count: 1,
            scan_filter_count: 1,
            residual_filter_count: 1,
        }
    );
}

#[test]
fn custom_pipeline_appends_rules_in_order() {
    let pipeline = OptimizerPipeline::new(vec![]).with_rule(Arc::new(ConstantFoldingRule));
    let plan = project(vec![plus(plus(lit(1), lit(2)), lit(3))]);

    let optimized = pipeline.optimize(plan).unwrap();
    let LogicalPlan::Project { exprs, .. } = optimized else {
        panic!("expected a Project node");
    };
    assert_eq!(exprs, vec![lit(6)]);
}

#[test]
fn optimizer_reports_owner_for_unlowered_scan_filter_subquery() {
    let base = project(vec![lit(1)]);
    let LogicalPlan::Project { input, .. } = base else {
        unreachable!();
    };
    let LogicalPlan::Scan { table, schema, .. } = *input else {
        unreachable!();
    };
    let plan = LogicalPlan::Scan {
        table,
        projection: None,
        filters: vec![LogicalExpr::ScalarSubquery(Box::new(LogicalPlan::Scan {
            table: std::sync::Arc::new(plan::TableMetadata::new(
                "inner",
                std::sync::Arc::new(*schema.clone()),
            )),
            projection: None,
            filters: vec![],
            schema: schema.clone(),
        }))],
        schema,
    };

    let error = OptimizerPipeline::default()
        .optimize(plan)
        .expect_err("unsupported scan-filter subquery should fail in planning");
    assert!(error.to_string().contains("Scan.filters"), "{error}");
    assert!(error.to_string().contains("not lowered by the optimizer"), "{error}");
}
