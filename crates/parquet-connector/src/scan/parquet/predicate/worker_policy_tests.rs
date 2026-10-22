use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use common::protocol::{
    BinaryExprOp, ExprKind, PhysicalExpr, ScanPredicate, ScanPredicateStage,
};
use parquet::schema::types::{SchemaDescriptor, Type as ParquetType};
use types::{ColumnId, LogicalLiteral, ResolvedColumn, VeinDataType, VeinSchema};

use crate::scan::parquet::metrics::ScanPredicateMetrics;
use exec::expression::eval::{ExpressionKernelProvider, ExpressionKernelRegistry};
use exec::runtime::filter::RuntimeFilterSet;
use functions::FunctionRegistry;

use super::super::adaptive_order::AdaptiveFilterOrder;
use super::{ScanPredicateContext, ScanPredicatePipeline, build_scan_predicates};

fn setup() -> (Schema, SchemaDescriptor, VeinSchema, ScanPredicate, Arc<FunctionRegistry>) {
    let schema = Schema::new(vec![Field::new("key", DataType::Int64, true)]);
    let descriptor = SchemaDescriptor::new(Arc::new(
        ParquetType::group_type_builder("schema")
            .with_fields(vec![Arc::new(
                ParquetType::primitive_type_builder("key", parquet::basic::Type::INT64)
                    .build()
                    .expect("field type"),
            )])
            .build()
            .expect("schema type"),
    ));
    let key = Arc::new(ResolvedColumn::new(
        ColumnId(0),
        "key",
        None,
        VeinDataType::Int64,
    ));
    let stage = ScanPredicateStage {
        predicate_count: 1,
        reorder_safe: true,
        input: Arc::new(VeinSchema::new(vec![Arc::clone(&key)])),
        predicate: PhysicalExpr {
            return_type: VeinDataType::Boolean,
            kind: ExprKind::Binary {
                op: BinaryExprOp::Equal,
                left: Box::new(PhysicalExpr {
                    return_type: VeinDataType::Int64,
                    kind: ExprKind::Column { index: 0 },
                }),
                right: Box::new(PhysicalExpr {
                    return_type: VeinDataType::Int64,
                    kind: ExprKind::Literal(LogicalLiteral::Int64(4)),
                }),
            },
        },
    };
    let pushdown = ScanPredicate {
        predicate_count: 2,
        stages: vec![stage.clone(), stage],
        statistics: Vec::new(),
    };
    (
        schema,
        descriptor,
        VeinSchema::new(vec![key]),
        pushdown,
        Arc::new(FunctionRegistry::new()),
    )
}

fn pipeline(
    schema: &Schema,
    parquet_schema: &SchemaDescriptor,
    projected: &VeinSchema,
    pushdown: &ScanPredicate,
    registry: &Arc<FunctionRegistry>,
    adaptive_order: Option<Arc<AdaptiveFilterOrder>>,
) -> ScanPredicatePipeline {
    let expression_kernels: Arc<dyn ExpressionKernelProvider> =
        Arc::new(ExpressionKernelRegistry::with_builtins());
    build_scan_predicates(ScanPredicateContext {
        arrow_schema: schema,
        parquet_schema,
        path: "worker-policy.parquet",
        projected,
        pushdown: Some(pushdown),
        registry,
        expression_kernels: &expression_kernels,
        detailed_metrics_enabled: false,
        predicate_metrics: &Arc::new(ScanPredicateMetrics::default()),
        runtime_filters: &RuntimeFilterSet::empty(),
        adaptive_order,
    })
        .expect("predicate pipeline should bind")
}

#[test]
fn adaptive_policies_are_isolated_between_parallel_scan_workers() {
    let (schema, descriptor, projected, pushdown, registry) = setup();
    let first = pipeline(&schema, &descriptor, &projected, &pushdown, &registry, None)
        .adaptive_order
        .expect("first worker policy");
    let second = pipeline(&schema, &descriptor, &projected, &pushdown, &registry, None)
        .adaptive_order
        .expect("second worker policy");

    assert!(!Arc::ptr_eq(&first, &second));
}

#[test]
fn one_scan_worker_reuses_its_policy_across_files() {
    let (schema, descriptor, projected, pushdown, registry) = setup();
    let first = pipeline(&schema, &descriptor, &projected, &pushdown, &registry, None)
        .adaptive_order
        .expect("worker policy");
    let next_file = pipeline(
        &schema,
        &descriptor,
        &projected,
        &pushdown,
        &registry,
        Some(Arc::clone(&first)),
    )
        .adaptive_order
        .expect("same worker policy");

    assert!(Arc::ptr_eq(&first, &next_file));
}

#[test]
fn adaptive_order_is_disabled_when_any_stage_is_unsafe() {
    let (schema, descriptor, projected, mut pushdown, registry) = setup();
    pushdown.stages[1].reorder_safe = false;
    let pipeline = pipeline(&schema, &descriptor, &projected, &pushdown, &registry, None);

    assert!(pipeline.adaptive_order.is_none());
}

#[test]
fn worker_policy_is_recreated_when_predicate_stage_count_changes() {
    let (schema, descriptor, projected, pushdown, registry) = setup();
    let first = pipeline(&schema, &descriptor, &projected, &pushdown, &registry, None)
        .adaptive_order
        .expect("two-stage worker policy");
    let mut changed = pushdown.clone();
    changed.stages.push(changed.stages[0].clone());
    let next_file = pipeline(
        &schema,
        &descriptor,
        &projected,
        &changed,
        &registry,
        Some(Arc::clone(&first)),
    )
        .adaptive_order
        .expect("policy for changed stage count");

    assert_eq!(next_file.stage_count(), 3);
    assert!(!Arc::ptr_eq(&first, &next_file));
}

#[test]
fn same_stage_count_does_not_reuse_observations_for_different_predicates() {
    let (schema, descriptor, projected, pushdown, registry) = setup();
    let first = pipeline(&schema, &descriptor, &projected, &pushdown, &registry, None)
        .adaptive_order.expect("original worker policy");
    let mut changed = pushdown.clone();
    changed.stages[1].predicate = PhysicalExpr {
        return_type: VeinDataType::Boolean,
        kind: ExprKind::Literal(LogicalLiteral::Boolean(false)),
    };
    let next = pipeline(&schema, &descriptor, &projected, &changed,
                        &registry, Some(Arc::clone(&first))).adaptive_order.expect("replacement policy");
    assert_eq!(first.stage_count(), next.stage_count());
    assert!(!Arc::ptr_eq(&first, &next));
}
