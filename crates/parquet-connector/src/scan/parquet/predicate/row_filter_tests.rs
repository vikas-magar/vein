use std::sync::Arc;

use arrow::array::{Array, BooleanArray, BooleanBuilder, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use common::protocol::{
    BinaryExprOp, ExprKind, PhysicalExpr, ScanPredicate, ScanPredicateStage,
};
use parquet::schema::types::{SchemaDescriptor, Type as ParquetType};
use types::{ColumnId, LogicalLiteral, ResolvedColumn, VeinDataType, VeinSchema};

use exec::expression::eval::{ExpressionKernelProvider, ExpressionKernelRegistry};
use crate::scan::parquet::metrics::ScanPredicateMetrics;
use exec::pipeline::DataChunkView;
use exec::runtime::filter::{RuntimeFilter, RuntimeFilterSet};
use functions::FunctionRegistry;

use super::{ScanPredicateContext, build_scan_predicates};

#[derive(Debug)]
struct KeepColumnValue {
    column: ColumnId,
    expected: i64,
}

impl RuntimeFilter for KeepColumnValue {
    fn name(&self) -> &'static str {
        "test_keep_column_value"
    }

    fn required_columns(&self) -> &[ColumnId] {
        std::slice::from_ref(&self.column)
    }

    fn memory_bytes(&self) -> usize {
        0
    }

    fn evaluate(
        &self,
        view: &DataChunkView,
        column_positions: &[usize],
    ) -> common::Result<BooleanArray> {
        let [position] = column_positions else {
            return Err(error::VeinError::internal("test filter expects one key"));
        };
        let mut output = BooleanBuilder::with_capacity(view.num_rows());
        for row in 0..view.num_rows() {
            let matches = match view.resolve(*position, row)? {
                Some((array, index)) => {
                    let values = array
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| error::VeinError::internal("test filter key must be Int64"))?;
                    values.is_valid(index) && values.value(index) == self.expected
                }
                None => false,
            };
            output.append_value(matches);
        }
        Ok(output.finish())
    }
}

#[test]
fn compiled_scan_predicate_runs_as_narrow_parquet_filter_and_preserves_nulls() {
    let arrow_schema = Schema::new(vec![Field::new("key", DataType::Int64, true)]);
    let parquet_schema = SchemaDescriptor::new(Arc::new(
        ParquetType::group_type_builder("schema")
            .with_fields(vec![Arc::new(
                ParquetType::primitive_type_builder("key", parquet::basic::Type::INT64)
                    .build().expect("Parquet key field"),
            )])
            .build().expect("Parquet schema"),
    ));
    let key = Arc::new(ResolvedColumn::new(
        ColumnId(0), "key", None, VeinDataType::Int64,
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
                    kind: ExprKind::Literal(LogicalLiteral::Int64(7)),
                }),
            },
        },
    };
    let pushdown = ScanPredicate { predicate_count: 1, stages: vec![stage], statistics: vec![] };
    let metrics = Arc::new(ScanPredicateMetrics::default());
    let expression_kernels: Arc<dyn ExpressionKernelProvider> =
        Arc::new(ExpressionKernelRegistry::with_builtins());
    let pipeline = build_scan_predicates(ScanPredicateContext {
        arrow_schema: &arrow_schema,
        parquet_schema: &parquet_schema,
        path: "test.parquet",
        projected: &VeinSchema::new(vec![key]),
        pushdown: Some(&pushdown),
        registry: &Arc::new(FunctionRegistry::new()),
        expression_kernels: &expression_kernels,
        detailed_metrics_enabled: true,
        predicate_metrics: &metrics,
        runtime_filters: &RuntimeFilterSet::empty(),
        adaptive_order: None,
    }).expect("compiled Parquet predicate");

    let mut predicates = pipeline.row_filter.expect("row filter").into_predicates();
    let input = RecordBatch::try_new(
        Arc::new(arrow_schema),
        vec![Arc::new(Int64Array::from(vec![Some(7), Some(8), None]))],
    ).expect("predicate input batch");
    let result = predicates[0].evaluate(input).expect("predicate evaluation");

    assert_eq!(result, BooleanArray::from(vec![Some(true), Some(false), None]));
    assert_eq!(metrics.rows_before_filter.load(std::sync::atomic::Ordering::Relaxed), 3);
    assert_eq!(metrics.rows_kept.load(std::sync::atomic::Ordering::Relaxed), 1);
}

#[test]
fn runtime_filters_bind_as_independent_narrow_parquet_predicates() {
    let arrow_schema = Schema::new(vec![
        Field::new("key_a", DataType::Int64, true),
        Field::new("key_b", DataType::Int64, true),
    ]);
    let parquet_schema = SchemaDescriptor::new(Arc::new(
        ParquetType::group_type_builder("schema")
            .with_fields(vec!["key_a", "key_b"].into_iter().map(|name| {
                Arc::new(
                    ParquetType::primitive_type_builder(name, parquet::basic::Type::INT64)
                        .build()
                        .expect("Parquet key field"),
                )
            }).collect())
            .build()
            .expect("Parquet schema"),
    ));
    let key_a = Arc::new(ResolvedColumn::new(
        ColumnId(10), "key_a", None, VeinDataType::Int64,
    ));
    let key_b = Arc::new(ResolvedColumn::new(
        ColumnId(11), "key_b", None, VeinDataType::Int64,
    ));
    let projected = VeinSchema::new(vec![key_a, key_b]);
    let runtime_filters = RuntimeFilterSet::new(vec![
        Arc::new(KeepColumnValue { column: ColumnId(10), expected: 2 }),
        Arc::new(KeepColumnValue { column: ColumnId(11), expected: 3 }),
    ]);
    let expression_kernels: Arc<dyn ExpressionKernelProvider> =
        Arc::new(ExpressionKernelRegistry::with_builtins());
    let pipeline = build_scan_predicates(ScanPredicateContext {
        arrow_schema: &arrow_schema,
        parquet_schema: &parquet_schema,
        path: "test.parquet",
        projected: &projected,
        pushdown: None,
        registry: &Arc::new(FunctionRegistry::new()),
        expression_kernels: &expression_kernels,
        detailed_metrics_enabled: false,
        predicate_metrics: &Arc::new(ScanPredicateMetrics::default()),
        runtime_filters: &runtime_filters,
        adaptive_order: None,
    })
        .expect("runtime filter predicates");
    let mut predicates = pipeline.row_filter.expect("row filter").into_predicates();

    assert_eq!(predicates.len(), 2, "each runtime filter must have its own stage");
    let input_a = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("key_a", DataType::Int64, true)])),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
        .expect("first narrow filter batch");
    let input_b = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("key_b", DataType::Int64, true)])),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
        .expect("second narrow filter batch");

    assert_eq!(
        predicates[0].evaluate(input_a).expect("first runtime filter"),
        BooleanArray::from(vec![false, true, false])
    );
    assert_eq!(
        predicates[1].evaluate(input_b).expect("second runtime filter"),
        BooleanArray::from(vec![false, false, true])
    );
}
