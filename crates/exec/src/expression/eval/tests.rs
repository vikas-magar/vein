use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use common::protocol::{BinaryExprOp, ExprKind, PhysicalExpr};
use types::{LogicalLiteral, VeinDataType};

use crate::expression::eval::{
    ExpressionEvaluationContext, ExpressionEvaluator, ExpressionKernelFn, ExpressionKernelRegistry,
    ExpressionNodeKind,
};
use crate::expression::value::ExpressionValue;
use crate::pipeline::DataChunkView;
use crate::udf::FunctionRegistry;

fn evaluate(
    expression: &PhysicalExpr,
    input: &Arc<DataChunkView>,
    functions: &FunctionRegistry,
) -> common::Result<ExpressionValue> {
    let kernels = ExpressionKernelRegistry::with_builtins();
    ExpressionEvaluator::new(&kernels, functions).evaluate(expression, input)
}

fn input() -> Arc<DataChunkView> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, true),
    ]));
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![1, 2, 3])),
        Arc::new(StringArray::from(vec![Some("A"), None, Some("C")])),
    ];
    Arc::new(
        crate::pipeline::arrow_adapter::from_record_batch(
            RecordBatch::try_new(schema, arrays).unwrap(),
        )
            .unwrap(),
    )
}

fn column(index: u32, data_type: VeinDataType) -> PhysicalExpr {
    PhysicalExpr {
        return_type: data_type,
        kind: ExprKind::Column { index },
    }
}

#[test]
fn typed_arithmetic_evaluates_columns_and_broadcast_literals() {
    let expression = PhysicalExpr {
        return_type: VeinDataType::Int64,
        kind: ExprKind::Binary {
            op: BinaryExprOp::Add,
            left: Box::new(column(0, VeinDataType::Int64)),
            right: Box::new(PhysicalExpr {
                return_type: VeinDataType::Int64,
                kind: ExprKind::Literal(LogicalLiteral::Int64(5)),
            }),
        },
    };
    let value = evaluate(&expression, &input(), &FunctionRegistry::new()).unwrap();
    let result = value.materialized_array().unwrap();
    assert_eq!(
        result
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[6, 7, 8]
    );
}

#[test]
fn typed_like_preserves_nulls_and_uses_arrow_vector_kernel() {
    let expression = PhysicalExpr {
        return_type: VeinDataType::Boolean,
        kind: ExprKind::Like {
            expr: Box::new(column(1, VeinDataType::Utf8)),
            pattern: Box::new(PhysicalExpr {
                return_type: VeinDataType::Utf8,
                kind: ExprKind::Literal(LogicalLiteral::String("%".into())),
            }),
            negated: false,
        },
    };
    let value = evaluate(&expression, &input(), &FunctionRegistry::new()).unwrap();
    let result = value.materialized_array().unwrap();
    assert_eq!(
        result.as_any().downcast_ref::<BooleanArray>().unwrap(),
        &BooleanArray::from(vec![Some(true), None, Some(true)])
    );
}

#[test]
fn typed_column_evaluation_materializes_a_zero_copy_source_slice() {
    let view = input();
    let source = view.source_arrays_for_column(0).unwrap()[0].clone();
    let expression = column(0, VeinDataType::Int64);
    let value = evaluate(&expression, &view, &FunctionRegistry::new()).unwrap();
    let ExpressionValue::ViewColumn(column) = value else {
        panic!("column expressions must preserve the source view")
    };
    let (materialized, report) = column.materialize_with_report().unwrap();
    assert_eq!(
        source.to_data().buffers()[0].as_ptr(),
        materialized.to_data().buffers()[0].as_ptr(),
        "a contiguous view may create an Arrow slice wrapper but must share its value buffer"
    );
    assert_eq!(report.shared_columns, 1);
    assert_eq!(report.gathered_columns, 0);
}

fn replacement_literal(
    _expression: &PhysicalExpr,
    _context: &dyn ExpressionEvaluationContext,
) -> common::Result<ExpressionValue> {
    Ok(ExpressionValue::Scalar(Arc::new(Int64Array::from(vec![
        42,
    ]))))
}

#[test]
fn evaluator_uses_injected_kernel_for_a_typed_expression_node() {
    let mut kernels = ExpressionKernelRegistry::with_builtins();
    kernels.replace(Arc::new(ExpressionKernelFn::new(
        ExpressionNodeKind::Literal,
        replacement_literal,
    )));
    let functions = FunctionRegistry::new();
    let expression = PhysicalExpr {
        return_type: VeinDataType::Int64,
        kind: ExprKind::Literal(LogicalLiteral::Int64(1)),
    };

    let value = ExpressionEvaluator::new(&kernels, &functions)
        .evaluate(&expression, &input())
        .expect("registered expression kernel should execute");

    let result = value.materialized_array().unwrap();
    assert_eq!(
        result
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        42
    );
}

#[test]
fn kernel_registry_rejects_duplicate_registration() {
    let mut kernels = ExpressionKernelRegistry::with_builtins();
    let plugin = Arc::new(ExpressionKernelFn::new(
        ExpressionNodeKind::Literal,
        replacement_literal,
    ));

    assert!(kernels.register(plugin).is_err());
}
