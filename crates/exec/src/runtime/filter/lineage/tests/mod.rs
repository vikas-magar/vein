use super::*;
use common::identifiers::TaskId;
use common::protocol::{BinaryExprOp, DataFileFormatId, FileScanTask, LimitSpec, PhysicalExpr};
use std::sync::Arc;
use types::{LogicalLiteral, ResolvedColumn, VeinDataType, VeinSchema};

fn column(index: u32) -> PhysicalExpr {
    PhysicalExpr { return_type: VeinDataType::Int64, kind: ExprKind::Column { index } }
}

fn schema(ids: &[usize]) -> Arc<VeinSchema> {
    Arc::new(VeinSchema::new(ids.iter().map(|&id| Arc::new(ResolvedColumn::new(
        ColumnId(id), format!("column_{id}"), None, VeinDataType::Int64,
    ))).collect()))
}

fn source(program: ExpressionProgram, output: Arc<VeinSchema>) -> Arc<PhysicalPlanNode> {
    let input = schema(&[10, 20]);
    Arc::new(PhysicalPlanNode { output_schema: output, operator: PhysicalPlanOperator::Pipeline {
        source: Arc::new(DataSource::File(FileScanTask {
            files: vec![], storage_metadata: vec![], data_file_format: DataFileFormatId::new("test"),
            format_options: vec![], column_encoding_hints: vec![], byte_range: None, scan_unit_range: None,
            projected: Arc::clone(&input), pushdown: None, task_id: TaskId::new(), catalog_snapshot: None,
            runtime_statistics: None,
        })), input_schema: input, program: Arc::new(program),
    } })
}

fn join() -> JoinPlan {
    JoinPlan { join_type: common::protocol::JoinKind::Inner, probe_keys: vec![0], build_keys: vec![0],
        residual: None, outputs: vec![], probe_types: vec![VeinDataType::Int64; 2], build_types: vec![VeinDataType::Int64] }
}

fn comparison(op: BinaryExprOp) -> PhysicalExpr {
    PhysicalExpr { return_type: VeinDataType::Boolean, kind: ExprKind::Binary {
        op, left: Box::new(column(0)), right: Box::new(PhysicalExpr {
            return_type: VeinDataType::Int64, kind: ExprKind::Literal(LogicalLiteral::Int64(0)),
        }),
    } }
}

#[test]
fn safe_filter_and_aliased_projection_map_to_original_source_identity() {
    let program = ExpressionProgram { steps: vec![
        ExpressionStep::Filter(comparison(BinaryExprOp::Greater)),
        ExpressionStep::Projection(vec![column(1)]),
    ] };
    let input = source(program, schema(&[99]));
    let node = PhysicalPlanNode { output_schema: schema(&[99]), operator: PhysicalPlanOperator::Transform {
        input, program: Arc::new(ExpressionProgram { steps: vec![ExpressionStep::Filter(comparison(BinaryExprOp::Greater))] }),
    } };
    let (target, columns) = direct_scan_probe_columns(&node, &join()).unwrap();
    assert_eq!(columns, vec![ColumnId(20)]);
    assert!(matches!(target, DataSource::File(_)));
}

#[test]
fn unsafe_evaluation_and_computed_keys_block_filter_movement() {
    let opaque = PhysicalExpr { return_type: VeinDataType::Boolean,
        kind: ExprKind::Call { name: "user_function".into(), args: vec![column(0)] } };
    for step in [
        ExpressionStep::Filter(opaque),
        ExpressionStep::Filter(comparison(BinaryExprOp::Divide)),
        ExpressionStep::Projection(vec![comparison(BinaryExprOp::Add)]),
    ] {
        let input = source(ExpressionProgram { steps: vec![step] }, schema(&[99]));
        assert!(direct_scan_probe_columns(&input, &join()).is_none());
    }
}

#[test]
fn limit_boundary_and_mismatched_join_types_are_not_crossed() {
    let input = source(ExpressionProgram::empty(), schema(&[10, 20]));
    let limited = PhysicalPlanNode { output_schema: schema(&[10, 20]), operator: PhysicalPlanOperator::Limit {
        input: Arc::clone(&input), spec: LimitSpec { offset: 0, count: 1 },
    } };
    assert!(direct_scan_probe_columns(&limited, &join()).is_none());
    let mut plan = join();
    plan.build_types[0] = VeinDataType::Int32;
    assert!(direct_scan_probe_columns(&input, &plan).is_none());
}
