use super::*;
use arrow::array::Int64Array;
use std::sync::Arc;

fn scan_leaf(schema: &Arc<types::VeinSchema>) -> Arc<PhysicalPlanNode> {
    plan_node(
        Arc::clone(schema),
        PhysicalPlanOperator::Pipeline {
            source: Arc::new(DataSource::File(FileScanTask {
                files: vec!["1.parquet".into(), "2.parquet".into(), "3.parquet".into()],
                format: FileFormat::Parquet,
                byte_range: None,
                scan_unit_range: None,
                projected: Arc::clone(schema),
                dictionary_columns: Vec::new(),
                pushdown: None,
                task_id: TaskId::new(),
                catalog_snapshot: None,
                runtime_statistics: None,
            })),
            input_schema: Arc::clone(schema),
            program: Arc::new(MicroProgram::empty()),
        },
    )
}

#[tokio::test]
async fn scan_join_vm_aggregate_sort_preserve_passthrough_arrays_to_view_sink() {
    let builder = Arc::new(MultiFileFeedBuilder::new());
    let runner = DefaultTaskExecutor::new(builder.clone());
    let scan_schema = int64_schema();
    let join_schema = schema_for_types(&[types::VeinDataType::Int64, types::VeinDataType::Int64]);
    let join = plan_node(
        Arc::clone(&join_schema),
        PhysicalPlanOperator::Join {
            probe: scan_leaf(&scan_schema),
            build: scan_leaf(&scan_schema),
            plan: JoinPlan {
                join_type: JoinKind::Inner,
                probe_keys: vec![0],
                build_keys: vec![0],
                residual: None,
                outputs: vec![JoinOutput::Probe(0), JoinOutput::Build(0)],
                probe_types: vec![types::VeinDataType::Int64],
                build_types: vec![types::VeinDataType::Int64],
            },
        },
    );
    let transformed_schema =
        schema_for_types(&[types::VeinDataType::Int64, types::VeinDataType::Int64]);
    let transformed = plan_node(
        Arc::clone(&transformed_schema),
        PhysicalPlanOperator::Transform {
            input: join,
            program: Arc::new(MicroProgram {
                register_count: 5,
                instructions: vec![
                    MicroOp::Add {
                        dst: 2,
                        src1: 0,
                        src2: 1,
                    },
                    MicroOp::LoadConstant {
                        dst: 3,
                        value: types::LogicalLiteral::Int64(2),
                    },
                    MicroOp::CmpGt {
                        dst: 4,
                        src1: 2,
                        src2: 3,
                    },
                    MicroOp::FilterMask { bool_reg: 4 },
                    MicroOp::ProjectRegisters { out_regs: vec![0, 2] },
                ],
            }),
        },
    );
    let aggregate_schema =
        schema_for_types(&[types::VeinDataType::Int64, types::VeinDataType::Int64]);
    let aggregate = plan_node(
        Arc::clone(&aggregate_schema),
        PhysicalPlanOperator::Aggregate {
            input: transformed,
            plan: AggregatePlan {
                groups: 1,
                estimated_groups: None,
                grouping_sets: None,
                emit_empty_group_on_empty_input: false,
                aggs: vec![AggOpCode::Sum],
                aggregate_input_types: vec![types::VeinDataType::Int64],
                types: vec![types::VeinDataType::Int64, types::VeinDataType::Int64],
            },
        },
    );
    let sorted = plan_node(
        Arc::clone(&aggregate_schema),
        PhysicalPlanOperator::Sort {
            input: aggregate,
            plan: SortPlan {
                keys: vec![SortKey {
                    column: 1,
                    ascending: false,
                    nulls_first: false,
                }],
            },
            key_program: None,
            output_columns: 2,
        },
    );
    let mut task = task();
    task.plan = plan_with_input(&task, sorted);

    let result = runner
        .execute_views(&task, CancellationToken::new())
        .await
        .expect("view pipeline should finish");
    assert_eq!(result.row_count, 2);
    let source_arrays = builder.source_arrays.lock().expect("source array lock");
    let mut rows = Vec::new();
    for batch in &result.output {
        for row in 0..batch.num_rows() {
            let (key_array, key_row) = batch.view().resolve(0, row).unwrap().unwrap();
            let key_values = key_array.as_any().downcast_ref::<Int64Array>().unwrap();
            assert!(
                source_arrays
                    .iter()
                    .any(|source| std::ptr::eq(source.as_ref(), key_values)),
                "group key must still reference a scan array after join, VM, aggregate, and sort"
            );
            let (sum_array, sum_row) = batch.view().resolve(1, row).unwrap().unwrap();
            let sum_values = sum_array.as_any().downcast_ref::<Int64Array>().unwrap();
            rows.push((key_values.value(key_row), sum_values.value(sum_row)));
        }
    }
    assert_eq!(rows, vec![(3, 6), (2, 4)]);
}
