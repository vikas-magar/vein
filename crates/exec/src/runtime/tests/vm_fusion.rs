use super::*;

#[tokio::test]
async fn fused_source_program_matches_recursive_transform_execution() {
    let program = Arc::new(MicroProgram::new(
        3,
        vec![
            MicroOp::LoadConstant {
                dst: 1,
                value: types::LogicalLiteral::Int64(2),
            },
            MicroOp::CmpGt {
                dst: 2,
                src1: 0,
                src2: 1,
            },
            MicroOp::FilterMask { bool_reg: 2 },
            MicroOp::ProjectRegisters { out_regs: vec![0] },
        ],
    ));
    let empty_program = Arc::new(MicroProgram::empty());

    let mut fused_task = task();
    set_input_plan(
        &mut fused_task,
        InputPlanFixture::Pipeline {
            source: DataSource::Memory,
            program: Arc::clone(&program),
        },
    );
    let mut recursive_task = task();
    set_input_plan(
        &mut recursive_task,
        InputPlanFixture::Transform {
            input: Box::new(InputPlanFixture::Pipeline {
                source: DataSource::Memory,
                program: empty_program,
            }),
            program,
        },
    );

    let values = vec![Some(1), None, Some(3), Some(4), Some(5)];
    let fused_runner = DefaultTaskExecutor::new(Arc::new(
        MemoryFeedBuilder::with_nullable_values(values.clone()),
    ));
    let recursive_runner =
        DefaultTaskExecutor::new(Arc::new(MemoryFeedBuilder::with_nullable_values(values)));
    let fused = fused_runner
        .execute(&fused_task, CancellationToken::new())
        .await
        .expect("fused source program should execute");
    let recursive = recursive_runner
        .execute(&recursive_task, CancellationToken::new())
        .await
        .expect("recursive transform should execute");

    assert_eq!(fused.row_count, recursive.row_count);
    assert_eq!(fused.output.len(), recursive.output.len());
    for (fused_batch, recursive_batch) in fused.output.iter().zip(&recursive.output) {
        assert_eq!(fused_batch.schema(), recursive_batch.schema());
        assert_eq!(fused_batch.num_rows(), recursive_batch.num_rows());
        for column_index in 0..fused_batch.num_columns() {
            assert_eq!(
                fused_batch.column(column_index).to_data(),
                recursive_batch.column(column_index).to_data(),
                "fused and recursive execution must produce identical column values"
            );
        }
    }
    let values = fused.output[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("projected values should remain Int64");
    assert_eq!(
        (0..values.len())
            .map(|row| values.value(row))
            .collect::<Vec<_>>(),
        vec![3, 4, 5]
    );
}

#[tokio::test]
async fn fused_and_recursive_programs_preserve_null_outputs() {
    let program = Arc::new(MicroProgram::new(
        1,
        vec![MicroOp::ProjectRegisters { out_regs: vec![0] }],
    ));
    let values = vec![Some(1), None, Some(3)];
    let nullable_schema = Arc::new(types::VeinSchema::new(vec![Arc::new(
        types::ResolvedColumn::new(types::ColumnId(0), "column_0", None, VeinDataType::Int64)
            .with_nullable(true),
    )]));
    let mut fused_task = task();
    fused_task.plan = plan_with_input(
        &fused_task,
        plan_node(
            Arc::clone(&nullable_schema),
            PhysicalPlanOperator::Pipeline {
                source: Arc::new(DataSource::Memory),
                input_schema: Arc::clone(&nullable_schema),
                program: Arc::clone(&program),
            },
        ),
    );
    let mut recursive_task = task();
    recursive_task.plan = plan_with_input(
        &recursive_task,
        plan_node(
            Arc::clone(&nullable_schema),
            PhysicalPlanOperator::Transform {
                input: plan_node(
                    Arc::clone(&nullable_schema),
                    PhysicalPlanOperator::Pipeline {
                        source: Arc::new(DataSource::Memory),
                        input_schema: nullable_schema,
                        program: Arc::new(MicroProgram::empty()),
                    },
                ),
                program,
            },
        ),
    );
    let fused = DefaultTaskExecutor::new(Arc::new(MemoryFeedBuilder::with_nullable_values(
        values.clone(),
    )))
        .execute(&fused_task, CancellationToken::new())
        .await
        .expect("fused projection should preserve NULL values");
    let recursive =
        DefaultTaskExecutor::new(Arc::new(MemoryFeedBuilder::with_nullable_values(values)))
            .execute(&recursive_task, CancellationToken::new())
            .await
            .expect("recursive projection should preserve NULL values");

    let fused_values = fused.output[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("fused projection should return Int64 values");
    let recursive_values = recursive.output[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("recursive projection should return Int64 values");
    assert_eq!(fused_values, recursive_values);
    assert!(fused_values.is_null(1));
}

#[tokio::test]
async fn fused_and_recursive_programs_return_the_same_vm_error() {
    let invalid_program = Arc::new(MicroProgram::new(
        2,
        vec![MicroOp::CmpGt {
            dst: 1,
            src1: 0,
            src2: 99,
        }],
    ));
    let mut fused_task = task();
    set_input_plan(
        &mut fused_task,
        InputPlanFixture::Pipeline {
            source: DataSource::Memory,
            program: Arc::clone(&invalid_program),
        },
    );
    let mut recursive_task = task();
    set_input_plan(
        &mut recursive_task,
        InputPlanFixture::Transform {
            input: Box::new(InputPlanFixture::Pipeline {
                source: DataSource::Memory,
                program: Arc::new(MicroProgram::empty()),
            }),
            program: invalid_program,
        },
    );

    let fused_error = runner()
        .execute(&fused_task, CancellationToken::new())
        .await
        .expect_err("fused program must reject an out-of-range register");
    let recursive_error = runner()
        .execute(&recursive_task, CancellationToken::new())
        .await
        .expect_err("recursive program must reject an out-of-range register");
    assert_eq!(fused_error.to_string(), recursive_error.to_string());
}
