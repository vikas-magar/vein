use super::*;

fn filtering_projection() -> Arc<ExpressionProgram> {
    Arc::new(ExpressionProgram {
        steps: vec![
            ExpressionStep::Filter(binary_expr(
                BinaryExprOp::Greater,
                column_expr(0, VeinDataType::Int64),
                literal_expr(LogicalLiteral::Int64(2), VeinDataType::Int64),
                VeinDataType::Boolean,
            )),
            ExpressionStep::Projection(vec![column_expr(0, VeinDataType::Int64)]),
        ],
    })
}

#[tokio::test]
async fn fused_source_and_recursive_expression_pipeline_match() {
    let program = filtering_projection();
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
                program: Arc::new(ExpressionProgram::empty()),
            }),
            program,
        },
    );
    let values = vec![Some(1), None, Some(3), Some(4), Some(5)];
    let fused = DefaultTaskExecutor::new(Arc::new(
        MemoryFeedBuilder::with_nullable_values(values.clone()),
    ))
    .execute(&fused_task, CancellationToken::new())
    .await
    .expect("fused expression pipeline should execute");
    let recursive = DefaultTaskExecutor::new(Arc::new(
        MemoryFeedBuilder::with_nullable_values(values),
    ))
    .execute(&recursive_task, CancellationToken::new())
    .await
    .expect("recursive expression pipeline should execute");

    assert_eq!(fused.row_count, recursive.row_count);
    assert_eq!(fused.output.len(), recursive.output.len());
    for (left, right) in fused.output.iter().zip(&recursive.output) {
        assert_eq!(left.schema(), right.schema());
        assert_eq!(view_nullable_int64_values(left, 0), view_nullable_int64_values(right, 0));
    }
    assert_eq!(view_int64_values(&fused.output[0], 0), vec![3, 4, 5]);
}

#[tokio::test]
async fn projection_expression_preserves_nulls() {
    let program = Arc::new(ExpressionProgram {
        steps: vec![ExpressionStep::Projection(vec![column_expr(
            0,
            VeinDataType::Int64,
        )])],
    });
    let schema = Arc::new(types::VeinSchema::new(vec![Arc::new(
        types::ResolvedColumn::new(types::ColumnId(0), "column_0", None, VeinDataType::Int64)
            .with_nullable(true),
    )]));
    let mut execution_task = task();
    execution_task.plan = plan_with_input(
        &execution_task,
        plan_node(
            Arc::clone(&schema),
            PhysicalPlanOperator::Pipeline {
                source: Arc::new(DataSource::Memory),
                input_schema: schema,
                program,
            },
        ),
    );
    let result = DefaultTaskExecutor::new(Arc::new(MemoryFeedBuilder::with_nullable_values(
        vec![Some(1), None, Some(3)],
    )))
    .execute(&execution_task, CancellationToken::new())
    .await
    .expect("column expression should preserve NULLs");
    assert_eq!(
        view_nullable_int64_values(&result.output[0], 0),
        vec![Some(1), None, Some(3)]
    );
}

#[tokio::test]
async fn expression_pipeline_rejects_columns_outside_the_input_view() {
    let program = Arc::new(ExpressionProgram {
        steps: vec![ExpressionStep::Filter(column_expr(
            99,
            VeinDataType::Boolean,
        ))],
    });
    let mut execution_task = task();
    set_input_plan(
        &mut execution_task,
        InputPlanFixture::Pipeline { source: DataSource::Memory, program },
    );
    let error = runner()
        .execute(&execution_task, CancellationToken::new())
        .await
        .expect_err("an invalid expression column ordinal must fail clearly");
    assert_eq!(error.category(), error::ErrorCategory::Planning);
    assert!(error.to_string().contains("99"), "unexpected error: {error}");
}
