use super::*;

#[tokio::test]
async fn view_results_reject_non_client_destinations() {
    let mut task = task();
    let PhysicalPlanOperator::Sink { input, .. } = &task.plan.root.operator else {
        panic!("client task fixture requires a sink");
    };
    let input = Arc::clone(input);
    task.plan = PhysicalPlan {
        root: plan_node(
            Arc::clone(&input.output_schema),
            PhysicalPlanOperator::Sink {
                input,
                sink: Arc::new(DataSink::Discard),
            },
        ),
    };
    let error = runner()
        .execute_views(&task, CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(error.category(), error::ErrorCategory::Planning);
    assert!(
        error
            .to_string()
            .contains("view results require a client sink")
    );
}

#[tokio::test]
async fn both_result_apis_preserve_zero_column_counts_and_empty_input() {
    let mut task = task();
    let input = plan_node(
        Arc::new(types::VeinSchema::empty()),
        PhysicalPlanOperator::Pipeline {
            source: Arc::new(DataSource::Memory),
            input_schema: int64_schema(),
            program: projection_program(Vec::new()),
        },
    );
    task.plan = plan_with_input(&task, input);
    for values in [vec![], vec![1, 2, 3]] {
        let runner = runner_with_values(values.clone());
        let arrow = runner
            .execute(&task, CancellationToken::new())
            .await
            .unwrap();
        let views = runner
            .execute_views(&task, CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(arrow.row_count, values.len() as u64);
        assert_eq!(views.row_count, arrow.row_count);
        assert_eq!(arrow.output.len(), usize::from(!values.is_empty()));
        assert_eq!(views.output.len(), arrow.output.len());
        for (batch, view) in arrow.output.iter().zip(&views.output) {
            assert_eq!(batch.num_columns(), 0);
            assert_eq!(batch.num_rows(), values.len());
            assert_eq!(view.view().num_columns(), 0);
            assert_eq!(view.num_rows(), values.len());
        }
    }
}
