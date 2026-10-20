use super::*;

#[tokio::test]
async fn discard_sink_returns_no_client_views_and_preserves_row_count() {
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
    let result = runner()
        .execute(&task, CancellationToken::new())
        .await
        .expect("discard sinks execute without producing client results");
    assert_eq!(result.row_count, 3);
    assert!(result.output.is_empty());
}

#[tokio::test]
async fn client_views_preserve_zero_column_counts_and_empty_input() {
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
        let result = runner
            .execute(&task, CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(result.row_count, values.len() as u64);
        assert_eq!(result.output.len(), usize::from(!values.is_empty()));
        for view in &result.output {
            assert_eq!(view.num_columns(), 0);
            assert_eq!(view.num_rows(), values.len());
        }
    }
}
