use super::*;

#[tokio::test]
async fn configured_memory_limit_reaches_recursive_aggregate_state() {
    let input = InputPlanFixture::Pipeline {
        source: DataSource::Memory,
        program: Arc::new(MicroProgram::empty()),
    };
    let aggregate = InputPlanFixture::Aggregate {
        input: Box::new(input),
        plan: AggregatePlan {
            groups: 0,
            estimated_groups: None,
            grouping_sets: Some(vec![Vec::new()]),
            emit_empty_group_on_empty_input: true,
            aggs: vec![AggOpCode::Count],
            aggregate_input_types: vec![VeinDataType::Int64],
            types: vec![VeinDataType::Int64],
        },
    };
    let mut task = task();
    set_input_plan(&mut task, aggregate);
    let runner = DefaultTaskExecutor::with_memory_limit(
        Arc::new(MemoryFeedBuilder::with_values(vec![1, 2, 3])),
        0,
    );

    let error = runner
        .execute(&task, CancellationToken::new())
        .await
        .expect_err("recursive aggregate must use the configured execution budget");
    assert_eq!(error.category(), error::ErrorCategory::Resource);
    let message = error.to_string();
    assert!(
        matches!(
                &error,
                VeinError::ResourceExhausted {
                    resource,
                    requested,
                    limit: 0,
                    unit: error::ResourceUnit::Bytes,
                } if resource == "aggregate group state" && *requested > 0
            ),
        "expected a bounded aggregate-state memory error, received {error:?}"
    );
    assert!(
        message.starts_with("Resource exhausted: aggregate group state, requested "),
        "resource errors should identify the bounded state concisely: {message}"
    );
    assert!(message.ends_with("bytes, limit 0 bytes"), "{message}");
    assert!(!message.contains("PhysicalPlanOperator"), "{message}");
}
