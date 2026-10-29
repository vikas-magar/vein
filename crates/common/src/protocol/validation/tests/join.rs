use super::*;
#[test]
fn rejects_join_key_positions_outside_recursive_child_schema() {
    let side_schema = schema("key");
    let probe = pipeline(Arc::clone(&side_schema), ExpressionProgram::empty());
    let build = pipeline(Arc::clone(&side_schema), ExpressionProgram::empty());
    let joined = node(
        Arc::clone(&side_schema),
        PhysicalPlanOperator::Join {
            probe,
            build,
            plan: JoinPlan {
                join_type: JoinKind::Inner,
                probe_keys: vec![1],
                build_keys: vec![0],
                residual: None,
                outputs: vec![JoinOutput::Probe(0)],
                probe_types: vec![VeinDataType::Int64],
                build_types: vec![VeinDataType::Int64],
            },
        },
    );

    let error = client_plan(joined, side_schema)
        .validate()
        .expect_err("join key must be inside the probe input schema");
    assert!(error.to_string().contains("probe key column 1"));
}

#[test]
fn validates_join_output_positions_nullability_and_repeated_columns() {
    let input_schema = schema("key");
    let probe = pipeline(Arc::clone(&input_schema), ExpressionProgram::empty());
    let build = pipeline(Arc::clone(&input_schema), ExpressionProgram::empty());
    let output_schema = Arc::new(VeinSchema::new(vec![
        Arc::new(ResolvedColumn::new(
            ColumnId(1),
            "left_key",
            None,
            VeinDataType::Int64,
        )),
        Arc::new(
            ResolvedColumn::new(ColumnId(2), "right_key", None, VeinDataType::Int64)
                .with_nullable(true),
        ),
    ]));

    let valid_left_join = node(
        Arc::clone(&output_schema),
        PhysicalPlanOperator::Join {
            probe: Arc::clone(&probe),
            build: Arc::clone(&build),
            plan: JoinPlan {
                join_type: JoinKind::Left,
                probe_keys: vec![0],
                build_keys: vec![0],
                residual: None,
                outputs: vec![JoinOutput::Probe(0), JoinOutput::Build(0)],
                probe_types: vec![VeinDataType::Int64],
                build_types: vec![VeinDataType::Int64],
            },
        },
    );
    client_plan(valid_left_join, Arc::clone(&output_schema))
        .validate()
        .expect("left join must make unmatched build-side outputs nullable");

    let invalid_output = node(
        Arc::clone(&input_schema),
        PhysicalPlanOperator::Join {
            probe: Arc::clone(&probe),
            build: Arc::clone(&build),
            plan: JoinPlan {
                join_type: JoinKind::Inner,
                probe_keys: vec![0],
                build_keys: vec![0],
                residual: None,
                outputs: vec![JoinOutput::Build(1)],
                probe_types: vec![VeinDataType::Int64],
                build_types: vec![VeinDataType::Int64],
            },
        },
    );
    let error = client_plan(invalid_output, Arc::clone(&input_schema))
        .validate()
        .expect_err("join output positions must resolve on the declared child side");
    assert!(error.to_string().contains("build output column 1"));

    let repeated_output_schema = Arc::new(VeinSchema::new(vec![
        Arc::new(ResolvedColumn::new(
            ColumnId(3),
            "first_copy",
            None,
            VeinDataType::Int64,
        )),
        Arc::new(ResolvedColumn::new(
            ColumnId(4),
            "second_copy",
            None,
            VeinDataType::Int64,
        )),
    ]));
    let repeated_output = node(
        Arc::clone(&repeated_output_schema),
        PhysicalPlanOperator::Join {
            probe,
            build,
            plan: JoinPlan {
                join_type: JoinKind::Inner,
                probe_keys: vec![0],
                build_keys: vec![0],
                residual: None,
                outputs: vec![JoinOutput::Probe(0), JoinOutput::Probe(0)],
                probe_types: vec![VeinDataType::Int64],
                build_types: vec![VeinDataType::Int64],
            },
        },
    );
    client_plan(repeated_output, repeated_output_schema)
        .validate()
        .expect("repeated output positions are valid projections");
}
