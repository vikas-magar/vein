use super::*;
#[test]
fn rejects_aggregate_group_and_sort_key_positions_outside_schema() {
    let schema = schema("value");
    let aggregate = node(
        Arc::clone(&schema),
        PhysicalPlanOperator::Aggregate {
            input: pipeline(Arc::clone(&schema), ExpressionProgram::empty()),
            plan: AggregatePlan {
                groups: 1,
                estimated_groups: None,
                grouping_sets: Some(vec![vec![1]]),
                emit_empty_group_on_empty_input: true,
                aggs: Vec::new(),
                aggregate_input_types: Vec::new(),
                types: vec![VeinDataType::Int64],
            },
        },
    );
    let error = client_plan(aggregate, Arc::clone(&schema))
        .validate()
        .expect_err("grouping set positions must be within the declared key width");
    assert!(error.to_string().contains("grouping set 0 column 1"));

    let sorted = node(
        Arc::clone(&schema),
        PhysicalPlanOperator::Sort {
            input: pipeline(Arc::clone(&schema), ExpressionProgram::empty()),
            plan: SortPlan {
                keys: vec![crate::protocol::SortKey {
                    column: 1,
                    ascending: true,
                    nulls_first: false,
                }],
            },
            key_program: None,
            output_columns: 1,
        },
    );
    let error = client_plan(sorted, schema)
        .validate()
        .expect_err("sort keys must resolve against the key schema");
    assert!(error.to_string().contains("sort key column 1"));
}

#[test]
fn rejects_non_sink_root_and_malformed_aggregate_widths() {
    let schema = schema("value");
    let source = pipeline(Arc::clone(&schema), ExpressionProgram::empty());
    let rootless = PhysicalPlan {
        root: node(
            Arc::clone(&schema),
            PhysicalPlanOperator::Sort {
                input: source,
                plan: SortPlan { keys: Vec::new() },
                key_program: None,
                output_columns: 1,
            },
        ),
    };
    assert!(
        rootless
            .validate()
            .unwrap_err()
            .to_string()
            .contains("root operator must be a sink")
    );

    let aggregate = node(
        Arc::clone(&schema),
        PhysicalPlanOperator::Aggregate {
            input: pipeline(Arc::clone(&schema), ExpressionProgram::empty()),
            plan: AggregatePlan {
                groups: 1,
                estimated_groups: None,
                grouping_sets: None,
                emit_empty_group_on_empty_input: true,
                aggs: Vec::new(),
                aggregate_input_types: Vec::new(),
                types: Vec::new(),
            },
        },
    );
    let error = client_plan(aggregate, schema)
        .validate()
        .expect_err("aggregate output metadata must match its declared schema");
    assert!(
        error.to_string().contains(
            "aggregate operation, input type, output type, and schema widths disagree"
        )
    );
}

#[test]
fn validates_distinct_grouping_contract_and_empty_schema() {
    let schema = schema("value");
    let valid = node(
        Arc::clone(&schema),
        PhysicalPlanOperator::Distinct {
            input: pipeline(Arc::clone(&schema), ExpressionProgram::empty()),
            plan: AggregatePlan {
                groups: 1,
                estimated_groups: None,
                grouping_sets: Some(vec![vec![0]]),
                emit_empty_group_on_empty_input: false,
                aggs: Vec::new(),
                aggregate_input_types: Vec::new(),
                types: vec![VeinDataType::Int64],
            },
        },
    );
    client_plan(valid, Arc::clone(&schema))
        .validate()
        .expect("distinct should group its complete input schema");

    let empty_schema = Arc::new(VeinSchema::empty());
    let zero_column_distinct = node(
        Arc::clone(&empty_schema),
        PhysicalPlanOperator::Distinct {
            input: pipeline(Arc::clone(&empty_schema), ExpressionProgram::empty()),
            plan: AggregatePlan {
                groups: 0,
                estimated_groups: None,
                grouping_sets: Some(vec![vec![]]),
                emit_empty_group_on_empty_input: false,
                aggs: Vec::new(),
                aggregate_input_types: Vec::new(),
                types: Vec::new(),
            },
        },
    );
    client_plan(zero_column_distinct, empty_schema)
        .validate()
        .expect("zero-column distinct should preserve its empty grouping set");

    let malformed = node(
        Arc::clone(&schema),
        PhysicalPlanOperator::Distinct {
            input: pipeline(Arc::clone(&schema), ExpressionProgram::empty()),
            plan: AggregatePlan {
                groups: 1,
                estimated_groups: None,
                grouping_sets: Some(vec![vec![]]),
                emit_empty_group_on_empty_input: false,
                aggs: Vec::new(),
                aggregate_input_types: Vec::new(),
                types: vec![VeinDataType::Int64],
            },
        },
    );
    let error = client_plan(malformed, schema)
        .validate()
        .expect_err("distinct must not omit input columns from its grouping keys");
    assert!(
        error
            .to_string()
            .contains("distinct must group every input column")
    );
}
