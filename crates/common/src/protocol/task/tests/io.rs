use super::*;
#[test]
fn sort_plan_preserves_key_priority_and_null_order_over_wire_format() {
    let plan = SortPlan {
        keys: vec![
            SortKey {
                column: 3,
                ascending: false,
                nulls_first: true,
            },
            SortKey {
                column: 1,
                ascending: true,
                nulls_first: false,
            },
        ],
    };

    let encoded = serde_json::to_vec(&plan).expect("serialize sort plan");
    let decoded: SortPlan = serde_json::from_slice(&encoded).expect("deserialize sort plan");

    assert_eq!(decoded, plan);
}

#[test]
fn aggregate_estimate_round_trips_and_defaults_for_older_wire_plans() {
    let plan = AggregatePlan {
        groups: 1,
        estimated_groups: Some(12),
        grouping_sets: None,
        emit_empty_group_on_empty_input: true,
        aggs: vec![AggOpCode::Count],
        aggregate_input_types: vec![types::VeinDataType::Int64],
        types: vec![types::VeinDataType::Int64; 2],
    };
    let mut value = serde_json::to_value(&plan).expect("serialize aggregate plan");
    value
        .as_object_mut()
        .expect("aggregate plan object")
        .remove("estimated_groups");
    value
        .as_object_mut()
        .expect("aggregate plan object")
        .remove("emit_empty_group_on_empty_input");

    let decoded: AggregatePlan =
        serde_json::from_value(value).expect("deserialize legacy aggregate plan");

    assert_eq!(decoded.estimated_groups, None);
    assert!(decoded.emit_empty_group_on_empty_input);
}

#[test]
fn sink_contracts_follow_their_consumer_boundary() {
    let input = node(PhysicalPlanOperator::Pipeline {
        source: Arc::new(DataSource::Memory),
        input_schema: Arc::new(VeinSchema::empty()),
        program: Arc::new(ExpressionProgram::empty()),
    });
    for (sink, expected) in [
        (DataSink::Discard, InputMaterialization::None),
        (
            DataSink::Client {
                schema: Arc::new(VeinSchema::empty()),
            },
            InputMaterialization::ConsumerBoundary,
        ),
        (
            DataSink::Storage(super::StorageTarget::new(
                "test",
                Vec::new(),
                Arc::new(VeinSchema::empty()),
            )),
            InputMaterialization::WholeBatch,
        ),
    ] {
        let operator = PhysicalPlanOperator::Sink {
            input: Arc::clone(&input),
            sink: Arc::new(sink),
        };
        let contract = operator.data_flow();
        assert_eq!(contract.input_materialization, expected);
        assert_eq!(contract.output, OutputRepresentation::Sink);
    }
}
