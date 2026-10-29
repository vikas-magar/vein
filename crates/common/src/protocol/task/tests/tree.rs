use super::*;
#[test]
fn canonical_physical_plan_round_trips_every_operator_node() {
    let leaf = || {
        node(PhysicalPlanOperator::Pipeline {
            source: Arc::new(DataSource::Memory),
            input_schema: Arc::new(VeinSchema::empty()),
            program: Arc::new(ExpressionProgram::empty()),
        })
    };
    let join = node(PhysicalPlanOperator::Join {
        probe: leaf(),
        build: leaf(),
        plan: JoinPlan {
            join_type: JoinKind::Inner,
            probe_keys: vec![0],
            build_keys: vec![0],
            residual: None,
            outputs: vec![JoinOutput::Probe(0)],
            probe_types: vec![types::VeinDataType::Int64],
            build_types: vec![types::VeinDataType::Int64],
        },
    });
    let transformed = node(PhysicalPlanOperator::Transform {
        input: join,
        program: Arc::new(ExpressionProgram::empty()),
    });
    let aggregated = node(PhysicalPlanOperator::Aggregate {
        input: transformed,
        plan: AggregatePlan {
            groups: 0,
            estimated_groups: None,
            grouping_sets: None,
            emit_empty_group_on_empty_input: true,
            aggs: vec![AggOpCode::Count],
            aggregate_input_types: vec![types::VeinDataType::Int64],
            types: vec![types::VeinDataType::Int64],
        },
    });
    let distinct = node(PhysicalPlanOperator::Distinct {
        input: aggregated,
        plan: AggregatePlan {
            groups: 0,
            estimated_groups: None,
            grouping_sets: Some(vec![vec![]]),
            emit_empty_group_on_empty_input: false,
            aggs: Vec::new(),
            aggregate_input_types: Vec::new(),
            types: Vec::new(),
        },
    });
    let sorted = node(PhysicalPlanOperator::Sort {
        input: distinct,
        plan: SortPlan {
            keys: vec![SortKey {
                column: 0,
                ascending: true,
                nulls_first: false,
            }],
        },
        key_program: None,
        output_columns: 1,
    });
    let limited = node(PhysicalPlanOperator::Limit {
        input: sorted,
        spec: LimitSpec {
            offset: 1,
            count: 10,
        },
    });
    let plan = PhysicalPlan {
        root: node(PhysicalPlanOperator::Sink {
            input: limited,
            sink: Arc::new(DataSink::Discard),
        }),
    };

    let encoded = serde_json::to_vec(&plan).expect("serialize canonical physical plan");
    let wire = std::str::from_utf8(&encoded).unwrap();
    assert!(!wire.contains("input_materialization"));
    assert!(!wire.contains("output_representation"));
    let decoded: PhysicalPlan =
        serde_json::from_slice(&encoded).expect("deserialize canonical physical plan");
    let mut names = Vec::new();
    operator_names(&decoded.root, &mut names);

    assert_eq!(
        names,
        vec![
            "pipeline",
            "pipeline",
            "join",
            "transform",
            "aggregate",
            "distinct",
            "sort",
            "limit",
            "sink",
        ]
    );
}

#[test]
fn canonical_plan_clones_share_immutable_operator_nodes() {
    let child = node(PhysicalPlanOperator::Pipeline {
        source: Arc::new(DataSource::Memory),
        input_schema: Arc::new(VeinSchema::empty()),
        program: Arc::new(ExpressionProgram::empty()),
    });
    let plan = PhysicalPlan {
        root: node(PhysicalPlanOperator::Limit {
            input: Arc::clone(&child),
            spec: LimitSpec {
                offset: 0,
                count: 5,
            },
        }),
    };
    let cloned = plan.clone();

    assert!(Arc::ptr_eq(&plan.root, &cloned.root));
    let (
        PhysicalPlanOperator::Limit { input, .. },
        PhysicalPlanOperator::Limit {
            input: cloned_input,
            ..
        },
    ) = (&plan.root.operator, &cloned.root.operator)
    else {
        panic!("cloned plan should retain its root node");
    };
    assert!(Arc::ptr_eq(input, cloned_input));
}
