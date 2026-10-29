use super::*;
#[tokio::test]
async fn canonical_node_ids_are_deterministic_preorder_ids() {
    let leaf = source();
    let root = node(PhysicalPlanOperator::Limit {
        input: Arc::clone(&leaf),
        spec: LimitSpec {
            offset: 1,
            count: 3,
        },
    });

    let first = ExecutionTrace::new(&root, "q".into(), "t".into());
    let second = ExecutionTrace::new(&root, "q".into(), "t".into());

    assert_eq!(first.node_id(&root), 0);
    assert_eq!(first.node_id(&leaf), 1);
    assert_eq!(first.node_id(&root), second.node_id(&root));
    assert_eq!(first.nodes[0].child_ids.get().unwrap(), &[1]);
    let explain = first.format_explain();
    assert_eq!(
        explain,
        second.format_explain(),
        "explain IDs and ordering are stable"
    );
    assert!(explain.contains("#0 limit children=[1] [offset=1, count=3]"));
    assert!(
        explain.contains("#1 pipeline children=[] [source=memory, expression_steps=0]")
    );
    assert!(explain.contains("flow=reference-input->References"));
}

#[tokio::test]
async fn stable_explain_describes_chained_joins_aggregate_sort_and_boundaries() {
    let filtered = node(PhysicalPlanOperator::Transform {
        input: source(),
        program: Arc::new(ExpressionProgram::empty()),
    });
    let join_plan = || JoinPlan {
        join_type: JoinKind::Inner,
        probe_keys: vec![0],
        build_keys: vec![0],
        residual: None,
        outputs: vec![JoinOutput::Probe(0), JoinOutput::Build(0)],
        probe_types: vec![VeinDataType::Int64],
        build_types: vec![VeinDataType::Int64],
    };
    let first_join = node(PhysicalPlanOperator::Join {
        probe: filtered,
        build: source(),
        plan: join_plan(),
    });
    let second_join = node(PhysicalPlanOperator::Join {
        probe: first_join,
        build: source(),
        plan: join_plan(),
    });
    let aggregate = node(PhysicalPlanOperator::Aggregate {
        input: second_join,
        plan: AggregatePlan {
            groups: 0,
            estimated_groups: None,
            grouping_sets: Some(vec![Vec::new()]),
            emit_empty_group_on_empty_input: true,
            aggs: vec![AggOpCode::Count],
            aggregate_input_types: vec![VeinDataType::Int64],
            types: vec![VeinDataType::Int64],
        },
    });
    let root = node(PhysicalPlanOperator::Sort {
        input: aggregate,
        plan: SortPlan {
            keys: vec![SortKey {
                column: 0,
                ascending: false,
                nulls_first: false,
            }],
        },
        key_program: None,
        output_columns: 1,
    });

    let trace = ExecutionTrace::new(&root, "query".into(), "task".into());
    let explain = trace.format_explain();

    assert_eq!(
        trace.nodes.len(),
        8,
        "shared canonical nodes are visited once"
    );
    assert_eq!(explain.matches(" join children=").count(), 2);
    assert!(explain.contains("probe_keys=[0], build_keys=[0]"));
    assert!(explain.contains("aggregate children="));
    assert!(explain.contains("sort children="));
    assert!(explain.contains("flow=kernel-columns->ResultArrays"));
    assert!(
        explain.contains("keys=[SortKey { column: 0, ascending: false, nulls_first: false }]")
    );
}
