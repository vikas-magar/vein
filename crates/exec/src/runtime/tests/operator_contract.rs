use super::*;

#[test]
fn whole_batch_adapter_checks_the_canonical_operator_contract() {
    let source = Arc::new(PhysicalPlanNode {
        output_schema: Arc::new(types::VeinSchema::empty()),
        operator: PhysicalPlanOperator::Pipeline {
            source: Arc::new(DataSource::Memory),
            input_schema: Arc::new(types::VeinSchema::empty()),
            program: Arc::new(MicroProgram::empty()),
        },
    });
    let aggregate = AggregatePlan {
        groups: 0,
        estimated_groups: None,
        grouping_sets: None,
        emit_empty_group_on_empty_input: true,
        aggs: Vec::new(),
        aggregate_input_types: Vec::new(),
        types: Vec::new(),
    };
    let sink = || {
        Box::new(MaterializationCollector {
            chunks: Arc::new(Mutex::new(Vec::new())),
            flushed: Arc::new(AtomicBool::new(false)),
        }) as Box<dyn PipelineNode<DataChunk>>
    };
    for (operator, boundary) in [
        (
            PhysicalPlanOperator::Aggregate {
                input: Arc::clone(&source),
                plan: aggregate.clone(),
            },
            "aggregate",
        ),
        (
            PhysicalPlanOperator::Distinct {
                input: Arc::clone(&source),
                plan: aggregate,
            },
            "distinct",
        ),
    ] {
        let adapter = MaterializeViewNode::for_operator(sink(), &operator).unwrap();
        assert_eq!(adapter.boundary, boundary);
    }
    let limit = PhysicalPlanOperator::Limit {
        input: Arc::clone(&source),
        spec: LimitSpec {
            offset: 0,
            count: 1,
        },
    };
    let discard = PhysicalPlanOperator::Sink {
        input: Arc::clone(&source),
        sink: Arc::new(DataSink::Discard),
    };
    let client = PhysicalPlanOperator::Sink {
        input: Arc::clone(&source),
        sink: Arc::new(DataSink::Client {
            schema: Arc::new(types::VeinSchema::empty()),
        }),
    };
    for operator in [&source.operator, &limit, &discard, &client] {
        let error = MaterializeViewNode::for_operator(sink(), operator)
            .err()
            .unwrap();
        assert!(
            error
                .to_string()
                .contains("does not require whole-batch materialization")
        );
        assert!(error.to_string().contains(operator.data_flow().operator));
    }
}

#[test]
fn materialization_boundary_preserves_values_and_accumulates_conversion_work() {
    use arrow::array::Int64Array;
    use common::arrow::SelectionVector;

    let chunks = Arc::new(Mutex::new(Vec::new()));
    let flushed = Arc::new(AtomicBool::new(false));
    let mut node = MaterializeViewNode::new(
        Box::new(MaterializationCollector {
            chunks: Arc::clone(&chunks),
            flushed: Arc::clone(&flushed),
        }),
        "result",
    );
    let source: arrow::array::ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30]));
    for selection in [
        SelectionVector::new_dense(3),
        SelectionVector::new_sparse(vec![2, 0]),
    ] {
        node.push(
            DataChunkView::from_chunk(DataChunk::new(vec![Arc::clone(&source)], selection))
                .unwrap(),
        )
            .unwrap();
    }
    assert_eq!(node.batches, 2);
    assert_eq!(node.rows, 5);
    assert_eq!(node.report.shared_columns, 1);
    assert_eq!(node.report.gathered_columns, 1);
    let output = chunks.lock().unwrap();
    let shared = output[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let gathered = output[1]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(
        shared.values().as_ptr(),
        source
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .as_ptr()
    );
    assert_eq!(gathered, &Int64Array::from(vec![30, 10]));
    assert_eq!(
        node.report.gathered_output_buffer_bytes,
        gathered.get_buffer_memory_size()
    );
    drop(output);
    assert!(!node.finished());
    node.flush().unwrap();
    assert!(flushed.load(Ordering::Relaxed));
    assert!(node.finished());
}

#[test]
fn physical_join_kinds_map_to_their_executor_types() {
    let mappings = [
        (JoinKind::Inner, JoinType::Inner),
        (JoinKind::Left, JoinType::Left),
        (JoinKind::Right, JoinType::Right),
        (JoinKind::Full, JoinType::Full),
        (JoinKind::Semi, JoinType::Semi),
        (JoinKind::Anti, JoinType::Anti),
        (JoinKind::Single, JoinType::Single),
        (JoinKind::Cross, JoinType::Cross),
    ];

    for (physical, executor) in mappings {
        assert_eq!(
            join_type(physical).expect("join flavor is supported"),
            executor
        );
    }
}
