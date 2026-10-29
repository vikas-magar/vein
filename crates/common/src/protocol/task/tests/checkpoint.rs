use super::*;
#[test]
fn checkpoint_split_preserves_producer_and_replaces_only_the_continuation_input() {
    let schema = Arc::new(VeinSchema::empty());
    let source = node(PhysicalPlanOperator::Pipeline {
        source: Arc::new(DataSource::Memory),
        input_schema: Arc::clone(&schema),
        program: Arc::new(ExpressionProgram::empty()),
    });
    let boundary = Arc::new(PhysicalPlanNode {
        output_schema: Arc::clone(&schema),
        operator: PhysicalPlanOperator::Transform {
            input: source,
            program: Arc::new(ExpressionProgram::empty()),
        },
    });
    let plan = PhysicalPlan {
        root: Arc::new(PhysicalPlanNode {
            output_schema: Arc::clone(&schema),
            operator: PhysicalPlanOperator::Sink {
                input: Arc::clone(&boundary),
                sink: Arc::new(DataSink::Client {
                    schema: Arc::clone(&schema),
                }),
            },
        }),
    };
    let source_id = IntermediateSourceId::new();

    let fragments = plan
        .split_at_checkpoint(&boundary, source_id)
        .expect("selected subplan should split into two fragments");
    let PhysicalPlanOperator::Sink {
        input: producer_input,
        ..
    } = &fragments.producer.root.operator
    else {
        panic!("producer fragment must end at a checkpoint sink");
    };
    assert!(Arc::ptr_eq(producer_input, &boundary));

    let PhysicalPlanOperator::Sink {
        input: continuation_input,
        ..
    } = &fragments.continuation.root.operator
    else {
        panic!("continuation must preserve the original output sink");
    };
    // The selected transform runs in the producer. Its entire subtree is
    // replaced in the continuation so it cannot be executed a second time.
    let PhysicalPlanOperator::Pipeline { source, .. } = &continuation_input.operator
    else {
        panic!("boundary should be replaced by an intermediate source");
    };
    let DataSource::Intermediate(actual) = source.as_ref() else {
        panic!("continuation must read the checkpoint relation");
    };
    assert_eq!(*actual, source_id);

    assert!(
        plan.split_at_checkpoint(&plan.root, source_id).is_none(),
        "the terminal sink cannot be used as a checkpoint boundary"
    );
}
