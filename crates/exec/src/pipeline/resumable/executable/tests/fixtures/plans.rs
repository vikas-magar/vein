use common::protocol::{AggregatePlan, DataSink, DataSource, ExpressionProgram, JoinKind, JoinPlan,
                       LimitSpec, PhysicalPlan, PhysicalPlanNode, PhysicalPlanOperator, SortKey, SortPlan};
use std::sync::Arc;
use types::{ColumnId, ResolvedColumn, VeinDataType, VeinSchema, VeinSchemaRef};

pub(in crate::pipeline::resumable::executable::tests) fn schema() -> VeinSchemaRef {
    Arc::new(VeinSchema::new(vec![Arc::new(ResolvedColumn::new(ColumnId(1), "v", None, VeinDataType::Int64))]))
}
pub(in crate::pipeline::resumable::executable::tests) fn empty() -> VeinSchemaRef {
    Arc::new(VeinSchema::empty())
}
pub(in crate::pipeline::resumable::executable::tests) fn node(schema: VeinSchemaRef, operator: PhysicalPlanOperator) -> Arc<PhysicalPlanNode> {
    Arc::new(PhysicalPlanNode { output_schema: schema, operator })
}
pub(in crate::pipeline::resumable::executable::tests) fn source(schema: VeinSchemaRef) -> Arc<PhysicalPlanNode> {
    node(Arc::clone(&schema), PhysicalPlanOperator::Pipeline {
        source: Arc::new(DataSource::Memory),
        input_schema: schema,
        program: Arc::new(ExpressionProgram::empty()),
    })
}
pub(in crate::pipeline::resumable::executable::tests) fn client(input: Arc<PhysicalPlanNode>) -> PhysicalPlan {
    PhysicalPlan {
        root: node(Arc::clone(&input.output_schema),
                   PhysicalPlanOperator::Sink { input, sink: Arc::new(DataSink::Discard) })
    }
}
pub(in crate::pipeline::resumable::executable::tests) fn join(probe: Arc<PhysicalPlanNode>, build: Arc<PhysicalPlanNode>) -> Arc<PhysicalPlanNode> {
    node(empty(), PhysicalPlanOperator::Join {
        probe,
        build,
        plan: JoinPlan {
            join_type: JoinKind::Inner,
            probe_keys: vec![],
            build_keys: vec![],
            residual: None,
            outputs: vec![],
            probe_types: vec![],
            build_types: vec![],
        },
    })
}
pub(in crate::pipeline::resumable::executable::tests) fn aggregate(input: Arc<PhysicalPlanNode>) -> Arc<PhysicalPlanNode> {
    node(empty(), PhysicalPlanOperator::Aggregate {
        input,
        plan: AggregatePlan {
            groups: 0,
            estimated_groups: None,
            grouping_sets: None,
            emit_empty_group_on_empty_input: true,
            aggs: vec![],
            aggregate_input_types: vec![],
            types: vec![],
        },
    })
}
pub(in crate::pipeline::resumable::executable::tests) fn sort(input: Arc<PhysicalPlanNode>) -> Arc<PhysicalPlanNode> {
    let keys = if input.output_schema.fields.is_empty() { vec![] } else { vec![SortKey { column: 0, ascending: true, nulls_first: false }] };
    node(Arc::clone(&input.output_schema), PhysicalPlanOperator::Sort {
        output_columns: input.output_schema.fields.len() as u32,
        input,
        plan: SortPlan { keys },
        key_program: None,
    })
}
pub(in crate::pipeline::resumable::executable::tests) fn limit(input: Arc<PhysicalPlanNode>) -> Arc<PhysicalPlanNode> {
    node(Arc::clone(&input.output_schema), PhysicalPlanOperator::Limit { input, spec: LimitSpec { offset: 0, count: 3 } })
}
pub(in crate::pipeline::resumable::executable::tests) fn pointer(node: &Arc<PhysicalPlanNode>) -> usize { Arc::as_ptr(node) as usize }
