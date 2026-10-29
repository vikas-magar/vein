use super::*;
use super::{
    AggOpCode, AggregatePlan, DataSink, DataSource, ExpressionProgram, InputMaterialization, JoinKind, JoinOutput,
    JoinPlan, LimitSpec, OutputRepresentation, PhysicalPlan, PhysicalPlanNode,
    PhysicalPlanOperator, SortKey, SortPlan,
};
use crate::identifiers::IntermediateSourceId;
use std::sync::Arc;
use types::VeinSchema;

fn node(operator: PhysicalPlanOperator) -> Arc<PhysicalPlanNode> {
    Arc::new(PhysicalPlanNode {
        output_schema: Arc::new(VeinSchema::empty()),
        operator,
    })
}
fn operator_names(node: &PhysicalPlanNode, names: &mut Vec<&'static str>) {
    let contract = node.operator.data_flow();
    // Exhaustive against the operator enum: a new variant requires both a
    // production contract and regression expectations, without a wildcard.
    let (input, output) = match &node.operator {
        PhysicalPlanOperator::Extension { .. } => (InputMaterialization::ImplementationDefined, OutputRepresentation::ImplementationDefined),
        PhysicalPlanOperator::Pipeline { .. } | PhysicalPlanOperator::Transform { .. } => (
            InputMaterialization::KernelColumns,
            OutputRepresentation::ReferencesAndComputed,
        ),
        PhysicalPlanOperator::Join { .. } | PhysicalPlanOperator::Sort { .. } => (
            InputMaterialization::KernelColumns,
            OutputRepresentation::References,
        ),
        PhysicalPlanOperator::Limit { .. } => {
            (InputMaterialization::None, OutputRepresentation::References)
        }
        PhysicalPlanOperator::Aggregate { .. } => (
            InputMaterialization::KernelColumns,
            OutputRepresentation::ResultArrays,
        ),
        PhysicalPlanOperator::Distinct { .. } => (
            InputMaterialization::KernelColumns,
            OutputRepresentation::ResultArrays,
        ),
        PhysicalPlanOperator::Sink { sink, .. } => (
            match sink.as_ref() {
                DataSink::Client { .. } => InputMaterialization::ConsumerBoundary,
                DataSink::Storage(_) => InputMaterialization::WholeBatch,
                DataSink::Discard => InputMaterialization::None,
            },
            OutputRepresentation::Sink,
        ),
    };
    assert_eq!(contract.input_materialization, input);
    assert_eq!(contract.output, output);
    match &node.operator {
        PhysicalPlanOperator::Extension { inputs, .. } => {
            for input in inputs.iter() { operator_names(input, names); }
            names.push("extension");
        }
        PhysicalPlanOperator::Pipeline { .. } => names.push("pipeline"),
        PhysicalPlanOperator::Transform { input, .. } => {
            operator_names(input, names);
            names.push("transform");
        }
        PhysicalPlanOperator::Join { probe, build, .. } => {
            operator_names(probe, names);
            operator_names(build, names);
            names.push("join");
        }
        PhysicalPlanOperator::Aggregate { input, .. } => {
            operator_names(input, names);
            names.push("aggregate");
        }
        PhysicalPlanOperator::Distinct { input, .. } => {
            operator_names(input, names);
            names.push("distinct");
        }
        PhysicalPlanOperator::Sort { input, .. } => {
            operator_names(input, names);
            names.push("sort");
        }
        PhysicalPlanOperator::Limit { input, .. } => {
            operator_names(input, names);
            names.push("limit");
        }
        PhysicalPlanOperator::Sink { input, .. } => {
            operator_names(input, names);
            names.push("sink");
        }
    }
    assert_eq!(names.last().copied(), Some(contract.operator));
}

mod io;
mod tree;
mod checkpoint;
