use super::{PhysicalExtensionLowerer, PhysicalExtensionLoweringContext};
use crate::builder::{DefaultPipelineStageBuilder, PipelineStageBuilder};
use common::protocol::extension::{ExtensionInput, ExtensionInputMode, OperatorRole, OperatorTypeId, PhysicalExtension};
use common::protocol::{PhysicalPlanNode, PhysicalPlanOperator};
use plan::{ColumnId, LogicalPlan, PlanTraversal, ResolvedColumn, VeinDataType, VeinSchema};
use std::sync::Arc;

struct TestLowerer {
    role: OperatorRole,
    wrong_schema: bool,
}
impl PhysicalExtensionLowerer for TestLowerer {
    fn operator_type_id(&self) -> OperatorTypeId { OperatorTypeId::new("tests.lowering").unwrap() }
    fn lower(&self, logical: &LogicalPlan, context: &dyn PhysicalExtensionLoweringContext) -> common::Result<Option<Arc<PhysicalPlanNode>>> {
        let LogicalPlan::Call { input, name, .. } = logical else { return Ok(None) };
        if name != "test_extension" { return Ok(None); }
        let child = context.lower_input(input)?;
        let descriptor = PhysicalExtension {
            factory_id: self.operator_type_id(),
            configuration_version: 1,
            configuration: Arc::from([]),
            role: self.role,
            inputs: vec![ExtensionInput {
                schema: Arc::clone(&child.output_schema),
                mode: ExtensionInputMode::Streaming
            }].into(),
        };
        let output = if self.wrong_schema { VeinSchema::empty() } else { logical.schema().clone() };
        PhysicalPlanNode::extension(Arc::new(descriptor), vec![child], Arc::new(output)).map(Some)
    }
}

fn logical() -> LogicalPlan {
    let schema = VeinSchema::new(vec![Arc::new(ResolvedColumn::new(ColumnId(1), "v", None, VeinDataType::Int64))]);
    LogicalPlan::Call {
        input: Box::new(LogicalPlan::Intermediate {
            source_id: 1,
            row_count: 0,
            schema: Box::new(schema.clone()),
        }),
        name: "test_extension".into(),
        args: vec![],
        schema: Box::new(schema),
    }
}

#[test]
fn registered_lowering_builds_the_canonical_extension_and_derived_pipeline() {
    let builder = DefaultPipelineStageBuilder::new().with_extension_lowerer(Arc::new(
        TestLowerer { role: OperatorRole::Transform, wrong_schema: false })).unwrap();
    let stage = builder.build(&logical()).unwrap();
    assert_eq!(stage.tasks().len(), 1);
    let plan = &stage.tasks()[0].plan;
    plan.validate().unwrap();
    assert!(plan.has_extensions());
    let PhysicalPlanOperator::Sink { input, .. } = &plan.root.operator else { panic!() };
    let PhysicalPlanOperator::Extension { inputs, .. } = &input.operator else { panic!() };
    assert_eq!(inputs.len(), 1);
    assert_eq!(stage.pipeline_graph().pipelines().len(), 1);
}

#[test]
fn terminal_extension_lowering_keeps_the_extension_as_the_root() {
    let builder = DefaultPipelineStageBuilder::new().with_extension_lowerer(Arc::new(
        TestLowerer { role: OperatorRole::Sink, wrong_schema: false })).unwrap();
    let stage = builder.build(&logical()).unwrap();
    assert!(matches!(stage.tasks()[0].plan.root.operator, PhysicalPlanOperator::Extension { .. }));
    stage.tasks()[0].plan.validate().unwrap();
}

#[test]
fn duplicate_lowerers_and_output_schema_mismatches_are_rejected() {
    let builder = DefaultPipelineStageBuilder::new().with_extension_lowerer(Arc::new(
        TestLowerer { role: OperatorRole::Transform, wrong_schema: false })).unwrap();
    assert!(builder.with_extension_lowerer(Arc::new(
        TestLowerer { role: OperatorRole::Transform, wrong_schema: false })).is_err());
    let invalid = DefaultPipelineStageBuilder::new().with_extension_lowerer(Arc::new(
        TestLowerer { role: OperatorRole::Transform, wrong_schema: true })).unwrap();
    assert!(invalid.build(&logical()).is_err());
    assert!(DefaultPipelineStageBuilder::new().build(&logical()).is_err());
}
