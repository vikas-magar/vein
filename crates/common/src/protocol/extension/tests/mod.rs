use super::*;
use crate::protocol::{DataSink, DataSource, DerivedPipelineGraph, ExpressionProgram, PhysicalPlan, PhysicalPlanNode, PhysicalPlanOperator};
use std::sync::Arc;
use types::{ColumnId, ResolvedColumn, VeinDataType, VeinSchema, VeinSchemaRef};

fn schema() -> VeinSchemaRef {
    Arc::new(VeinSchema::new(vec![Arc::new(ResolvedColumn::new(ColumnId(0), "v", None, VeinDataType::Int64))]))
}
fn source() -> Arc<PhysicalPlanNode> {
    let schema = schema();
    Arc::new(PhysicalPlanNode {
        output_schema: Arc::clone(&schema),
        operator: PhysicalPlanOperator::Pipeline {
            source: Arc::new(DataSource::Memory),
            input_schema: schema,
            program: Arc::new(ExpressionProgram::empty()),
        },
    })
}
fn descriptor(role: OperatorRole, modes: &[ExtensionInputMode]) -> PhysicalExtension {
    PhysicalExtension {
        factory_id: OperatorTypeId::new("tests.extension").unwrap(),
        configuration_version: 7,
        configuration: Arc::from([3, 1]),
        role,
        inputs: modes.iter().map(|mode| ExtensionInput { schema: schema(), mode: *mode })
            .collect::<Vec<_>>().into(),
    }
}
fn client(input: Arc<PhysicalPlanNode>) -> PhysicalPlan {
    PhysicalPlan {
        root: Arc::new(PhysicalPlanNode {
            output_schema: Arc::clone(&input.output_schema),
            operator: PhysicalPlanOperator::Sink { input, sink: Arc::new(DataSink::Discard) },
        })
    }
}
mod descriptor;
mod graph;
