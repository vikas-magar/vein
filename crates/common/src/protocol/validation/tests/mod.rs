use super::*;
use crate::protocol::{
    AggregatePlan, DataSource, ExprKind, ExpressionProgram, ExpressionStep, JoinKind,
    JoinPlan, LimitSpec, PhysicalExpr, PhysicalPlanNode, SortPlan,
};
use std::sync::Arc;
use types::{ColumnId, ResolvedColumn, VeinDataType};

fn schema(name: &str) -> Arc<VeinSchema> {
    Arc::new(VeinSchema::new(vec![Arc::new(ResolvedColumn::new(
        ColumnId(0),
        name,
        None,
        VeinDataType::Int64,
    ))]))
}

fn node(schema: Arc<VeinSchema>, operator: PhysicalPlanOperator) -> Arc<PhysicalPlanNode> {
    Arc::new(PhysicalPlanNode {
        output_schema: schema,
        operator,
    })
}

fn pipeline(schema: Arc<VeinSchema>, program: ExpressionProgram) -> Arc<PhysicalPlanNode> {
    node(
        Arc::clone(&schema),
        PhysicalPlanOperator::Pipeline {
            source: Arc::new(DataSource::Memory),
            input_schema: schema,
            program: Arc::new(program),
        },
    )
}

fn client_plan(input: Arc<PhysicalPlanNode>, schema: Arc<VeinSchema>) -> PhysicalPlan {
    PhysicalPlan {
        root: node(
            Arc::clone(&schema),
            PhysicalPlanOperator::Sink {
                input,
                sink: Arc::new(DataSink::Client { schema }),
            },
        ),
    }
}

mod expression;
mod join;
mod reduction;
