use super::*;
use arrow::array::{ArrayRef, Int64Array};
use common::Result;
use common::protocol::{
    AggOpCode, AggregatePlan, DataSource, ExpressionProgram, JoinKind, JoinOutput, JoinPlan,
    LimitSpec, SortKey, SortPlan,
};
use std::io::Write;
use std::sync::Arc;
use types::{VeinDataType, VeinSchema};

fn node(operator: PhysicalPlanOperator) -> Arc<PhysicalPlanNode> {
    Arc::new(PhysicalPlanNode {
        output_schema: Arc::new(VeinSchema::empty()),
        operator,
    })
}

fn source() -> Arc<PhysicalPlanNode> {
    node(PhysicalPlanOperator::Pipeline {
        source: Arc::new(DataSource::Memory),
        input_schema: Arc::new(VeinSchema::empty()),
        program: Arc::new(ExpressionProgram::empty()),
    })
}

struct Discard {
    expected: ArrayRef,
}

#[async_trait::async_trait]
impl PipelineNode for Discard {
    async fn push(&mut self, batch: DataChunkView) -> Result<()> {
        let (array, _) = batch.resolve(0, 0)?.expect("source row remains present");
        assert!(
            Arc::ptr_eq(array, &self.expected),
            "observer must preserve source arrays"
        );
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}


mod plan;
mod observation;
mod cancellation;
