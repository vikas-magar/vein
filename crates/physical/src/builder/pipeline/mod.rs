//! Linear pipeline builder abstraction: translates unbroken operator sequences into a single [`Stage`].

use crate::stage::Stage;
use common::identifiers::{QueryId, StageId};
use common::protocol::{DataSource, Operator};
use common::Result;
use error::VeinError;
use plan::schema::VeinSchema;
use plan::LogicalPlan;

/// Trait for translating linear, non-blocking logical plans into a single physical [`Stage`].
pub trait PipelineStageBuilder: Send + Sync {
    /// Translates a linear logical plan into a single physical stage with `DataSink::Client`.
    fn build(
        &self,
        query_id: QueryId,
        plan: &LogicalPlan,
        stages: &mut Vec<Stage>,
    ) -> Result<StageId>;

    /// Recursively collects the leaf data source and intermediate operators in execution order.
    fn collect_pipeline(
        &self,
        plan: &LogicalPlan,
        operators: &mut Vec<Operator>,
    ) -> Result<(DataSource, VeinSchema)>;
}

/// Default linear pipeline stage builder abstraction.
#[derive(Debug, Default, Clone)]
pub struct DefaultPipelineStageBuilder;

impl DefaultPipelineStageBuilder {
    /// Creates a new default linear pipeline stage builder.
    pub fn new() -> Self {
        Self
    }
}

impl PipelineStageBuilder for DefaultPipelineStageBuilder {
    fn build(
        &self,
        _query_id: QueryId,
        _plan: &LogicalPlan,
        _stages: &mut Vec<Stage>,
    ) -> Result<StageId> {
        Err(VeinError::internal("DefaultPipelineStageBuilder::build is not yet implemented"))
    }

    fn collect_pipeline(
        &self,
        _plan: &LogicalPlan,
        _operators: &mut Vec<Operator>,
    ) -> Result<(DataSource, VeinSchema)> {
        Err(VeinError::internal("DefaultPipelineStageBuilder::collect_pipeline is not yet implemented"))
    }
}
