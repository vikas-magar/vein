//! Safe, finite checkpoint selection and replacement for adaptive continuations.

use std::sync::Arc;

use common::identifiers::IntermediateSourceId;
use common::statistics::QueryStatisticsContext;
use common::{Result, VeinError};
use plan::{LogicalPlan, PlanTraversal};

mod execution;
mod policy;
pub(super) use policy::StatisticsCheckpointPolicy;
pub(super) use execution::{AdaptiveCoordinator, prepare_checkpoint};

#[cfg(test)]
#[path = "adaptive/integration_tests.rs"]
mod integration_tests;

/// Chooses one complete input to an unexecuted join. Implementations can use
/// source metadata and prior checkpoint observations from the same query.
pub trait AdaptiveCheckpointPolicy: Send + Sync {
    /// Returns a boundary only when another join can benefit from its feedback.
    fn choose_boundary(
        &self,
        plan: &LogicalPlan,
        statistics: Arc<QueryStatisticsContext>,
    ) -> Option<AdaptiveBoundary>;
}

/// A complete subtree consumed exactly once before the remaining plan resumes.
#[derive(Clone)]
pub struct AdaptiveBoundary {
    /// Producer subtree. Its original predicates and projections stay intact.
    pub producer: LogicalPlan,
    path: Vec<usize>,
    output_schema: Box<plan::VeinSchema>,
    estimated_rows: Option<u64>,
}

impl AdaptiveBoundary {
    /// Diagnostic label, independent of source format or table naming rules.
    pub fn table_name(&self) -> &str {
        self.producer
            .table_name()
            .unwrap_or(self.producer.operator_name())
    }

    /// Output population estimated before executing this subtree.
    pub fn estimated_rows(&self) -> Option<u64> {
        self.estimated_rows
    }

    /// Schema of the exact population used by the checkpoint's synopses.
    pub fn output_schema(&self) -> Arc<plan::VeinSchema> {
        Arc::new(*self.output_schema.clone())
    }

    /// Replace exactly this structural occurrence, preserving self-join lineage.
    pub fn replace(
        &self,
        plan: LogicalPlan,
        source_id: IntermediateSourceId,
        row_count: u64,
    ) -> Result<LogicalPlan> {
        replace_at(
            plan,
            &self.path,
            source_id.into_raw(),
            row_count,
            &self.output_schema,
        )
    }
}

fn replace_at(
    plan: LogicalPlan,
    path: &[usize],
    source_id: u128,
    rows: u64,
    schema: &plan::VeinSchema,
) -> Result<LogicalPlan> {
    let Some((&target, remaining)) = path.split_first() else {
        return Ok(LogicalPlan::Intermediate {
            source_id,
            row_count: rows,
            schema: Box::new(schema.clone()),
        });
    };
    let mut index = 0;
    let mut replaced = false;
    let result = plan.map_children(&mut |child| {
        let current = index;
        index += 1;
        if current == target {
            replaced = true;
            Ok(Box::new(replace_at(
                *child, remaining, source_id, rows, schema,
            )?))
        } else {
            Ok(child)
        }
    })?;
    if !replaced {
        return Err(VeinError::planning(
            "adaptive checkpoint path no longer exists",
            None,
        ));
    }
    Ok(result)
}

#[cfg(test)]
#[path = "adaptive/tests.rs"]
mod tests;
