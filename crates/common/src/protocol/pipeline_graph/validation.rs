use super::*;
use std::collections::HashSet;

impl DerivedPipelineGraph {
    /// Rejects malformed stage boundaries, dependency identities and cycles.
    pub fn validate(&self) -> Result<()> {
        for (index, pipeline) in self.pipelines.iter().enumerate() {
            if pipeline.id != PipelineId(index) {
                return Err(VeinError::planning("pipeline identities must match deterministic construction order", None));
            }
            pipeline.stages()?;
        }
        let mut edges = HashSet::with_capacity(self.dependencies.len());
        for dependency in &self.dependencies {
            let pair = (dependency.prerequisite.0, dependency.dependent.0);
            if pair.0 >= self.pipelines.len() || pair.1 >= self.pipelines.len() {
                return Err(VeinError::planning("pipeline dependency references an unknown pipeline", None));
            }
            if !edges.insert(pair) {
                return Err(VeinError::planning("pipeline dependency is duplicated", None));
            }
        }
        if self.execution_order().is_none() {
            return Err(VeinError::planning("pipeline dependencies contain a cycle", None));
        }
        Ok(())
    }
}
