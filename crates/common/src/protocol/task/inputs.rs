use super::*;

/// Allocation-free iterator over children in canonical input-port order.
pub struct PhysicalInputs<'a> {
    first: Option<&'a Arc<PhysicalPlanNode>>,
    second: Option<&'a Arc<PhysicalPlanNode>>,
    remaining: std::slice::Iter<'a, Arc<PhysicalPlanNode>>,
}

impl<'a> Iterator for PhysicalInputs<'a> {
    type Item = &'a Arc<PhysicalPlanNode>;
    fn next(&mut self) -> Option<Self::Item> {
        self.first.take().or_else(|| self.second.take()).or_else(|| self.remaining.next())
    }
}

impl PhysicalPlanOperator {
    /// Borrows canonical children without copying subtrees or allocating a collection.
    pub fn inputs(&self) -> PhysicalInputs<'_> {
        let (first, second, remaining) = match self {
            Self::Pipeline { .. } => (None, None, [].as_slice()),
            Self::Extension { inputs, .. } => (None, None, inputs.as_ref()),
            Self::Join { probe, build, .. } => (Some(probe), Some(build), [].as_slice()),
            Self::Transform { input, .. } | Self::Aggregate { input, .. }
            | Self::Distinct { input, .. } | Self::Sort { input, .. }
            | Self::Limit { input, .. } | Self::Sink { input, .. } => (Some(input), None, [].as_slice()),
        };
        PhysicalInputs { first, second, remaining: remaining.iter() }
    }
}
