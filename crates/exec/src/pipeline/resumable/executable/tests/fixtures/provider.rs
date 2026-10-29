use super::super::super::{PipelineOperatorProvider, PipelineOperatorState};
use super::operator::{IdleOperator, Local};
use super::plans::pointer;
use crate::pipeline::resumable::{ErasedResumableOperator, InputProperties, OperatorCapabilities, OperatorMetadata,
                                 OrderingGuarantee, ParallelismCapability, PartitioningGuarantee};
use common::protocol::pipeline_graph::PipelinePhase;
use common::protocol::{PhysicalPlanNode, PhysicalPlanOperator};
use common::{Result, VeinError};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

pub(in crate::pipeline::resumable::executable::tests) struct Request {
    pub node: usize,
    pub phases: Vec<PipelinePhase>,
}
#[derive(Default)]
pub(in crate::pipeline::resumable::executable::tests) struct Record {
    pub created: Mutex<Vec<PipelinePhase>>,
    pub combined: Mutex<Vec<PipelinePhase>>,
}
#[derive(Default)]
pub(in crate::pipeline::resumable::executable::tests) struct Provider {
    pub requests: Mutex<Vec<Request>>,
    pub records: Mutex<HashMap<usize, Arc<Record>>>,
    pub requirements: HashMap<usize, InputProperties>,
    pub partitions: HashMap<usize, PartitioningGuarantee>,
    pub serial: HashSet<usize>,
    pub combine: bool,
}

struct State {
    node: Arc<PhysicalPlanNode>,
    metadata: Arc<OperatorMetadata>,
    record: Arc<Record>,
    requirement: Option<InputProperties>,
    partition: PartitioningGuarantee,
    parallelism: ParallelismCapability,
}

impl PipelineOperatorProvider for Provider {
    fn prepare(&self, node: &Arc<PhysicalPlanNode>, metadata: Arc<OperatorMetadata>, phases: &[PipelinePhase]) -> Result<Arc<dyn PipelineOperatorState>> {
        let identity = pointer(node);
        self.requests.lock().unwrap().push(Request { node: identity, phases: phases.to_vec() });
        let record = Arc::new(Record::default());
        self.records.lock().unwrap().insert(identity, Arc::clone(&record));
        Ok(Arc::new(State {
            node: Arc::clone(node),
            metadata,
            record,
            requirement: self.requirements.get(&identity).cloned(),
            partition: self.partitions.get(&identity).cloned().unwrap_or(PartitioningGuarantee::Unknown),
            parallelism: if self.serial.contains(&identity) { ParallelismCapability::Serial } else if self.combine { ParallelismCapability::CombineLocalState } else { ParallelismCapability::Independent },
        }))
    }
}

impl PipelineOperatorState for State {
    fn capabilities(&self, phase: PipelinePhase) -> Result<OperatorCapabilities> {
        let mut inputs = vec![InputProperties::any(); self.metadata.children.len()];
        if let Some(required) = &self.requirement { inputs[0] = required.clone(); }
        let order = match (&self.node.operator, phase) {
            (PhysicalPlanOperator::Sort { plan, key_program: None, .. }, PipelinePhase::Source) if !plan.keys.is_empty() =>
                OrderingGuarantee::Ordered(plan.keys.clone().into()),
            (PhysicalPlanOperator::Limit { .. }, PipelinePhase::Transform(_)) => OrderingGuarantee::PreservesInput(0),
            _ => OrderingGuarantee::Unknown,
        };
        let output_partition = if matches!(phase, PipelinePhase::Source | PipelinePhase::Transform(_)) {
            self.partition.clone()
        } else { PartitioningGuarantee::Unknown };
        Ok(OperatorCapabilities::new(inputs, order, output_partition, self.parallelism))
    }
    fn create_stage(&self, phase: PipelinePhase) -> Result<Box<dyn ErasedResumableOperator>> {
        self.record.created.lock().unwrap().push(phase);
        Ok(Box::new(IdleOperator::new(phase)))
    }
    fn combine_stage(&self, phase: PipelinePhase, local: Box<dyn std::any::Any + Send>) -> Result<()> {
        let local = local.downcast::<Local>().map_err(|_| VeinError::internal("invalid test local state"))?;
        assert_eq!(phase, local.phase);
        self.record.combined.lock().unwrap().push(phase);
        Ok(())
    }
}
