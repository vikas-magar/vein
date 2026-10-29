use super::super::super::{
    ExecutablePipelinePlan, PipelineOperatorProvider, PipelineOperatorState,
};
use super::super::fixtures::{client, limit, pointer, schema, source};
use super::stage::{Action, Record, Script};
use crate::pipeline::resumable::executor::PipelineExecutor;
use crate::pipeline::resumable::operator::OperatorAdapter;
use crate::pipeline::resumable::result::ResultProducer;
use crate::pipeline::resumable::{
    ErasedResumableOperator, InputProperties, OperatorCapabilities, OperatorFactoryRegistry,
    OperatorMetadata, OrderingGuarantee, ParallelismCapability, PartitioningGuarantee,
};
use common::protocol::{PhysicalPlanNode, pipeline_graph::PipelinePhase};
use common::{Result, VeinError};
use std::any::Any;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

type ScriptEntry = (VecDeque<Action>, Arc<Mutex<Record>>);
struct Provider {
    scripts: Mutex<HashMap<usize, ScriptEntry>>,
    result: Mutex<Option<ResultProducer>>,
}
struct State {
    metadata: Arc<OperatorMetadata>,
    actions: Mutex<Option<VecDeque<Action>>>,
    record: Arc<Mutex<Record>>,
    result: Mutex<Option<ResultProducer>>,
}

impl PipelineOperatorProvider for Provider {
    fn prepare(
        &self,
        node: &Arc<PhysicalPlanNode>,
        metadata: Arc<OperatorMetadata>,
        _: &[PipelinePhase],
    ) -> Result<Arc<dyn PipelineOperatorState>> {
        let (actions, record) = self.scripts.lock().unwrap().remove(&pointer(node)).unwrap();
        let result = if metadata.role == crate::pipeline::resumable::OperatorRole::Sink {
            self.result.lock().unwrap().take()
        } else { None };
        Ok(Arc::new(State {
            metadata,
            actions: Mutex::new(Some(actions)),
            record,
            result: Mutex::new(result),
        }))
    }
}

impl PipelineOperatorState for State {
    fn capabilities(&self, _: PipelinePhase) -> Result<OperatorCapabilities> {
        Ok(OperatorCapabilities::new(
            vec![InputProperties::any(); self.metadata.children.len()],
            OrderingGuarantee::Unknown,
            PartitioningGuarantee::Unknown,
            ParallelismCapability::CombineLocalState,
        ))
    }
    fn create_stage(&self, phase: PipelinePhase) -> Result<Box<dyn ErasedResumableOperator>> {
        if let Some(producer) = self.result.lock().unwrap().take() {
            return Ok(Box::new(OperatorAdapter::for_phase(producer.into_sink(), &self.metadata, phase)?));
        }
        let script = Script {
            actions: self.actions.lock().unwrap().take().unwrap(),
            record: Arc::clone(&self.record),
            sink: phase.role() == crate::pipeline::resumable::OperatorRole::Sink,
            input: None,
        };
        Ok(Box::new(OperatorAdapter::for_phase(
            script,
            &self.metadata,
            phase,
        )?))
    }
    fn combine_stage(&self, _: PipelinePhase, local: Box<dyn Any + Send>) -> Result<()> {
        local
            .downcast::<()>()
            .map_err(|_| VeinError::internal("wrong test local state"))?;
        self.record.lock().unwrap().combined += 1;
        Ok(())
    }
}

pub(super) fn pipeline(scripts: Vec<Vec<Action>>) -> (PipelineExecutor, Vec<Arc<Mutex<Record>>>) {
    let (prepared, records) = prepared(scripts);
    let worker = prepared.pipelines()[0].create_worker().unwrap();
    (PipelineExecutor::new(worker), records)
}

pub(in crate::pipeline::resumable::executable::tests) fn prepared(
    scripts: Vec<Vec<Action>>,
) -> (ExecutablePipelinePlan, Vec<Arc<Mutex<Record>>>) {
    prepared_result(scripts, None)
}

pub(in crate::pipeline::resumable::executable::tests) fn prepared_result(
    scripts: Vec<Vec<Action>>, result: Option<ResultProducer>,
) -> (ExecutablePipelinePlan, Vec<Arc<Mutex<Record>>>) {
    assert!(scripts.len() >= 2);
    let mut current = source(schema());
    let mut nodes = vec![Arc::clone(&current)];
    for _ in 2..scripts.len() {
        current = limit(current);
        nodes.push(Arc::clone(&current));
    }
    let plan = client(current);
    nodes.push(Arc::clone(&plan.root));
    let mut entries = HashMap::new();
    let mut records = Vec::new();
    for (node, script) in nodes.iter().zip(scripts) {
        let record = Arc::new(Mutex::new(Record::default()));
        entries.insert(pointer(node), (script.into(), Arc::clone(&record)));
        records.push(record);
    }
    let prepared = ExecutablePipelinePlan::prepare(
        &plan,
        &OperatorFactoryRegistry::new(),
        &Provider { scripts: Mutex::new(entries), result: Mutex::new(result) },
    )
        .unwrap();
    (prepared, records)
}
