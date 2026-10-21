//! Implementation of the runtime task-executor contract for the default engine.

use async_trait::async_trait;
use common::Result;
use common::cancellation::CancellationToken;
use common::identifiers::{IntermediateSourceId, QueryId};
use common::protocol::{DataSink, ExecuteTask};
use error::VeinError;
use std::sync::{Arc, Mutex};

use crate::pipeline::DataChunkView;
use crate::runtime::checkpoint::{CheckpointTaskResult, IntermediateRelation};

use super::{DefaultTaskExecutor, QueryExecutionResources, TaskExecutor, TaskResult};

#[async_trait]
impl TaskExecutor for DefaultTaskExecutor {
    fn create_query_resources(
        &self,
        cancel: CancellationToken,
    ) -> common::Result<Arc<QueryExecutionResources>> {
        let spill_storage = self.spill_storage_factory.create_for_query()?;
        let memory = crate::memory::QueryMemoryTracker::with_spill_storage(
            self.resource_limits,
            cancel,
            spill_storage,
        );
        Ok(Arc::new(QueryExecutionResources { memory }))
    }

    async fn execute(&self, task: &ExecuteTask, cancel: CancellationToken) -> Result<TaskResult> {
        self.execute_with_output(task, cancel, |builder, sink, _memory| builder.build_sink(sink))
            .await
    }

    async fn execute_to_checkpoint(
        &self,
        task: &ExecuteTask,
        cancel: CancellationToken,
    ) -> Result<CheckpointTaskResult> {
        let resources = self.create_query_resources(cancel.clone())?;
        self.execute_to_checkpoint_with_resources(task, cancel, resources).await
    }

    async fn execute_to_checkpoint_with_resources(
        &self,
        task: &ExecuteTask,
        cancel: CancellationToken,
        resources: Arc<QueryExecutionResources>,
    ) -> Result<CheckpointTaskResult> {
        let relation_slot = Arc::new(Mutex::new(None));
        let slot = Arc::clone(&relation_slot);
        let result = self
            .execute_with_output_and_resources(
                task,
                cancel,
                Some(Arc::clone(&resources)),
                move |_builder, sink, memory| {
                    let DataSink::Client { schema } = sink else {
                        return Err(VeinError::planning(
                            "checkpoint output requires a client-result task",
                            None,
                        ));
                    };
                    let relation = IntermediateRelation::new(
                        Arc::new(schema.to_arrow()),
                        memory.clone(),
                    );
                    *slot.lock().map_err(|_| {
                        VeinError::internal("checkpoint result slot poisoned")
                    })? = Some(relation.clone());
                    Ok(Box::new(relation.sink()))
                },
            )
            .await?;
        let relation = relation_slot
            .lock()
            .map_err(|_| VeinError::internal("checkpoint result slot poisoned"))?
            .take()
            .ok_or_else(|| VeinError::internal("checkpoint task completed without a relation"))?;
        Ok(CheckpointTaskResult {
            relation,
            row_count: result.row_count,
            metrics: result.metrics,
        })
    }

    fn register_intermediate_source(
        &self,
        query_id: QueryId,
        source_id: IntermediateSourceId,
        relation: IntermediateRelation,
    ) -> common::Result<crate::runtime::checkpoint::IntermediateSourceRegistration> {
        self.intermediate_sources.register(query_id, source_id, relation)
    }

    async fn execute_streaming(
        &self,
        task: &ExecuteTask,
        cancel: CancellationToken,
        output: flume::Sender<DataChunkView>,
    ) -> Result<TaskResult> {
        self.execute_with_output(task, cancel, move |builder, sink, _memory| {
            builder.build_streaming_sink(sink, output)
        })
            .await
    }

    async fn execute_streaming_with_resources(
        &self,
        task: &ExecuteTask,
        cancel: CancellationToken,
        output: flume::Sender<DataChunkView>,
        resources: Arc<QueryExecutionResources>,
    ) -> Result<TaskResult> {
        self.execute_with_output_and_resources(
            task,
            cancel,
            Some(resources),
            move |builder, sink, _memory| builder.build_streaming_sink(sink, output),
        )
            .await
    }
}
