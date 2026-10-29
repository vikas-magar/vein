use super::*;

pub(super) struct ObservedOutputNode {
    pub(super) next: Box<dyn PipelineNode>,
    pub(super) metrics: Arc<NodeMetrics>,
    pub(super) query_id: String,
    pub(super) task_id: String,
}

#[async_trait::async_trait]
impl PipelineNode for ObservedOutputNode {
    async fn push(&mut self, view: DataChunkView) -> common::Result<()> {
        let rows = view.num_rows() as u64;
        self.metrics.statistics.record_output(rows);

        if tracing::enabled!(target: "vein.audit", tracing::Level::DEBUG) {
            let (array_count, array_bytes) = view.source_array_footprint();
            self.metrics.source_array_count_seen.add(array_count as u64);
            self.metrics.source_array_bytes_seen.add(array_bytes as u64);
            tracing::debug!(
                target: "vein.audit",
                event = "physical_operator_batch",
                query_id = %self.query_id,
                task_id = %self.task_id,
                node_id = self.metrics.id,
                operator = self.metrics.operator,
                rows,
                columns = view.num_columns(),
                source_array_count = array_count,
                source_array_footprint_bytes = array_bytes,
                "physical_operator_batch"
            );
        }

        let started = Instant::now();
        let result = self.next.push(view).await;
        self.metrics
            .downstream_push_ns
            .add(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
        result
    }

    async fn flush(&mut self) -> common::Result<()> {
        self.next.flush().await
    }

    fn finished(&self) -> bool {
        self.next.finished()
    }

    fn fork_local(&self) -> common::Result<Option<Box<dyn PipelineNode>>> {
        let Some(next) = self.next.fork_local()? else {
            return Ok(None);
        };
        Ok(Some(Box::new(Self {
            next,
            metrics: Arc::clone(&self.metrics),
            query_id: self.query_id.clone(),
            task_id: self.task_id.clone(),
        })))
    }

    fn combine_local(
        self: Box<Self>,
        canonical: &mut dyn PipelineNode,
    ) -> common::Result<()> {
        let target = canonical
            .as_any_mut()
            .downcast_mut::<ObservedOutputNode>()
            .ok_or_else(|| {
                error::VeinError::internal("trace local state has a mismatched target")
            })?;
        if !Arc::ptr_eq(&self.metrics, &target.metrics)
            || self.query_id != target.query_id
            || self.task_id != target.task_id
        {
            return Err(error::VeinError::internal(
                "cannot combine trace nodes from different executions",
            ));
        }
        self.next.combine_local(target.next.as_mut())
    }
}
