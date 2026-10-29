use super::*;

pub(super) struct DistinctActive {
    aggregate: usize,
    grouping_set: usize,
    columns: Vec<usize>,
    cursor: Box<AggregateOutputCursor>,
}
impl AggregateOutputCursor {
    pub(super) fn step_distinct(&mut self) -> Result<OperatorStep<()>> {
        let Some(mut active) = self.active.take() else {
            if let Some(collector) = self.distinct.pop_front() {
                self.active = Some(DistinctActive {
                    aggregate: collector.aggregate_index,
                    grouping_set: collector.grouping_set_id,
                    columns: collector.parent_columns,
                    cursor: Box::new(AggregateOutputCursor::new(*collector.sink, self.batch_rows)),
                });
            } else { self.phase = Phase::Prepare; }
            if self.active.is_some() { self.phase = Phase::Distinct; }
            return Ok(OperatorStep::Runnable);
        };
        match active.cursor.step()? {
            OperatorStep::Output(batch) => {
                self.sink.merge_distinct_batch(active.aggregate, active.grouping_set, &active.columns, &batch)?;
                self.active = Some(active);
            }
            OperatorStep::Finished => {}
            OperatorStep::Runnable => self.active = Some(active),
            _ => return Err(VeinError::internal("aggregate finalization requested unexpected external input")),
        }
        self.phase = Phase::Distinct;
        Ok(OperatorStep::Runnable)
    }
}
