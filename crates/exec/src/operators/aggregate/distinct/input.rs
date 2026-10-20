use super::super::*;
use super::DistinctCollector;

#[derive(Clone)]
enum DistinctInput {
    Chunk(DataChunk),
    View(DataChunkView),
}

impl DistinctInput {
    fn accumulate(self, sink: &mut AggregateSink) -> Result<()> {
        match self {
            Self::Chunk(chunk) => sink.accumulate(chunk),
            Self::View(view) => sink.accumulate_view(view),
        }
    }
}

impl AggregateSink {
    pub(in crate::operators::aggregate) fn initialize_distinct_collectors(&mut self) -> Result<()> {
        let mut collectors = Vec::new();
        for (aggregate_index, op) in self.plan.aggs.iter().enumerate() {
            if *op != AggOpCode::CountDistinct {
                continue;
            }
            for (grouping_set_id, grouping_set) in self.grouping_sets.iter().enumerate() {
                let mut parent_columns = grouping_set
                    .iter()
                    .map(|&column| column as usize)
                    .collect::<Vec<_>>();
                parent_columns.sort_unstable();
                let mut types = parent_columns
                    .iter()
                    .map(|&column| self.plan.types[column].clone())
                    .collect::<Vec<_>>();
                types.push(self.plan.aggregate_input_types[aggregate_index].clone());
                let groups = u32::try_from(types.len())
                    .map_err(|_| VeinError::planning("too many DISTINCT grouping columns", None))?;
                let collector_plan = AggregatePlan {
                    groups,
                    estimated_groups: None,
                    grouping_sets: None,
                    emit_empty_group_on_empty_input: false,
                    aggs: Vec::new(),
                    aggregate_input_types: Vec::new(),
                    types,
                };
                let mut sink =
                    Self::with_memory_tracker(collector_plan, self.tracker.clone())?;
                sink.hash_builder = self.hash_builder.clone();
                collectors.push(DistinctCollector {
                    aggregate_index,
                    grouping_set_id,
                    parent_columns,
                    sink: Box::new(sink),
                });
            }
        }
        self.distinct_collectors = collectors;
        Ok(())
    }

    pub(in crate::operators::aggregate) fn accumulate_distinct(&mut self, chunk: &DataChunk) -> Result<()> {
        let parent_groups = self.plan.groups as usize;
        for collector_index in 0..self.distinct_collectors.len() {
            let collector = &self.distinct_collectors[collector_index];
            let mut columns = collector
                .parent_columns
                .iter()
                .map(|&column| Arc::clone(chunk.column(column)))
                .collect::<Vec<_>>();
            columns.push(Arc::clone(
                chunk.column(parent_groups + collector.aggregate_index),
            ));
            self.push_distinct_input(
                collector_index,
                DistinctInput::Chunk(DataChunk::new(columns, chunk.selection.clone())),
            )?;
        }
        Ok(())
    }

    pub(in crate::operators::aggregate) fn accumulate_distinct_view(
        &mut self,
        view: &DataChunkView,
    ) -> Result<()> {
        let parent_groups = self.plan.groups as usize;
        for collector_index in 0..self.distinct_collectors.len() {
            let collector = &self.distinct_collectors[collector_index];
            let mut columns = collector.parent_columns.clone();
            columns.push(parent_groups + collector.aggregate_index);
            self.push_distinct_input(
                collector_index,
                DistinctInput::View(view.project(&columns)?),
            )?;
        }
        Ok(())
    }

    fn push_distinct_input(&mut self, collector_index: usize, input: DistinctInput) -> Result<()> {
        let result = input
            .clone()
            .accumulate(&mut self.distinct_collectors[collector_index].sink);
        match result {
            Ok(()) => Ok(()),
            Err(VeinError::ResourceExhausted { .. }) => {
                self.flush_current_run()?;
                for (index, collector) in self.distinct_collectors.iter_mut().enumerate() {
                    if index != collector_index {
                        collector.sink.flush_current_run()?;
                    }
                }
                input.accumulate(&mut self.distinct_collectors[collector_index].sink)
            }
            Err(error) => Err(error),
        }
    }

    pub(in crate::operators::aggregate) fn distinct_layout_matches(&self, other: &AggregateSink) -> bool {
        self.distinct_collectors.len() == other.distinct_collectors.len()
            && self
                .distinct_collectors
                .iter()
                .zip(&other.distinct_collectors)
                .all(|(left, right)| {
                    left.aggregate_index == right.aggregate_index
                        && left.grouping_set_id == right.grouping_set_id
                        && left.parent_columns == right.parent_columns
                        && left.sink.plan == right.sink.plan
                })
    }

    pub(in crate::operators::aggregate) fn inherit_hash_builders(&mut self, source: &AggregateSink) -> Result<()> {
        if !self.distinct_layout_matches(source) {
            return Err(VeinError::internal(
                "cannot inherit hash seeds across different aggregate layouts",
            ));
        }
        self.hash_builder = source.hash_builder.clone();
        for (target, origin) in self.distinct_collectors.iter_mut().zip(&source.distinct_collectors) {
            target.sink.inherit_hash_builders(&origin.sink)?;
        }
        Ok(())
    }
}
