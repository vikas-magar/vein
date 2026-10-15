use super::*;
use super::executor::{InputPlanFixture, MemoryFeedBuilder};
use super::plans::{set_input_plan, task};

pub(in crate::runtime::tests) struct CountingSpillStorageFactory(pub(in crate::runtime::tests) Arc<AtomicUsize>);

impl SpillStorageFactory for CountingSpillStorageFactory {
    fn create_for_query(&self) -> Result<Arc<dyn SpillStorage>> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Ok(Arc::new(LocalSpillStorage::new()))
    }
}


pub(in crate::runtime::tests) struct MaterializationCollector {
    pub(in crate::runtime::tests) chunks: Arc<Mutex<Vec<DataChunk>>>,
    pub(in crate::runtime::tests) flushed: Arc<AtomicBool>,
}

impl PipelineNode<DataChunk> for MaterializationCollector {
    fn push(&mut self, chunk: DataChunk) -> Result<()> {
        self.chunks.lock().unwrap().push(chunk);
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.flushed.store(true, Ordering::Relaxed);
        Ok(())
    }

    fn finished(&self) -> bool {
        self.flushed.load(Ordering::Relaxed)
    }
}


// Compact fixture DSL; runtime tests immediately lower it to the canonical plan tree.

/// Builds a full join task for spill tests.
pub(in crate::runtime::tests) fn full_join_spill_task() -> ExecuteTask {
    let input = || InputPlanFixture::Pipeline {
        source: DataSource::Memory,
        program: Arc::new(MicroProgram::empty()),
    };
    let plan = JoinPlan {
        join_type: JoinKind::Full,
        probe_keys: vec![0],
        build_keys: vec![0],
        residual: None,
        outputs: vec![JoinOutput::Probe(0), JoinOutput::Build(0)],
        probe_types: vec![VeinDataType::Int64],
        build_types: vec![VeinDataType::Int64],
    };
    let mut task = task();
    let join = InputPlanFixture::Join {
        probe: Box::new(input()),
        build: Box::new(input()),
        plan,
    };
    set_input_plan(&mut task, join);
    task
}

pub(in crate::runtime::tests) fn full_join_feed() -> Arc<MemoryFeedBuilder> {
    let mut values = (0..128).map(Some).collect::<Vec<_>>();
    values.extend([Some(20), None]);
    Arc::new(MemoryFeedBuilder::with_nullable_values(values))
}

pub(in crate::runtime::tests) fn join_result_pairs(output: &[RecordBatch]) -> Vec<(Option<i64>, Option<i64>)> {
    let mut pairs = output
        .iter()
        .flat_map(|batch| {
            let probe = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .expect("probe output");
            let build = batch
                .column(1)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .expect("build output");
            (0..batch.num_rows()).map(|row| {
                (
                    probe.is_valid(row).then(|| probe.value(row)),
                    build.is_valid(row).then(|| build.value(row)),
                )
            })
        })
        .collect::<Vec<_>>();
    pairs.sort_unstable();
    pairs
}
