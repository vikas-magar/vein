use super::*;

/// Shares a build sink with the join executor while a source owns its pipeline.
pub(super) struct SharedBuildSink(pub(super) Arc<Mutex<Option<BuildSink>>>);

impl PipelineNode for SharedBuildSink {
    fn push(&mut self, view: DataChunkView) -> Result<()> {
        self.0
            .lock()
            .map_err(|_| VeinError::internal("join build mutex poisoned"))?
            .as_mut()
            .ok_or_else(|| VeinError::internal("join build sink already finished"))?
            .push(view)
    }
    fn flush(&mut self) -> Result<()> {
        self.0
            .lock()
            .map_err(|_| VeinError::internal("join build mutex poisoned"))?
            .as_mut()
            .ok_or_else(|| VeinError::internal("join build sink already finished"))?
            .flush()
    }

    fn fork_local(&self) -> Result<Option<Box<dyn PipelineNode>>> {
        let build_sink = self
            .0
            .lock()
            .map_err(|_| VeinError::internal("join build mutex poisoned"))?;
        let build_sink = build_sink
            .as_ref()
            .ok_or_else(|| VeinError::internal("join build sink already finished"))?;
        Ok(build_sink
            .fork_worker_local()?
            .map(|local| Box::new(LocalBuildSink(local)) as Box<dyn PipelineNode>))
    }
}

/// Worker-private adapter whose completed table is merged into `SharedBuildSink`.
pub(super) struct LocalBuildSink(BuildSink);

impl PipelineNode for LocalBuildSink {
    fn push(&mut self, view: DataChunkView) -> Result<()> {
        self.0.push(view)
    }

    fn flush(&mut self) -> Result<()> {
        self.0.flush()
    }

    fn combine_local(
        self: Box<Self>,
        canonical: &mut dyn PipelineNode,
    ) -> Result<()> {
        let canonical = canonical
            .as_any_mut()
            .downcast_mut::<SharedBuildSink>()
            .ok_or_else(|| VeinError::internal("join build local state has a mismatched target"))?;
        let mut canonical_sink = canonical
            .0
            .lock()
            .map_err(|_| VeinError::internal("join build mutex poisoned"))?;
        let canonical_sink = canonical_sink
            .as_mut()
            .ok_or_else(|| VeinError::internal("join build sink already finished"))?;
        canonical_sink.combine_worker(self.0)
    }
}

/// Transfers owned chunks from scan workers to the sole downstream owner.
///
/// The bounded channel moves `DataChunkView` handles; Arrow buffers remain shared.
/// `finished` lets source readers stop after a downstream limit has completed.
/// Counts active rows and chunks entering a join without inspecting or cloning columns.
pub(super) struct JoinProbeCounter {
    pub(super) inner: Box<dyn PipelineNode>,
    pub(super) rows: Arc<AtomicU64>,
    pub(super) batches: Arc<AtomicU64>,
}

impl PipelineNode for JoinProbeCounter {
    fn push(&mut self, view: DataChunkView) -> Result<()> {
        self.rows
            .fetch_add(view.num_rows() as u64, Ordering::Relaxed);
        self.batches.fetch_add(1, Ordering::Relaxed);
        self.inner.push(view)
    }
    fn flush(&mut self) -> Result<()> {
        self.inner.flush()
    }
    fn finished(&self) -> bool {
        self.inner.finished()
    }

    fn fork_local(&self) -> Result<Option<Box<dyn PipelineNode>>> {
        let Some(inner) = self.inner.fork_local()? else {
            return Ok(None);
        };
        Ok(Some(Box::new(Self {
            inner,
            rows: Arc::clone(&self.rows),
            batches: Arc::clone(&self.batches),
        })))
    }

    fn combine_local(
        self: Box<Self>,
        canonical: &mut dyn PipelineNode,
    ) -> Result<()> {
        let target = canonical
            .as_any_mut()
            .downcast_mut::<JoinProbeCounter>()
            .ok_or_else(|| VeinError::internal("join probe counter has a mismatched target"))?;
        self.inner.combine_local(target.inner.as_mut())
    }
}

pub(super) type SharedJoinOutput = Arc<Mutex<Box<dyn PipelineNode>>>;

/// Forwards partition results into the one downstream operator instance.
/// HashJoin flushes once per partition; this adapter defers that flush until
/// every partition has completed.
#[derive(Clone)]
struct JoinPartitionOutput(SharedJoinOutput);

impl PipelineNode for JoinPartitionOutput {
    fn push(&mut self, view: DataChunkView) -> Result<()> {
        self.0
            .lock()
            .map_err(|_| VeinError::internal("join partition output mutex poisoned"))?
            .push(view)
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    fn finished(&self) -> bool {
        self.0.lock().map(|next| next.finished()).unwrap_or(false)
    }
}

/// Probe-side pipeline used after the build sink spills. It materializes the
/// probe side once into an IPC run, then recursively executes bounded hash
/// partitions through the same HashJoin operator used by the in-memory path.
pub(super) struct SpilledJoinProbe {
    pub(super) probe_writer: Option<JoinSpillWriter>,
    pub(super) build_run: JoinSpillRun,
    pub(super) build_schema: JoinSpillSchema,
    pub(super) probe_schema: JoinSpillSchema,
    pub(super) plan: JoinPlan,
    pub(super) builder: Arc<dyn ExecutorBuilder>,
    pub(super) tracker: QueryMemoryTracker,
    pub(super) next: SharedJoinOutput,
}

impl SpilledJoinProbe {
    fn process_partitions(
        &self,
        build_runs: Vec<JoinSpillRun>,
        probe_runs: Vec<JoinSpillRun>,
        bit: u32,
    ) -> Result<()> {
        self.tracker.check_cancelled()?;
        let partition_build_rows = build_runs
            .iter()
            .map(|run| run.rows)
            .fold(0_u64, u64::saturating_add);
        let partition_probe_rows = probe_runs
            .iter()
            .map(|run| run.rows)
            .fold(0_u64, u64::saturating_add);
        let mut table = HashTable::with_memory_tracker(
            join_keys(&self.plan.build_keys),
            join_keys(&self.plan.probe_keys),
            self.tracker.clone(),
        )?;
        let mut overflow_reason = None;
        'build_runs: for run in &build_runs {
            let reader = open_reader(run)?;
            for batch in reader {
                self.tracker.check_cancelled()?;
                let batch = batch.map_err(|error| {
                    VeinError::internal(format!("decode spilled join build batch: {error}"))
                })?;
                // Leasing decoded payload arrays is part of loading a partition
                // into the hash table. It can exhaust the budget before
                // insert_view gets a chance to reject the row references, so
                // treat either failure as a signal to repartition.
                let view = match crate::pipeline::arrow_adapter::from_record_batch(batch)
                    .and_then(|view| payload_view(view, &self.build_schema, &self.tracker))
                {
                    Ok(view) => view,
                    Err(VeinError::ResourceExhausted { .. }) => {
                        overflow_reason = Some("build_payload_memory");
                        break 'build_runs;
                    }
                    Err(error) => return Err(error),
                };
                match table.insert(view) {
                    Ok(()) => {}
                    Err(VeinError::ResourceExhausted { .. }) => {
                        overflow_reason = Some("build_index_memory");
                        break 'build_runs;
                    }
                    Err(error) => return Err(error),
                }
            }
        }

        // Load the complete build partition first, but do not emit any output
        // until the largest probe batch can coexist with it. If it cannot,
        // repartition both sides before the downstream operator sees rows.
        if overflow_reason.is_none() {
            let working_set = self.tracker.used();
            'probe_runs: for run in &probe_runs {
                let reader = open_reader(run)?;
                for batch in reader {
                    self.tracker.check_cancelled()?;
                    let batch = batch.map_err(|error| {
                        VeinError::internal(format!("decode spilled join probe batch: {error}"))
                    })?;
                    let batch_bytes = batch
                        .columns()
                        .iter()
                        .map(|array| array.get_array_memory_size())
                        .fold(0_usize, usize::saturating_add);
                    if working_set
                        .checked_add(batch_bytes)
                        .is_none_or(|required| required > self.tracker.limit())
                    {
                        overflow_reason = Some("probe_batch_memory");
                        break 'probe_runs;
                    }
                }
            }
        }

        if let Some(overflow_reason) = overflow_reason {
            let query_memory_used_bytes = self.tracker.used();
            drop(table);
            tracing::debug!(
                target: "vein.audit",
                event = "join_spill_repartition",
                partition_bit = bit,
                partition_build_rows,
                partition_probe_rows,
                build_runs = build_runs.len(),
                probe_runs = probe_runs.len(),
                overflow_reason,
                query_memory_used_bytes,
                query_memory_limit_bytes = self.tracker.limit(),
                "join spill partition still exceeds memory budget"
            );
            if bit >= u64::BITS {
                return Err(VeinError::resource_exhausted(
                    "recursive hash join partition",
                    u128::from(bit) + 1,
                    u128::from(u64::BITS),
                ));
            }
            let build_children = if build_runs.is_empty() {
                [None, None]
            } else {
                partition_runs(&build_runs, &self.build_schema, &self.tracker, bit)?
            };
            let probe_children = if probe_runs.is_empty() {
                [None, None]
            } else {
                partition_runs(&probe_runs, &self.probe_schema, &self.tracker, bit)?
            };
            for run in build_runs.into_iter().chain(probe_runs) {
                remove_run(run)?;
            }
            for partition in 0..2 {
                let build: Vec<JoinSpillRun> =
                    build_children[partition].clone().into_iter().collect();
                let probe: Vec<JoinSpillRun> =
                    probe_children[partition].clone().into_iter().collect();
                if !build.is_empty() || !probe.is_empty() {
                    self.process_partitions(build, probe, bit + 1)?;
                }
            }
            return Ok(());
        }

        let build_rows = table.len() as u64;
        tracing::debug!(
            target: "vein.audit",
            event = "join_spill_partition_loaded",
            partition_bit = bit,
            build_rows,
            "join spill partition loaded"
        );
        let join_type = join_type(self.plan.join_type)?;
        let next = Box::new(JoinPartitionOutput(Arc::clone(&self.next)));
        let join = HashJoin::new(
            Arc::new(table),
            join_type,
            join_outputs(&self.plan.outputs),
            join_types(&self.plan.probe_types),
            join_types(&self.plan.build_types),
            next,
        );
        let join = install_join_residual(self.builder.as_ref(), &self.plan, join)?;
        let mut join: Box<dyn PipelineNode> = Box::new(join);
        for run in &probe_runs {
            let reader = open_reader(run)?;
            for batch in reader {
                self.tracker.check_cancelled()?;
                let batch = batch.map_err(|error| {
                    VeinError::internal(format!("decode spilled join probe batch: {error}"))
                })?;
                let view = crate::pipeline::arrow_adapter::from_record_batch(batch)?;
                join.push(payload_view(view, &self.probe_schema, &self.tracker)?)?;
            }
        }
        join.flush()?;
        for run in build_runs.into_iter().chain(probe_runs) {
            remove_run(run)?;
        }
        Ok(())
    }
}

impl PipelineNode for SpilledJoinProbe {
    fn push(&mut self, view: DataChunkView) -> Result<()> {
        self.probe_writer
            .as_mut()
            .ok_or_else(|| VeinError::internal("spilled join probe is already finished"))?
            .push(&view, &join_keys(&self.plan.probe_keys))
    }

    fn flush(&mut self) -> Result<()> {
        let probe_run = self
            .probe_writer
            .take()
            .ok_or_else(|| VeinError::internal("spilled join probe has no writer"))?
            .finish()?;
        tracing::info!(
            target: "vein.audit",
            event = "join_spill_probe_finished",
            build_rows = self.build_run.rows,
            build_bytes = self.build_run.bytes,
            probe_rows = probe_run.rows,
            probe_bytes = probe_run.bytes,
            "join spill inputs materialized"
        );
        self.process_partitions(vec![self.build_run.clone()], vec![probe_run], 0)?;
        self.next
            .lock()
            .map_err(|_| VeinError::internal("join spill output mutex poisoned"))?
            .flush()
    }

    fn finished(&self) -> bool {
        self.next
            .lock()
            .map(|next| next.finished())
            .unwrap_or(false)
    }
}
