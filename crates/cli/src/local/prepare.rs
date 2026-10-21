use std::sync::Arc;
use std::time::Instant;

use binder::LogicalBinder;
use common::identifiers::RequestId;
use common::protocol::{DataSink, PhysicalPlanOperator};
use common::statistics::TableStatisticsLog;
use common::{Result, VeinError};
use physical::PhysicalPlanner;
use plan::LogicalPlan;

use super::adaptive;
use super::query_context::query_statistics_context;
use super::{LocalQuery, LocalQueryEngine};

impl LocalQueryEngine {
    /// Parses, binds, optimizes, and plans a Pipe query for local execution.
    pub async fn prepare(&self, query: &str) -> Result<LocalQuery> {
        let prepare_started = Instant::now();

        let parse_started = Instant::now();
        let mut parser = parser::Parser::new(query);
        let statement = parser
            .parse_statement()
            .map_err(|error| VeinError::planning(format!("query parse failed: {error}"), None))?;
        let parse_us = parse_started.elapsed().as_micros() as u64;

        let bind_started = Instant::now();
        let bound = self.binder.bind_with_catalog_revision(&statement).await?;
        let bind_us = bind_started.elapsed().as_micros() as u64;

        let statistics_context = Arc::new(query_statistics_context(&bound.plan));
        for source in statistics_context.sources() {
            let statistics = source.statistics.as_deref();
            tracing::info!(
                target: "vein.statistics",
                event = "query_source_statistics",
                source_key = %source.source_key,
                table = %source.table_name,
                statistics_available = statistics.is_some(),
                rows = ?statistics.and_then(|value| value.num_rows),
                total_bytes = ?statistics.and_then(|value| value.total_bytes),
                file_count = ?statistics.and_then(|value| value.file_count),
                row_group_count = ?statistics.and_then(|value| value.row_group_count),
                compressed_bytes = ?statistics.and_then(|value| value.compressed_bytes),
                uncompressed_bytes = ?statistics.and_then(|value| value.uncompressed_bytes),
                statistics_summary = %statistics.map_or_else(
                    || "unavailable".to_owned(),
                    |value| TableStatisticsLog::new(value).to_string(),
                ),
                "query source statistics snapshot"
            );
        }

        let optimize_started = Instant::now();
        let optimized = self
            .optimizer
            .optimize_with_statistics_context(bound.plan, Arc::clone(&statistics_context))?
            .0;
        let optimize_us = optimize_started.elapsed().as_micros() as u64;

        if let LogicalPlan::Write {
            target, options, ..
        } = &optimized
        {
            self.write_storage.validate_target(target, options)?;
        }

        let plan_started = Instant::now();
        let request_id = RequestId::new();
        let adaptive_boundary = self
            .adaptive_policy
            .choose_boundary(&optimized, Arc::clone(&statistics_context));
        let job = self.planner.create_physical_plan_with_statistics(
            request_id,
            optimized,
            Arc::clone(&statistics_context),
        )?;
        let plan_us = plan_started.elapsed().as_micros() as u64;

        let task_started = Instant::now();
        let (request_id, query_id, mode, logical_plan, stage) = job.into_parts();
        let first_sink = stage
            .tasks
            .first()
            .and_then(|task| match &task.plan.root.operator {
                PhysicalPlanOperator::Sink { sink, .. } => Some(sink.as_ref()),
                _ => None,
            });
        let is_write = first_sink.is_some_and(|sink| matches!(sink, DataSink::Storage(_)));
        if is_write && stage.tasks.len() != 1 {
            return Err(VeinError::planning(
                "a local WRITE must execute as one atomic task",
                None,
            ));
        }
        let stage_id = stage.stage_id;
        let mut tasks = Vec::with_capacity(stage.tasks.len());
        for task in stage.tasks {
            let PhysicalPlanOperator::Sink { sink, .. } = &task.plan.root.operator else {
                return Err(VeinError::planning(
                    "physical task plan must have a sink root",
                    None,
                ));
            };
            match (sink.as_ref(), is_write) {
                (DataSink::Client { .. }, false) | (DataSink::Storage(_), true) => {}
                _ => {
                    return Err(VeinError::planning(
                        "physical stage contains inconsistent sink types",
                        None,
                    ));
                }
            }
            let mut task = task.into_execute_task(request_id, query_id, stage_id, mode);
            task.statistics_context = Some(Arc::clone(&statistics_context));
            tasks.push(task);
        }
        let adaptive_scan = if is_write {
            None
        } else {
            adaptive_boundary.and_then(|boundary| {
                adaptive::prepare_checkpoint(
                    self.planner.as_ref(),
                    boundary,
                    Arc::clone(&statistics_context),
                    request_id,
                    query_id,
                    &tasks,
                    &logical_plan,
                )
            })
        };
        let task_build_us = task_started.elapsed().as_micros() as u64;

        tracing::info!(
            target: "vein.audit",
            event = "prepare_finished",
            parse_us,
            bind_us,
            optimize_us,
            plan_us,
            task_build_us,
            total_us = prepare_started.elapsed().as_micros() as u64,
            task_count = tasks.len(),
            is_write,
            "prepare_finished"
        );

        Ok(LocalQuery {
            tasks,
            executor: self.executor.clone(),
            statistics_context,
            logical_plan,
            is_write,
            adaptive_checkpoint: adaptive_scan,
            adaptive_policy: Arc::clone(&self.adaptive_policy),
            optimizer: Arc::clone(&self.optimizer),
            planner: Arc::clone(&self.planner),
            request_id,
            query_id,
        })
    }
}
