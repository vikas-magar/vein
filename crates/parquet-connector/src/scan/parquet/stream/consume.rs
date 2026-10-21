use std::fmt::Display;
use std::future::poll_fn;
use std::pin::Pin;
use std::time::{Duration, Instant};

use arrow::record_batch::RecordBatch;
use common::Result;
use common::statistics::runtime::{RuntimeStatisticsEnqueue, RuntimeStatisticsInputBatch};
use error::VeinError;
use exec::pipeline::arrow_adapter::from_record_batch;
use futures::Stream;

use super::context::ScanStreamContext;
use super::metrics::ScanStreamMetrics;
use crate::scan::parquet::reader::thread_cpu_time;

pub(in crate::scan::parquet) async fn consume_batches<S, E>(
    mut stream: S,
    mut context: ScanStreamContext<'_>,
) -> Result<ScanStreamMetrics>
where
    S: Stream<Item=std::result::Result<RecordBatch, E>> + Unpin,
    E: Display,
{
    let mut metrics = ScanStreamMetrics::default();
    let io_wall_before_first_poll = context.io.fetch_wall_us.load(std::sync::atomic::Ordering::Relaxed);
    let mut last_progress = Instant::now();
    loop {
        let mut poll_cpu = Duration::ZERO;
        let mut polls = 0u64;
        let read_started = Instant::now();
        let next_batch = poll_fn(|cx| {
            let cpu_started = thread_cpu_time();
            let polled = Pin::new(&mut stream).poll_next(cx);
            poll_cpu += thread_cpu_time().saturating_sub(cpu_started);
            polls += 1;
            polled
        })
            .await;
        let poll_time = read_started.elapsed();
        metrics.poll_thread_cpu += poll_cpu;
        metrics.poll_count += polls;
        metrics.parquet_read_time += poll_time;
        if metrics.batches == 0 {
            metrics.first_poll_time = poll_time;
            metrics.first_poll_thread_cpu = poll_cpu;
            metrics.first_poll_polls = polls;
            metrics.first_poll_io_us = context
                .io
                .fetch_wall_us
                .load(std::sync::atomic::Ordering::Relaxed)
                .saturating_sub(io_wall_before_first_poll);
        } else {
            metrics.rest_poll_time += poll_time;
        }
        let Some(batch_result) = next_batch else { break };
        let batch = batch_result
            .map_err(|error| VeinError::internal(format!("Parquet read error: {error}")))?;
        consume_batch(batch, &mut metrics, &mut context)?;
        if context.next.finished() {
            break;
        }
        log_progress(&metrics, &context, &mut last_progress);
    }
    if context.has_row_filter {
        metrics.physical_rows_read = usize::try_from(
            context.predicate_metrics.rows_before_filter.load(std::sync::atomic::Ordering::Relaxed),
        )
            .unwrap_or(usize::MAX);
    }
    Ok(metrics)
}

fn consume_batch(
    batch: RecordBatch,
    metrics: &mut ScanStreamMetrics,
    context: &mut ScanStreamContext<'_>,
) -> Result<()> {
    let view = from_record_batch(batch)?;
    if metrics.batches == 0 {
        metrics.first_batch_us = context.scan_started.elapsed().as_micros() as u64;
    }
    metrics.total_rows = metrics.total_rows.saturating_add(view.num_rows());
    if !context.has_row_filter {
        metrics.physical_rows_read = metrics.physical_rows_read.saturating_add(view.num_rows());
    }
    metrics.batches += 1;
    queue_runtime_statistics(&view, metrics, context)?;
    push_output(view, metrics, context)
}

fn queue_runtime_statistics(
    view: &exec::pipeline::DataChunkView,
    metrics: &mut ScanStreamMetrics,
    context: &ScanStreamContext<'_>,
) -> Result<()> {
    let Some(scan) = context.statistics_scan else { return Ok(()) };
    let started = Instant::now();
    let statistics_view = view.project(context.output_positions)?;
    let schema = statistics_view
        .schema()
        .cloned()
        .ok_or_else(|| VeinError::internal("Parquet statistics view is missing its schema"))?;
    let retained_bytes = statistics_view
        .source_array_memory_sizes()?
        .into_iter()
        .try_fold(0usize, |total, (_, bytes)| {
            total.checked_add(bytes).ok_or_else(|| {
                VeinError::internal("runtime statistics retained-byte count overflow")
            })
        })?;
    let outcome = context.runtime_statistics.enqueue_input(RuntimeStatisticsInputBatch {
        scan: scan.clone(),
        schema,
        input: statistics_view,
        retained_bytes,
    });
    match outcome {
        RuntimeStatisticsEnqueue::Queued => metrics.statistics_batches_queued += 1,
        RuntimeStatisticsEnqueue::QueueFull => metrics.statistics_batches_queue_full += 1,
        RuntimeStatisticsEnqueue::BatchTooLarge => metrics.statistics_batches_too_large += 1,
        RuntimeStatisticsEnqueue::WorkerUnavailable => metrics.statistics_batches_worker_unavailable += 1,
    }
    metrics.statistics_enqueue_us += started.elapsed().as_micros() as u64;
    Ok(())
}

fn push_output(
    view: exec::pipeline::DataChunkView,
    metrics: &mut ScanStreamMetrics,
    context: &mut ScanStreamContext<'_>,
) -> Result<()> {
    if view.num_rows() == 0 { return Ok(()); }
    let chunk = view.project(context.output_positions)?;
    tracing::trace!(rows = chunk.num_rows(), "Pushing selected batch down pipeline");
    if context.detailed_metrics_enabled {
        let (array_count, array_bytes) = chunk.source_array_footprint();
        metrics.output_source_array_count = metrics.output_source_array_count.saturating_add(array_count as u64);
        metrics.output_source_array_bytes = metrics.output_source_array_bytes.saturating_add(array_bytes as u64);
        tracing::trace!(target: "vein.audit", event = "scan_view_batch_emitted",
            rows = chunk.num_rows(), columns = chunk.num_columns(), source_array_count = array_count,
            source_array_bytes = array_bytes, representation = "DataChunkView",
            arrow_array_handles_moved = true, "scan_view_batch_emitted");
    }
    let downstream_started = Instant::now();
    context.next.push(chunk)?;
    metrics.downstream_time += downstream_started.elapsed();
    Ok(())
}

fn log_progress(metrics: &ScanStreamMetrics, context: &ScanStreamContext<'_>, last: &mut Instant) {
    if last.elapsed() < Duration::from_secs(10) { return; }
    tracing::info!(target: "vein.audit", event = "parquet_scan_progress", path = context.path,
        rows = metrics.total_rows, batches = metrics.batches,
        bytes_read = context.bytes_read.load(std::sync::atomic::Ordering::Relaxed)
            .saturating_sub(context.bytes_before_file), file_size_bytes = context.file_size_bytes,
        parquet_read_us = metrics.parquet_read_time.as_micros() as u64,
        downstream_us = metrics.downstream_time.as_micros() as u64,
        elapsed_us = context.scan_started.elapsed().as_micros() as u64,
        "parquet_scan_progress");
    *last = Instant::now();
}
