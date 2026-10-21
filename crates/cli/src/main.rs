use std::time::Instant;

use cli::local::build_local_engine;
use common::{Result, VeinError};
use exec::pipeline::DataChunkView;
use futures::StreamExt;

mod query_timing;
mod result_format;
mod view_pretty;
#[cfg(test)]
mod tests;

/// Rows the CLI renders before summarising the rest.
///
/// `pretty_format_batches` allocates a string per cell and lays out every row, so
/// its cost grows with the result while a terminal can only show a screenful. The
/// CLI renders a bounded prefix and reports the true total; set `VEIN_DISPLAY_ROWS`
/// to raise the bound, or to `0` to render every row.
const DISPLAY_ROWS: usize = 1_000;
const QUERY_USAGE: &str = "usage: vein \"FROM table |> SELECT *\"";

#[tokio::main]
async fn main() -> Result<()> {
    let args = std::env::args().skip(1);

    common::logging::init("cli")?;
    let output_format = result_format::OutputFormat::from_env()?;

    let query = query_argument(args.collect::<Vec<_>>().join(" "))?;

    let configured_data_root = std::env::var_os("VEIN_DATA_DIR");
    let data_root_source = if configured_data_root.is_some() {
        "environment"
    } else {
        "default"
    };
    let data_root = configured_data_root
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from("./data"));
    tracing::info!(
        target: "vein.audit",
        event = "cli_started",
        data_root_source,
        "cli_started"
    );

    let engine = build_local_engine(data_root)?;
    let started = Instant::now();
    let query = engine.prepare(&query).await?;
    let prepare_us = started.elapsed().as_micros() as u64;
    let request_id = query.request_id();
    let query_id = query.query_id();

    if query.is_write() {
        let write_started = Instant::now();
        let receipt = query.execute_write().await?;
        let execute_us = write_started.elapsed().as_micros() as u64;
        let total_us = started.elapsed().as_micros() as u64;
        tracing::info!(
            target: "vein.audit",
            event = "query_finished",
            request_id = %request_id,
            query_id = %query_id,
            mode = "write",
            prepare_us,
            execute_us,
            format_us = 0u64,
            rows = receipt.row_count,
            batches = 0u64,
            total_us,
            "query_finished"
        );
        println!(
            "WRITE OK: table '{}' ({} rows)",
            receipt.destination, receipt.row_count
        );
        eprintln!("Query completed in {} ms", started.elapsed().as_millis());
        return Ok(());
    }

    let mut batches = query.into_stream();
    let mut row_count = 0u64;
    let mut batch_count = 0u64;
    let display_rows = display_row_limit();
    let stdout = std::io::stdout();
    let mut json_writer = output_format
        .is_json_lines()
        .then(|| result_format::JsonRowsWriter::new(stdout.lock()));
    let mut format_us = 0u64;

    // Keep the rendered prefix as row mappings into the original Arrow buffers.
    let mut shown: Vec<DataChunkView> = Vec::new();
    let mut shown_rows = 0usize;

    while let Some(batch) = batches.next().await {
        let batch = batch?;
        let batch_rows = batch.num_rows();
        row_count += batch_rows as u64;
        batch_count += 1;
        if let Some(writer) = json_writer.as_mut() {
            let format_started = Instant::now();
            writer.write_view_batch(&batch)?;
            format_us = format_us.saturating_add(format_started.elapsed().as_micros() as u64);
        } else if display_rows.is_none_or(|limit| shown_rows < limit) {
            let take = display_rows.map_or(batch_rows, |limit| {
                (limit - shown_rows).min(batch_rows)
            });
            if take > 0 {
                shown.push(batch.slice_rows(0, take)?);
                shown_rows += take;
            }
        }
    }

    let format_started = Instant::now();
    if let Some(writer) = json_writer.as_mut() {
        writer.finish()?;
    } else {
        if shown_rows == 0 {
            println!("Query returned 0 rows");
        } else {
            println!("{}", view_pretty::pretty_format_views(&shown)?);
            if row_count > shown_rows as u64 {
                println!("({shown_rows} of {row_count} rows shown)");
            }
        }
    }
    format_us = format_us.saturating_add(format_started.elapsed().as_micros() as u64);

    let total_us = started.elapsed().as_micros() as u64;
    let execute_us = total_us.saturating_sub(prepare_us).saturating_sub(format_us);
    tracing::info!(
        target: "vein.audit",
        event = "query_finished",
        request_id = %request_id,
        query_id = %query_id,
        mode = "read",
        prepare_us,
        execute_us,
        format_us,
        rows = row_count,
        rows_shown = if output_format.is_json_lines() { row_count } else { shown_rows as u64 },
        batches = batch_count,
        total_us,
        "query_finished"
    );
    eprintln!(
        "Query completed: {row_count} rows in {} ms",
        total_us / 1000
    );
    if output_format.is_json_lines() {
        eprintln!(
            "{}",
            query_timing::machine_timing_line(prepare_us, execute_us, format_us, total_us)
        );
    }
    Ok(())
}

fn query_argument(query: String) -> Result<String> {
    if query.is_empty() {
        Err(VeinError::invalid_input(QUERY_USAGE))
    } else {
        Ok(query)
    }
}

/// The row-render bound: `VEIN_DISPLAY_ROWS`, defaulting to [`DISPLAY_ROWS`], with
/// `0` meaning render every row.
fn display_row_limit() -> Option<usize> {
    match std::env::var("VEIN_DISPLAY_ROWS") {
        Ok(value) => match value.trim().parse::<usize>() {
            Ok(0) => None,
            Ok(limit) => Some(limit),
            Err(_) => Some(DISPLAY_ROWS),
        },
        Err(_) => Some(DISPLAY_ROWS),
    }
}
