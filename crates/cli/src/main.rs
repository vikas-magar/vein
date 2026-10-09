use std::time::Instant;

use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use cli::local::LocalQueryEngine;

/// Rows the CLI renders before summarising the rest.
///
/// `pretty_format_batches` allocates a string per cell and lays out every row, so
/// its cost grows with the result while a terminal can only show a screenful. The
/// CLI renders a bounded prefix and reports the true total; set `VEIN_DISPLAY_ROWS`
/// to raise the bound, or to `0` to render every row.
const DISPLAY_ROWS: usize = 1_000;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().skip(1);

    common::logging::init("cli")?;

    let query = args.collect::<Vec<_>>().join(" ");
    if query.is_empty() {
        return Err("usage: vein \"FROM table |> SELECT *\"".into());
    }

    let data_root = std::env::var_os("VEIN_DATA_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from("./data"));
    tracing::info!(
        target: "vein.audit",
        event = "cli_started",
        data_root = %data_root.display(),
        query = %query,
        "cli_started"
    );

    let engine = LocalQueryEngine::new(data_root);
    let started = Instant::now();
    let query = engine.prepare(&query).await?;
    let prepare_us = started.elapsed().as_micros() as u64;

    if query.is_write() {
        let write_started = Instant::now();
        let receipt = query.execute_write().await?;
        let execute_us = write_started.elapsed().as_micros() as u64;
        let total_us = started.elapsed().as_micros() as u64;
        tracing::info!(
            target: "vein.audit",
            event = "query_finished",
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

    // Only the rendered prefix is retained. Slicing shares the batch's buffers, so
    // discarding the rest costs no copy of the rows that are not shown.
    let mut shown: Vec<RecordBatch> = Vec::new();
    let mut shown_rows = 0usize;

    while let Some(batch) = batches.next().await {
        let batch = batch?;
        row_count += batch.num_rows() as u64;
        batch_count += 1;
        if display_rows.is_none_or(|limit| shown_rows < limit) {
            let take = display_rows.map_or(batch.num_rows(), |limit| {
                (limit - shown_rows).min(batch.num_rows())
            });
            if take > 0 {
                shown.push(batch.slice(0, take));
                shown_rows += take;
            }
        }
    }

    let format_started = Instant::now();
    if shown_rows == 0 {
        println!("Query returned 0 rows");
    } else {
        println!("{}", arrow::util::pretty::pretty_format_batches(&shown)?);
        if row_count > shown_rows as u64 {
            println!("({shown_rows} of {row_count} rows shown)");
        }
    }
    let format_us = format_started.elapsed().as_micros() as u64;

    let total_us = started.elapsed().as_micros() as u64;
    tracing::info!(
        target: "vein.audit",
        event = "query_finished",
        mode = "read",
        prepare_us,
        execute_us = total_us.saturating_sub(prepare_us).saturating_sub(format_us),
        format_us,
        rows = row_count,
        rows_shown = shown_rows,
        batches = batch_count,
        total_us,
        "query_finished"
    );
    eprintln!(
        "Query completed: {row_count} rows in {} ms",
        total_us / 1000
    );
    Ok(())
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
