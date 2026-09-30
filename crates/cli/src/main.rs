mod local;
mod parquet_catalog;
mod parquet_storage;

use std::time::Instant;

use futures::StreamExt;
use local::LocalQueryEngine;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    common::logging::init("cli")?;

    let query = std::env::args().skip(1).collect::<Vec<_>>().join(" ");
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
        println!("WRITE OK: table '{}' ({} rows)", receipt.destination, receipt.row_count);
        eprintln!("Query completed in {} ms", started.elapsed().as_millis());
        return Ok(());
    }

    let mut batches = query.into_stream();
    let mut row_count = 0u64;
    let mut batch_count = 0u64;
    let mut format_us = 0u64;

    while let Some(batch) = batches.next().await {
        let batch = batch?;
        row_count += batch.num_rows() as u64;
        batch_count += 1;
        let format_started = Instant::now();
        println!("{}", arrow::util::pretty::pretty_format_batches(&[batch])?);
        format_us += format_started.elapsed().as_micros() as u64;
    }

    if batch_count == 0 {
        println!("Query returned 0 rows");
    }

    let total_us = started.elapsed().as_micros() as u64;
    tracing::info!(
        target: "vein.audit",
        event = "query_finished",
        mode = "read",
        prepare_us,
        execute_us = total_us.saturating_sub(prepare_us).saturating_sub(format_us),
        format_us,
        rows = row_count,
        batches = batch_count,
        total_us,
        "query_finished"
    );
    eprintln!("Query completed: {row_count} rows in {} ms", total_us / 1000);
    Ok(())
}
