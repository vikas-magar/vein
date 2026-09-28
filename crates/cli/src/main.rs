mod local;
mod parquet_catalog;

use std::time::Instant;

use futures::StreamExt;
use local::LocalQueryEngine;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // let query = std::env::args().skip(1).collect::<Vec<_>>().join(" ");
    let query = "FROM './input' |> WHERE id < 10 |> SELECT *";

    let data_root = std::env::var_os("VEIN_DATA_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from("./data"));
    let engine = LocalQueryEngine::new(data_root);
    let started = Instant::now();
    let query = engine.prepare(&query).await?;
    let mut batches = query.into_stream();
    let mut row_count = 0u64;
    let mut saw_batch = false;

    while let Some(batch) = batches.next().await {
        let batch = batch?;
        row_count += batch.num_rows() as u64;
        saw_batch = true;
        println!("{}", arrow::util::pretty::pretty_format_batches(&[batch])?);
    }

    if !saw_batch {
        println!("Query returned 0 rows");
    }
    eprintln!(
        "Query completed: {row_count} rows in {} ms",
        started.elapsed().as_millis()
    );
    Ok(())
}
