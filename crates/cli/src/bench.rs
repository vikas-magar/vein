//! Phase 0 benchmark harness (`vein --bench [iterations]`).
//!
//! Records the environment that `vein_design/08-performance-engineering.md` §3
//! requires and times each scenario's prepare and execution phases separately,
//! reporting median and p95 over repeated iterations (§5).
//!
//! Result formatting is deliberately excluded: it is a CLI concern, not engine
//! work, and it was previously measured to dominate end-to-end wall time.
//!
//! Bytes read/decompressed/materialized (§1) are emitted by the engine's
//! `vein.audit` tracing events rather than returned here; folding them into this
//! report needs the storage counters work tracked separately.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use serde_json::Value;
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
use tracing_subscriber::registry;

use crate::local::LocalQueryEngine;

/// A scenario and its DuckDB equivalent.
///
/// §4 requires a baseline on the same hardware and dataset with equivalent
/// semantics, so both engines read the same Parquet files and compute the same
/// rows. Each scenario is included only when both engines can run its shape.
struct Scenario {
    name: &'static str,
    vein: &'static str,
    duckdb: &'static str,
    validation: ValidationMode,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ValidationMode {
    /// SQL defines the output order, so rows must match positionally.
    Ordered,
    /// SQL does not define output order, so compare the output as a multiset.
    Unordered,
    /// LIMIT without ORDER BY may legally return different rows in each engine.
    RowCountOnly,
}

const SCENARIOS: &[Scenario] = &[
    Scenario {
        name: "scan/full",
        vein: "FROM orders\n|> SELECT *",
        duckdb: "SELECT * FROM './data/tpch/orders/*.parquet'",
        validation: ValidationMode::Unordered,
    },
    Scenario {
        name: "scan/narrow",
        vein: "FROM orders\n|> SELECT o_orderkey",
        duckdb: "SELECT o_orderkey FROM './data/tpch/orders/*.parquet'",
        validation: ValidationMode::Unordered,
    },
    Scenario {
        name: "scan/wide",
        vein: "FROM orders\n|> SELECT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate",
        duckdb: "SELECT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate FROM './data/tpch/orders/*.parquet'",
        validation: ValidationMode::Unordered,
    },
    Scenario {
        name: "scan/selective",
        vein: "FROM orders\n|> WHERE o_orderkey = 1\n|> SELECT o_orderkey, o_totalprice",
        duckdb: "SELECT o_orderkey, o_totalprice FROM './data/tpch/orders/*.parquet' WHERE o_orderkey = 1",
        validation: ValidationMode::Unordered,
    },
    Scenario {
        name: "scan/no-match",
        vein: "FROM orders\n|> WHERE o_orderkey = 99999999\n|> SELECT o_orderkey",
        duckdb: "SELECT o_orderkey FROM './data/tpch/orders/*.parquet' WHERE o_orderkey = 99999999",
        validation: ValidationMode::Unordered,
    },
    Scenario {
        name: "temporal/truncate-month",
        vein: "FROM orders\n|> EXTEND TRUNCATE(o_orderdate, 'MONTH') AS order_month\n|> SELECT o_orderkey, order_month",
        duckdb: "SELECT o_orderkey, CAST(date_trunc('month', o_orderdate) AS DATE) AS order_month FROM './data/tpch/orders/*.parquet'",
        validation: ValidationMode::Unordered,
    },
    // A self-join, so both sides read the same files: one match per row, which
    // makes the row count checkable against DuckDB's.
    Scenario {
        name: "join/self-key",
        vein: "FROM orders AS a\n|> JOIN orders AS b ON a.o_orderkey = b.o_orderkey\n|> SELECT a.o_orderkey, b.o_totalprice",
        duckdb: "SELECT a.o_orderkey, b.o_totalprice FROM './data/tpch/orders/*.parquet' a JOIN './data/tpch/orders/*.parquet' b ON a.o_orderkey = b.o_orderkey",
        validation: ValidationMode::Unordered,
    },
    // A limit is the one scenario answered by *not* reading. The window fills
    // inside the first batch, so the scan stops there and the number to beat is
    // DuckDB's, which stops the same way.
    Scenario {
        name: "limit/5",
        vein: "FROM orders\n|> SELECT o_orderkey, o_totalprice\n|> LIMIT 5",
        duckdb: "SELECT o_orderkey, o_totalprice FROM './data/tpch/orders/*.parquet' LIMIT 5",
        validation: ValidationMode::RowCountOnly,
    },
    Scenario {
        name: "aggregate/post-filter",
        vein: "FROM orders\n|> AGGREGATE count(o_orderkey) AS order_count GROUP BY o_orderstatus\n|> WHERE order_count > 50000",
        duckdb: "SELECT o_orderstatus, count(o_orderkey) AS order_count FROM './data/tpch/orders/*.parquet' GROUP BY o_orderstatus HAVING count(o_orderkey) > 50000",
        validation: ValidationMode::Unordered,
    },
    Scenario {
        name: "aggregate/high-cardinality",
        vein: "FROM orders\n|> AGGREGATE count(o_orderkey) AS order_count GROUP BY o_orderkey",
        duckdb: "SELECT o_orderkey, count(o_orderkey) AS order_count FROM './data/tpch/orders/*.parquet' GROUP BY o_orderkey",
        validation: ValidationMode::Unordered,
    },
    Scenario {
        name: "order/multi-key-limit",
        vein: "FROM orders\n|> SELECT o_orderkey, o_totalprice\n|> ORDER o_totalprice DESC, o_orderkey ASC\n|> LIMIT 100",
        duckdb: "SELECT o_orderkey, o_totalprice FROM './data/tpch/orders/*.parquet' ORDER BY o_totalprice DESC, o_orderkey ASC LIMIT 100",
        validation: ValidationMode::Ordered,
    },
    Scenario {
        name: "order/computed-key-limit",
        vein: "FROM orders\n|> SELECT o_orderkey, o_totalprice\n|> ORDER o_orderkey + 1 DESC\n|> LIMIT 100",
        duckdb: "SELECT o_orderkey, o_totalprice FROM './data/tpch/orders/*.parquet' ORDER BY o_orderkey + 1 DESC LIMIT 100",
        validation: ValidationMode::Ordered,
    },
];

/// One scenario's samples.
struct Measurement {
    rows: u64,
    bytes_read: u64,
    operator_metrics: BTreeMap<&'static str, BTreeMap<&'static str, u64>>,
    prepare: Vec<Duration>,
    execute: Vec<Duration>,
}

/// Accumulates `bytes_read` from the engine's `scan_completed` audit events.
///
/// The counters needed by `08-performance-engineering.md` §1 already exist as
/// instrumentation (Rule 18); this reads them instead of adding a second
/// measurement path into the engine. Only `scan_completed` is counted —
/// `task_finished` repeats the same totals, so summing both would double count.
#[derive(Clone, Default)]
struct IoCounters {
    bytes_read: Arc<AtomicU64>,
    operator_metrics: Arc<Mutex<BTreeMap<&'static str, BTreeMap<&'static str, u64>>>>,
}

impl IoCounters {
    fn taken(&self, before: u64) -> u64 {
        self.bytes_read.load(Ordering::Relaxed).saturating_sub(before)
    }

    fn reset_operator_metrics(&self) {
        if let Ok(mut metrics) = self.operator_metrics.lock() {
            metrics.clear();
        }
    }

    fn operator_metrics(&self) -> BTreeMap<&'static str, BTreeMap<&'static str, u64>> {
        self.operator_metrics
            .lock()
            .map_or_else(|_| BTreeMap::new(), |metrics| metrics.clone())
    }
}

#[derive(Default)]
struct FieldVisitor {
    kind: Option<String>,
    bytes_read: u64,
    metrics: BTreeMap<&'static str, u64>,
}

impl Visit for FieldVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "event" {
            self.kind = Some(value.to_owned());
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "event" {
            self.kind = Some(format!("{value:?}").trim_matches('"').to_owned());
        }
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        if field.name() == "bytes_read" {
            self.bytes_read = value;
        } else {
            self.metrics.insert(field.name(), value);
        }
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        if let Ok(value) = u64::try_from(value) {
            self.metrics.insert(field.name(), value);
        }
    }
}

impl<S: Subscriber> Layer<S> for IoCounters {
    fn on_event(&self, event: &Event<'_>, _context: Context<'_, S>) {
        let mut visitor = FieldVisitor::default();
        event.record(&mut visitor);
        if visitor.kind.as_deref() == Some("scan_completed") {
            self.bytes_read
                .fetch_add(visitor.bytes_read, Ordering::Relaxed);
        } else {
            let metric_event = match visitor.kind.as_deref() {
                Some("aggregate_finished") => Some("aggregate_finished"),
                Some("sort_finished") => Some("sort_finished"),
                _ => None,
            };
            if let (Some(kind), Ok(mut metrics)) = (metric_event, self.operator_metrics.lock()) {
                metrics.insert(kind, visitor.metrics);
            }
        }
    }
}

/// Runs the Phase 0 scenario suite.
pub async fn run(iterations: usize) -> Result<(), Box<dyn std::error::Error>> {
    let data_root = std::env::var_os("VEIN_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("./data"));

    // Scan events fire on tokio worker threads, so the collector must be a
    // process-wide subscriber rather than a thread-local default.
    let counters = IoCounters::default();
    tracing::subscriber::set_global_default(registry().with(counters.clone()))?;

    report_environment(&data_root);

    println!(
        "\n{:<16} {:>10} {:>10} {:>10} {:>10} {:>10} {:>12} {:>12}",
        "scenario", "prep p50", "exec p50", "exec p95", "rows", "bytes", "duckdb p50", "rows/sec"
    );
    println!("Aggregate phase metrics are reported below aggregate scenarios (last iteration).");

    // Collect results for the DuckDB comparison summary
    let mut comparison_results = Vec::new();
    for scenario in SCENARIOS {
        let duckdb_ms = duckdb_millis(scenario.duckdb, &data_root, iterations);
        let duckdb_str = duckdb_ms.map_or_else(|| "n/a".to_string(), |ms| format!("{ms:.2}ms"));
        match measure(&data_root, scenario.vein, iterations, &counters).await {
            Ok(sample) => {
                if duckdb_ms.is_some() {
                    let validated_rows = validate_scenario(&data_root, scenario).await?;
                    if sample.rows != validated_rows {
                        return Err(std::io::Error::other(format!(
                            "{} benchmark row count changed between timing and validation: {} vs {}",
                            scenario.name, sample.rows, validated_rows
                        ))
                            .into());
                    }
                    println!(
                        "  correctness: {} rows verified ({})",
                        validated_rows,
                        validation_description(scenario.validation)
                    );
                }
                print_sample(scenario.name, &sample, &duckdb_str);
                let vein_p50 = percentile(&sample.execute, 0.50).as_secs_f64() * 1e3;
                if let Some(duck_ms) = duckdb_ms {
                    comparison_results.push((scenario.name, vein_p50, duck_ms));
                }
            }
            // An unsupported scenario is reported, never silently skipped.
            Err(error) => println!("{:<16} unsupported: {error}", scenario.name),
        }
    }

    // Print DuckDB comparison summary
    if !comparison_results.is_empty() {
        println!("\n--- DuckDB Comparison Summary ---");
        println!(
            "{:20} {:>10} {:>10} {:>12}",
            "scenario", "Vein p50", "DuckDB p50", "Ratio"
        );
        for (name, vein_ms, duck_ms) in &comparison_results {
            let ratio = vein_ms / duck_ms;
            let ratio_str = if ratio < 1.0 {
                format!("{:.1}x faster", 1.0 / ratio)
            } else if ratio > 1.0 {
                format!("{:.1}x slower", ratio)
            } else {
                "parity".to_string()
            };
            println!(
                "{:20} {:>9.2}ms {:>9.2}ms {:>12}",
                name, vein_ms, duck_ms, ratio_str
            );
        }
    }

    Ok(())
}

fn print_sample(name: &str, sample: &Measurement, duckdb: &str) {
    let p50 = percentile(&sample.execute, 0.50);
    println!(
        "{:<16} {:>9.2}ms {:>9.2}ms {:>9.2}ms {:>10} {:>10} {:>12} {:>12.0}",
        name,
        millis(percentile(&sample.prepare, 0.50)),
        millis(p50),
        millis(percentile(&sample.execute, 0.95)),
        sample.rows,
        sample.bytes_read,
        duckdb,
        sample.rows as f64 / p50.as_secs_f64().max(f64::MIN_POSITIVE),
    );
    if let Some(metrics) = sample.operator_metrics.get("aggregate_finished") {
        println!(
            "  aggregate: input_rows={} batches={} sets={} groups={} probes={} candidates={} hits={} misses={} hash_matches={} key_comparisons={} retries={} new_groups={} key_bytes_copied={} retained_key_slots={} index_capacity={} growth_events={} entries_at_growth={}",
            metric(metrics, "input_rows"),
            metric(metrics, "input_batches"),
            metric(metrics, "grouping_sets"),
            metric(metrics, "groups"),
            metric(metrics, "grouping_probes"),
            metric(metrics, "group_index_candidate_checks"),
            metric(metrics, "group_index_hits"),
            metric(metrics, "group_index_misses"),
            metric(metrics, "full_hash_matches"),
            metric(metrics, "group_key_comparisons"),
            metric(metrics, "group_collision_retries"),
            metric(metrics, "new_groups"),
            metric(metrics, "string_key_bytes_copied"),
            metric(metrics, "retained_key_slots_initialized"),
            metric(metrics, "group_index_capacity"),
            metric(metrics, "group_index_capacity_growth_events"),
            metric(metrics, "group_index_entries_at_capacity_growth"),
        );
        println!(
            "  aggregate phase ns: hash={} (setup={}, key={}, finalize={}); resolve={} (index_entries={} nested); reserve={}; key_materialize={}; group_id_distribution={}; state_update={}; output_materialize={}; finish={}",
            metric(metrics, "hash_generation_ns"),
            metric(metrics, "hasher_setup_ns"),
            metric(metrics, "key_hash_ns"),
            metric(metrics, "hash_finalize_ns"),
            metric(metrics, "group_resolution_ns"),
            metric(metrics, "group_index_entries_ns"),
            metric(metrics, "group_index_reserve_ns"),
            metric(metrics, "group_materialize_ns"),
            metric(metrics, "group_ids_distribution_ns"),
            metric(metrics, "state_update_ns"),
            metric(metrics, "output_materialize_ns"),
            metric(metrics, "finish_ns"),
        );
    }
    if let Some(metrics) = sample.operator_metrics.get("sort_finished") {
        println!(
            "  sort: input_rows={} batches={} keys={} candidates={} insertions={} replacements={} key_bytes_copied={} output_rows={}",
            metric(metrics, "input_rows"),
            metric(metrics, "input_batches"),
            metric(metrics, "keys"),
            metric(metrics, "candidate_rows_examined"),
            metric(metrics, "candidate_insertions"),
            metric(metrics, "candidate_replacements"),
            metric(metrics, "candidate_key_bytes_copied"),
            metric(metrics, "output_rows"),
        );
        println!(
            "  sort phase us: key_encode={} selection={} sort={} emit={}",
            metric(metrics, "key_encode_us"),
            metric(metrics, "selection_us"),
            metric(metrics, "sort_us"),
            metric(metrics, "emit_us"),
        );
    }
}

fn metric(metrics: &BTreeMap<&'static str, u64>, name: &'static str) -> u64 {
    metrics.get(name).copied().unwrap_or_default()
}

/// DuckDB's own measured execution time for the equivalent query.
///
/// `EXPLAIN ANALYZE` reports the query's total time and prints no rows, so the
/// number is not polluted by result rendering — the same exclusion the vein side
/// gets by timing only `into_stream()`. Returns `None` when DuckDB is absent.
fn duckdb_millis(sql: &str, data_root: &Path, iterations: usize) -> Option<f64> {
    let mut samples = Vec::with_capacity(iterations);
    let sql = duckdb_sql_for_data_root(sql, data_root);
    for _ in 0..iterations.max(1) {
        let query = format!("SET threads=1; EXPLAIN ANALYZE {sql}");
        let output = Command::new("duckdb")
            .args([":memory:", "-c", &query])
            .output()
            .ok()?;
        if !output.status.success() {
            return None;
        }
        let text = String::from_utf8_lossy(&output.stdout);
        // The plan is box-drawn, so take the first whitespace-delimited token
        // after the label rather than trimming the whole (padded) line.
        let seconds = text
            .lines()
            .find_map(|line| line.split("Total Time:").nth(1))
            .and_then(|value| value.split_whitespace().next())
            .and_then(|value| value.trim_end_matches('s').parse::<f64>().ok())?;
        samples.push(seconds * 1e3);
    }
    samples.sort_by(f64::total_cmp);
    samples.get(samples.len() / 2).copied()
}

/// Runs one untimed pass through both engines and compares result values.
///
/// The benchmark timing path only counts rows. Keeping value validation separate
/// prevents formatting and cross-engine comparison from contaminating timings.
async fn validate_scenario(
    data_root: &Path,
    scenario: &Scenario,
) -> Result<u64, Box<dyn std::error::Error>> {
    let engine = LocalQueryEngine::new(data_root.to_path_buf());
    let prepared = engine.prepare(scenario.vein).await?;
    let mut stream = prepared.into_stream();
    let mut schema = None;
    let mut vein_rows = Vec::new();

    while let Some(batch) = stream.next().await {
        let batch = batch?;
        schema.get_or_insert_with(|| batch.schema());
        vein_rows.extend(batch_rows_as_json(&batch)?);
    }

    let duckdb_sql = match &schema {
        Some(schema) => validation_sql(scenario.duckdb, data_root, schema),
        None => duckdb_sql_for_data_root(scenario.duckdb, data_root),
    };
    let output = Command::new("duckdb")
        .args(["-json", ":memory:", "-c", &duckdb_sql])
        .output()?;
    if !output.status.success() {
        return Err(std::io::Error::other(format!(
            "DuckDB correctness query for {} failed: {}",
            scenario.name,
            String::from_utf8_lossy(&output.stderr).trim()
        ))
            .into());
    }

    let duckdb_objects: Vec<serde_json::Map<String, Value>> =
        serde_json::from_slice(&output.stdout)?;
    let duckdb_rows = duckdb_objects
        .into_iter()
        .map(|row| {
            (0..schema.as_ref().map_or(0, |schema| schema.fields().len()))
                .map(|index| {
                    row.get(&format!("vein_col_{index}"))
                        .cloned()
                        .ok_or_else(|| {
                            std::io::Error::other(format!(
                                "DuckDB correctness output for {} is missing column {index}",
                                scenario.name
                            ))
                        })
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .collect::<Result<Vec<_>, _>>()?;
    if !rows_match(&vein_rows, &duckdb_rows, scenario.validation) {
        return Err(std::io::Error::other(format!(
            "{} returned different values from DuckDB (Vein rows: {}, DuckDB rows: {}, mode: {})",
            scenario.name,
            vein_rows.len(),
            duckdb_rows.len(),
            validation_description(scenario.validation)
        ))
            .into());
    }

    Ok(u64::try_from(vein_rows.len())?)
}

fn batch_rows_as_json(batch: &RecordBatch) -> Result<Vec<Vec<Value>>, Box<dyn std::error::Error>> {
    let mut rows = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let values = batch
            .columns()
            .iter()
            .map(|column| {
                if column.is_null(row) {
                    Ok(Value::Null)
                } else {
                    Ok(Value::String(arrow::util::display::array_value_to_string(
                        column.as_ref(), row,
                    )?))
                }
            })
            .collect::<Result<Vec<_>, arrow::error::ArrowError>>()?;
        rows.push(values);
    }
    Ok(rows)
}

fn validation_sql(sql: &str, data_root: &Path, schema: &arrow::datatypes::Schema) -> String {
    let projection = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            format!(
                "CAST(bench_result.{} AS VARCHAR) AS {}",
                quote_identifier(field.name()),
                quote_identifier(&format!("vein_col_{index}"))
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "SELECT {projection} FROM ({}) AS bench_result",
        duckdb_sql_for_data_root(sql, data_root)
    )
}

fn duckdb_sql_for_data_root(sql: &str, data_root: &Path) -> String {
    let table_root = if data_root.join("orders").is_dir() {
        data_root.to_path_buf()
    } else {
        data_root.join("tpch")
    };
    let escaped_path = table_root.to_string_lossy().replace('\'', "''");
    sql.replace("./data/tpch", &escaped_path)
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn rows_match(left: &[Vec<Value>], right: &[Vec<Value>], mode: ValidationMode) -> bool {
    if left.len() != right.len() {
        return false;
    }
    match mode {
        ValidationMode::RowCountOnly => true,
        ValidationMode::Ordered => left == right,
        ValidationMode::Unordered => {
            let mut left = canonical_rows(left);
            let mut right = canonical_rows(right);
            left.sort_unstable();
            right.sort_unstable();
            left == right
        }
    }
}

fn canonical_rows(rows: &[Vec<Value>]) -> Vec<String> {
    rows.iter()
        .map(|row| serde_json::to_string(row).expect("JSON values always serialize"))
        .collect()
}

fn validation_description(mode: ValidationMode) -> &'static str {
    match mode {
        ValidationMode::Ordered => "ordered values",
        ValidationMode::Unordered => "unordered values",
        ValidationMode::RowCountOnly => "row count (LIMIT without ORDER BY)",
    }
}

async fn measure(
    data_root: &Path,
    query: &str,
    iterations: usize,
    counters: &IoCounters,
) -> Result<Measurement, Box<dyn std::error::Error>> {
    let engine = LocalQueryEngine::new(data_root.to_path_buf());
    let mut prepare = Vec::with_capacity(iterations);
    let mut execute = Vec::with_capacity(iterations);
    let mut rows = 0u64;
    let mut bytes_read = 0u64;
    let mut operator_metrics = BTreeMap::new();

    for _ in 0..iterations.max(1) {
        let before = counters.bytes_read.load(Ordering::Relaxed);
        let started = Instant::now();
        let prepared = engine.prepare(query).await?;
        prepare.push(started.elapsed());

        counters.reset_operator_metrics();
        let started = Instant::now();
        let mut stream = prepared.into_stream();
        rows = 0;
        while let Some(batch) = stream.next().await {
            rows += batch?.num_rows() as u64;
        }
        execute.push(started.elapsed());
        bytes_read = counters.taken(before);
        operator_metrics = counters.operator_metrics();
    }
    Ok(Measurement {
        rows,
        bytes_read,
        operator_metrics,
        prepare,
        execute,
    })
}

fn percentile(samples: &[Duration], quantile: f64) -> Duration {
    let mut sorted: Vec<Duration> = samples.to_vec();
    sorted.sort_unstable();
    let index = (sorted.len().saturating_sub(1) as f64 * quantile).round() as usize;
    sorted.get(index).copied().unwrap_or_default()
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1e3
}

/// §3 report header. Platform probes degrade to `unknown` rather than failing.
fn report_environment(data_root: &Path) {
    println!("Vein version:  {}", env!("CARGO_PKG_VERSION"));
    println!("CPU:           {}", cpu_description());
    println!("RAM:           {}", ram_description());
    println!("Cores:         {}", core_count());
    println!("Arch:          {}", std::env::consts::ARCH);
    println!("OS:            {}", os_version());
    println!("Compiler:      {}", command("rustc", &["--version"]));
    println!("Build flags:   debug_assertions={}", cfg!(debug_assertions));
    println!("DuckDB threads: 1 (single-thread comparison)");
    println!("Dataset:       {}", data_root.display());
    println!("Cold/Warm:     warm");
}

/// `sysctl` is denied under a sandboxed runner, so fall back to the facts std can
/// report rather than printing a CPU model that was never read.
fn cpu_description() -> String {
    let brand = command("sysctl", &["-n", "machdep.cpu.brand_string"]);
    if brand == UNKNOWN {
        format!("unknown ({} cores)", core_count())
    } else {
        brand
    }
}

fn ram_description() -> String {
    match command("sysctl", &["-n", "hw.memsize"]).parse::<u64>() {
        Ok(bytes) => format!("{:.1} GiB", bytes as f64 / (1u64 << 30) as f64),
        Err(_) => UNKNOWN.to_string(),
    }
}

fn core_count() -> usize {
    std::thread::available_parallelism().map_or(0, |count| count.get())
}

fn os_version() -> String {
    let release = command("uname", &["-sr"]);
    let product = command("sw_vers", &["-productVersion"]);
    if product == UNKNOWN {
        release
    } else {
        format!("{release} (macOS {product})")
    }
}

const UNKNOWN: &str = "unknown";

fn command(program: &str, args: &[&str]) -> String {
    Command::new(program)
        .args(args)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| UNKNOWN.to_string())
}

#[cfg(test)]
mod tests {
    use super::{ValidationMode, rows_match};
    use serde_json::json;

    #[test]
    fn ordered_validation_preserves_row_order() {
        let expected = vec![vec![json!("first")], vec![json!("second")]];
        let reversed = vec![vec![json!("second")], vec![json!("first")]];

        assert!(!rows_match(&expected, &reversed, ValidationMode::Ordered));
    }

    #[test]
    fn unordered_validation_compares_values_as_a_multiset() {
        let expected = vec![vec![json!("first")], vec![json!("second")]];
        let reversed = vec![vec![json!("second")], vec![json!("first")]];

        assert!(rows_match(&expected, &reversed, ValidationMode::Unordered));
        assert!(!rows_match(
            &expected,
            &[vec![json!("first")], vec![json!("wrong")]],
            ValidationMode::Unordered
        ));
    }

    #[test]
    fn unordered_validation_preserves_duplicate_row_counts() {
        let expected = vec![vec![json!("same")], vec![json!("same")]];
        let missing_duplicate = vec![vec![json!("same")], vec![json!("different")]];

        assert!(!rows_match(
            &expected,
            &missing_duplicate,
            ValidationMode::Unordered
        ));
    }

    #[test]
    fn unordered_limit_validation_only_requires_equal_row_counts() {
        let vein = vec![vec![json!("row A")]];
        let duckdb = vec![vec![json!("row B")]];

        assert!(rows_match(&vein, &duckdb, ValidationMode::RowCountOnly));
        assert!(!rows_match(
            &vein,
            &[vec![json!("row B")], vec![json!("row C")]],
            ValidationMode::RowCountOnly
        ));
    }
}
