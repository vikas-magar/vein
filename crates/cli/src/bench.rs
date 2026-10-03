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

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use futures::StreamExt;
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
}

const SCENARIOS: &[Scenario] = &[
    Scenario {
        name: "scan/full",
        vein: "FROM orders\n|> SELECT *",
        duckdb: "SELECT * FROM './data/tpch/orders/*.parquet'",
    },
    Scenario {
        name: "scan/narrow",
        vein: "FROM orders\n|> SELECT o_orderkey",
        duckdb: "SELECT o_orderkey FROM './data/tpch/orders/*.parquet'",
    },
    Scenario {
        name: "scan/wide",
        vein: "FROM orders\n|> SELECT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate",
        duckdb: "SELECT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate FROM './data/tpch/orders/*.parquet'",
    },
    Scenario {
        name: "scan/selective",
        vein: "FROM orders\n|> WHERE o_orderkey = 1\n|> SELECT o_orderkey, o_totalprice",
        duckdb: "SELECT o_orderkey, o_totalprice FROM './data/tpch/orders/*.parquet' WHERE o_orderkey = 1",
    },
    Scenario {
        name: "scan/no-match",
        vein: "FROM orders\n|> WHERE o_orderkey = 99999999\n|> SELECT o_orderkey",
        duckdb: "SELECT o_orderkey FROM './data/tpch/orders/*.parquet' WHERE o_orderkey = 99999999",
    },
    // A self-join, so both sides read the same files: one match per row, which
    // makes the row count checkable against DuckDB's.
    Scenario {
        name: "join/self-key",
        vein: "FROM orders AS a\n|> JOIN orders AS b ON a.o_orderkey = b.o_orderkey\n|> SELECT a.o_orderkey, b.o_totalprice",
        duckdb: "SELECT a.o_orderkey, b.o_totalprice FROM './data/tpch/orders/*.parquet' a JOIN './data/tpch/orders/*.parquet' b ON a.o_orderkey = b.o_orderkey",
    },
    // A limit is the one scenario answered by *not* reading. The window fills
    // inside the first batch, so the scan stops there and the number to beat is
    // DuckDB's, which stops the same way.
    Scenario {
        name: "limit/5",
        vein: "FROM orders\n|> SELECT o_orderkey, o_totalprice\n|> LIMIT 5",
        duckdb: "SELECT o_orderkey, o_totalprice FROM './data/tpch/orders/*.parquet' LIMIT 5",
    },
    Scenario {
        name: "aggregate/post-filter",
        vein: "FROM orders\n|> AGGREGATE count(o_orderkey) AS order_count GROUP BY o_orderstatus\n|> WHERE order_count > 50000",
        duckdb: "SELECT o_orderstatus, count(o_orderkey) AS order_count FROM './data/tpch/orders/*.parquet' GROUP BY o_orderstatus HAVING count(o_orderkey) > 50000",
    },
];

/// One scenario's samples.
struct Measurement {
    rows: u64,
    bytes_read: u64,
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
}

impl IoCounters {
    fn taken(&self, before: u64) -> u64 {
        self.bytes_read.load(Ordering::Relaxed).saturating_sub(before)
    }
}

#[derive(Default)]
struct FieldVisitor {
    kind: Option<String>,
    bytes_read: u64,
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
    for scenario in SCENARIOS {
        let duckdb = duckdb_millis(scenario.duckdb, iterations)
            .map_or_else(|| "n/a".to_string(), |ms| format!("{ms:.2}ms"));
        match measure(&data_root, scenario.vein, iterations, &counters).await {
            Ok(sample) => {
                let p50 = percentile(&sample.execute, 0.50);
                println!(
                    "{:<16} {:>9.2}ms {:>9.2}ms {:>9.2}ms {:>10} {:>10} {:>12} {:>12.0}",
                    scenario.name,
                    millis(percentile(&sample.prepare, 0.50)),
                    millis(p50),
                    millis(percentile(&sample.execute, 0.95)),
                    sample.rows,
                    sample.bytes_read,
                    duckdb,
                    sample.rows as f64 / p50.as_secs_f64().max(f64::MIN_POSITIVE),
                );
            }
            // An unsupported scenario is reported, never silently skipped.
            Err(error) => println!("{:<16} unsupported: {error}", scenario.name),
        }
    }
    Ok(())
}

/// DuckDB's own measured execution time for the equivalent query.
///
/// `EXPLAIN ANALYZE` reports the query's total time and prints no rows, so the
/// number is not polluted by result rendering — the same exclusion the vein side
/// gets by timing only `into_stream()`. Returns `None` when DuckDB is absent.
fn duckdb_millis(sql: &str, iterations: usize) -> Option<f64> {
    let mut samples = Vec::with_capacity(iterations);
    for _ in 0..iterations.max(1) {
        let output = Command::new("duckdb")
            .args([":memory:", "-c", &format!("EXPLAIN ANALYZE {sql}")])
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

    for _ in 0..iterations.max(1) {
        let before = counters.bytes_read.load(Ordering::Relaxed);
        let started = Instant::now();
        let prepared = engine.prepare(query).await?;
        prepare.push(started.elapsed());

        let started = Instant::now();
        let mut stream = prepared.into_stream();
        rows = 0;
        while let Some(batch) = stream.next().await {
            rows += batch?.num_rows() as u64;
        }
        execute.push(started.elapsed());
        bytes_read = counters.taken(before);
    }
    Ok(Measurement {
        rows,
        bytes_read,
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
