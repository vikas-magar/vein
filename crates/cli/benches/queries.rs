use std::path::{Path, PathBuf};
use std::process::Command;

use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use common::Result;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use error::{ErrorCategory, VeinError, VeinResultExt};
use futures::StreamExt;
use serde_json::{Map, Value};
use tokio::runtime::Runtime;

use cli::local::LocalQueryEngine;

#[derive(Clone, Copy, Debug)]
enum Validation {
    Ordered,
    Unordered,
    RowCountOnly,
}

struct Scenario {
    name: &'static str,
    pipe: &'static str,
    duckdb: &'static str,
    validation: Validation,
}

const SCENARIOS: &[Scenario] = &[
    Scenario {
        name: "scan/full",
        pipe: "FROM orders |> SELECT *",
        duckdb: "SELECT * FROM './data/tpch/orders/*.parquet'",
        validation: Validation::Unordered,
    },
    Scenario {
        name: "scan/narrow",
        pipe: "FROM orders |> SELECT o_orderkey",
        duckdb: "SELECT o_orderkey FROM './data/tpch/orders/*.parquet'",
        validation: Validation::Unordered,
    },
    Scenario {
        name: "scan/wide",
        pipe: "FROM orders |> SELECT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate",
        duckdb: "SELECT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate FROM './data/tpch/orders/*.parquet'",
        validation: Validation::Unordered,
    },
    Scenario {
        name: "scan/selective",
        pipe: "FROM orders |> WHERE o_orderkey = 1 |> SELECT o_orderkey, o_totalprice",
        duckdb: "SELECT o_orderkey, o_totalprice FROM './data/tpch/orders/*.parquet' WHERE o_orderkey = 1",
        validation: Validation::Unordered,
    },
    Scenario {
        name: "scan/no-match",
        pipe: "FROM orders |> WHERE o_orderkey = 99999999 |> SELECT o_orderkey",
        duckdb: "SELECT o_orderkey FROM './data/tpch/orders/*.parquet' WHERE o_orderkey = 99999999",
        validation: Validation::Unordered,
    },
    Scenario {
        name: "temporal/truncate-month",
        pipe: "FROM orders |> EXTEND TRUNCATE(o_orderdate, 'MONTH') AS order_month |> SELECT o_orderkey, order_month",
        duckdb: "SELECT o_orderkey, CAST(date_trunc('month', o_orderdate) AS DATE) AS order_month FROM './data/tpch/orders/*.parquet'",
        validation: Validation::Unordered,
    },
    Scenario {
        name: "join/self-key",
        pipe: "FROM orders AS a |> JOIN orders AS b ON a.o_orderkey = b.o_orderkey |> SELECT a.o_orderkey, b.o_totalprice",
        duckdb: "SELECT a.o_orderkey, b.o_totalprice FROM './data/tpch/orders/*.parquet' a JOIN './data/tpch/orders/*.parquet' b ON a.o_orderkey = b.o_orderkey",
        validation: Validation::Unordered,
    },
    Scenario {
        name: "limit/5",
        pipe: "FROM orders |> SELECT o_orderkey, o_totalprice |> LIMIT 5",
        duckdb: "SELECT o_orderkey, o_totalprice FROM './data/tpch/orders/*.parquet' LIMIT 5",
        validation: Validation::RowCountOnly,
    },
    Scenario {
        name: "aggregate/post-filter",
        pipe: "FROM orders |> AGGREGATE count(o_orderkey) AS order_count GROUP BY o_orderstatus |> WHERE order_count > 50000",
        duckdb: "SELECT o_orderstatus, count(o_orderkey) AS order_count FROM './data/tpch/orders/*.parquet' GROUP BY o_orderstatus HAVING count(o_orderkey) > 50000",
        validation: Validation::Unordered,
    },
    Scenario {
        name: "aggregate/high-cardinality",
        pipe: "FROM orders |> AGGREGATE count(o_orderkey) AS order_count GROUP BY o_orderkey",
        duckdb: "SELECT o_orderkey, count(o_orderkey) AS order_count FROM './data/tpch/orders/*.parquet' GROUP BY o_orderkey",
        validation: Validation::Unordered,
    },
    Scenario {
        name: "order/multi-key-limit",
        pipe: "FROM orders |> SELECT o_orderkey, o_totalprice |> ORDER o_totalprice DESC, o_orderkey ASC |> LIMIT 100",
        duckdb: "SELECT o_orderkey, o_totalprice FROM './data/tpch/orders/*.parquet' ORDER BY o_totalprice DESC, o_orderkey ASC LIMIT 100",
        validation: Validation::Ordered,
    },
    Scenario {
        name: "order/computed-key-limit",
        pipe: "FROM orders |> SELECT o_orderkey, o_totalprice |> ORDER o_orderkey + 1 DESC |> LIMIT 100",
        duckdb: "SELECT o_orderkey, o_totalprice FROM './data/tpch/orders/*.parquet' ORDER BY o_orderkey + 1 DESC LIMIT 100",
        validation: Validation::Ordered,
    },
];

fn criterion_benchmark(criterion: &mut Criterion) {
    let data_root = std::env::var_os("VEIN_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("./data"));
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("create Tokio runtime for query benchmarks");

    validate_against_duckdb(&runtime, &data_root);

    let mut group = criterion.benchmark_group("query");
    for scenario in SCENARIOS {
        let engine = LocalQueryEngine::new(data_root.clone());
        group.throughput(Throughput::Elements(1));
        group.bench_function(format!("prepare/{}", scenario.name), |bencher| {
            bencher.to_async(&runtime).iter(|| engine.prepare(scenario.pipe));
        });
        group.bench_function(format!("end_to_end/{}", scenario.name), |bencher| {
            bencher.to_async(&runtime).iter(|| async {
                let query = engine
                    .prepare(scenario.pipe)
                    .await
                    .unwrap_or_else(|error| panic!("prepare {}: {error}", scenario.name));
                let mut stream = query.into_stream();
                let mut rows = 0_u64;
                while let Some(batch) = stream.next().await {
                    rows += batch
                        .unwrap_or_else(|error| panic!("execute {}: {error}", scenario.name))
                        .num_rows() as u64;
                }
                std::hint::black_box(rows)
            });
        });
    }
    group.finish();
}

fn validate_against_duckdb(runtime: &Runtime, data_root: &Path) {
    if Command::new("duckdb").arg("--version").output().is_err() {
        eprintln!("DuckDB not found; skipping benchmark correctness preflight");
        return;
    }

    for scenario in SCENARIOS {
        if let Err(error) = runtime.block_on(validate_scenario(data_root, scenario)) {
            panic!("correctness preflight failed for {}: {error}", scenario.name);
        }
    }
}

async fn validate_scenario(data_root: &Path, scenario: &Scenario) -> Result<()> {
    let engine = LocalQueryEngine::new(data_root.to_path_buf());
    let query = engine.prepare(scenario.pipe).await?;
    let mut stream = query.into_stream();
    let mut schema = None;
    let mut vein_rows = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        schema.get_or_insert_with(|| batch.schema());
        vein_rows.extend(batch_rows(&batch)?);
    }

    let duckdb_sql = match &schema {
        Some(schema) => validation_sql(scenario.duckdb, data_root, schema),
        None => format!(
            "SELECT count(*) AS row_count FROM ({}) AS bench_result",
            duckdb_sql_for_data_root(scenario.duckdb, data_root)
        ),
    };
    let output = Command::new("duckdb")
        .args(["-json", ":memory:", "-c", &duckdb_sql])
        .output()?;
    if !output.status.success() {
        return Err(VeinError::execution_failed(format!(
            "DuckDB validation command failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    let duckdb_objects: Vec<Map<String, Value>> = serde_json::from_slice(&output.stdout)
        .map_external_err(ErrorCategory::Data, "decoding DuckDB validation output")?;
    let Some(schema) = schema else {
        let expected: u64 = duckdb_objects
            .first()
            .and_then(|row| row.get("row_count"))
            .and_then(|value| {
                value.as_u64().or_else(|| value.as_str().and_then(|text| text.parse().ok()))
            })
            .ok_or_else(|| VeinError::serialization("DuckDB row-count output was not an integer"))?;
        if expected != vein_rows.len() as u64 {
            return Err(VeinError::execution_failed(format!(
                "row count differs: Vein {}, DuckDB {expected}",
                vein_rows.len()
            )));
        }
        return Ok(());
    };
    let columns = schema.fields().len();
    let duckdb_rows = duckdb_objects
        .into_iter()
        .map(|row| {
            (0..columns)
                .map(|index| {
                    row.get(&format!("vein_col_{index}"))
                        .cloned()
                        .ok_or_else(|| {
                            VeinError::serialization(format!(
                                "missing DuckDB output column {index}"
                            ))
                        })
                })
                .collect::<Result<Vec<_>>>()
        })
        .collect::<Result<Vec<_>>>()?;

    let matches = match scenario.validation {
        Validation::RowCountOnly => vein_rows.len() == duckdb_rows.len(),
        Validation::Ordered => vein_rows == duckdb_rows,
        Validation::Unordered => {
            let mut left = canonical_rows(&vein_rows)?;
            let mut right = canonical_rows(&duckdb_rows)?;
            left.sort_unstable();
            right.sort_unstable();
            left == right
        }
    };
    if !matches {
        return Err(VeinError::execution_failed(format!(
            "rows differ (Vein {}, DuckDB {}, validation {:?})",
            vein_rows.len(),
            duckdb_rows.len(),
            scenario.validation
        )));
    }
    Ok(())
}

fn batch_rows(batch: &RecordBatch) -> Result<Vec<Vec<Value>>> {
    (0..batch.num_rows())
        .map(|row| {
            batch.columns().iter().map(|column| {
                if column.is_null(row) {
                    Ok(Value::Null)
                } else {
                    Ok(Value::String(arrow::util::display::array_value_to_string(column.as_ref(), row)?))
                }
            }).collect()
        })
        .collect()
}

fn canonical_rows(rows: &[Vec<Value>]) -> Result<Vec<String>> {
    rows.iter()
        .map(|row| {
            serde_json::to_string(row)
                .map_external_err(ErrorCategory::Data, "encoding validation row")
        })
        .collect()
}

fn validation_sql(sql: &str, data_root: &Path, schema: &arrow::datatypes::Schema) -> String {
    let projection = schema.fields().iter().enumerate().map(|(index, field)| {
        format!("CAST(bench_result.\"{}\" AS VARCHAR) AS \"vein_col_{index}\"", field.name().replace('"', "\"\""))
    }).collect::<Vec<_>>().join(", ");
    format!("SELECT {projection} FROM ({}) AS bench_result", duckdb_sql_for_data_root(sql, data_root))
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

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
