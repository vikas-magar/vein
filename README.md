# Vein

## Put analytics inside your Rust product

Vein is an embedded analytical engine for teams building Rust applications that need to query columnar data close to the workflow that uses it.

With Vein, your product can analyze local Parquet data through a composable query pipeline without requiring a separate database service for the current local workflow. The engine is built in Rust with Arrow-backed columnar execution and extension points for functions and data providers.

**The promise:** bring practical analytics closer to your application, its data, and its users.

## Why Vein

- **Analytics without another service to operate.** The current local workflow runs in a single process over files in your configured data directory.
- **A query language made for pipelines.** Compose data sources, joins, filters, projections, aggregates, and ordering as readable steps.
- **Columnar foundations.** Vein uses Arrow arrays and reference-backed batch views in execution paths, with an ongoing focus on reducing data movement.
- **Rust extensibility.** Built-in and registered scalar and aggregate functions let applications extend query behavior in Rust.
- **A foundation for local-first products.** Vein is being developed for application-owned analytics in desktop, edge, and other data-intensive Rust workflows.

Vein is an early-access developer preview. Query coverage, performance, APIs, and zero-copy behavior are still evolving; it is not yet a production-ready replacement for established analytical databases.

## TPC-H evaluation

Vein includes paired pipe-style and ANSI SQL definitions for all 22 TPC-H queries, plus a comparison helper that runs Vein and DuckDB against the same dataset and checks result rows.

Use this to explore query coverage, correctness differences, and per-query timings:

```bash
VEIN_DATA_DIR=/path/to/tpch python3 assets/sql/tpch/compare.py
```

The helper requires the DuckDB command-line tool and an existing TPC-H Parquet dataset. Run it without query IDs to compare the full suite.

The helper compares row multisets, preserving duplicates while ignoring row order. Its displayed timings include process startup and are useful for development comparisons, not as controlled benchmark claims. For meaningful performance claims, benchmark a fixed dataset and machine with repeated runs, warmups, and matching engine settings.

The query definitions are in [pipe format](assets/sql/tpch/pipe/queries.pipe) and [ANSI SQL](assets/sql/tpch/ansi/queries.sql).

## Try Vein

Install a current stable Rust toolchain and build the workspace:

```bash
cargo build --workspace
cargo test --workspace
```

Vein does not include a dataset. Point `VEIN_DATA_DIR` at a directory containing one folder per table, with Parquet files inside:

```text
/path/to/data/
├── orders/
│   ├── orders.1.parquet
│   └── orders.2.parquet
└── lineitem/
    └── lineitem.1.parquet
```

Run a query from the repository root:

```bash
VEIN_DATA_DIR=/path/to/data cargo run -p cli -- "FROM orders |> WHERE o_orderkey = 1 |> SELECT o_orderkey, o_orderdate, o_orderstatus, o_totalprice"
```

Without `VEIN_DATA_DIR`, Vein looks under `./data`. Terminal-table output is the default. Set `VEIN_OUTPUT_FORMAT=jsonl` for JSON Lines:

```bash
VEIN_DATA_DIR=/path/to/data VEIN_OUTPUT_FORMAT=jsonl cargo run -p cli -- "FROM orders |> WHERE o_orderkey = 1 |> SELECT o_orderkey, o_orderdate, o_totalprice"
```

## Future scope

Vein is being built incrementally. The product roadmap includes:

- A stable embedding API for Rust applications.
- Broader query correctness and workload coverage.
- Faster Parquet scans, stronger statistics and pruning, and validated runtime adaptation.
- Fewer materialization and copying boundaries across the full query path.
- Additional storage backends and file-format connectors.
- A complete browser/WebAssembly runtime and data-access interface.
- Client and database-protocol integrations that fit application use cases.
- Distributed execution after the single-node engine is dependable.

TPC-H results and other workload measurements will guide this work. Reproducible queries, datasets, and application use cases are welcome.
