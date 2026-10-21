# Catalog, Storage, and Connector Refactor Plan

Status: implementation in progress — T1 through T6 complete; T7 writer migration is next
Scope: the catalog-to-storage-to-scan path and its engine/CLI composition.

## Goal

Consolidate all Parquet-specific behavior in one connector, retain one generic catalog contract and metadata cache, and
give execution one format-neutral multi-file scan path. Keep query orchestration independent from concrete connectors
and storage backends.

The internal execution batch remains `DataChunkView`. Parquet decoding necessarily creates Arrow buffers; the zero-copy
requirement is that Vein does not add batch/column copies between decoding, scan output, operators, and sinks.

## Current source analysis

- The workspace has `catalog`, `storage`, `exec`, and `cli`, but no generic `engine` or Parquet connector crate.
  See [Cargo.toml](/Users/anvi/Next/vein/Cargo.toml:1).
- `catalog` already has format-neutral table/statistics contracts, catalog sessions, snapshots, and a typed metadata
  cache. Its docs say format implementations should live above it.
  See [catalog/src/lib.rs](/Users/anvi/Next/vein/crates/catalog/src/lib.rs:3)
  and [catalog/src/contract.rs](/Users/anvi/Next/vein/crates/catalog/src/contract.rs:61).
- The cache payload is format-neutral. `MetadataKey` uses the storage-provided object identity, size, version facts, and
  metadata kind; it declines caching when storage supplies no version validator.
  See [catalog metadata key](/Users/anvi/Next/vein/crates/catalog/src/store/key.rs:7).
- `storage` exposes typed locations, object paths and identities, optional object-version facts, and streaming listing
  alongside stat and range reads. The OpenDAL filesystem adapter implements those contracts, and the catalog cache
  consumes the supplied snapshot rather than doing filesystem canonicalization or stat calls.
  See [storage read contracts](/Users/anvi/Next/vein/crates/storage/src/read/contracts.rs:15), [typed locations](/Users/anvi/Next/vein/crates/storage/src/read/location.rs:4),
  and [metadata](/Users/anvi/Next/vein/crates/storage/src/read/metadata.rs:5).
- `exec` has a provider registry and borrowed `ScanRequest`. Providers own scan-unit planning through
  `ScanPlanningRequest`; the default registry still installs `ArrowParquetScanProvider`.
  See [scan/provider.rs](/Users/anvi/Next/vein/crates/exec/src/operators/source/scan/provider.rs:11)
  and [scan/registry.rs](/Users/anvi/Next/vein/crates/exec/src/operators/source/scan/registry.rs:12).
- Parquet scan and catalog behavior now live in `parquet-connector`; catalog binding and execution share its storage-backed
  metadata service and the same `MetadataStore`. `cli` still owns the transactional Parquet writer, which is the T7
  migration. See [connector catalog](/Users/anvi/Next/vein/crates/parquet-connector/src/catalog/provider.rs:17),
  [connector analysis](/Users/anvi/Next/vein/crates/parquet-connector/src/catalog/analysis.rs:17),
  [generic scan dispatch](/Users/anvi/Next/vein/crates/exec/src/factory/scan_partition.rs:10),
  and [CLI writer](/Users/anvi/Next/vein/crates/cli/src/parquet_storage.rs:1).
- The connector catalog adapter uses storage discovery and the shared metadata service for schema/footer loading,
  snapshot identities, manifest handling, and external statistics. Its ANALYZE provider reads through the connector scan
  path and publishes only after source revision and identity validation. The CLI currently supplies a narrow publisher
  adapter to the existing writer; T7 will move the writer implementation.
- `LocalQueryEngine` constructs concrete Parquet catalog/storage implementations and orchestrates the query inside
  `cli`. The CLI main also reads configuration and renders results.
  See [cli/local/mod.rs](/Users/anvi/Next/vein/crates/cli/src/local/mod.rs:29)
  and [cli/main.rs](/Users/anvi/Next/vein/crates/cli/src/main.rs:21).

## DuckDB reference and design consequence

DuckDB's catalog models database catalog entries; file-format scans use registered table functions. The physical
table-scan operator invokes the registered scan callback
([catalog.hpp](/Users/anvi/Next/vein/reference_code/duckdb/src/include/duckdb/catalog/catalog.hpp:106), [physical_table_scan.cpp](/Users/anvi/Next/vein/reference_code/duckdb/src/execution/operator/scan/physical_table_scan.cpp:33)).
Its generic multi-file wrapper adapts a single-file table function, forwards its projection/filter/statistics
capabilities, and coordinates multiple files
([table_function_multi_file.hpp](/Users/anvi/Next/vein/reference_code/duckdb/src/include/duckdb/common/multi_file/table_function_multi_file.hpp:342), [table_function_multi_file.cpp](/Users/anvi/Next/vein/reference_code/duckdb/src/common/multi_file/table_function_multi_file.cpp:775)).
The Parquet extension registers its single-file reader through this wrapper and supplies Parquet-specific scheduling and
read-ahead behavior
([read_single_parquet_file.cpp](/Users/anvi/Next/vein/reference_code/duckdb/extension/parquet/read_single_parquet_file.cpp:511)).
Its metadata cache validates source version information such as ETag and modification time
([parquet_file_metadata_cache.cpp](/Users/anvi/Next/vein/reference_code/duckdb/extension/parquet/parquet_file_metadata_cache.cpp:90)).

The Vein equivalent should have one generic scan coordinator in `exec`, with a Parquet connector that plans Parquet scan
units and opens a single-file reader. A directory-backed Parquet catalog is a connector adapter; it is not the generic
catalog implementation.

## Target architecture

```text
                          +----------------------+
                          |          CLI         |
                          | config, output, wire |
                          +----------+-----------+
                                     |
                          constructs providers
                                     |
                          +----------v-----------+
                          |        engine        |
                          | bind/optimize/execute|
                          +------+----------+----+
                                 |          |
                  +--------------v-+      +-v----------------+
                  |    catalog     |      |       exec       |
                  | contracts,     |      | generic scan and |
                  | metadata, cache|      | multi-file driver|
                  +-------+--------+      +---------+--------+
                          ^                         ^
                          |                         |
                  +-------+-------------------------+-------+
                  |              parquet-connector          |
                  | metadata service, Parquet scan/catalog  |
                  | adapter, pruning, analysis, writes       |
                  +----------------------+-------------------+
                                         |
                                  +------v-------+
                                  |   storage    |
                                  | list/stat/read|
                                  +--------------+
```

### Crate responsibilities

- **catalog**: logical table metadata, catalog/provider contracts, planning sessions and snapshots, format-neutral
  statistics, and the shared typed metadata cache. It does not parse Parquet or perform local filesystem I/O.
- **storage**: backend-neutral object locations and identity/version, listing, stat, and range reads. Storage supplies
  identity/version data; the cache key consumes it rather than stat'ing a local path. Storage does not depend on catalog
  or execution.
- **exec**: the one generic scan coordinator and multi-file execution path. It owns scheduling, streaming, common
  projection/filter interfaces, runtime feedback, and metrics. It contains no Parquet implementation or format-name
  branches.
- **parquet-connector (new)**: the sole owner of Parquet-specific code: one metadata service, scan provider/reader,
  catalog adapter, statistics conversion, pruning, analysis adapter, manifests, and writer. Its catalog adapter and scan
  provider are separate modules; both use the same metadata service and cache.
- **engine (new)**: query preparation and execution orchestration over injected catalog, function, scan-provider, and
  storage interfaces. It depends on abstractions and does not depend on a concrete file format.
- **cli**: reads configuration, constructs the engine and selected providers/backends, and presents results. It contains
  no catalog, scan-reader, or writer implementation.

### Dependency and data-flow rules

- `storage` is below the format and engine layers. It never depends on `catalog` or `exec`.
- `catalog` consumes only a backend-neutral storage identity/version contract for cache keys; it does not depend on a
  storage implementation.
- `exec` consumes catalog/storage contracts and an injected provider registry. It does not depend on
  `parquet-connector`.
- `parquet-connector` depends on `catalog`, `storage`, and the generic scan/sink contracts in `exec`. It owns
  format-specific policy and decoding.
- `engine` depends on generic parser, binder, optimizer, physical planner, catalog, functions, and execution contracts.
  It does not depend on `cli` or a connector.
- `cli` is the composition root: it selects and registers a catalog adapter, scan provider, storage backend, and writer.
- The Parquet catalog adapter and scan provider call one connector metadata service. The service parses/loads metadata
  once per object version and publishes the typed payload to the shared bounded cache.
- The generic coordinator schedules opaque connector-defined scan units; Parquet may use row groups, while another
  connector can use its own units. The coordinator streams `DataChunkView` without materializing between operators.

## Ordered implementation tasks

Each task is independently reviewable, adds focused regression tests for changed logic, and keeps every new/refactored
Rust source and test file at or below 200 lines. Use `mod.rs` only for declarations and re-exports. Use the existing
RustRover `Test`, `Check`, `Build`, and `Query` run configurations; do not add run configurations.

### T0 — Freeze contracts and migration baseline

- Record public catalog, storage, scan, sink, and engine entry points; map the current dependency graph and query
  construction path.
- Capture current Parquet catalog resolution, multi-file scan, ANALYZE, write, and representative query behavior.
- Acceptance: the plan has one target owner for each responsibility and no migration changes behavior silently.

### T1 — Add backend-neutral object identity and discovery — DONE

- Replace path-only storage roots with a typed object location; expose size plus optional stable version/ETag and
  modification time.
- Add generic list/discovery capability alongside stat/range-read. Backends may report discovery unsupported; do not
  make format code call `std::fs`.
- Acceptance tests: local and in-memory backend contract tests for listing, ranges, equivalent locations, missing
  version metadata, and changed object version.
- Completed: typed `StorageLocation`, `StorageObjectPath`, and `StorageObjectIdentity`; version metadata; streaming
  listing; OpenDAL filesystem adapter; local and in-memory contract tests, including zero-copy range slicing.
- Validation: RustRover Check and Build passed; focused storage contract tests passed.

### T2 — Re-key the catalog metadata cache — DONE

- Make `MetadataKey` use metadata kind plus storage-provided object identity/version; remove filesystem
  canonicalization/stat from the cache.
- Preserve a bounded, typed, shared cache. Add per-key single-flight initialization so concurrent catalog/scan requests
  do not parse the same metadata repeatedly.
- Acceptance tests: one parse under concurrent requests, cache hit across catalog and scan consumers, invalidation on
  version change, bounded eviction, and no local-I/O requirement.
- Complete: snapshots flow from catalog metadata through `FileScanTask` into scan planning and reader setup; the bounded
  typed cache is keyed by storage identity/version and coordinates concurrent parser misses per key. The local engine
  passes the catalog's cache handle to the executor builder, so both consumers share the same store.
- Focused validation passed: version/size invalidation, no caching without a provider validator, metadata kind/type
  isolation, FIFO eviction, concurrent parse-once, retry after parse failure, catalog footer population, and scan
  planning from a cached footer. RustRover Check and Build pass, and `FROM region |> SELECT r_regionkey |> LIMIT 1`
  exits successfully.
- Historical workspace Test failure was resolved during T5 validation: physical planner fixtures now declare their
  required test format. The current full workspace Test run passes.

### T3 — Create one generic multi-file scan coordinator — DONE

- Replace the whole-file provider callback plus Parquet-specific factory partitioning with a format-neutral coordinator
  and connector-defined scan units/readers.
- Move scheduling, work claiming, byte balancing, cancellation, common filter/projection contracts, runtime statistics,
  and metrics into that coordinator.
- Keep row-group/page interpretation in the Parquet connector. Do not encode Parquet row groups as a generic engine
  assumption.
- Acceptance tests use a fake provider with multiple files and uneven units; verify every unit is executed exactly once,
  failures/cancellation propagate, runtime filters are passed, and output views retain source buffers.
- Completed first slice: provider-owned `plan_work_units` receives a borrowed planning request; generic factory dispatch
  validates the file task and resolves providers by `DataFileFormatId`; default providers split by file; Parquet footer
  pruning and byte-balanced row-group planning live with the Parquet provider. Common file-work-unit and range-coalescing
  helpers live under `scan/planning`.
- Completed: the generic executor plans provider-owned units, bounds and schedules work, forwards runtime-filter state,
  and streams `DataChunkView` output. Parquet row-group interpretation remains in its provider. Focused tests cover
  uneven custom-provider planning, exact-once execution across files/ranges, runtime-filter planning/application,
  worker failure propagation, cancellation cleanup, and Arrow value-buffer identity.
- Focused validation passed, including the end-to-end custom-provider regression. RustRover Check and incremental
  `build_project` passed. The historical workspace Test failure was resolved during T5 validation; the current full
  workspace Test run passes.

### T4 — Add the Parquet connector and one metadata service — DONE

- Add `crates/parquet-connector` to the workspace with focused modules for metadata, catalog adapter, scan provider,
  pruning/statistics, analysis, and writer.
- Implement one storage-backed metadata service for footer/schema/page-index loading. Catalog binding, scan planning,
  and reader fallback all use this service and the same metadata cache.
- Acceptance tests: parse-once behavior across catalog/scan, concurrent initialization, cache invalidation, schema
  derivation, and files with/without optional page indexes.
- Completed: added the `parquet-connector` workspace crate and `ParquetMetadataService`. The service owns the canonical
  footer metadata kind, versioned cache access, seekable and asynchronous footer loading, optional page-index parsing,
  and Arrow schema derivation. CLI catalog discovery and the scan reader both use this service with the shared store.
- Validation: connector cache/schema regression and CLI catalog discovery regression passed; RustRover Check,
  incremental `build_project`, and a Parquet-backed Query run passed. The historical workspace Test failure was resolved
  during T5 validation; the current full workspace Test run passes.
- Remaining migration work: move the Parquet catalog and ANALYZE adapter out of CLI in T6. T5 now owns the scan provider
  and reader migration.

Migration order: establish the connector-owned metadata service first; move the scan provider and reader in T5 so the
connector can implement `exec`'s format-neutral provider contract without creating a crate cycle; then move the catalog
adapter and ANALYZE implementation in T6. Keep the existing paths operational until each replacement is wired and
covered, then delete the old path in that same migration step.

### T5 — Move Parquet scanning and pruning out of exec — DONE

- Move the Parquet reader, I/O backend, projection mapping, predicate ordering/evaluation, row-group/page pruning,
  Bloom/dictionary checks, and format statistics conversion into the connector.
- Register its scan provider through injection; remove the default Parquet provider from generic `exec`.
- Remove the temporary `exec -> parquet-connector` dependency while moving the reader/provider; the final dependency
  direction is `parquet-connector -> exec` for implementation of the generic scan contract.
- Acceptance tests: existing scan semantics, filter/projection pushdown, row-group/page pruning, runtime-filter
  interaction, diagnostics, and zero-copy `DataChunkView` handoff.
- Implemented: moved the Parquet scan reader, I/O backends, projection/predicate handling, pruning, and Parquet statistics
  into `parquet-connector`. The connector implements the generic `exec::ScanProvider` contract; CLI registers that
  provider explicitly. `exec` has no Parquet production or test dependency, format import, or implicit provider.
- Moved Parquet scan, runtime-filter, row-group, and provider-planning tests beside the connector; retained a fake-provider
  dispatch regression in `exec`.
- The scan stream's batch-consumption loop now lives in focused `stream/consume`, `stream/context`, and `stream/metrics`
  modules. It retains the existing poll/CPU timings, runtime-statistics queue accounting, progress reporting, and
  `DataChunkView` handoff.
- Implemented: split the per-file scan coordinator into preparation, pruning, predicate installation, stream consumption,
  and audit modules. Dictionary loading/pruning, metadata probes, Bloom checks, and predicate classification have their
  own focused modules. The Arrow batch/view path is unchanged; only shared metadata handles cross asynchronous stages.
- Cleanup audit: `exec/src` has no Parquet references. Its old Parquet directory tree contains no source files; it is
  an untracked empty-directory remnant, not a second implementation. T6 moved catalog and ANALYZE ownership out of CLI;
  the CLI writer remains active until T7.
- Validation: the focused `parquet-connector` Test run passed all 80 tests; the physical crate tests and full workspace
  Test configuration pass; RustRover Check and incremental `build_project(rebuild: false)` pass; and Query successfully
  ran `FROM lineitem |> WHERE l_shipmode = 'MAIL' |> AGGREGATE count(*) AS n` with exit code 0. The named Build
  configuration failed to start in RustRover, so the IDE's incremental build tool was used instead.
- Cleanup: a single test-only external-scan fixture now supplies the required format option; the shared catalog-scan
  fixture uses the same format constant. This fixed the stale planner test setup without weakening production checks.

### T6 — Move the Parquet catalog and ANALYZE adapter out of CLI

- Move deterministic discovery, schema resolution/validation, manifest handling, snapshot construction, and
  analyzed-statistics application into the connector's catalog module.
- Use storage discovery and the shared metadata service. Keep format-neutral statistics contracts in `catalog` and
  query-scoped runtime statistics in their existing shared statistics context.
- Acceptance tests: directory discovery, manifests, inconsistent schemas, source replacement/revision changes, external
  statistics, and ANALYZE over a fake/local backend.
- Completed: moved catalog discovery, schema resolution, manifest/sidecar loading, snapshot construction, source identity
  validation, footer-stat conversion, and statistics application into `parquet-connector/src/catalog`. Catalog and scan
  share the connector metadata service and the same typed metadata cache.
- Completed: implemented the connector's `StatisticsAnalysisProvider` over the connector Parquet scan stream. It validates
  source revision/schema/identities before publication. CLI retains only a `ParquetStatisticsPublisher` adapter that
  delegates to the existing writer, pending T7.
- Removed `cli/src/parquet_catalog.rs` and its analysis module, and deleted obsolete read-side catalog helpers from the
  CLI writer. The CLI no longer contains a second catalog or metadata-loading implementation.
- Regression coverage: connector catalog tests verify deterministic discovery, shared footer caching, schema mismatch
  rejection, and file/row-group/table footer statistics. The CLI ANALYZE integration test verifies external statistics
  analysis/publication, sidecar loading, source-replacement invalidation, and re-analysis.
- Validation: workspace Test and Check pass; Query successfully ran `FROM region |> SELECT r_name |> LIMIT 1` through the
  configured TPCH root. The named Build configuration could not start in RustRover, but incremental
  `build_project(rebuild: false)` passed with no problems. The formatter reported a RustRover PSI/index mismatch on one
  small test fixture; Check and Test passed after that attempt.

### T7 — Move Parquet writing and manifest lifecycle (Complete)

- Move Parquet encoding, manifests, sidecars, and source-identity collection to the connector's writer module. Keep the
  generic executor sink contract in `exec`.
- Route file operations through storage contracts where supported; avoid introducing a second transaction or
  file-identity implementation.
- Acceptance tests: empty writes, commit/abort, stale concurrent writers, manifest path validation, statistics sidecar
  freshness, and `DataChunkView` input.
- **Completed.** `parquet-connector/src/writer` now owns Parquet encoding, write transaction lifecycle, managed-table
  manifests, external statistics sidecars, source identity validation, and atomic publication. The connector's
  `ParquetWriteStorage` implements the existing generic `exec::operators::sink::WriteStorage` contract; the executor
  remains format-neutral. The storage crate currently exposes read/list/stat contracts only, so write and atomic-publish
  filesystem operations remain inside the Parquet connector instead of duplicating an unsupported storage abstraction.
- Removed the CLI-local `parquet_storage`, `parquet_statistics_publisher`, and `local/statistics` implementations.
  CLI composition injects the connector writer as both the generic write backend and statistics publisher. The generic
  query statistics-context collector was moved to `local/query_context.rs`; it is not Parquet-specific and will move
  with query orchestration in T8. CLI's Parquet dependency is now test-only for existing Parquet fixtures.
- Writer regression coverage includes empty schema-only writes, create/append/overwrite, commit and abort cleanup,
  stale concurrent writers, manifest path validation, statistics freshness/source identities, and `DataChunkView`
  input. The view remains the writer input; conversion is reported at the Arrow/Parquet writer boundary where the
  format API requires an Arrow `RecordBatch`.
- **Validation:** workspace Test and Check passed. The existing Build run configuration failed to start in RustRover;
  incremental `build_project(rebuild: false)` succeeded. The existing Query configuration successfully ran
  `FROM lineitem |> AGGREGATE count(*) AS n` against the configured TPCH data root.

### T8 — Extract generic query orchestration into engine

- Move `LocalQueryEngine` preparation/execution orchestration out of `cli` into a generic engine builder with injected
  catalog, function registry, scan providers, storage, writer, optimizer, and planner.
- Do not let engine construction silently install a concrete Parquet provider.
- Acceptance tests: prepare and execute with fake catalog/provider implementations; verify dependency injection and
  query statistics sharing.

### T9 — Make CLI the composition root and remove legacy paths

- Wire the generic engine, local storage backend, Parquet catalog adapter, Parquet scan provider, and Parquet writer
  from CLI configuration.
- Delete migrated implementations and stale exports. Remove direct Parquet dependencies from `exec` and `cli`; the
  connector owns the Parquet dependency.
- Acceptance: searches show one Parquet metadata loader, one Parquet scan provider, one multi-file scheduler, and no
  Parquet imports or format-name branches in `catalog`/generic `exec`.

### T10 — End-to-end validation and completion

- Run existing `Test`, `Check`, and `Build` configurations, then use the existing `Query` configuration for
  representative scans, Q12, ANALYZE, and writes.
- Compare results with existing baselines; inspect logs for metadata cache hit/miss/version, units
  planned/selected/read, bytes read, rows filtered, and runtime statistics.
- Completion requires all tests green, no duplicate catalog/metadata/scan paths, source files within the line limit, and
  the CLI composing the same generic engine used by non-CLI callers.

## Completion criteria

- There is one catalog contract/cache, one generic scan coordinator, one Parquet connector/metadata service, and one
  engine orchestration path.
- Catalog discovery and execution share typed metadata for the same source version.
- Format-specific work remains in the connector; generic crates do not branch on Parquet.
- No extra batch/column materialization is introduced between scan and execution operators.
- New/refactored Rust files, including tests, are at most 200 lines; modules have one clear responsibility.
- The CLI only composes providers/configuration and renders results.

## Source references

-
Vein: [workspace](/Users/anvi/Next/vein/Cargo.toml:1), [catalog contract](/Users/anvi/Next/vein/crates/catalog/src/contract.rs:61), [metadata cache key](/Users/anvi/Next/vein/crates/catalog/src/store/key.rs:16), [storage read contract](/Users/anvi/Next/vein/crates/storage/src/read/contracts.rs:15), [typed storage identity](/Users/anvi/Next/vein/crates/storage/src/read/identity.rs:3), [scan provider](/Users/anvi/Next/vein/crates/exec/src/operators/source/scan/provider.rs:11), [generic scan dispatch](/Users/anvi/Next/vein/crates/exec/src/factory/scan_partition.rs:10), [Parquet planning](/Users/anvi/Next/vein/crates/parquet-connector/src/scan/parquet/planning.rs:17), [CLI composition](/Users/anvi/Next/vein/crates/cli/src/local/mod.rs:29).
-
DuckDB: [catalog](/Users/anvi/Next/vein/reference_code/duckdb/src/include/duckdb/catalog/catalog.hpp:106), [generic scan callback](/Users/anvi/Next/vein/reference_code/duckdb/src/execution/operator/scan/physical_table_scan.cpp:172), [multi-file wrapper](/Users/anvi/Next/vein/reference_code/duckdb/src/common/multi_file/table_function_multi_file.cpp:775), [Parquet single-file function](/Users/anvi/Next/vein/reference_code/duckdb/extension/parquet/read_single_parquet_file.cpp:511), [metadata cache validation](/Users/anvi/Next/vein/reference_code/duckdb/extension/parquet/parquet_file_metadata_cache.cpp:90).
