# Vein Optimizer Improvement Plan

## Goal

Close the highest-impact optimizer and scan-planning gaps found by comparing Vein with the local DuckDB source at `reference_code/duckdb/src/optimizer`. Keep Vein's plans semantically correct, generic, extensible, observable, and efficient for the single-node engine. Preserve reference-backed / zero-copy execution where the operator semantics allow it.

All tasks are pending unless marked complete. Each logic change must include regression tests for both the rewritten case and cases where the rewrite must not happen.

## Priority 0 — High impact

### P0.1 — Add cardinality and selectivity estimates for planning — Complete

**Current state:** The production pipeline estimates its converged logical tree and returns per-node estimates in `OptimizationDiagnostics.cardinalities`. `StatisticsProvider` supplies borrowed snapshots with explicit freshness. Generic `StatisticsEstimator<P, S>` shares one iterative traversal between static and dynamic strategies through `CardinalityEstimator`, `SelectivityEstimator`, and `ColumnStatisticsLookup`. Cost-based plan selection remains P0.2 work.

**Tasks**
- [x] Define reusable estimates for scan rows, filter selectivity, join output, and aggregate output.
- [x] Use available table and column statistics; represent unknown estimates explicitly and fall back conservatively.
- [x] Track estimate provenance and confidence so decisions can be inspected and tested.
- [x] Add tests for known, missing, stale, and contradictory statistics, including safe fallback behavior.

**Implementation and limits**

- Statistics and literal bounds are borrowed. Estimation reads no rows, copies no column buffers, and clones no logical plans. Small scalar summaries, maps, and traversal stacks still allocate; child summaries are consumed through an iterator without a new vector per node.
- Scan, filter, equality join, aggregate, DISTINCT, projection lineage, limits, sampling, and UNION ALL have models. NULL comparisons preserve SQL three-valued behavior. Numeric/date/time/decimal range estimates use checked comparisons and distances.
- Uniformity and independence are model assumptions, explicitly marked with model confidence. Unsupported expressions, missing distributions, stale snapshots, contradictory metadata, and arithmetic failures remain unknown. Estimates never justify row pruning or semantic rewrites.
- Catalog snapshots are `Unverified` because metadata has no revision guarantee. An injected provider can report `Current` or `Stale`. Current TPC-H catalog snapshots expose row counts without column distributions, so many filtered/join estimates correctly remain unknown.
- Existing scan pruning now uses the same provider and shared bound validation/comparison helpers. It rejects stale and contradictory statistics and continues to use metadata proofs independently of estimates.
- Debug events `optimizer_statistics_snapshot` and `optimizer_cardinality_estimate` expose freshness, statistics availability, operator, estimated rows, confidence, provenance, and filter selectivity. Enable with `RUST_LOG=warn,vein.optimizer=debug`.

**Validation (2026-10-11)**

- Added 47 estimator regressions and 3 scan-pruning safety regressions; retained the existing pruning tests. An existing tracing test now supplies a known clock interval instead of assuming an instantaneous timer must measure more than zero.
- Incremental IDE build, existing workspace `Check`, and complete workspace `Test` passed.
- Existing `Query` configuration smoke checks: `FROM region |> AGGREGATE count(*) AS n` returned 5; `FROM nation |> JOIN region ON n_regionkey = r_regionkey |> AGGREGATE count(*) AS n` returned 25; `FROM nation |> WHERE n_regionkey = 1 |> AGGREGATE count(*) AS n` returned 5. Observed scan/aggregate debug estimates were 5/1 for the region query.
- These are correctness/diagnostic smoke checks. No DuckDB comparison or speedup measurement is claimed for this increment. Next: P0.2 cost-based join ordering; further statistics collection is needed to estimate workloads whose distributions are currently absent.

**DuckDB reference:** `optimizer/join_order/join_order_optimizer.cpp` (cardinality estimator and cost model).

**Vein reference:** `crates/optimizer/src/optimizer.rs`, `crates/optimizer/src/rule.rs`, `crates/optimizer/src/context.rs`.

### P0.2 — Implement cost-based join ordering

**Current state:** Vein keeps the logical join order; the production optimizer applies heuristic rules only.

**Tasks**
- Build a join-graph representation from reorderable joins and their predicates.
- Enumerate alternative orders using estimates from P0.1 and a bounded, extensible search strategy.
- Preserve semantics for outer, semi, anti, cross, correlated, and residual-predicate joins; only reorder shapes proven safe.
- Fall back to the original logical order when estimates or legality checks are insufficient.
- Expose the chosen order, alternatives considered, and cost inputs through optimizer diagnostics.
- Add tests for selective and skewed inputs, multi-join plans, ambiguous estimates, and non-reorderable joins.

**DuckDB reference:** `optimizer/join_order/join_order_optimizer.cpp`; `optimizer/optimizer.cpp:355-360`.

### P0.3 — Choose hash-join build and probe sides

**Current state:** Vein's physical builder lowers the logical left child as probe and right child as build.

**Tasks**
- Choose sides using estimated cardinalities, memory estimates, join type, and join semantics.
- Keep output column identity and ordering correct when inputs are physically reversed.
- Ensure the selected build side works with memory accounting, spilling, and runtime filters.
- Add tests covering both orientations, ties/unknown estimates, outer/semi/anti semantics, residual predicates, and spill behavior.
- Record the decision and reason in plan diagnostics.

**DuckDB reference:** `optimizer/build_probe_side_optimizer.cpp`.

**Vein reference:** `crates/physical/src/builder/pipeline.rs:59-60`.

### P0.4 — Add runtime join filters and push them into eligible scans

**Current state:** Vein has no runtime join-filter path. DuckDB derives filters from join keys and makes them available to scans.

**Tasks**
- Define a generic runtime-filter contract between join build and eligible probe-side scans.
- Derive safe filter forms from join keys (initially equality-key min/max, then other supported filter forms as justified).
- Apply filters during scan so rejected rows avoid downstream join work; integrate with Parquet predicate evaluation without decoding unneeded projected columns.
- Make unsupported filters a no-op that preserves the original scan behavior.
- Respect join type, null handling, residual predicates, cancellation, pipeline dependencies, concurrent execution, and spilling.
- Add correctness tests for empty and duplicate keys, nulls, boundary values, multiple batches, and each join type where filtering is valid.
- Add metrics for filter construction, rows considered/rejected, scan bytes/row groups affected, and fallback reasons.

**DuckDB reference:** `optimizer/join_filter_pushdown_optimizer.cpp:325-410`; optimizer pass registration in `optimizer/optimizer.cpp`.

**Vein reference:** `crates/exec/src/operators/source/scan/parquet.rs:414-431`.

### P0.5 — Prune Parquet row groups and partitions from statistics

**Current state:** Vein can prove an entire scan empty from table-level statistics. Its Parquet path applies row filtering, but no Vein optimizer pass was found for per-row-group or partition pruning.

**Tasks**
- Extend statistics access to the scan units exposed by the storage format (row groups and partitions), without assuming TPC-H table names or fixed schemas.
- Skip a unit only when its metadata proves the predicate cannot match; preserve unknown-statistics units.
- Apply compatible filter predicates before column decoding and projection materialization.
- Support sorted-storage `ORDER BY + LIMIT` pruning only when ordering metadata proves it safe.
- Report candidate, skipped, and read units and the statistics used.
- Add tests for min/max boundaries, null counts, missing statistics, multiple files/row groups, partition columns, and false-positive-safe behavior.

**DuckDB reference:** `optimizer/row_group_pruner.cpp`.

**Vein reference:** `crates/optimizer/src/rules/scan_statistics_pruning.rs`; `crates/exec/src/operators/source/scan/parquet.rs`.

### P0.6 — Wire `ORDER BY + LIMIT` to the bounded TopN path

**Current state:** Vein has bounded TopN selection code, but the physical plan lowers Sort and Limit separately and the runtime sort constructor is called without a limit.

**Tasks**
- Add a logical rewrite for eligible constant limits and offsets.
- Pass the resulting candidate bound to the production sort operator.
- Preserve ordering, ties, null ordering, offset, and limit semantics; retain full sort as a safe fallback.
- Test small/zero/large limits, offsets, ties, nulls, ascending/descending keys, and plans where fusion is illegal.
- Measure retained candidates and memory against full sort.

**DuckDB reference:** `optimizer/topn_optimizer.cpp`; `optimizer/optimizer.cpp:451-455`.

**Vein reference:** `crates/physical/src/builder/pipeline.rs:223-263`; `crates/exec/src/runtime/mod.rs:1349-1362`; `crates/exec/src/operators/transform/sort.rs:172-205`.

## Priority 1 — Medium impact

### P1.1 — Reorder safe filter expressions by estimated cost

**Current state:** Vein normalizes and pushes predicates, but no cost-based expression-ordering pass was found.

**Tasks**
- Estimate expression cost/selectivity using generic expression properties and available statistics.
- Evaluate cheap, selective expressions earlier when SQL error, volatility, and short-circuit semantics allow it.
- Leave volatile, throwing, or otherwise order-sensitive expressions in a safe order.
- Add tests for reordered filters, stable ordering when estimates tie, and expressions that must not move.

**DuckDB reference:** `optimizer/expression_heuristics.cpp:18-70`.

### P1.2 — Add safe expression-specific optimizer rewrites

**Current state:** Vein has general constant folding and expression simplification. DuckDB has additional specialized rules for LIKE/prefix, regex, and monotone preimages.

**Tasks**
- Identify rewrites that Vein's expression model and storage operators can evaluate more efficiently.
- Preserve collation, escaping, null, and pattern semantics; do not infer a prefix when a pattern does not have one.
- Add paired tests for equivalent rewrites and patterns that must remain unchanged.
- Keep each rewrite independently testable and register it through the optimizer rule interface.

**DuckDB reference:** `optimizer/optimizer.cpp:85-100` and the corresponding expression rule implementations.

### P1.3 — Add late materialization where it avoids payload reads

**Current state:** Vein prunes unused columns and has reference-backed `DataChunkView` execution. No row-identity-based late-materialization optimizer pass was found.

**Tasks**
- Identify operators that can carry row references or row selections while postponing wide payload reads.
- Define a storage-independent interface with a Parquet implementation; retain eager materialization as fallback.
- Ensure references remain valid across filtering, sorting, joins, and spill boundaries.
- Add tests for value correctness, lifetime, memory accounting, and reduced payload decoding.
- Compare the added indirection cost against the avoided reads before enabling a rewrite.

**DuckDB reference:** `optimizer/late_materialization.cpp`.

### P1.4 — Push partial aggregation below joins when legal

**Current state:** Vein has aggregate execution and spill support, but no optimizer pass was found that pushes partial aggregates below joins.

**Tasks**
- Define legality conditions for aggregate decomposition and join interactions.
- Push partial state only when it reduces rows without changing duplicate, null, grouping-set, or outer-join semantics.
- Use mergeable aggregate state interfaces rather than hardcoded aggregate names.
- Add tests for decomposable and non-decomposable aggregates, duplicates, nulls, grouping, and joins.
- Compare intermediate cardinality and memory before and after the rewrite.

**DuckDB reference:** `optimizer/optimizer.cpp` aggregate optimization passes.

### P1.5 — Reuse repeated work

**Current state:** I found no Vein optimizer passes for common-subplan materialization, general common-subexpression elimination, or DuckDB-style aggregate reuse.

**Tasks**
- Detect equivalent reusable subplans or expressions using stable structural identity and semantic safety checks.
- Decide whether reuse is beneficial using estimated recomputation and materialization costs.
- Respect volatility, side effects, memory limits, and query lifetime.
- Add tests for repeated and near-identical expressions/subplans, volatile expressions, and reuse disabled by cost.
- Report reuse decisions and materialization costs.

**DuckDB reference:** `optimizer/common_subplan_optimizer.cpp`; aggregate reuse registration in `optimizer/optimizer.cpp:362-368`.

## Priority 2 — Additional optimizer coverage

### P2.1 — Add join elimination and outer-join simplification

**Current state:** I found no matching Vein optimizer passes in the reviewed production pipeline.

**Tasks**
- Identify provably redundant joins from constraints, uniqueness, null-rejection, and required-column information.
- Simplify outer joins only when predicates prove the transformation preserves results.
- Require metadata or logical proof; never rely on table names or assumed key constraints.
- Add positive and negative tests for duplicates, nulls, missing constraints, and all affected join types.

**DuckDB reference:** `optimizer/optimizer.cpp:349-353` for outer-join simplification; DuckDB's optimizer pass sources for join elimination.

### P2.2 — Expand physical join algorithm selection

**Current state:** Vein's execution join path is centered on hash join. DuckDB has additional join operators and chooses algorithms for supported predicate shapes.

**Tasks**
- Inventory Vein's supported join semantics and workloads before adding operators.
- Add an extensible physical cost/legality interface for selecting algorithms.
- Consider nested-loop, range, and other algorithms only where they outperform hash join and preserve semantics.
- Add algorithm-specific correctness, memory, and selection tests; keep hash join as fallback.

**DuckDB reference:** DuckDB optimizer and physical join operator implementations under `src/optimizer` and `src/execution/operator/join`.

## Priority 3 — Research after the deterministic optimizer

### P3.1 — Evaluate RLCD-style calibrated decisions for optimizer choices

**What RLCD means here:** Reinforcement Learning for Calibrated Decisions trains a decision model to return probabilities whose confidence is calibrated against observed outcomes. The paper's method uses a strictly proper scoring rule and a calibrate-then-reinforce training process. It is a machine-learning decision method, not a relational rewrite, join-order algorithm, or DuckDB optimizer pass. No RLCD implementation was found in the local DuckDB optimizer source.

**Vein fit:** RLCD could be evaluated as a policy for choosing among legal physical plans produced by Vein's deterministic optimizer, or for deciding whether a query merits additional planning work. It must not generate arbitrary plans or bypass semantic validation. This depends on the cost model, candidate enumeration, and execution outcome telemetry in P0.

**Tasks**
- First establish the deterministic cost-based optimizer and plan candidates from P0 as the baseline.
- Define a bounded decision problem: optimizer-visible state, legal candidate-plan actions, and measured outcomes such as latency and resource use.
- Build an offline dataset by executing equivalent candidate plans and checking result equality; account for measurement noise, cache state, workload changes, and resource contention.
- Evaluate whether RLCD's calibrated probability objective is appropriate for plan selection, compared with simpler cost models and supervised calibration. Calibration alone must not be treated as proof that a plan is faster.
- Measure calibration (for example, Brier score and expected calibration error), plan regret against the best measured candidate, result correctness, model inference overhead, memory use, and out-of-distribution behavior.
- Keep the learned policy in offline or shadow mode until it demonstrates a repeatable net benefit. At runtime, restrict it to validated candidates and fall back to the deterministic optimizer on low confidence, missing features, model errors, or policy disablement.
- Add reproducible benchmark and decision-policy tests before considering production enablement.

**Reference:** [OpenJev-RLCD paper](https://arxiv.org/abs/2609.38850); no direct DuckDB equivalent was found in `reference_code/duckdb/src/optimizer/`.

## Validation requirements for every task

- Add regression tests in the same change as each optimizer or execution logic change.
- Test both the optimized plan and the fallback/unsafe cases.
- Run the existing RustRover **Test** configuration after adding or changing tests; run **Build** and **Check** configurations as applicable.
- Validate representative Vein queries with the existing **Query** configuration and compare results with DuckDB when the query is supported by both.
- Record plan shape, rows read/filtered, bytes, memory, spill activity, and elapsed time. Separate engine execution time from process/build startup time.
- Do not add run configurations for these tasks.
- Do not enable a rewrite by default until correctness, fallback behavior, and measured benefit are established.

## Source review snapshot

- Vein production optimizer: `crates/optimizer/src/optimizer.rs`, `crates/optimizer/src/rule.rs`, and `crates/optimizer/src/rules/`.
- Vein physical and scan paths: `crates/physical/src/builder/pipeline.rs`, `crates/exec/src/runtime/mod.rs`, and `crates/exec/src/operators/source/scan/parquet.rs`.
- DuckDB reference: `reference_code/duckdb/src/optimizer/`.
- This plan records source-level gaps found in the current review. A dependency may provide lower-level behavior not represented by a Vein optimizer pass.
