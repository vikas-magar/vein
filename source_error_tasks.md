# Source Error and Query Coverage Tasks

This plan tracks confirmed findings from the Q1–Q22 run and workspace diagnostics. Tasks are ordered by correctness, breadth of impact, and dependency. Complete one task at a time; keep this list updated as work lands.

## Engineering constraints

- Preserve the zero-copy columnar path: operate on Arrow arrays and `DataChunkView` where possible; do not add per-row or whole-batch materialization to fix these issues.
- Resolve columns by stable identity and schema lineage, not query names or benchmark-specific cases.
- Keep execution vectorized and bounded; measure allocations, bytes copied/materialized, and latency for affected paths.
- Every behavior change gets focused regression tests plus workspace `Test` and `Check` validation using the existing RustRover configurations.
- Validate corrected query shapes and results against DuckDB where available; record failures separately from unsupported features.

## Tasks

### T0 — Establish a test-suite baseline [P0, complete]
The previous workspace failure was a stale assertion in `exec::trace::tests::canonical_node_ids_are_deterministic_preorder_ids`: it expected `program_instructions=0`, but the current formatter reports `expression_steps=0`. Updated the assertion to match the current trace contract. The focused regression passes; the workspace `Test` configuration now passes all 1,177 unit and integration tests, and workspace `Check` passes.

- Identify the failing test(s) through the existing `Test` configuration and RustRover results.
- Determine whether failures are regressions, flaky tests, or existing failures; record the exact crate and test names.
- Acceptance: a reproducible baseline is documented before engine fixes are validated.

### T1 — Repair physical join output-column lineage [P0, complete]
Q2, Q7, Q9, Q10, and Q11 fail because a join output column is absent from both physical inputs. The check is in `crates/physical/src/builder/pipeline/join_layout.rs:52`, called from `join_lowering.rs:78`.

- Trace schema construction, projection pruning, recursive join lowering, and build/probe orientation.
- Fix the general column-identity/lineage mismatch; do not special-case columns or TPC-H queries.
- Tests: direct join-layout tests for chained joins, pruned columns, reordered build/probe sides, and nested joins; end-to-end coverage for affected shapes.
- Acceptance: physical output references preserve declared `ColumnId` lineage across pruned, reordered, and build/probe-swapped joins; the chained-join regression query succeeds. The remaining Q7/Q9/Q10/Q11 errors now occur in later aggregate/subquery paths and are tracked separately; full-suite correctness remains part of T10.

### T2 — Fix the Q13 NOT LIKE input-type mismatch [P0, complete]
Q13 reaches the Arrow LIKE kernel with `Int64 NLIKE Utf8`, although the predicate references `orders.o_comment` (UTF-8). Relevant paths: `crates/physical/src/compiler/expr.rs:135` and `crates/exec/src/vm/ops/comparison.rs:57`.

- Trace bound column identity through join residual compilation and runtime input mapping.
- Ensure the compiled predicate reads the correct typed input column; preserve SQL NULL behavior.
- Tests: NOT LIKE on a UTF-8 join-side column, NULL values, chained joins, and a neighboring integer column to catch slot confusion.
- Acceptance: focused compiler/executor tests preserve the UTF-8 build-side residual mapping, and the Q13-shaped left join plus grouped result now completes successfully with 43 rows.

### T3 — Correct the Q18 and Q20 example subqueries [P1, complete]
These are query-definition errors: their `IN` subqueries return multiple columns. Binder validation is at `crates/binder/src/expr.rs:278`.

- Q18 must select `l_orderkey` from its grouped lineitem subquery.
- Q20 must select `ps_suppkey` from the partsupp/part subquery.
- Tests: parse/bind both corrected query shapes and assert the subquery has exactly one output column.
- Acceptance: the current Q18/Q20 definitions project only `l_orderkey` and `ps_suppkey` respectively, and binder tests accept one projected column while rejecting multiple columns. Keep the one-column rule for `IN`.

### T4 — Support typed aggregate group keys [P1, complete]
Q3 failed because `GroupKeyColumn` did not support Arrow Date32 arrays: `crates/exec/src/operators/aggregate/group_key/access.rs`. Validation also exposed unsupported Int32 group keys in Q7/Q9 and Decimal128 group keys in Q10. Group-key hashing, equality, output materialization, and spill serialization now support Date32, signed Int8/16/32/64, Decimal128, and Decimal256 keys while retaining shared Arrow buffers for live input batches.

- Added typed hashing/equality and checked output handling for the supported key layouts; spill builders preserve decimal precision, scale, and NULL values.
- Tests: Date32 NULL/repeated keys across batches and spill; Int8/16/32 NULL and repeated keys; Decimal128 and wide Decimal256 NULL/repeated keys through spill/merge; batch-versus-spill hash equivalence.
- Acceptance: the workspace Test suite passes, Check passes, and Q3/Q7/Q9/Q10-shaped queries complete successfully. Live keys retain shared Arrow arrays; serialization constructs typed output arrays only at the spill boundary.

### T11 — Lower scalar aggregate subqueries in filters [P1, complete]
Q11 reached physical planning but failed because the scalar aggregate subquery in `Filter.predicate` was not lowered by the optimizer. The binder normalizes an aggregate expression such as `SUM(...) * 0.0001` as a `Project` over a global `Aggregate`; decorrelation previously recognized only a bare aggregate node.

- Added a generic rewrite for immutable one-column projections over uncorrelated global aggregates. It retains the aggregate and projection as the scalar side of a cross join, preserving exactly-one-row scalar semantics on empty input and SQL NULL behavior without query-specific handling.
- Tests cover binder-normalized decimal arithmetic in a filter and an empty inner input.
- Validation: workspace Test, Check, and incremental build passed. The Q11-shaped query returned 52 rows; its ordered `(ps_partkey, total_value)` results matched DuckDB against the same Parquet data. An empty-input scalar aggregate filter returned `count(*) = 0`.

### T5 — Lower scalar expressions around aggregates [P1, complete]
Q8 and Q14 fail because aggregate lowering expects a bare function call, rejecting arithmetic around aggregate results: `crates/physical/src/compiler/plan.rs:1192`.

- Represent aggregate calls as aggregate outputs, then compile the surrounding scalar expression over those outputs using the existing vector VM.
- Tests: arithmetic around SUM, multiple aggregates in one expression, CASE, division, and NULL semantics.
- Acceptance: expressions remain vectorized; no row-at-a-time post-aggregation evaluation. The Q14-shaped aggregate ratio now completes end to end.

### T6 — Unify AVG/decimal arithmetic coercion [P1, complete]
Q17 cannot coerce `Float64` and `Decimal128(1, 1)`; the common-type error originates at `crates/types/src/coercion.rs:58`.

- Define and document a consistent numeric promotion rule for aggregate results and decimal literals.
- Infer decimal arithmetic result precision and scale using Arrow-compatible rules for add/subtract/multiply/remainder. For ordinary `/` with Decimal128/Decimal256 operands, cast both sides to Float64 to match DuckDB's SQL behavior and retain result precision.
- Tests: decimal AVG multiplied by decimal/float literals, overflow/scale boundaries, and NULLs.
- Acceptance: preserve numeric correctness and avoid unnecessary casts or copied columns. The Q17-shaped AVG query and Q14-shaped ratio both execute successfully.

### T7 — Implement COUNT(DISTINCT ...) [P1, complete]
Q16 was rejected by the binder because aggregate DISTINCT was unsupported in `crates/binder/src/expr.rs`.

- The binder now preserves DISTINCT as per-aggregate metadata and restricts this implementation to `COUNT(DISTINCT expr)`; unsupported forms such as `COUNT(DISTINCT *)` and `SUM(DISTINCT expr)` fail during binding.
- Physical lowering uses a distinct-count opcode. Execution reuses the aggregate hash sink and spill/merge machinery to deduplicate each aggregate argument within each grouping set, then merges unique values into the parent count state. The hot input path retains Arrow array references rather than materializing rows.
- Tests cover duplicates, NULL arguments, multiple groups, grouping-set identity, bounded spill and exact merge, and worker-local fan-in deduplication; binder and physical compiler tests cover metadata propagation and opcode selection.
- Validation: focused distinct tests, workspace Test, and Check passed. Incremental `build_project(rebuild: false)` passed; RustRover's Build run configuration failed to start twice. A Q16-shaped join/filter/grouped distinct query executes; its multi-row membership predicates are covered by the completed T12 rewrite.

### T12 — Lower membership subqueries with SQL NULL semantics [P1, complete]
Q16's multi-row `NOT IN` RHS was being lowered as a scalar subquery, which incorrectly enforced single-row cardinality. Its positive `p_size IN (...)` also exposed that hash joins accepted only Int64 and UTF-8 keys, even though `p_size` is Int32.

- Added a relational rewrite for eligible top-level filter conjuncts: `IN` becomes a semi join and `NOT IN` becomes a null-aware anti join. Correlated membership and membership nested under `OR` remain explicit planning errors rather than receiving an unsafe rewrite.
- Implemented SQL three-valued `NOT IN` behavior for empty build input, NULL probe keys, and NULL keys in the build input. Native Int32 keys are now supported by the shared hash-join path; retained Arrow key arrays are read in place without widening their value buffers.
- Tests cover membership rewrite shape, nested-OR rejection, native Int32 hash matching, empty and non-empty build semantics, duplicate matches, NULL probes, and NULL build keys.
- Validation: workspace Test and Check pass; `build_project(rebuild: false)` succeeds. The complete Q16-shaped query completes in Vein. Its top ten ordered `(p_brand, p_type, p_size, supplier_cnt)` rows match DuckDB on the same Parquet files.

### T8 — Support correlated non-equality EXISTS predicates [P1, complete]
Q21 uses correlated `<>` predicates. Decorrelation previously rejected every correlated predicate that was not a direct equality key.

- Keep direct inner-to-outer equality terms as hash keys and lower other immutable correlated conjuncts to join residuals by column identity. The existing vectorized semi/anti hash join evaluates residuals over `DataChunkView`; no per-row nested-loop fallback is introduced.
- Correlated non-equality predicates require at least one hashable equality key. Range-only correlations remain explicit planning errors because the physical join path has no non-equi execution operator.
- Tests cover EXISTS and NOT EXISTS with equality plus inequality, duplicates, NULLs, and empty inner inputs.
- Validation: focused decorrelation and join tests, workspace Test, Check, and incremental build passed. A Q21-shaped query completed successfully through the Query run configuration.

### T9 — Register and execute SUBSTRING [P1, complete]
Q22 initially failed because `substring` was absent from binder and executable scalar built-ins. The binder now models optional trailing arguments generically (`min_arg_count` plus the signature's maximum arity); SUBSTRING accepts its two- and three-argument forms. Arrow batch execution implements SQL character indexing, bounds, and NULL propagation without allocating an intermediate Rust `String` per row.

- Tests cover both binder arities, invalid types and arities, positive/zero/negative starts, positive/negative/omitted lengths, UTF-8 boundaries, and NULLs.
- Validation: workspace Test, Check, incremental Build, and a five-row three-argument SUBSTRING Query all passed. The saved Build run configuration failed to start; incremental `build_project(rebuild: false)` succeeded.
- Follow-up found during full Q22 validation: planning rejected the deterministic scalar `AVG` subquery because generic expression volatility conservatively treats every scalar subquery as volatile. T10 resolves this with plan-aware volatility inspection in EXISTS decorrelation; genuinely volatile expressions still block the rewrite.

### T10 — Re-run and compare the complete query suite [P2, in progress]
Ran Q1–Q22 through the existing Query configuration using the pipe definitions and DuckDB ANSI definitions. Vein timings include preparation, execution, and formatting one displayed row (`VEIN_DISPLAY_ROWS=1`); the complete result was still consumed. DuckDB ran all 22 definitions successfully. Result counts match for all 22 Vein queries that completed; first-row comparisons have been checked, but full ordered result equality has not yet been established.

| Query | Vein result | DuckDB rows |
|---|---:|---:|
| Q1 | 4 rows, 437 ms | 4 |
| Q2 | 255 rows, 1,125 ms | 255 |
| Q3 | 18,431 rows, 4,254 ms | 18,431 |
| Q4 | 5 rows, 828 ms | 5 |
| Q5 | 5 rows, 6,746 ms | 5 |
| Q6 | 1 row, 88 ms | 1 |
| Q7 | 4 rows, 17,101 ms | 4 |
| Q8 | 2 rows, 8,333 ms | 2 |
| Q9 | 175 rows, 14,443 ms | 175 |
| Q10 | 60,971 rows, 2,419 ms | 60,971 |
| Q11 | 52 rows, 155 ms | 52 |
| Q12 | 2 rows, 189 ms | 2 |
| Q13 | 43 rows, 3,674 ms | 43 |
| Q14 | 1 row, 381 ms | 1 |
| Q15 | 1 row, 416 ms | 1 |
| Q16 | 22,897 rows, 1,305 ms | 22,897 |
| Q17 | 1 row, 36,933 ms; CLI displays `535732.331428` | 1 |
| Q18 | 94 rows, 16,235 ms | 94 |
| Q19 | 1 row, 1,133 ms | 1 |
| Q20 | 273 rows, 2,796 ms | 273 |
| Q21 | 650 rows, 24,309 ms | 650 |
| Q22 | 7 rows, 568 ms | 7 |

- The earlier Q8 (`0.03781923`), Q14 (`16.483182755636`), and Q17 (`535732.331428`) values were real Decimal result precision loss, not display truncation. Decimal `/` now binds both operands as `Float64`, matching DuckDB's ordinary division semantics. Binder and execution regressions cover the bound operand/result types, numeric precision, and NULL propagation.
- Validation through `Query` with JSONL output now returns Q8 `0.03781923245010446` and `0.03588441759268987`, Q14 `16.483182755636385`, and Q17 `535732.3314285714`, matching the previously captured DuckDB results. This run observed 3,175 ms, 269 ms, and 5,276 ms respectively; these are not controlled DuckDB timing comparisons.
- Q12's two complete rows match DuckDB. Q17's correlated projected aggregate lowers into a grouped aggregate and left join. Its old 36,933 ms timing predates this validation and is not directly comparable to the new sample. The full Q1–Q22 ordered result comparison and controlled timing comparison remain open.
- Q20 now completes through a scope-aware membership rewrite and composite-key correlated aggregate decorrelation. It returns 273 rows, matching DuckDB's count. Q22 also completes with 7 rows; DuckDB confirms Q17, Q20, and Q22 are valid query shapes and returns 1, 273, and 7 rows respectively.
- Q22's optimizer volatility analysis descends into scalar-subquery plans, allowing its deterministic global `AVG` beside `NOT EXISTS` while preserving rejection for nested `RANDOM()` and other volatile work. The earlier Q22 validation run passed Test and Check and returned 7 rows in 568 ms with `VEIN_DISPLAY_ROWS=1`. The current workspace Test and Check baseline passes. Full ordered result equality remains open.
- Remaining work: compare complete ordered result sets and collect comparable per-query timings. The table above records earlier measurements, not a controlled current benchmark.
- Acceptance: all 22 queries either produce DuckDB-equivalent results or have a documented, intentional unsupported-feature status; report performance and remaining gaps explicitly.
