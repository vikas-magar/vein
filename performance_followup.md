# Performance follow-up — 2026-10-23

## Scope and current status

- [x] Validate the previously pending changes: Test, Check, and incremental build.
- [x] Trace runtime-filter placement through metadata pruning and staged decoding.
- [x] Correct misleading runtime-filter capability logging.
- [x] Add per-stage membership diagnostics and coverage.
- [x] Broaden inner-join search to preserve safe residual predicates exactly once.
- [x] Introduce an injectable build/probe/output work cost model.
- [ ] General delimiter-domain/dependent execution: approved for this session; implementation pending.
- [x] Execute Q1–Q22 once and compare all successful results with DuckDB.
- [x] Fix six execution failures through shared scalar-run output semantics.
- [x] Rerun all 22 queries after the fix and compare complete result values.
- [x] Run the workspace suite after the fix: 1,308 passed, zero failed.
- [ ] Repeat paired performance measurements; current timings are individual observations.

## Validation

Latest completed workspace Test: 1,344 tests passed, zero failed (including checkpoint domains).
Check and incremental build passed after the latest formatting changes.
RustRover's named Build configuration failed to launch; its incremental build tool passed.
All 22 TPC-H queries now execute and match DuckDB result values.
Q1, Q7, Q8, Q9, Q11, and Q12 have output type differences despite matching numeric values.
The former Q7/Q8/Q9/Q11/Q12/Q16 scalar-run execution failures are repaired.

## Timing observations

Earlier complete validation pass: all Q1–Q22 queries, run sequentially.
The final checkpoint-domain and placement-guard pass is in `performance_validation.md`.
Current development build; OS cache uncontrolled; one run per query in this pass.
This pass follows the shared repeated-run expression fix. The six affected queries were
validated first, followed by the other 16; each query completed before the next started.
Vein time is its reported execution phase, excluding preparation and JSON output formatting.
DuckDB time is its CLI timer for the result-producing SQL, including result presentation.
These scopes differ and are not a fair repeated engine-only benchmark.
JSON output is unlimited so complete rows can be checked.

| Query | Vein execution ms | DuckDB CLI timer ms | Result |
|---|---:|---:|---|
| Q1 | 915.6 | 103.0 | Values match; output types differ |
| Q2 | 623.3 | 25.0 | Values match |
| Q3 | 267.8 | 76.0 | Values match |
| Q4 | 168.6 | 42.0 | Values match |
| Q5 | 248.8 | 58.0 | Values match |
| Q6 | 115.1 | 34.0 | Values match |
| Q7 | 342.7 | 65.0 | Values match; output types differ |
| Q8 | 369.3 | 76.0 | Values match; output types differ |
| Q9 | 1499.5 | 128.0 | Values match; output types differ |
| Q10 | 988.4 | 210.0 | Values match |
| Q11 | 114.1 | 12.0 | Values match; output types differ |
| Q12 | 226.7 | 45.0 | Values match; output types differ |
| Q13 | 786.7 | 104.0 | Values match |
| Q14 | 137.5 | 49.0 | Values match |
| Q15 | 374.3 | 39.0 | Values match |
| Q16 | 1374.0 | 44.0 | Values match |
| Q17 | 4724.6 | 60.0 | Values match |
| Q18 | 8543.9 | 86.0 | Values match |
| Q19 | 325.6 | 71.0 | Values match |
| Q20 | 3169.6 | 49.0 | Values match |
| Q21 | 577.9 | 162.0 | Values match |
| Q22 | 460.5 | 18.0 | Values match |

Q13: 43 rows; exact typed multiset match.
Q21: 650 rows; exact typed multiset match.
The comparator preserves duplicates; this pass does not establish ORDER BY correctness.
Floating comparisons use 8 ULP-scaled epsilons; decimal comparison is exact.

Q10 additionally spent 2,526.9 ms formatting its 60,971 rows.
Q3 additionally spent 148.5 ms formatting its 18,431 rows.
Output conversion must be investigated separately from operator execution.

## Runtime-filter diagnosis

Source:
- crates/parquet-connector/src/scan/parquet/execute/file/pruning/predicates.rs
- crates/parquet-connector/src/scan/parquet/predicate/row_filter.rs

Runtime range predicates already reach footer, page-index, and page-header pruning.
Runtime hash Bloom membership is evaluated as a Parquet row-filter stage.
The previous log incorrectly described all runtime filters as unable to prune pages.
Static predicate stages initially precede runtime membership stages.
Existing adaptive reordering is enabled only when every stage is proven safe.
Its worker-local sampling budget can exceed the number of batches assigned to a worker.

A diagnostic Q21 run showed:
- 19,200,120 visits to date-comparison stages.
- Supplier membership: 6,067,279 rows examined; 258,640 retained.
- Order membership: 1,168,730 examined; 113,281 retained.
- 286 assigned row-group visits; zero footer-pruned groups.
- 1,661 considered page-range visits; zero skipped ranges.
- About 220.2 MB reported read across scan visits.
These are aggregate visits across repeated scans, not distinct table populations.
Verbose tracing execution was 1,412.3 ms; earlier low-logging execution was 518.0 ms (latest rerun: 496.1 ms).
Do not attribute that entire difference to logging without controlled repeated measurements.

The runtime-bounds regression confirms that rejected page payload ranges are not read
when metadata proves exclusion. This Q21 run offered no such exclusion.

## Shared repeated-run expression bug — fixed

Source:
- crates/exec/src/expression/value/segment.rs
- crates/exec/src/expression/kernels/mapped_kernel.rs
- crates/functions/src/segments/mod.rs

A view may resolve consecutive logical rows to one physical source row.
Its segment is correctly represented as a scalar with a larger logical length.
Unary and binary kernels may consequently return one physical result.
Previously the output collector incorrectly required physical length equal to logical length.
Function invocation also rejected mapped column inputs whose current runs were all scalar.
The fix preserves physical/logical lengths and aligns all arguments to the shortest run.

Implemented fix:
- One shared functions::OutputSegment contract containing the retained Arrow array,
  logical length, and explicit scalar flag.
- Explicit dense versus repeated semantics; no copied broadcast array.
- Preserve scalar-run semantics through chained expressions and function calls.
- Validate kernel shape against whether inputs were scalar, rather than accepting
  every unexpected single-row output as a broadcast.
- Cover arithmetic, comparisons, temporal functions, NULL runs, mixed dense/scalar
  runs, malformed outputs, and Arrow buffer identity.

## Remaining output type differences

- Q1: sum_charge decimal scale 8 in Vein versus 6 in DuckDB.
- Q7/Q8/Q9: YEAR returns Int32 in Vein versus BIGINT in DuckDB.
- Q11: total_value decimal scale 4 versus 2.
- Q12: conditional SUM returns Int64 versus HUGEINT.
- Arrow/SQL aliases such as Utf8/VARCHAR and Float64/DOUBLE are normalized by the comparator.
These differences are separate from the repaired execution failure; type parity is not complete.

## General correlated execution design

Use a delimiter domain rather than per-outer-row subquery execution:
1. Retain the outer DataChunkViews once under query resource accounting.
2. Extract distinct correlation tuples with SQL grouping NULL semantics.
3. Expose the shared domain through a query-scoped delimiter scan.
4. Propagate domain identity through the inner relational operators.
5. Execute inner work in batches over domain tuples.
6. Reattach using NULL-safe domain identity without multiplying outer duplicates.
7. Preserve scalar cardinality errors, empty COUNT defaults, EXISTS/NOT EXISTS,
   and IN/NOT IN three-valued logic.
8. Integrate canonical physical dependencies, memory limits, spill, cancellation,
   and RAII cleanup.
9. Preserve existing cheaper equality decorrelation when proven applicable.

New logical and physical node types are required. A semi/anti join plus a Bloom filter
alone is not a general delimiter implementation. New exhaustive matches must receive
explicit semantics, not wildcard fallbacks.

## Source audit follow-up

- [x] Generic semi-join placement below independent inner joins.
  - Owned plan rewrite with an injectable predicate-semantics strategy.
  - No table names, query identifiers, statistics constants, or row payload copies.
  - Eleven tests, including 432 bag-equivalence populations with duplicates/NULLs/empty inputs,
    both input directions, residuals, USING and semantic boundaries, and convergence.
  - Test: 1,319 passed; Check and incremental build passed.
  - Q18: all 94 rows match DuckDB exactly after decimal JSON normalization;
    complete row order also matched on this run (ties remain unspecified by SQL).
  - Diagnostic execution: 5,763.9 ms versus the earlier audit's 8,057.9 ms.
    Single development-profile observations, not a controlled speedup benchmark.
  - Largest join build: 2,400,000 rows, down from 9,600,060.
  - Tracked memory peak: 2,136,505,036 bytes versus 2,508,711,448 previously.
  - Remaining: fallback estimates still orient the membership join to build all orders;
    the grouped subquery remains expensive. No DuckDB performance parity claim.
  - [x] Cost guard preserves selective inner joins before expensive existence tests.
    Four regressions cover increased/unknown probe work, equality membership, and residuals.
    Q21's interim 2,062.4 ms regression is repaired: final execution 521.3 ms;
    Q18 remains improved at 5,347.1 ms. Complete values match for both.
- [ ] Correlated-domain restriction and general dependent execution (Q17/Q20).
  - [x] Restrict equality-grouped inputs using an existing retained checkpoint tuple domain.
    Owned optimizer rewrite; original grouped rows, outer defaults, and schema stay intact.
    No new runtime path, per-outer-row execution, or copied domain payloads.
  - [x] Optimizer and full-engine adaptive execution regressions.
    Covers aliases, composite tuples, duplicates, NULLs, missing groups, unsupported
    boundaries, idempotence, and retained Arrow key-buffer identity.
  - [x] Test: 1,332 passed; Check and incremental build passed.
  - [x] Q17: 10,011 aggregate input rows and 327 groups (previously 9,600,060 / 320,000).
    Diagnostic execution 328.2 ms; lower-logging executions 258.5 / 254.6 ms versus the audit's
    earlier 4,710.1 ms. Tracked memory peak 6,081,388 versus 617,517,928 bytes.
    Individual development-profile observations; no controlled benchmark claim.
  - [x] Q2 (255 rows), Q17 (1), Q20 (273): complete values and row order match DuckDB
    after decimal JSON normalization. Q17 scalar result: 535732.3314285714.
  - [x] Q20 checkpoint scheduling/continuation now follows pending tuple-domain work.
    Shared eligibility handles either grouped side of inner joins; the materialization
    cost gate remains mandatory. Existing build barriers no longer suppress a domain producer.
    Retained domain: 13,844 composite tuples; grouped input: 15,718 rows, down from 1,457,857.
    Final execution: 228.1 ms versus the earlier 3,367.9 ms observation.
    Diagnostic tracked peak: 192,202,702 bytes; scan I/O remains about 103 MB.
  - [ ] General dependent joins, non-equality domains, and correlated empty-COUNT defaults.
- [ ] Typed batch key access and batched grouping probes with bounded reservations.
- [ ] Partitioned parallel aggregate combination.
- [ ] Per-plan-revision estimate caching and richer build/probe policy.
- [ ] View-aware output serialization without tiny RecordBatch construction.
- [ ] Consolidate duplicated predicate helpers and split oversized modules by responsibility.
