# Performance follow-up — 2026-10-23

## Scope and current status

- [x] Validate the previously pending changes: Test, Check, and incremental build.
- [x] Trace runtime-filter placement through metadata pruning and staged decoding.
- [x] Correct misleading runtime-filter capability logging.
- [x] Add per-stage membership diagnostics and coverage.
- [x] Broaden inner-join search to preserve safe residual predicates exactly once.
- [x] Introduce an injectable build/probe/output work cost model.
- [ ] General delimiter-domain/dependent execution: design approval pending.
- [x] Execute Q1–Q22 once and compare all successful results with DuckDB.
- [ ] Fix six execution failures exposed by the full query pass; approval pending.
- [ ] Rerun the failed queries and repeat the full suite after their fixes.
- [ ] Repeat paired performance measurements; current timings are individual observations.

## Validation

Latest completed workspace Test: 1,295 tests passed, zero failed.
Check and incremental build passed after the latest formatting changes.
All 16 successful TPC-H queries matched DuckDB values.
Q1 has output type differences despite matching numeric results within the comparison tolerance.
Q7, Q8, Q9, Q11, Q12, and Q16 failed execution and have no correctness comparison.

## Timing observations

Current development build; OS cache uncontrolled; one run per query.
Vein time is its reported execution phase, excluding preparation and JSON output formatting.
DuckDB time is its CLI timer for the result-producing SQL, including result presentation.
These scopes differ and are not a fair repeated engine-only benchmark.
JSON output is unlimited so complete rows can be checked.

| Query | Vein execution ms | DuckDB CLI timer ms | Result |
|---|---:|---:|---|
| Q1 | 966.5 | 104.0 | Values match; output types differ |
| Q2 | 630.2 | 25.0 | Values match |
| Q3 | 279.1 | 76.0 | Values match |
| Q4 | 169.8 | 42.0 | Values match |
| Q5 | 260.2 | 60.0 | Values match |
| Q6 | 118.2 | 36.0 | Values match |
| Q7 | — | — | Execution error: mapped expression kernel returned 1 rows for a 2-row segment |
| Q8 | — | — | Execution error: mapped expression kernel returned 1 rows for a 2-row segment |
| Q9 | — | — | Execution error: function has no column input segment |
| Q10 | 973.9 | 212.0 | Values match |
| Q11 | — | — | Execution error: mapped expression kernel returned 1 rows for a 1024-row segment |
| Q12 | — | — | Execution error: mapped expression kernel returned 1 rows for a 2-row segment |
| Q13 | 866.0 | 107.0 | Values match |
| Q14 | 150.0 | 54.0 | Values match |
| Q15 | 410.6 | 38.0 | Values match |
| Q16 | — | — | Execution error: mapped expression kernel returned 1 rows for a 4-row segment |
| Q17 | 4878.6 | 68.0 | Values match |
| Q18 | 8574.9 | 89.0 | Values match |
| Q19 | 291.2 | 73.0 | Values match |
| Q20 | 3195.1 | 52.0 | Values match |
| Q21 | 518.0 | 165.0 | Values match |
| Q22 | 452.0 | 19.0 | Values match |

Q13: 43 rows; exact typed multiset match.
Q21: 650 rows; exact typed multiset match.
The comparator preserves duplicates; this pass does not establish ORDER BY correctness.
Floating comparisons use 8 ULP-scaled epsilons; decimal comparison is exact.

Q10 additionally spent 13,050.7 ms formatting its 60,971 rows.
Q3 additionally spent 1,220.7 ms formatting its 18,431 rows.
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
Verbose tracing execution was 1,412.3 ms; low-logging execution was 518.0 ms.
Do not attribute that entire difference to logging without controlled repeated measurements.

The runtime-bounds regression confirms that rejected page payload ranges are not read
when metadata proves exclusion. This Q21 run offered no such exclusion.

## Shared repeated-run expression bug

Source:
- crates/exec/src/expression/value/segment.rs
- crates/exec/src/expression/kernels/mapped_kernel.rs
- crates/functions/src/segments/mod.rs

A view may resolve consecutive logical rows to one physical source row.
Its segment is correctly represented as a scalar with a larger logical length.
Unary and binary kernels may consequently return one physical result.
The output segment collector incorrectly requires physical length equal to logical length.
Function invocation also rejects a mapped input whose current runs are all scalar,
even though the logical function arguments are column inputs.

Proposed fix:
- One shared output-run contract containing the retained Arrow array and logical length.
- Explicit dense versus repeated semantics; no copied broadcast array.
- Preserve scalar-run semantics through chained expressions and function calls.
- Validate kernel shape against whether inputs were scalar, rather than accepting
  every unexpected single-row output as a broadcast.
- Cover arithmetic, comparisons, temporal functions, NULL runs, mixed dense/scalar
  runs, malformed outputs, and Arrow buffer identity.

## General correlated execution design awaiting approval

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
