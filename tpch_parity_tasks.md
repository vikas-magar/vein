# TPC-H Q1–Q22 Performance and Precision Parity

## Goal

For the fixed local TPC-H dataset and documented machine/configuration, Vein should return results with DuckDB-equivalent numeric precision and run each supported query in no more time than DuckDB. Treat correctness and performance as separate acceptance gates: a faster incorrect result does not pass.

## Baseline

Measured on 2026-10-18 with DuckDB v1.5.6 and data at `/Users/anvi/Next/vein/data/tpch`. The existing comparison utility ran each query once, Vein then DuckDB, comparing complete unordered row multisets after converting output cells to strings. Its elapsed times include process startup; they are directional measurements, not a controlled benchmark. Q1, Q8, Q11, Q14, and Q17 timing values were collected in a follow-up paired run using the same subprocess timing method because the comparator omits timings on result differences.

| Query | Rows | Vein (s) | DuckDB (s) | Vein / DuckDB | Result comparison |
|---|---:|---:|---:|---:|---|
| Q1 | 4 | 1.219 | 0.112 | 10.9x | Decimal representation differs |
| Q2 | 255 | 1.831 | 0.041 | 44.7x | Exact string match |
| Q3 | 18,431 | 4.948 | 0.098 | 50.5x | Exact string match |
| Q4 | 5 | 1.467 | 0.060 | 24.5x | Exact string match |
| Q5 | 5 | 7.378 | 0.081 | 91.1x | Exact string match |
| Q6 | 1 | 0.797 | 0.052 | 15.3x | Exact string match |
| Q7 | 4 | 18.338 | 0.275 | 66.7x | Exact string match |
| Q8 | 2 | 8.871 | 0.097 | 91.5x | Floating result precision differs |
| Q9 | 175 | 15.324 | 0.320 | 47.9x | Exact string match |
| Q10 | 60,971 | 3.767 | 0.277 | 13.6x | Exact string match |
| Q11 | 52 | 0.855 | 0.026 | 32.9x | Decimal scale/format differs |
| Q12 | 2 | 0.944 | 0.073 | 12.9x | Exact string match |
| Q13 | 43 | 4.543 | 0.118 | 38.5x | Exact string match |
| Q14 | 1 | 1.054 | 0.066 | 16.0x | Floating result precision differs |
| Q15 | 1 | 1.233 | 0.077 | 16.0x | Exact string match |
| Q16 | 22,897 | 2.103 | 0.066 | 31.9x | Exact string match |
| Q17 | 1 | 37.087 | 0.162 | 228.9x | Floating result precision differs |
| Q18 | 94 | 15.635 | 0.209 | 74.8x | Exact string match |
| Q19 | 1 | 2.118 | 0.096 | 22.1x | Exact string match |
| Q20 | 273 | 3.435 | 0.071 | 48.4x | Exact string match |
| Q21 | 650 | 23.830 | 0.186 | 128.1x | Exact string match |
| Q22 | 7 | 1.259 | 0.036 | 35.0x | Exact string match |

- 17/22 queries matched exactly as rendered strings. The other five had equal row counts; observed differences were numeric scale/precision in output. String comparison does not prove numeric equivalence, so they remain precision failures until checked with typed numeric comparison.
- Every query was slower in this run. Summed process wall time: Vein 158.036 s, DuckDB 2.599 s (60.8x). Geometric-mean slowdown: 37.2x.
- Largest gaps: Q17 228.9x, Q21 128.1x, Q8 91.5x, Q5 91.1x, Q18 74.8x, Q7 66.7x.
- The run was single-pass and sequential. Cache state, process startup, thread settings, and run order were not controlled sufficiently for a release benchmark.

## Tasks

### P0 — Establish a repeatable benchmark and typed result comparator

- [x] Keep the existing Q1–Q22 query definitions as the shared workload; query IDs and order are checked against both files, with no query-text fork.
- [x] Record engine versions, host details, and a dataset identity digest from Parquet paths and file metadata; DuckDB thread and memory settings are captured.
- [ ] Capture Vein's effective worker count and memory limit in the low-overhead timing run. These currently appear only in INFO audit events; the default benchmark suppresses those logs to avoid changing measured latency.
- [x] Separate subprocess wall time from Vein's internal CLI elapsed time and estimate process startup overhead.
- [x] Capture Vein prepare, execute, format, and total durations in JSONL mode without enabling verbose audit logs; optimizer time is included in prepare.
- [ ] Split optimizer time from prepare and collect DuckDB internal operator phases; neither is currently exposed by this harness.
- [x] Warm up, alternate engine order, and collect at least five measurements by default; report median and p95.
- [ ] Measure cold-cache separately. The runner reports that OS cache state is uncontrolled and does not claim cold-cache results.
- [x] Preserve unordered row-multiset comparison; compare DECIMAL numerically and exactly, and floating values under the documented type-scaled epsilon contract. Report result type differences separately.
- [x] Report per-query row counts, elapsed time, Vein/DuckDB ratio, correctness, status, and measurement conditions, including process errors and timeouts.

Run the benchmark after the existing Build configuration with `VEIN_DATA_DIR=/path/to/tpch python3 assets/sql/tpch/compare.py`; pass query IDs to select queries. The JSON report uses one warmup and five measured pairs by default.

#### Harness smoke validation (2026-10-18)

- After compact phase reporting, one Q12 pair with no warmup: Vein wall 152.449 ms (prepare 31.318 ms, execute 111.730 ms, format 0.035 ms, internal total 143.083 ms); DuckDB wall 63.288 ms. Values matched; `Int64` versus `HUGEINT` types still differ. This smoke run validates report fields, not performance acceptance.
- Q1, one measured pair: Vein 409.150 ms, DuckDB 112.632 ms (3.633×); values matched, output type signatures differ, including a DECIMAL scale difference.
- Q6, one measured pair: Vein 88.326 ms, DuckDB 51.168 ms (1.726×); DECIMAL values and type signatures matched exactly.
- Q12, one warmup plus five alternating measured pairs: Vein median 152.739 ms / p95 154.884 ms; DuckDB median 64.492 ms / p95 65.771 ms; ratio 2.368×. Row values matched, with `Int64` versus `HUGEINT` output types.
- The Q12 timings are harness smoke checks, not the controlled Q1–Q22 acceptance run. Cold cache, Vein runtime settings, optimizer-only timing, and DuckDB operator phases remain open above.

#### Full-suite screening pass (2026-10-18)

One no-warmup measurement per query; Vein ran first for each pair. Wall time includes process startup and OS cache state was uncontrolled. All 44 engine invocations completed, but this is a screening run, not a repeated performance result.

| Query | Vein (ms) | DuckDB (ms) | Ratio | Comparator |
|---|---:|---:|---:|---|
| Q1 | 422.895 | 111.272 | 3.801× | Values match; types differ |
| Q2 | 1,314.502 | 36.539 | 35.975× | Values and types match |
| Q3 | 4,153.531 | 96.639 | 42.980× | Values and types match |
| Q4 | 694.903 | 57.293 | 12.129× | Values and types match |
| Q5 | 6,457.270 | 78.190 | 82.584× | Values and types match |
| Q6 | 88.169 | 51.136 | 1.724× | Values and types match |
| Q7 | 29,753.651 | 259.368 | 114.716× | Values match; types differ |
| Q8 | 8,103.998 | 99.416 | 81.516× | Not compared: DECIMAL vs DOUBLE |
| Q9 | 13,748.600 | 155.287 | 88.537× | Values match; types differ |
| Q10 | 2,320.100 | 256.236 | 9.055× | Values and types match |
| Q11 | 142.233 | 28.577 | 4.977× | Values match; types differ |
| Q12 | 171.170 | 65.595 | 2.609× | Values match; types differ |
| Q13 | 3,666.413 | 114.201 | 32.105× | Values and types match |
| Q14 | 335.437 | 68.883 | 4.870× | Not compared: DECIMAL vs DOUBLE |
| Q15 | 367.099 | 56.573 | 6.489× | Values and types match |
| Q16 | 1,379.067 | 63.769 | 21.626× | Values and types match |
| Q17 | 8,553.605 | 81.877 | 104.469× | Not compared: DECIMAL vs DOUBLE |
| Q18 | 14,723.505 | 212.882 | 69.163× | Values and types match |
| Q19 | 993.147 | 96.271 | 10.316× | Values and types match |
| Q20 | 2,739.953 | 70.093 | 39.090× | Values and types match |
| Q21 | 24,325.604 | 190.150 | 127.928× | Values and types match |
| Q22 | 535.328 | 35.551 | 15.058× | Values and types match |

- Vein was slower on all 22 queries in this pass; the geometric mean ratio was 20.992×. This is not evidence of a before/after change because the older baseline used a different output and timing path.
- The comparator confirmed equal values and types for 14 queries, equal values but different types for five, and could not establish numeric equality for Q8, Q14, or Q17 because the result types cross DECIMAL/DOUBLE families. Their values remain unverified, not proven incorrect.
- Those three queries returned Vein types `Decimal128(38, 8)` (Q8 revenue column), `Decimal256(54, 12)` (Q14), and `Decimal128(38, 6)` (Q17), while DuckDB returned `DOUBLE` for those expressions. See the precision tasks below.

### P0 — Resolve numeric precision and result-type parity gaps

- [ ] Trace result types, coercion, aggregation state/output types, division, and formatting for Q1, Q7, Q8, Q9, Q11, Q12, Q14, and Q17. The original five were string-format discrepancies; typed comparison also found type differences in Q7, Q9, and Q12.
- [ ] Identify whether each discrepancy is a value error, a decimal scale/type mismatch, floating-point accumulation order, or display-only formatting.
- [ ] Match DuckDB's relevant type/scale and rounding behavior where Vein's SQL type semantics require it; do not hide value errors by formatting or arbitrary tolerances.
- [ ] Add focused operator tests for decimal addition/multiplication/division, SUM/AVG, mixed numeric types, nulls, overflow, and rounding, plus query-shaped regressions for all eight queries.
- [ ] Add typed end-to-end result checks against DuckDB's values. Require exact equality for integral and DECIMAL results; define and justify floating tolerances from the result type and operation rather than choosing them per query.

### P0 — Find the latency bottleneck before changing plans

- [ ] Capture a compact per-query profile for all 22: scan bytes/columns/row groups, rows before/after filters, operator wall/CPU time, join build/probe rows and time, aggregate input/state, materialization/copy bytes, sort work, memory peak, and spill.
- [ ] Keep query-level and operator-level timings separate; avoid verbose per-row/per-batch logging in performance runs.
- [ ] Compare Vein and DuckDB plans and operator profiles for the top gaps: Q17, Q21, Q8, Q5, Q18, Q7, and Q9. Attribute time to measured operators before selecting a fix.
- [ ] Fix one measured bottleneck at a time. Preserve SQL semantics and the zero-copy columnar path; add a regression test for every logic change.
- [ ] Re-run the affected query and the full suite after each fix. Record plan changes, result precision, bytes/rows processed, memory, and repeated timings.

### P1 — Close the full-suite parity gate

- [ ] Require typed result equivalence for Q1–Q22; no unresolved precision discrepancies.
- [ ] Require controlled median query latency at or below DuckDB for each query on the fixed benchmark setup. If a query misses, keep its measured gap and root-cause task open.
- [ ] Run the complete suite at each milestone and record any regression; do not infer improvement from one query or one run.

## Validation references

- Query definitions: `assets/sql/tpch/pipe/queries.pipe` and `assets/sql/tpch/ansi/queries.sql`.
- Existing paired runner and its startup-inclusive timing caveat: `assets/sql/tpch/compare.py:1-11`.
- Existing optimizer work and optimizer-specific backlog: `optimization.md`.
