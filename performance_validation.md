# Final TPC-H validation — 2026-10-23

Historical pass. See `performance_completion.md` for the latest fixes and validation.

Validated the tuple-domain checkpoint continuation and cost-aware semi-join placement.
Executed Q1–Q22 sequentially through RustRover's existing Query configuration.
Workspace Test: 1,344 passed, zero failed. Check and incremental build passed.

## Method and limits

- Current development profile; one observation per query; OS cache uncontrolled.
- Query configuration supplies the existing TPC-H data root.
- Error-level logging, JSONL output, unlimited displayed result rows.
- Compared every returned row with installed DuckDB over the same Parquet files.
- Complete row multisets preserve duplicates; decimal comparisons are exact.
- Floating comparison allows eight ULP-scaled epsilons.
- The complete suite comparison does not establish ORDER BY correctness.
- Q2, Q17, and Q20 additionally passed separate complete row-order checks.
- Times below come from Vein's VEIN_TIMING fields. Execution excludes preparation
  and output formatting. These are not paired engine-only performance benchmarks.
- Previously recorded DuckDB CLI timings have a different measurement scope;
  this report does not claim DuckDB performance parity.

## Final pass

All 22 queries executed successfully and matched DuckDB's complete result values.

| Query | Rows | Prepare ms | Execute ms | Format ms | Total ms | Comparison |
|---|---:|---:|---:|---:|---:|---|
| Q1 | 4 | 32.2 | 962.4 | 0.3 | 994.9 | Values match; types differ |
| Q2 | 255 | 69.3 | 61.1 | 7.5 | 137.8 | Match |
| Q3 | 18431 | 50.1 | 266.6 | 160.7 | 477.4 | Match |
| Q4 | 5 | 36.2 | 174.3 | 0.2 | 210.6 | Match |
| Q5 | 5 | 118.3 | 287.7 | 0.2 | 406.3 | Match |
| Q6 | 1 | 30.7 | 115.6 | 0.2 | 146.5 | Match |
| Q7 | 4 | 111.5 | 191.3 | 0.3 | 303.1 | Values match; types differ |
| Q8 | 2 | 178.6 | 284.0 | 0.2 | 462.8 | Values match; types differ |
| Q9 | 175 | 139.2 | 1504.9 | 1.3 | 1645.4 | Values match; types differ |
| Q10 | 60971 | 71.6 | 979.6 | 2325.6 | 3376.7 | Match |
| Q11 | 52 | 40.5 | 114.8 | 0.5 | 155.8 | Values match; types differ |
| Q12 | 2 | 35.0 | 137.6 | 0.2 | 172.8 | Values match; types differ |
| Q13 | 43 | 27.6 | 809.4 | 0.4 | 837.4 | Match |
| Q14 | 1 | 47.0 | 143.5 | 0.2 | 190.6 | Match |
| Q15 | 1 | 69.8 | 387.3 | 0.3 | 457.4 | Match |
| Q16 | 22897 | 41.3 | 1331.7 | 251.4 | 1624.4 | Match |
| Q17 | 1 | 76.2 | 201.6 | 0.2 | 277.9 | Match |
| Q18 | 94 | 56.5 | 5347.1 | 1.8 | 5405.4 | Match |
| Q19 | 1 | 46.7 | 277.6 | 0.2 | 324.5 | Match |
| Q20 | 273 | 83.0 | 228.1 | 2.1 | 313.3 | Match |
| Q21 | 650 | 111.1 | 521.3 | 3.3 | 635.7 | Match |
| Q22 | 7 | 30.5 | 463.8 | 0.3 | 494.6 | Match |

## Diagnosed changes

- Q20 previously retained nation and stopped before collecting the needed composite
  domain. Pending domain decisions now keep adaptive continuation alive, and
  cost-approved domain producers take priority over unrelated small inputs.
- The same eligibility analysis drives scheduling and rewriting. Inner joins support
  either grouped orientation; left joins restrict only their grouped right side.
- A normal hash-build barrier does not replace a checkpoint when the remaining
  grouped work needs to be replanned using the retained tuple domain.
- Q20 retained 13,844 composite tuples and restricted aggregate input to 15,718 rows
  instead of 1,457,857. Its diagnostic tracked memory peak was 192,202,702 bytes.
  The scan still read approximately 103 MB: reduced grouping work is not I/O pruning.
- The earlier unconditional semi-join movement regressed Q21 to 2,062.4 ms by
  placing an existence test before selective inner filtering. The generic cost guard
  rejects larger or unknown proposed probe populations; residual existence joins
  require an estimated probe reduction. Q21 then measured 544.8 and 521.3 ms.
- Equality-only membership can still move when estimated probe work is equal.
  Q18 retained its improvement, measuring 5,924.9 and 5,347.1 ms, but remains slow.
- Retained checkpoint columns share Arrow buffers. New aggregate values, hash tables,
  output formatting, and spill serialization still allocate; this is not an
  engine-wide zero-allocation claim.

## Remaining

- Q1/Q11 decimal scales, YEAR width in Q7/Q8/Q9, and Q12 conditional SUM width
  still differ from DuckDB despite matching values.
- General dependent execution, non-equality correlated domains, and correlated
  empty-COUNT defaults remain open; this change implements equality-grouped domains.
- Large grouped work and fallback estimates still limit Q18.
- Q10 result formatting remains expensive and is reported separately above.
- Repeat paired measurements with equivalent timing scopes before claiming speedup.
