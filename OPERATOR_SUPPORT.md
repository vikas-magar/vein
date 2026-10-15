# Parser Operator Support Roadmap

This document tracks the path from syntax accepted by the parser through binding, logical planning, physical planning, and execution.

“Complete” means the operator's supported semantics have an end-to-end execution path across the documented query shapes. Parser or binder support alone does not count. “Partial” means an execution path exists for a restricted subset, or depends on another supported operator/backend. “Not implemented” means the operator is represented in the parser/logical plan but has no executable local physical implementation.

## Pipe operators

| Parser operator | Status | Current implementation | Missing behavior / reason for status |
|---|---|---|---|
| `SELECT` | Partial | Binds to `Project`; supported expressions compile to VM instructions. | Unsupported expression forms lack bytecode lowering or runtime kernels. Projection after an aggregate is not supported. |
| `WHERE` | Partial | Binds to `Filter`; supported predicates run in the VM or are pushed into Parquet scans. Supported predicates can also filter aggregate output. | Predicate coverage is limited by expression support. General operators after ORDER and filters in placements the pipeline cannot preserve are rejected. |
| `QUALIFY` | Not implemented | Binder represents the window computation and filter in the logical plan. | The physical planner and executor have no `Window` implementation. |
| `AGGREGATE` | Partial | Executes supported aggregate functions, numeric layouts, and multiple grouping sets, including empty sets. Omitted grouping columns become `NULL`; decimal sums use checked scaled-integer state; `AVG` returns floating-point output. Supported post-aggregate `WHERE` predicates run against aggregate output. | Group key and aggregate input/result type combinations are still limited. Projection/`SELECT` and other general operators above the aggregate are not supported. |
| `ORDER BY` | Partial | Supported local input pipelines execute one global ORDER at the end. Direct and computed expressions, multiple keys, direction/null ordering, and a following `LIMIT`/`OFFSET` are supported. Computed keys use temporary VM columns that are removed from output. | Operators after ORDER other than LIMIT are rejected; LIMIT before ORDER and multiple ORDER operators are rejected. The full task input stays in memory; there is no spill path. |
| `LIMIT` | Partial | Runtime applies a row window, including above supported aggregate and sort plans. | Some placements are rejected when the current pipeline cannot preserve their position semantics, including LIMIT before WHERE or ORDER. |
| `EXTEND` | Partial | Lowers to projection; output column IDs are mapped to their VM registers so later pipe stages can read computed columns. Temporal `TRUNCATE` is covered through `EXTEND` followed by `SELECT`. | Inherits VM expression limits and the unsupported post-aggregate projection path. |
| `SET` | Partial | Lowers to projection. | Inherits VM expression limits and the unsupported post-aggregate projection path. |
| `DROP` | Partial | Lowers to projection. | Inherits VM expression limits and the unsupported post-aggregate projection path. |
| `RENAME` | Partial | Lowers to projection. | Inherits VM expression limits and the unsupported post-aggregate projection path. |
| `AS` | Partial | Lowers to a subquery alias. | Works only when the wrapped plan is supported; the alias does not add execution support for unsupported child operators. |
| `DISTINCT` | Partial | Reuses aggregate/grouping machinery. | Inherits supported group-key and aggregate execution limits. |
| `JOIN` | Partial | Local hash join supports selected join types, equality/`USING` keys, and plain-column output projections. | Cross joins, other join types, expression join keys, and expression projections are unsupported. Equality operand order can also prevent key planning. |
| `CALL` | Not implemented | Binder creates `LogicalPlan::Call`. | No physical or runtime implementation exists. |
| `TABLESAMPLE` | Not implemented | Binder creates a sample plan. | No physical or runtime sampling implementation exists. |
| `ASSERT` | Not implemented | Binder creates an assert plan. | No physical or runtime assertion implementation exists. |
| `UNION` | Not implemented | Binds to `SetOp`. | No physical or runtime set-operation implementation exists. |
| `INTERSECT` | Not implemented | Binds to `SetOp`. | No physical or runtime set-operation implementation exists. |
| `EXCEPT` | Not implemented | Binds to `SetOp`. | No physical or runtime set-operation implementation exists. |
| `PIVOT` | Not implemented | Binder creates a pivot plan. | No physical or runtime pivot implementation exists. |
| `UNPIVOT` | Not implemented | Binder creates an unpivot plan. | No physical or runtime unpivot implementation exists. |
| `WITH` | Partial | Binder expands CTE references into logical plans. | There is no independent CTE execution path; each expanded plan must use operators the physical planner supports. |
| `WRITE` | Partial | A transactional sink is available for configured storage backends. | Requires a configured backend and an input pipeline composed of supported operators. |
| `FORK` | Not implemented | Binder creates branch plans. | No branch execution path exists. |
| `TEE` | Not implemented | Binder creates branch plans. | No fan-out execution path exists. |

## Implemented end-to-end paths

No pipe operator is complete across its full syntax and semantics yet. These operator paths do execute end to end for the supported subsets described above:

- Parquet scan with supported column projection and filter pushdown.
- `WHERE` before aggregation and supported `WHERE` predicates after aggregation.
- `SELECT` and projection-backed `EXTEND`, `SET`, `DROP`, and `RENAME` with expressions the VM can compile and run.
- `LIMIT` in placements the physical pipeline can preserve, including above aggregation.
- `ORDER BY` at the end of a supported scan or join pipeline, optionally followed by `LIMIT`/`OFFSET`; direct and computed keys and multi-key ordering are supported. Sorting retains the input in memory, with no spill path.
- `AGGREGATE` with supported aggregate functions and type combinations, including multiple grouping sets and empty grouping sets.
- `DISTINCT` through the aggregate grouping machinery for supported key types.
- Restricted local hash joins for supported join types and equality/`USING` key forms.

## Query sources and statement forms

| Parser form | Status | Current implementation and remaining work |
|---|---|---|
| Table source | Partial | Local Parquet scans are implemented. Other formats and source backends need explicit support. |
| Subquery source | Partial | Works only when its inner plan lowers to supported operators. |
| Object-store source | Partial | Parser/binder accept URLs, but execution resolves paths through the local filesystem; general remote object-store access is not implemented. |
| `EXPLAIN` | Not implemented end to end | Parses and binds to `LogicalPlan::Explain`; plan display exists, but the default physical planner does not admit `Explain` and no execution/output route was found. |

## Cross-cutting expression gaps

Operator support also depends on expression support. The parser and binder represent more expression forms than the bytecode compiler and VM currently execute.

- Require Boolean operands for `AND` and `OR` during binding; today generic coercion can admit non-Boolean operands that fail in the VM.
- Validate function argument count and types against registered signatures during binding.
- Give built-in scalar functions runtime implementations, or clearly separate registered UDFs from built-ins. Physical compilation currently lowers function calls to UDF calls.
- Track expression lowering and runtime-kernel coverage independently from pipe-operator coverage.
- `TRUNCATE(value, unit)` runs as a vectorized scalar function for supported Arrow date, time, and timezone-naive timestamp layouts. Typed timestamp literals retain their Arrow type in VM registers. Timezone-aware timestamps remain unsupported. The local end-to-end test covers `TRUNCATE` through `EXTEND` and a timestamp literal; the CLI benchmark includes `temporal/truncate-month`.

## Correctness issues in currently implemented paths

Resolve these before expanding the operator surface:

1. Write nullability validation checks entire backing arrays, including rows excluded by selection.
2. Join build tasks do not observe task cancellation while reading the build side.
3. Join key planning assumes a fixed operand order for equality predicates.
4. Date literal validation accepts impossible month/day combinations; interval parsing narrows and multiplies values before checked arithmetic.

## Implementation tracking

For each operator marked partial or not implemented, define semantics and output schema, implement lowering and local execution, then add targeted tests for the operator alone and in combinations. Keep the supported-feature table synchronized with binder and physical-planner behavior so accepted syntax does not unexpectedly fail during execution.

This roadmap targets the single-node engine. It does not imply distributed execution.
