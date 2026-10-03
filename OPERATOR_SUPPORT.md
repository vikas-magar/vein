# Parser Operator Support Roadmap

This document tracks the path from syntax accepted by the parser through binding, logical planning, physical planning, and execution.

“Complete” means a supported query shape has an end-to-end execution path. Parser or binder support alone does not count. “Partial” means an execution path exists for a restricted subset, or depends on another supported operator/backend. “Not implemented” means the operator is represented in the parser/logical plan but has no executable local physical implementation.

## Pipe operators

| Parser operator | Status | Current implementation and remaining work |
|---|---|---|
| `SELECT` | Partial | Binds to `Project` and compiles supported expressions. Expression forms without bytecode lowering/runtime kernels remain unsupported. |
| `WHERE` | Partial | Binds to `Filter`; VM filtering and scan predicate pushdown work for supported expressions and plan shapes. |
| `QUALIFY` | Not implemented | Binder creates window computation plus a filter. Physical planning does not support `Window`. |
| `AGGREGATE` | Partial | One grouping set is executable when no operator follows it. Shared generic fold logic currently dispatches `Int64`, `Float64`, and `Decimal128` inputs; integer and decimal sums preserve exact values with checked overflow, `MIN`/`MAX` preserve their input type, and `AVG` returns `Float64`. Group keys currently support `Int64` and `Utf8`; other numeric layouts, post-aggregate operators, and multiple grouping sets remain unsupported. |
| `ORDER BY` | Not implemented | Binder creates `Sort`; no local physical sort implementation. |
| `LIMIT` | Partial | Runtime limit exists, but some placements are rejected where current pipeline placement would change pipe semantics. |
| `EXTEND` | Partial | Lowers to projection; inherits expression lowering/runtime limits. |
| `SET` | Partial | Lowers to projection; inherits expression lowering/runtime limits. |
| `DROP` | Partial | Lowers to projection; inherits expression lowering/runtime limits. |
| `RENAME` | Partial | Lowers to projection; inherits expression lowering/runtime limits. |
| `AS` | Partial | Lowers to an alias node; executable only when the wrapped plan is supported. |
| `DISTINCT` | Partial | Reuses aggregate/grouping machinery; inherits its selection and supported-key limitations. |
| `JOIN` | Partial | Local hash join supports restricted join types and key predicates. Cross joins and broader key expressions are unsupported; equality operand order can prevent planning. |
| `CALL` | Not implemented | Binder creates `LogicalPlan::Call`; no physical implementation. |
| `TABLESAMPLE` | Not implemented | Binder creates `Sample`; no physical implementation. |
| `ASSERT` | Not implemented | Binder creates `Assert`; no physical implementation. |
| `UNION` | Not implemented | Binds to `SetOp`; no physical implementation. |
| `INTERSECT` | Not implemented | Binds to `SetOp`; no physical implementation. |
| `EXCEPT` | Not implemented | Binds to `SetOp`; no physical implementation. |
| `PIVOT` | Not implemented | Binder creates `Pivot`; no physical implementation. |
| `UNPIVOT` | Not implemented | Binder creates `Unpivot`; no physical implementation. |
| `WITH` | Partial | Binder expands CTE references into plans. Runtime support depends on whether all operators in each CTE plan are supported. |
| `WRITE` | Partial | Transactional sink exists, but requires a configured backend and inherits pipeline/operator limits. |
| `FORK` | Not implemented | Binder creates branch plans; no branch execution path. |
| `TEE` | Not implemented | Binder creates branch plans; no fan-out execution path. |

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

## Correctness issues in currently implemented paths

Resolve these before expanding the operator surface:

1. Write nullability validation checks entire backing arrays, including rows excluded by selection.
2. Join build tasks do not observe task cancellation while reading the build side.
3. Join key planning assumes a fixed operand order for equality predicates.
4. Date literal validation accepts impossible month/day combinations; interval parsing narrows and multiplies values before checked arithmetic.

## Implementation tracking

For each operator marked partial or not implemented, define semantics and output schema, implement lowering and local execution, then add targeted tests for the operator alone and in combinations. Keep the supported-feature table synchronized with binder and physical-planner behavior so accepted syntax does not unexpectedly fail during execution.

This roadmap targets the single-node engine. It does not imply distributed execution.
