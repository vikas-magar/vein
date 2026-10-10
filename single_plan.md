# Vein Physical Execution: Complete Change Plan

## Goal

Evolve the current physical planner and executor into a robust, efficient single-node engine with one authoritative physical plan, explicit schema contracts, predictable operator execution, and a truthful zero-copy data path.

This document is a task list for implementation. Work through tasks in order, keep each implementation change paired with focused regression tests, and run the existing RustRover **Test** and **Check** configurations at the milestones. Do not add run configurations as part of this plan.

## Scope and design rules

- Target a production-quality **single-node batch engine**. Distributed execution, network shuffles, and cluster scheduling are outside this plan.
- Keep the logical plan as the SQL-semantic representation and lower it once into a canonical physical operator tree.
- Keep immutable physical operator configuration separate from per-execution mutable state.
- Derive execution pipelines and task scheduling from that canonical tree. Do not maintain a second, competing description of the same operators in task metadata.
- Treat schemas and column identities as contracts. Validate the input and output schema at each physical node boundary.
- Define zero-copy precisely: scans, direct projections, and selection-only filters should retain Arrow buffers by reference. Joins and sorts should carry row/column references internally where possible. Computed expressions and aggregates may allocate result arrays. Convert to ordinary Arrow RecordBatches only at a deliberate output boundary or when an operator requires contiguous arrays. Do not describe arbitrary joins or sorts as fully zero-copy if they gather values.
- Do not add query-name, table-name, or TPC-H-specific planner branches. Use operator semantics, schemas, and available statistics.
- Keep supported and unsupported logical operators explicit. Unsupported plans should fail during planning with concise, actionable diagnostics.
- Preserve SQL behavior for NULLs, duplicates, empty inputs, outer joins, ordering, and scalar-subquery cardinality.

## Current source findings

These findings were checked against the current Vein source and the local DuckDB and DataFusion source trees.

### Vein

- `crates/physical/src/planner/default.rs:53-82` dispatches only selected logical-plan roots. The unsupported-root error currently includes the full debug representation of the plan.
- `crates/physical/src/builder/pipeline.rs:44-145` builds a task and recursively creates `PhysicalInputPlan` nodes for join-containing shapes. The same file also has a separate linear scan/program path and scan splitting.
- `crates/common/src/protocol/task.rs:90-151, 253-313` defines recursive `PhysicalInputPlan`, while `ExecuteTask` also carries flat source/program/join/limit/sort/aggregate fields. These representations can disagree.
- `crates/exec/src/runtime/mod.rs:297-530, 764+` interprets recursive input plans, while the task executor also has a flat execution route. Multi-file recursive scans feed one downstream node through a mutex, which can serialize downstream processing.
- `crates/exec/src/pipeline/chunk.rs` stores Arrow columns plus a selection vector. This allows reference-based projection and selection, but a single selection vector does not represent arbitrary rows drawn from several input chunks.
- `crates/exec/src/operators/join/gather.rs` uses Arrow `interleave` to produce joined payload columns, which gathers values into new arrays. This conflicts with the no-copy wording currently recorded in `physical_issue.md:9`.
- `crates/exec/src/operators/transform/sort.rs` retains row references while sorting but materializes gathered output columns. `crates/exec/src/operators/sink/client.rs` materializes sparse selections for standard RecordBatch output.
- The optimizer and planner already cover a meaningful core: chained hash joins, multiple equality keys, residual join predicates, join types, aggregate and DISTINCT boundaries, several scalar-subquery rewrites, computed projections above aggregates, sort, and limit. Remaining details and regressions are tracked in `physical_issue.md`.

### Reference implementations

- DuckDB's `reference_code/duckdb/src/execution/physical_operator.cpp:245-274` derives pipelines by walking one physical operator tree: sources start pipelines, sinks split pipelines, and ordinary operators remain in the pipeline.
- DuckDB's `reference_code/duckdb/src/parallel/pipeline_executor.cpp:32-69` creates execution-local source, operator, and sink state. The operator tree remains the plan; runtime state is not another task-level operator plan.
- DuckDB's `reference_code/duckdb/src/common/types/vector.cpp:110-117, 201-224` provides buffer-reference and slice operations. DuckDB still materializes when an operator or output format requires it.
- DataFusion's `reference_code/datafusion/datafusion/physical-plan/src/execution_plan.rs:74-84, 162-168, 264-280` provides a useful Rust model for composable physical nodes, child replacement, properties, and invariant checks. Its partitioning/distribution machinery is broader than Vein's current single-node scope.
- Adopt DuckDB's single physical tree and derived pipeline model as the main architectural direction. Use DataFusion's explicit node contracts and validation ideas where they fit the single-node engine.

## Target architecture

```text
LogicalPlan
    │ validate supported semantics and required columns
    ▼
Canonical PhysicalPlan tree
    │ validate node schemas and operator invariants
    ├── explain / diagnostics
    └── derive source morsels, pipeline boundaries, and dependencies
             │
             ▼
       execution-local state
             │
             ▼
       DataChunk / row-reference views
             │
             ├── reference-preserving operators
             ├── allocating operators (expressions, aggregates)
             └── explicit RecordBatch/output materialization
```

A physical node owns its operator kind, child references, expressions/keys, input and output schema, and semantic properties. A query execution owns the mutable state for that node. Tasks represent schedulable source work or pipeline work and refer to the canonical plan; they do not repeat the operator tree in flat fields.

## Work plan

### Phase 0 — Establish contracts and a support inventory

#### Task 0.1 — Publish the physical operator capability matrix

- [x] Inventory every `LogicalPlan` variant in `crates/plan/src/plan/logical.rs`.
- [x] For each variant, record its planner entry status, known lowering/runtime limits, and current regression coverage gaps.
- [x] Mark the intended first production scope (single-node batch relational operators) and explicitly defer distributed-only features.
- [x] Include `WRITE` sink behavior, window/set/pivot/unpivot/sample/semantic operators, scalar subqueries, and external scans.

The status describes the **current physical planner and executor**, not parser/binder recognition. “Root accepted” means only that `DefaultPhysicalPlanner` dispatches the root to the stage builder; it does not guarantee that every expression or child shape is executable.

| Logical plan node | Current physical status | Verified boundary / known gap | Regression evidence and follow-up | Source locations |
|---|---|---|---|---|
| `Scan` | Partial | Parquet table scans are lowered. A declared non-Parquet table format is rejected; filters and projections depend on supported VM expressions and valid scan columns. | Scan and pruning regressions exist. Add format/error matrix coverage with scan support work. | `crates/physical/src/compiler/plan.rs:354-407, 470-543` |
| `ExternalScan` | Partial | External sources are accepted only as Parquet; URL/files and schema are lowered through the file source path. | External scan construction appears in planner/builder tests; add execution coverage for URL and explicit file lists. | `crates/physical/src/compiler/plan.rs:383-407` |
| `Filter` | Partial | Compiles in the linear path and as a transform over recursive inputs. Supported predicates are limited by VM expression lowering; correlated scalar support is rewrite-shape dependent. | Filter, residual, and post-aggregate regressions exist. Complete the expression-context matrix with expression/subquery tasks. | `crates/physical/src/compiler/plan.rs:275-285, 623-629`; `crates/physical/src/builder/pipeline.rs:254-268` |
| `Project` | Partial | Compiles expressions in linear and recursive transforms, including post-aggregate projections. Computed expressions depend on VM support; scalar projection decorrelation has supported and unsupported shapes. | Projection and scalar projection planner/runtime regressions exist. Computed correlated scalar cases remain open. | `crates/physical/src/compiler/plan.rs:283-301, 630-650`; `crates/physical/src/builder/pipeline.rs:254-268` |
| `Aggregate` | Partial | Grouping layouts and `COUNT`, `SUM`, `MIN`, `MAX`, `AVG`, and `SINGLE_VALUE` are mapped. Aggregation arguments and recursive input shapes must satisfy the compiler/runtime contracts. | Scan, join-input, post-aggregate, and scalar cardinality cases have regressions. Expand aggregate semantics and shape coverage as implementation proceeds. | `crates/physical/src/compiler/plan.rs:287-301, 594-621, 870-885`; `crates/physical/src/builder/pipeline.rs:182-219` |
| `Window` | Not dispatched | Not included in default physical planner dispatch or current recursive transform lowering. | No physical execution coverage identified. Defer or implement explicitly; do not infer support from logical-plan definition. | `crates/physical/src/planner/default.rs:63-81`; logical node: `crates/plan/src/plan/logical.rs:73-92` |
| `Join` | Partial | Cross joins are supported without a predicate. Other joins use hash execution and require at least one direct cross-input column equality; remaining conjuncts are residuals. Expression hash keys and pure non-equi joins are not supported. Join flavors map to distinct runtime kinds, subject to valid plan shape. | Extensive planner/runtime regressions cover core join types, key orientations, composite keys, residuals, and chained joins. Non-equi boundary and remaining edge cases are open. | `crates/physical/src/builder/pipeline.rs:71-145, 299-356` |
| `Sort` | Partial | Supported in linear and recursive paths, including computed recursive sort keys. It is a blocking operation and currently gathers/materializes output values. | Sort ordering and sort/limit-order regressions exist. Zero-copy sort output and resource limits remain open. | `crates/physical/src/builder/pipeline.rs:221-252`; `crates/exec/src/operators/transform/sort.rs` |
| `Limit` | Partial | Supported in linear and recursive paths; offset/count semantics depend on preserving the node's position relative to sort, join, and aggregate. | Recursive ordering regressions cover sort/limit permutations. Expand empty-input and boundary coverage as needed. | `crates/physical/src/builder/pipeline.rs:242-252`; `crates/exec/src/operators/transform/limit.rs` |
| `Distinct` | Partial | Lowered as grouping by all output columns, including recursive join inputs. Zero-column cardinality has dedicated handling. | Planner/runtime regressions cover join input, duplicates, empty input, and zero-column cases. Broader type/NULL behavior remains part of semantic validation. | `crates/physical/src/compiler/plan.rs:303-311, 611-621`; `crates/physical/src/builder/pipeline.rs:182-203` |
| `SubqueryAlias` | Partial | Treated as a schema/qualifier-preserving wrapper in linear and recursive paths; recursive aliasing rejects width changes. | Alias paths are exercised indirectly. Add explicit schema/qualifier and width-mismatch cases. | `crates/physical/src/compiler/plan.rs:320-325, 583-589`; `crates/physical/src/builder/pipeline.rs:254-268` |
| `Explain` | Not dispatched | Not included in default physical planner dispatch. It may be handled by a separate explain path; that path is outside this physical execution matrix and must be documented separately. | No physical execution coverage identified. | `crates/physical/src/planner/default.rs:63-81`; logical node: `crates/plan/src/plan/logical.rs:150-157` |
| `SetOp` | Not dispatched | UNION/INTERSECT/EXCEPT are not included in default planner dispatch. | No physical execution coverage identified. | `crates/physical/src/planner/default.rs:63-81`; logical node: `crates/plan/src/plan/logical.rs:158-170` |
| `Sample` | Not dispatched | Not included in default planner dispatch. | No physical execution coverage identified. | `crates/physical/src/planner/default.rs:63-81`; logical node: `crates/plan/src/plan/logical.rs:171-180` |
| `Assert` | Not dispatched | Not included in default planner dispatch. | No physical execution coverage identified. | `crates/physical/src/planner/default.rs:63-81`; logical node: `crates/plan/src/plan/logical.rs:182-191` |
| `Pivot` / `Unpivot` | Not dispatched | Neither reshape operator is included in default planner dispatch. | No physical execution coverage identified. | `crates/physical/src/planner/default.rs:63-81`; logical nodes: `crates/plan/src/plan/logical.rs:193-221` |
| `Fork` / `Tee` | Not dispatched | Multi-branch operators are not included in default planner dispatch; the current task/stage model is not a multi-sink execution contract. | No physical execution coverage identified. Requires an explicit single-node multi-branch design before support is claimed. | `crates/physical/src/planner/default.rs:63-81`; logical nodes: `crates/plan/src/plan/logical.rs:223-236` |
| `SemanticFilter` / `SemanticJoin` / `SemanticCalibrate` | Not dispatched | Semantic-layer operators are not included in default physical planner dispatch. | No physical execution coverage identified. Defer until their runtime and resource contracts are specified. | `crates/physical/src/planner/default.rs:63-81`; logical nodes: `crates/plan/src/plan/logical.rs:238-281` |
| `Write` | Partial | Accepted as a planner root and transparent in the linear source collector. The recursive join builder currently constructs a client sink, so join-containing WRITE must be fixed or rejected explicitly. | Linear write path has coverage; join-containing sink target/options regression remains open. | `crates/physical/src/planner/default.rs:63-81`; `crates/physical/src/builder/pipeline.rs:44-68`; `crates/physical/src/compiler/plan.rs:433-435` |
| `Call` | Not dispatched | Not included in default planner dispatch. | No physical execution coverage identified. | `crates/physical/src/planner/default.rs:63-81`; logical node: `crates/plan/src/plan/logical.rs:295-305` |

Scalar subqueries are expressions rather than `LogicalPlan` variants, so their status is tracked separately: current rewrites cover selected aggregate and one-column row-subquery forms in filters, root projections, and sort keys. Computed correlated scalar projections and references in other expression-owning operators remain open; see Task 2.4 and `physical_issue.md`.

**Acceptance:** Every logical-plan variant has a current status and a source-backed limitation or deferral. The matrix distinguishes planner dispatch from executable support and records known test gaps. Unsupported roots must receive concise, operator-specific planning errors as part of Task 2.1 / Task 5.3; this documentation task does not claim those diagnostics are already implemented.

**Tests:** Existing regression evidence and gaps are inventoried above. Add focused planner/runtime tests with each implementation change; add table-driven unsupported-root diagnostic tests when concise capability-aware errors are implemented in Task 5.3.

#### Task 0.2 — Correct zero-copy claims and define the output contract

- [x] Correct the inaccurate join-gathering claim in `physical_issue.md` and source comments: join `assemble` calls Arrow `interleave`, which gathers payload values into new arrays.
- [x] Define which internal paths preserve buffers and which allocate.
- [x] Define the existing client output contract and record the missing materialization-byte metric for the observability phase.

**Current contract:** Scans move Arrow array references into `DataChunk`; unchanged columns, direct VM register projections, and selection-only filters/limits can keep those buffers by reference. Same-input-chunk sorting can retain its arrays and selection vector. Computed VM expressions and aggregate results allocate result arrays. Join output uses `interleave` for selected probe/build values and allocates typed NULL arrays for unmatched sides. Sorting rows that span input chunks also gathers payload values. The client collector returns ordinary Arrow `RecordBatch` values: contiguous selections can use zero-copy slices, while sparse selections use `take` and allocate gathered values. Vein does not currently expose a public reference-backed result view.

The client sink records materialization time and row/batch counts, but not bytes copied. Task 5.2 owns adding per-operator gathered/materialized byte counters; do not report a byte count until that instrumentation exists.

- **Acceptance:** Source comments and issue documentation match these boundaries. No blanket claim says the full pipeline, join, sort, or client output is zero-copy.
- **Tests:** Added regressions verify scan-batch array sharing, VM direct-projection/filter buffer sharing, contiguous-slice sharing, sparse-selection allocation, and join-interleave allocation. Add matching tests when future changes move or add materialization boundaries.

### Phase 1 — Make one canonical physical plan authoritative

#### Task 1.1 — Design the canonical plan node model

- [x] Replace `PhysicalInputPlan` with a canonical physical plan type for sources, transforms, joins, aggregates, sorts, limits, distinct, and sinks; remove the unused public legacy type.
- [x] Store child links, expressions, key mappings, operator semantics, and each node's output schema in the node. Derive an operator's input schema from its child output schema rather than storing a duplicate.
- [x] Keep plan nodes immutable and shareable; hash tables, accumulators, sort buffers, and cursors remain execution-local.
- [x] Share source metadata and immutable plan subtrees through `Arc` rather than cloning large scan metadata and path lists into task operator fields.
- **Acceptance:** A plan can describe every supported query without consulting duplicate root-level operator fields.
- **Tests:** Protocol tests round-trip every operator variant and assert cloned plans share node references; planner and executor tests cover leaf, unary, binary, and breaker shapes. Structural schema validation is tracked by Task 1.3.
- **Validation:** RustRover `Build`, `Check`, and the complete `Test` configuration passed after the plan migration. The complete suite was run with `RUST_TEST_THREADS=1` to avoid intermittent failures under parallel test execution.

#### Task 1.2 — Remove duplicate task-level operator descriptions

- [x] Refactor `Task` and `ExecuteTask` so the canonical plan is the single source of physical operator semantics.
- [x] Keep task identity, attempt, source partition/morsel, cancellation and output routing as execution metadata.
- [x] Remove legacy join/build/sort/limit/aggregate fields after all callers use the canonical plan.
- [x] Remove the conflicting-plan validation requirement: deleting the legacy fields makes conflicting task representations unconstructible.
- **Acceptance:** No valid task can contain two conflicting operator plans; executor dispatch has one authoritative plan path.
- **Tests:** Task construction and runtime regression tests exercise canonical task plans. Search-based review confirms planner, coordinator, and executor no longer rely on removed task-level operator fields.
- **Validation:** RustRover `Build`, `Check`, and `Test` configurations passed after the migration.

#### Task 1.3 — Add reusable physical-plan validation

- [x] Validate recursive child structure and operator-specific requirements, including a sink-only root.
- [x] Resolve logical expression references against the immediate input schema while lowering; validate compiled VM register reads/writes against the declared physical input schema.
- [x] Validate output mapping widths and positions, data types, join null extension, join key sides, sink/passthrough schema contracts, and sort/aggregate indexes.
- [x] Run the cheap structural validator after planning and again before execution, so received or mutated task plans cannot bypass the contract.
- **Acceptance:** A missing logical column such as `ps_supplycost` fails during lowering with the owning recursive operator, column ID/name, and available input columns. A malformed canonical task plan fails before execution with its operator path and violated positional or schema contract.
- **Tests:** Regressions cover missing filter/projection references, uninitialized VM registers, invalid probe/build key and output positions, nullable outer-join outputs, repeated valid output positions, invalid aggregate grouping and sort indexes, malformed aggregate layouts, and sink/schema structure.
- **Validation:** RustRover `Check`, the complete workspace `Test` configuration, and incremental `build_project(rebuild: false)` passed after the physical-plan validation changes and regression updates.

### Phase 2 — Lower logical operators through one planner path

#### Task 2.1 — Replace root special cases with recursive lowering

- [x] Lower each currently supported logical relational node recursively into the canonical physical node model.
- [x] Keep logical order and breaker placement intact: sort, limit, filter, and projection remain on the correct side of joins and aggregates.
- [x] Fuse adjacent row-wise operators into one VM program within a source segment or above a recursive boundary, without crossing joins or blocking operators.
- [x] Derive file scheduling from the completed physical input tree; split only standalone source pipelines and share their VM/schema metadata between file tasks.
- **Acceptance:** A logical query enters one recursive lowering path regardless of whether it contains joins. There is no `contains_join` planner walk that selects a second semantic representation. Unsupported logical operators fail at the lowering boundary.
- **Tests:** Planner shape coverage verifies fused source filter/projection, fused filter/projection above a join, aggregate boundaries, DISTINCT, join/aggregate nesting, sort/limit order, output-schema projection, shared pipeline metadata when scheduling multiple files, and a named error for unsupported operators.
- **Validation:** RustRover `Test` passed for the full workspace, and incremental `Check` passed after the lowering and scheduling changes.

#### Task 2.2 — Make column requirements explicit through the tree

- [x] Compute required columns from outputs, expressions, join keys, residual predicates, grouping keys, aggregate arguments, sort keys, and correlated references.
- [x] Pass requirements down the tree without dropping columns needed by a later operator.
- [x] Preserve stable logical column identity while mapping to compact physical positions.
- [x] Verify scan projection and join output pruning agree with each node's schema.
- **Acceptance:** Every physical expression references a column present in its immediate input; pruning cannot remove an operator dependency.
- **Tests:** Regression tests cover filter-only columns and correlated dependencies through scalar/EXISTS subqueries, lambda captures, residual-only join inputs, computed sort keys, aggregate arguments, and narrow projections through multiway joins.
- **Validation:** RustRover full workspace `Test` passed; incremental `Check` and `build_project(rebuild: false)` passed.

#### Task 2.3 — Define join condition support and errors

- [x] Keep extraction of all cross-input direct-column equalities into composite hash keys.
- [x] Keep remaining `ON` predicates as residual conditions with correct outer-join match tracking.
- [x] Define the supported boundary: hash joins require at least one direct cross-input column equality with a matching supported hash-key type. Expression predicates can remain residuals when that key exists; expression-only and pure non-equi joins are rejected because no non-hash join algorithm is implemented.
- [x] Ensure Cross, Semi, Anti, Single, Inner, Left, Right, and Full semantics remain distinct and validated.
- **Acceptance:** Every accepted join condition has defined semantics and an executable physical implementation. No unsupported join is accidentally treated as an inner hash join.
- **Tests:** Key orientation, multiple keys, residual TRUE/FALSE/NULL, duplicate matches, NULL keys, unmatched rows, all outer join types, and explicit non-equi rejection or execution.
- **Validation:** RustRover full workspace `Test`, incremental `Check`, and `build_project(rebuild: false)` passed with Cross execution and exhaustive join-kind mapping regressions.

#### Task 2.4 — Complete scalar subquery lowering by supported operator context

- [x] Use the support matrix to enumerate every expression-owning operator in which scalar subqueries may occur.
- [x] Extend rewrites only where cardinality, null extension, and schema restoration are well-defined.
- [x] Handle deterministic computed correlated scalar projections in one build-side projection; reject volatile or otherwise unsafe computed expressions explicitly.
- [x] Preserve scalar result types through outer casts and mark projections that consume a decorrelated scalar as nullable when an unmatched correlated row can produce typed NULL.
- **Acceptance:** Supported scalar references are rewritten before physical expression compilation; unsupported contexts fail with a precise planner diagnostic.
- **Tests:** Scalar cardinality and typed NULL behavior for empty, one-row, and multiple-row inputs; correlated and uncorrelated projections; computed projection and outer casts preserving type/nullability; filter and sort rewrites; buffer identity for pass-through values.
- **Validation:** RustRover full workspace `Test`, incremental `Check`, and incremental `Build` all passed after the nullable schema fix.

The expression-owner inventory is exhaustive in `PlanTraversal::for_each_expression` and tested against each owner field. Current scalar/EXISTS handling is:

| Logical expression owner | Scalar subquery status |
|---|---|
| `Filter.predicate` | Supported when a known scalar or EXISTS decorrelation shape matches; otherwise optimizer diagnostic |
| `Project.exprs` | Supported for known scalar shapes, including local deterministic computed correlated row projections; computed outputs that consume scalar results retain their data type and are marked nullable; volatile, outer-dependent, nested-subquery, or schema-type-mismatched computations receive an optimizer diagnostic |
| `Sort.sort_keys` | Supported when a known scalar decorrelation shape matches; otherwise optimizer diagnostic |
| `Scan.filters`, `Aggregate.grouping_sets/aggs`, `Window.partition_by/order_by/exprs`, `Join.on`, `Assert.condition`, `Pivot.aggregate/values`, `Call.args` | Explicitly rejected by optimizer validation until a semantics-preserving rewrite exists |

After the optimizer reaches its fixpoint, any remaining subquery is reported with its owning operator field before physical expression compilation. Scalar row cardinality remains enforced by `single_value` for uncorrelated rows and the Single join for correlated rows. Empty uncorrelated results retain the scalar's declared type as NULL, and unmatched correlated rows expose nullable result columns.

#### Task 2.5 — Make sink lowering part of the canonical tree

- [x] Represent client output and WRITE/storage output as explicit sink nodes.
- [x] Preserve requested write target, format, and options when the input contains joins or other recursive shapes.
- [x] Define and enforce validation for unsupported storage targets or formats before task creation through the selected storage backend.
- **Acceptance:** Join-containing WRITE plans never silently route to a client sink.
- **Tests:** A WRITE-over-join regression verifies the storage sink type, recursive join input, destination, format and other options, Arrow schema payload, and output schema. Parquet target tests cover supported mode/format, invalid table paths, unsupported modes/formats/options, case-insensitive duplicates, reserved partition metadata, and runtime revalidation; a local prepare regression verifies unsupported compression fails before physical planning.
- **Validation:** RustRover full workspace `Test`, incremental `Check`, and `build_project(rebuild: false)` passed.

### Phase 3 — Unify execution and derived pipelines

#### Task 3.1 — Build one operator interpreter/executor

- [x] Execute every canonical physical node through one runtime path.
- [x] Keep operator-specific execution in reusable source, transform, join, aggregate, sort, limit, and sink implementations.
- [x] Ensure flat scan-only plans use the same node semantics as plans containing joins; they may still be fused into an efficient pipeline.
- [x] Use execution-local state objects with clear ownership and teardown.
- **Acceptance:** There is no separate flat-vs-recursive semantic execution branch. Pipeline fusion is an execution strategy over the same plan.
- **Tests:** Run equivalent queries through fused and non-fused planning modes in unit tests and compare schema, values, row order where promised, nulls, and errors.

#### Task 3.2 — Derive pipeline boundaries and dependencies from the physical tree

- [x] Identify sources and pipeline breakers (hash build, aggregate, global sort, and other blocking operators).
- [x] Derive pipeline/task dependencies from node inputs and output requirements.
- [x] Represent task plans as references/IDs into the canonical immutable plan plus source work ranges, not duplicated operator descriptions.
- [x] Preserve correct sequencing for build-before-probe and global blocking operators.
- **Acceptance:** Pipeline graph is a derivation of the canonical tree and can be inspected for every supported query.
- **Tests:** Graph tests verify source count, breaker boundaries, dependencies, and execution order for chained joins and aggregate/sort plans.

#### Task 3.3 — Remove scan fan-in serialization and define concurrency ownership

- [x] Replace the recursive scan's shared `Mutex<PipelineNode>` fan-in with a design that allows independent morsels to produce concurrently while preserving a single owner for stateful downstream operators.
- [x] Use bounded channels or a scheduler-owned merge where backpressure and cancellation are explicit.
- [x] Define which operators can process batches concurrently and how ordering is preserved when required.
- **Acceptance:** Parallel scan work does not serialize every downstream push behind a shared async worker mutex; memory and queue growth remain bounded.
- **Tests:** Multi-file scans verify concurrent progress, bounded pending batches, cancellation, worker failure propagation, and deterministic ordering only where the plan guarantees it.
- **Validation:** Runtime derives and enforces the dependency graph from the canonical plan; RustRover `Test` (`test --workspace`) and `Check` (`check --workspace`) passed. The incremental RustRover project build passed. A focused chained-join runtime regression and fused/recursive parity regressions passed.

### Phase 4 — Deliver reference-backed joined and sorted data

#### Task 4.1 — Introduce a composable row/column reference view

- [ ] Design a batch view that can reference columns in multiple source chunks and describe output rows through compact row mappings.
- [ ] Support probe/build column selection, duplicate output columns, unmatched outer-join rows, and nullable extension without copying unchanged payload arrays.
- [ ] Define lifetime/ownership so referenced input chunks stay alive exactly as long as downstream consumers need them.
- [ ] Keep the view batch-oriented; do not create one heap object per output cell or copy row payload values into a row store.
- **Acceptance:** Join output can represent arbitrary matches and outer nulls as references plus validity/selection metadata, with no copied payload arrays.
- **Tests:** View tests cover same-chunk and cross-chunk references, repeated rows, duplicate projected columns, empty inputs, null extension, and retained buffer identity.

#### Task 4.2 — Adapt operators to consume views or request explicit materialization

- [ ] Let direct projections, filters, limits, and compatible VM operations work over reference-backed inputs.
- [ ] Add one explicit materialization interface for kernels/operators that require contiguous Arrow arrays.
- [ ] Ensure computed expressions allocate only their result columns; untouched source columns remain references.
- [ ] Make sort retain row references until final output conversion instead of eagerly gathering all payload columns.
- **Acceptance:** Operators declare whether they preserve views or materialize, and the planner/runtime can account for that behavior.
- **Tests:** For each reference-preserving operator, assert Arrow buffer sharing; for materializing operators, assert correct values and a recorded materialization event/byte count.

#### Task 4.3 — Make the client output boundary explicit

- [ ] Keep an internal view result API where consumers can use it without flattening.
- [ ] For the existing RecordBatch API, materialize once at the boundary when required.
- [ ] Avoid materializing both in join/sort and again in the client sink.
- [ ] Preserve zero-column batch row counts and NULL semantics.
- **Acceptance:** Each result is materialized at most once for a RecordBatch consumer; callers that accept views can avoid that final copy.
- **Tests:** Client sink tests verify row counts, schemas, nulls, zero-column results, and that intermediate gather has not already copied payload columns.

### Phase 5 — Add resource limits and production observability

#### Task 5.1 — Bound operator memory and add spill paths

- [ ] Track memory for hash tables, aggregate state, sort buffers, buffered batches, and reference-retained source chunks.
- [ ] Add query/operator memory limits with cancellation-safe errors.
- [ ] Implement spill-to-disk or another bounded external algorithm for blocking operators that need to handle inputs larger than memory.
- [ ] Ensure reference-backed views do not retain whole source files/chunks after their rows are no longer reachable.
- **Acceptance:** Blocking operators have documented memory behavior and do not grow without bound; spill cleanup runs on success, error, and cancellation.
- **Tests:** Small configurable memory budgets force spill for join, aggregate, and sort; compare spilled results with in-memory results and test cleanup after cancellation/failure.

#### Task 5.2 — Add plan explanation and operator metrics

- [ ] Provide a stable physical-plan explanation showing nodes, schemas, key mappings, pipeline boundaries, and materialization points.
- [ ] Record per-node input/output rows, bytes read, elapsed time, peak/buffered memory, spill bytes, and materialized bytes.
- [ ] Keep detailed plan state in structured debug tracing; user-facing errors should remain concise.
- [ ] Avoid logging full file contents, row values, or entire plan debug trees in routine error messages.
- **Acceptance:** A failed or slow query can be localized to a plan node/pipeline and distinguish scan, compute, blocking, spill, and materialization costs.
- **Tests:** Snapshot/structured assertions for explain output and metrics on scan-filter, chained join, aggregate, sort, spill, cancellation, and planning-error cases.

#### Task 5.3 — Improve planning/runtime errors

- [ ] Replace full-plan debug errors with operator name, relevant column/key, concise input/output schemas, and a next action.
- [ ] Keep structured causes and query/operator IDs in tracing.
- [ ] Distinguish unsupported logical semantics, invalid physical invariants, resource exhaustion, and execution failures.
- **Acceptance:** Common failures do not emit an unbounded tree dump and identify the node and missing/invalid contract.
- **Tests:** Assert stable error category and concise message for unsupported node, missing column, invalid key, cardinality violation, memory limit, and sink mismatch.

### Phase 6 — Close known operator and workload gaps

#### Task 6.1 — Complete the open items in `physical_issue.md`

- [ ] Resolve all remaining unchecked items in `physical_issue.md` as implementation tasks or explicit accepted limitations.
- [ ] Specifically track task representation unification, join WRITE sink behavior, file-list clone removal, register reuse, concise errors, unsupported-node boundary, and TPCH named regressions.
- [ ] Update the issue list only after implementation and regression tests pass; do not mark an item complete based only on planning shape.
- **Acceptance:** Each item is either checked with linked implementation/tests and validation evidence or has a clear deferred rationale and user-visible limitation.

#### Task 6.2 — Add named end-to-end SQL regressions

- [ ] Add the reported TPC-H query shapes as named integration tests, not just parser/planner tests:
  - correlated minimum supply cost through a five-table chained join;
  - customer/orders/lineitem aggregation with a computed revenue expression;
  - correlated `EXISTS` with date predicates;
  - partsupp/supplier/nation grouped aggregate compared with a scalar aggregate;
  - order/lineitem conditional aggregates with `IN`, `CASE`, and date predicates;
  - customer LEFT JOIN with residual `NOT LIKE` and two aggregate stages.
- [ ] Assert expected output schema, row count, and semantic values against independently computed fixtures or DuckDB where reproducible.
- [ ] Keep test data small and deterministic for the normal suite; retain optional local TPC-H query validation as a developer workflow.
- **Acceptance:** Every reported planner failure has a named regression that would fail before its fix and checks execution output, not merely successful planning.
- **Tests:** Run the integration regressions and the complete existing workspace Test configuration.

#### Task 6.3 — Add reference comparison and differential cases

- [ ] For supported SQL semantics, compare Vein results against DuckDB on deterministic generated and hand-written datasets.
- [ ] Normalize only nondeterministic row ordering when the query has no ORDER BY; do not normalize away schema/type/null differences.
- [ ] Minimize failures into permanent regression fixtures.
- **Acceptance:** Differential tests cover NULL behavior, duplicate keys, empty inputs, outer joins, aggregate groups, scalar cardinality, and expression coercion for the supported matrix.
- **Tests:** Run deterministic seed sets in CI; keep larger randomized sweeps available as a separate existing test target if available.

### Phase 7 — Validate performance and release readiness

#### Task 7.1 — Add reproducible single-node benchmarks

- [ ] Measure scan-only, filter/project, chained joins, join plus aggregate, sort/limit, and correlated-subquery workloads.
- [ ] Track throughput, peak memory, bytes copied/materialized, and retained-buffer lifetime.
- [ ] Compare before/after against a checked-in baseline or documented local procedure; do not add query-specific optimization branches.
- **Acceptance:** The canonical plan path does not regress common scan workloads materially, join payload copies decrease as designed, and performance changes are attributable to node/pipeline metrics.
- **Tests/benchmarks:** Use existing benchmark/run configurations where available; do not introduce a new run configuration without updating the project workflow deliberately.

#### Task 7.2 — Production readiness gate

- [ ] All supported logical operators have planner, runtime, and semantic tests.
- [ ] All plan invariants are checked before execution.
- [ ] Cancellation, task failure, memory limit, and spill cleanup are covered.
- [ ] Test suite and incremental Check configuration pass through RustRover.
- [ ] At least one multi-file TPC-H validation query for each major operator path completes with results checked against an independent reference.
- [ ] Physical explain and metrics identify operators and materialization.
- **Acceptance:** Mark the first production single-node scope complete only when each gate has evidence and remaining unsupported behavior is explicit.

## Recommended implementation order

1. Complete Tasks 0.1–0.2 so support and zero-copy behavior are defined truthfully.
2. Implement Phase 1 (canonical plan, remove task duplication, invariants) before adding more operator cases.
3. Implement Phase 2 (recursive lowering and column requirements) and close currently known planner shape failures.
4. Implement Phase 3 (single executor and derived pipeline scheduling).
5. Implement Phase 4 (reference-backed views) before claiming zero-copy join/sort payload flow.
6. Implement Phase 5 (memory, metrics, and diagnostics) before broadening production workload claims.
7. Close Phase 6 gaps and add end-to-end/differential regressions.
8. Complete Phase 7 performance and release gates.

Each task should be handled as a small reviewable change: inspect the involved source and tests, explain the concrete defect and proposed fix, add regression coverage with the logic change, run the existing Test configuration, and run the incremental Check configuration. Use the existing Build configuration when a build-only validation is needed.
