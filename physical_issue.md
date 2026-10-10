# Physical Planner Issues and Completion Tasks

This checklist records the physical-planner issues found during the source audit. The first group contains confirmed defects or gaps that block the example queries. The following groups cover broader unsupported functionality, efficiency, diagnostics, and regression coverage.

## 1. Fix query-blocking planner defects

- [x] **Traverse join-containing aggregate plans during dispatch.** `contains_join` now traverses `Aggregate`, and a planner regression asserts that aggregate-over-join produces an aggregate transform over a recursive join input. Post-aggregate filter execution remains a separate issue below. `Distinct` over joins still needs an explicit recursive representation; see the new task below. See `crates/physical/src/builder/pipeline.rs:145-164` and `crates/physical/src/planner/default.rs`.
- [ ] **Plan DISTINCT over join inputs.** Represent DISTINCT as a recursive aggregation over a join result so top-level dispatch does not treat a join-containing DISTINCT as a linear scan. Test the physical input shape and output semantics, including duplicate rows. See `crates/physical/src/builder/pipeline.rs` and `crates/physical/src/compiler/plan.rs:583-594`.
- [x] **Preserve filters above joined aggregates.** Recursive join planning now compiles filters against the child schema at each transform boundary, including aggregate output schemas, so HAVING predicates execute after aggregation. Planner regression coverage verifies a filter instruction is carried in a transform directly above the aggregate. See `crates/physical/src/compiler/plan.rs:256-267` and `crates/physical/src/planner/default.rs:361-445`.
- [ ] **Lower complete join conditions.** Represent and execute residual `ON` predicates along with join keys. A predicate such as `key = key AND comment NOT LIKE ...` must be evaluated as part of matching, especially for LEFT/RIGHT/FULL joins; applying it after the join changes outer-join semantics. Add matching, rejected-match, and unmatched-row tests. See `crates/physical/src/builder/pipeline.rs:77-142, 251-288`.
- [ ] **Resolve equality join keys by input side, not operand order.** Accept either `probe.key = build.key` or `build.key = probe.key` and map each key to its actual input. Add tests for both orientations and verify the selected key positions. See `crates/physical/src/builder/pipeline.rs:251-287`.
- [ ] **Support multiple equi-join keys.** Extract and plan all supported equality terms in a compound join condition rather than requiring the complete `ON` expression to be one equality. Keep any remaining terms as residual predicates. Add composite-key tests. See `crates/physical/src/builder/pipeline.rs:94-105, 251-288`.
- [ ] **Execute post-aggregate operators in recursive join plans.** Define and enforce the operator ordering for aggregate, filter, sort, limit, and projection nodes represented inside recursive input plans. Ensure each operator executes exactly once and against the correct schema. Add end-to-end result tests for combinations of these nodes.

## 2. Implement required expression and subquery lowering

- [ ] **Add vectorized `CASE` and `IN` / `NOT IN` lowering.** These logical expressions are not handled by the current bytecode compiler, blocking queries that use conditional aggregates and list membership. Preserve SQL NULL semantics and test null values, empty lists where permitted, and negated forms. See `crates/physical/src/compiler/expr.rs:55-178`.
- [ ] **Implement correlated `EXISTS` / `NOT EXISTS`.** Plan these as existence-preserving semi/anti joins (or an equivalent operator), including correlation keys and residual predicates. Test matching, nonmatching, duplicate inner rows, and NULL behavior. The current expression compiler reports that `Exists` lowering is unimplemented. See `crates/physical/src/compiler/expr.rs:175-178`.
- [ ] **Implement scalar-subquery lowering and decorrelated aggregate execution.** Support scalar subqueries in expressions, including the aggregate arithmetic shape `sum(expression) * constant`. Preserve scalar cardinality and empty-input behavior; test correlated and uncorrelated cases, multiple rows where scalar cardinality must fail, NULLs, and decimal coercion. The current expression compiler rejects `ScalarSubquery`; aggregate planning also requires a direct one-argument aggregate call. See `crates/physical/src/compiler/expr.rs:175-178` and `crates/physical/src/compiler/plan.rs:782-797`.
- [ ] **Define join-type support explicitly.** Keep unsupported join kinds as clear planning errors until implemented. Add CROSS JOIN support only with defined semantics and tests; do not map it to another join kind. See `crates/physical/src/builder/pipeline.rs:231-243`.

## 3. Enforce schema and task invariants

- [ ] **Reject unexpected missing join outputs.** When a required logical output column cannot be mapped to either physical input, return a structured planning error instead of silently omitting it and only emitting a debug event. If optimizer pruning intentionally removes a column, make that condition explicit in the plan contract. See `crates/physical/src/builder/pipeline.rs:107-128, 299-330`.
- [ ] **Unify or validate the two task join representations.** A task currently has legacy `build` / `join` fields and the recursive `input_plan`. Define one authoritative representation or validate that incompatible combinations cannot be constructed or dispatched. See `crates/physical/src/task/mod.rs:26-45, 74-103, 149-192`.
- [ ] **Preserve WRITE sink behavior for plans containing joins.** The recursive join builder currently creates a client sink, while the linear builder constructs storage sinks for WRITE. Add recursive WRITE planning with the requested target/options, or return an explicit unsupported-plan error until implemented. Add a test that verifies the sink type and target. See `crates/physical/src/builder/pipeline.rs:49-75, 448-480`.

## 4. Improve efficiency and diagnostics

- [ ] **Avoid cloning the full file list while splitting scans.** `split_file_scan_source` clones the scan, including every path, once per file before replacing the list with a single path. Build each split from shared scan metadata so path copying scales linearly with file count. Add a focused allocation or structural test. See `crates/physical/src/builder/pipeline.rs:406-427`.
- [ ] **Bound temporary register growth.** Review expression compilation so temporary registers can be safely reused between independent expressions where lifetimes do not overlap. The allocator supports watermark release, but compilation currently allocates monotonically. Measure register counts for wide projections and deeply nested expressions before changing behavior. See `crates/physical/src/compiler/allocator.rs:21-63` and `crates/physical/src/compiler/expr.rs:53-109, 251-318`.
- [ ] **Make planning errors concise and actionable.** Avoid embedding the full logical-plan debug tree in common unsupported-node errors. Report the operator kind and relevant child/schema context while retaining detailed structured tracing for debugging. See `crates/physical/src/compiler/plan.rs:409-412`.

## 5. Add coverage and validate behavior

- [ ] **Add planner-level regressions for every fixed issue.** Cover aggregate-over-join dispatch, chained joins, reversed and composite equality keys, residual predicates, outer-join semantics, post-aggregate filters, and output schema invariants.
- [ ] **Add execution-level tests using small fixture data.** Assert rows and schemas, not only that planning returns a task. Include nulls, duplicate keys, unmatched outer rows, and empty inputs.
- [ ] **Add regression coverage for the reported TPCH query shapes.** Keep each query as a named test and assert expected schema and result semantics (using a DuckDB comparison or independently computed expected results where available).
- [ ] **Run the complete project Test configuration and Check configuration after implementation.** Also execute the affected TPCH examples against the local dataset after each relevant feature is implemented.

## Audit notes

- The current `unsupported source node in Parquet pipeline: Join` is first caused by join detection not traversing aggregate wrappers. Fixing that dispatch defect will expose the separate join-condition, expression-lowering, subquery, and post-aggregate gaps above.
- Unsupported logical plan families (for example window, set, pivot/unpivot, sampling, and semantic operators) should either receive deliberate implementations or clear, documented planning errors. The default physical planner currently admits only a subset of logical-plan roots; see `crates/physical/src/planner/default.rs:63-81`.
- The audit was source-based. Each task needs regression coverage and execution-level validation before its behavior can be considered complete.
