# DuckDB-style resumable executor task plan

## Goal and accepted direction

Build one extensible, single-node execution runtime shared by CLI, WASM and future clients such as JDBC.
Follow the local DuckDB reference's pipeline scheduling and suspension semantics while preserving DataChunkView ownership.
Keep one authoritative physical plan, one executable pipeline model and one query/result lifecycle.
Do not introduce query-specific shortcuts, client switches inside operators, duplicate runtimes or distributed execution.

## Implementation rules

- Every task remains incomplete until its acceptance criteria and relevant regression tests pass.
- Reuse the existing function registries, scan providers, metadata cache, statistics context, memory tracker and spill manager.
- Use traits at extension boundaries and generics for typed implementations; resolve registrations during construction.
- Keep ordinary computation synchronous and batch-oriented; suspend execution at explicit scheduling boundaries.
- Preserve Arrow payload ownership across pauses; distinguish computed output allocation from payload copying.
- Use module/mod.rs and module/tests/mod.rs; keep every new or changed implementation file within 200 lines.
- Explain dependency/type changes before implementation and follow the workspace's approval and IDE tooling requirements.
- Use existing Test, Check, Build and Query configurations; do not add run configurations.
- Preserve unrelated staged/unstaged work; do not reset the branch, commit or create branches without instruction.
- Temporary comparison harnesses belong in tests; the final production runtime must have one execution path.

## Current-state baseline

Existing: canonical plan, derived dependency graph, recursive execution, local-state combine, memory/spill services and provider injection.
Existing: bounded batch channels and acknowledged browser IPC packets, but JavaScript retains decoded results until completion.
In progress: async PipelineNode push/flush migration; this is not the selected final architecture.
Previously observed: 1,413 workspace tests passed; WASM check still had an unawaited pipeline.finish() call.
Those observations are historical evidence, not a fresh validation of the eventual resumable executor.
Q10 previously deadlocked when sorted output exceeded two 8,192-row batches on a single browser Worker.

## Tasks and acceptance criteria

### T00 — Establish a reproducible baseline
- [x] Inventory affected files, public contracts, extension points and the unfinished async migration; preserve unrelated worktree changes.
- [x] Native baseline: workspace Test and Check passed; incremental build passed. The existing Build run configuration failed to start.
- [x] Native Q10-shaped baseline: 43,838 rows; observed runs ranged from 563–717 ms. The audited run reported 620.651 ms total (70.376 ms prepare, 550.097 ms execute); the full result was drained while one row was displayed.
- [x] `wasm32-unknown-unknown` check passes after awaiting `pipeline.finish()` in the WASM scan path; the initial cross-target check exposed that missing await.
- [x] Rebuilt the release WASM package and ran a current-source Q10-shaped browser regression producing 43,838 rows in Chromium and WebKit. Chromium: 360.6 ms, 2.0 MB read, 29 ranges, 5.1 MB Arrow result; WebKit: 306.0 ms, 2.0 MB read, 27 ranges, 5.1 MB Arrow result.
- [x] Native audit captured 102,216,788 bytes peak execution memory, 52,616,384 bytes reserved, and 130,049,771 bytes read for the Q10-shaped run. Existing audit events do not expose complete payload-copy counters or browser peak memory; track those as T24 visibility work rather than infer them from Arrow result size.
- [x] Workspace Test and Check passed; incremental IDE build passed. The existing Build run configuration failed to start, so `build_project(rebuild: false)` supplied native build validation.
- [x] Done: failures and available measurements are reproducible, observability gaps are explicit, and the selective migration/cleanup map is recorded below.

### T01 — Specify execution and ownership contracts
- [x] Define one shared stage contract: sources are step-driven, transforms and sinks accept owned input events, combine is an optional owned-state capability, and EOF starts finalization before `Finished`.
- [x] Define `Output`, `Runnable`, `NeedsInput`, `Blocked`, and `Finished`; distinguish empty batches from EOF and return ownership on retry.
- [x] Contract tests cover each scheduling state, Arrow buffer identity across retry, duplicate EOF, error propagation, combine, and illegal re-entry without depending on a client.
- Role-specific factories and object-safe extension adapters remain T02; this task defines their shared execution protocol only.

### T02 — Define extension and capability contracts
- [x] Define immutable operator definitions, global/local state and typed factories with object-safe execution adapters.
- [x] Cover source/operator/sink roles, child schemas, ordering/partitioning requirements and guarantees, and safe local-state combination.
- [x] Add a client-independent test extension that registers and drives a source stage without scheduler or physical-plan edits.
- [x] Validate role transitions, output acknowledgement, EOF/finalization, factory lookup, capability column bounds, and Arrow value-buffer identity.
- [x] Workspace Test and Check passed; incremental IDE build passed. The existing Build run configuration failed to start, so `build_project(rebuild: false)` supplied build validation.
- [x] Done: typed extension construction and one-time type-erasure contracts are ready for T03 canonical-plan integration. No physical-plan or scheduler dispatch was changed.

### T03 — Connect extensions to canonical planning
- [x] Add one serializable canonical `Extension` node; retain explicit built-in semantics and shared child ownership.
- [x] Move shared identities, metadata and capabilities into `common::protocol::extension`; re-export them from `exec` without duplicate definitions.
- [x] Add registered typed configuration codecs, factory-owned roles and dependency modes, logical lowering hooks, and canonical stage construction.
- [x] Address input events by port and finalize only after every child port accepts EOF; reject missing ports and repeated EOF.
- [x] Derive source, streaming, blocking and terminal extension boundaries from canonical nodes. The linear graph explicitly rejects multiple simultaneous streaming inputs.
- [x] Prepare all definitions and validate schemas, roles, codec versions and dependencies before allocating operator state. Bind factory owners once per query node; stage execution performs no registry lookup.
- [x] Regression tests cover descriptor round trips, schema/dependency rejection, lowering, input ports, retained factories and Arrow buffer identity across worker instances.
- [x] Workspace Test diagnostics confirm 1,439 passed and zero failures across 35 suites. Native and WASM Check passed; incremental IDE build passed. The existing Build configuration still fails to start. WASM reports warnings in the existing statistics runtime.
- [x] Q10-shaped CLI joins, aggregation and ordering with `LIMIT 5` completed successfully through the existing Query configuration (exit code 0).
- [x] Keep changed Rust files below 200 lines; extract affected protocol, validation, tracing and recursive execution responsibilities into focused modules.
- [x] Done: lowering, graph derivation and prepared factory construction are connected. T04-T08 still supply executable pipelines, resumable driving, scheduling and host integration; the current production executor rejects extensions before scans rather than introducing a temporary second execution loop.

### T04 — Build executable pipelines from the existing graph
- [x] Derive executable source/operator/sink phase bindings from the existing graph, retaining canonical node, schema and computed-key program owners.
- [x] Bind one shared operator state per canonical node, with independent worker-local phases and explicit input ports; combine completed local state once when declared.
- [x] Preserve join-build dependencies and blocking boundaries; validate graph IDs, dependency cycles, canonical child edges and schema continuity.
- [x] Validate ordering and partitioning requirements conservatively, including direction, NULL placement and complete hash keys. Unknown guarantees never satisfy stronger requirements.
- [x] Reuse registered extension factories through a generic provider contract; prepare builtin state through ExecutorBuilder without introducing another production execution loop.
- [x] Cover deterministic construction, nested blockers, shared build/probe state, terminal extension finalization, schema/program ownership and Arrow buffer identity. Reject repeated phase consumption until explicit replay/multicast exists.
- [x] Workspace Test: 1,456 passed, zero failed across 35 suites; focused resumable Test: 30 passed. Native/WASM Check and incremental IDE build passed. WASM retains existing statistics warnings; the Build run configuration fails to start.
- [x] CLI join/aggregate/sort/LIMIT validation query completed with exit code 0. Every changed Rust file remains below 200 lines.
- [x] Done: executable construction is ready. T05 supplies the driving loop and later tasks migrate builtin kernels; production execution is not yet resumable.

### T05 — Implement the resumable pipeline executor
- [x] Retain the active-stage cursor, worker-local operators, pending DataChunkView events and EOF/finalization progress. Drain downstream before advancing producers; retain at most one pending event per edge.
- [x] Execute bounded turns and return Finished/Runnable/Blocked. Acknowledge output only after acceptance; preserve rejected input for retry without replaying accepted events.
- [x] Use executor-owned, generation-specific resume tokens to reject stale, duplicate and foreign wakeups. Stage failures are terminal; drop pending views on failure and release retained operators when the executor is dropped.
- [x] Combine completed local state once; propagate EOF only after upstream completion. Cover output during finalization and the terminal extension finalizer through the actual driver.
- [x] Eleven new regressions cover budgets 1–12, repeated suspension, batch/EOF retries, multiple outputs, deep chains, empty batches, duplicates, NULLs, sparse mappings and Arrow buffer identity. Focused resumable Test: 41 passed, zero failed.
- [x] Native/WASM Check and incremental IDE build passed; changed Rust files are below 200 lines. Existing WASM statistics warnings remain; the Build configuration still fails to start.
- [x] CLI Q12 regression query completed with exit code 0 and the expected two rows. This uses the existing production path and does not measure the new driver.
- [x] Workspace Test rerun completed with exit code 0 through the existing Test configuration. The IDE returned the final status directly; no excluded console-log read was needed.
- T06 supplies scheduling/readiness integration; later tasks migrate builtin kernels. The new driver is tested over canonical stage bindings but is not yet the production query runtime.

### T06 — Implement race-safe scheduling and wake-ups
- [x] Schedule canonical pipeline worker slots with a FIFO ready queue; requeue exhausted turns and park blocked tasks. Construct local stages lazily after graph prerequisites complete.
- [x] Respect serial capabilities and release a pipeline's dependents only after every worker finishes local-state combination through T05.
- [x] Move each running executor out of shared state under an exclusive lease. Concurrent callers cannot run the same local state; an unwound worker turn stops the scheduler instead of orphaning work.
- [x] Add a generic WaitRegistrar with owned unsubscription guards and standard Rust Wakers. Record synchronous readiness before parking; coalesce repeated wakeups and reject callbacks from old wait generations.
- [x] Keep operator calls, registration, host notifications, tracing callbacks and guard destruction outside scheduler locks. Cancellation retires queued/parked executors and unstarted definitions; in-flight bounded turns release their state on return.
- [x] Sixteen deterministic regressions cover wake-before-park, repeated/late wakes, callback re-entry, concurrent worker claims, FIFO fairness, serial workers, multi-worker join dependencies, extension finalization, cancellation races, registration/operator errors and unwind cleanup.
- [x] Workspace Test completed with exit code 0; focused scheduler Test: 16 passed. Native/WASM Check and incremental IDE build passed. Existing WASM statistics warnings remain; the Build configuration still fails to start.
- [x] CLI Q12 returned its expected two rows with exit code 0 (235 ms, existing production path; not a scheduler performance comparison). Every changed Rust file remains below 200 lines.
- [x] Done: host-independent scheduling is ready. T07 adds bounded output and T08 supplies host drivers; later tasks migrate builtin kernels and replace the recursive production runtime.

### T07 — Implement the shared result buffer
- [x] Add one shared client-neutral DataChunkView result buffer with producer admission permits, accounted pending output, publication/fetch ownership and explicit unordered/producer-ordered policies.
- [x] Reuse QueryMemoryTracker and retained-array leases. Deduplicate ArrayRef identities across pending/queued views; returned result handles retain query array/metadata leases after output capacity is released.
- [x] Include retained selections and full row maps in footprint estimates. Sparse views cannot evade their pinned-array or row-map footprint; zero-column rows still consume nonzero metadata credit.
- [x] Reserve configured ordered headroom so later read-ahead cannot occupy the earliest producer's output capacity. Oversized batches and hard query-memory failures return resource errors instead of indefinite waits.
- [x] Connect atomic subscribe-and-check readiness through T06's WaitRegistrar. Unsubscribe guards, close/cancel/failure wakeups and array/lease destruction occur outside buffer locks.
- [x] Add a reusable resumable ResultSink that acquires admission before requesting upstream input. Real canonical executor/scheduler tests verify single-batch backpressure, exactly-once source output and cancellation cleanup.
- [x] Add 24 regressions across buffer ownership/accounting, ordering, readiness, sparse metadata and scheduler integration. Workspace Test completed with exit code 0; native/WASM Check and incremental IDE build passed.
- [x] CLI Q12 returned its expected two rows with exit code 0 (240 ms, existing production path; not a buffer performance comparison). Changed Rust files remain below 200 lines. Existing WASM statistics warnings and the Build configuration launch failure remain.
- [x] Done: buffer contracts and scheduler integration are validated. These are explicit engine footprint estimates, not allocator/RSS telemetry; distinct arrays sharing internal buffers may be conservatively counted more than once. T08 and later host/client/operator migrations replace the existing production channels.

### T08 — Add native and cooperative host drivers
- [x] Add HostDriver over the existing scheduler: persistent scoped native workers park on generation-checked condition-variable notifications; a cooperative host awaits asynchronous readiness and yields after each productive bounded turn.
- [x] Keep host policy outside operators using a generic CooperativeHost interface and the existing WaitRegistrar. Driver drop, explicit cancellation, worker failure and panic stop scheduling and unregister parked I/O.
- [x] Add a reusable browser MessageChannel adapter. Yield messages contain only tickets; stale or abandoned messages cannot complete later yields. Release callback/port resources on drop and wake outside mutable borrows.
- [x] Add 17 regressions for host semantics, worker combination/dependencies, bounded turns, idle I/O, cancellation, failure, readiness races, slow-consumer backpressure and browser-ticket lifecycle. The same source Arrow value-buffer addresses survive both native and cooperative delivery.
- [x] Focused resumable Test: 94 passed; browser-ticket Test: 3 passed. Native/WASM Check and incremental IDE build passed. Changed Rust files are below 200 lines; existing WASM statistics warnings remain and the Build configuration still fails to start.
- [x] CLI Q12 returned the expected two rows with exit code 0 (248 ms, existing production executor). This is a regression check, not a performance measurement of the new host drivers.
- [x] Workspace Test rerun with explicit wait-for-exit completed with exit code 0. The IDE returned its final status directly; no excluded diagnostic logs were read.
- The browser adapter compiles for WASM and its ticket lifecycle is tested natively. Live browser query integration remains T22; builtin kernel migration remains T11-T19. Production queries still use the existing executor.
- [x] Done: workspace validation and one-thread/multi-thread host semantics pass, including actual pending scheduling boundaries. Later operator/client migrations connect these drivers to production queries.

### T09 — Introduce one query execution scope
- [x] Extend QueryExecutionResources as the authority for request/query identity, statistics, cancellation and memory/spill ownership. Reject foreign task identities/statistics contexts before execution and emit bounded scope lifecycle diagnostics.
- [x] Generalize the existing adaptive lifetime guard for prepared queries, streaming and writes. Construct one scope during preparation, transfer its guard through adaptive execution, and remove fresh cancellation tokens from final tasks.
- [x] Extend the existing CancellationToken with race-safe RAII subscriptions. Invoke callbacks and release captured owners outside locks; preserve existing async waits without monitor tasks or a second cancellation system.
- [x] Bind resumable host drivers and result buffers through query-resource factories. Cancellation wakes parked hosts and data/capacity waiters; close/failure releases queued output while fetched DataChunkView leases remain valid. Successful computation preserves unread output.
- [x] Retain checkpoint registrations through query cancellation subscriptions. Pruning drops them normally; cancellation unregisters them immediately even if the stream/cleanup handle remains alive. Spill runs and reservations retain their existing RAII cleanup.
- [x] Interrupt pending asynchronous task execution on the same cancellation root. Final task failures cancel sibling work; on unwind-enabled hosts task panics report an outcome instead of silently disconnecting.
- [x] Add 20 regressions for cancellation subscriptions, query identity, memory/spill cleanup, checkpoint unregistration, native/cooperative idle waits, output ownership and prepared/nonadaptive stream lifetimes. Extend the WRITE commit regression to check scope closure.
- [x] Focused validation passed: 13 scope-related executor tests, 10 cancellation tests and 8 CLI adaptive/lifecycle tests. Native/WASM Check and incremental IDE build passed; the Build configuration still fails to start. All changed Rust files stay below 200 lines.
- [x] Query validation passed: Q12 returned the expected two rows (238 ms); Q3 with LIMIT 5 completed (352 ms), checkpointed 48,315 customer rows and replanned its continuation. These are regression observations, not scheduler performance claims.
- [x] Confirm full workspace Test outcome: the approved bounded diagnostic read during T10 validation reported 1,568 passing tests, zero failures and no unfinished tests after correcting the coordinator cancellation race.
- IDE inspections retain unresolved cancellation-module diagnostics after the file-to-directory move; native and WASM compiler checks pass. The IDE scripting engine is unavailable, so no cache workaround was added to source.
- T09 validation is complete. Builtin execution migration remains subsequent work; the client query-handle API is covered by T10.

### T10 — Evolve the client-neutral query API
- [x] Evolve LocalQuery into prepared execution plus a query handle exposing schema, status, progress, incremental fetch, cancel and close.
- Specify session ownership, active-query/concurrency policy, repeated terminal fetches, early close and already-returned batch lifetimes.
- Done when fake CLI/browser/native consumers exercise one API without execution branches based on client type.

Implementation status: the client-neutral `engine::query` API now provides a single owning `QueryHandle`, stable schema, incremental leased `DataChunkView` results, shareable cancellation/status/progress controls, and confirmed write receipts. Typed backends may be thread-local; the erased native adapter remains `Send`. Independent queries have independent cancellation and cursor ownership. Future ODBC/JDBC adapters can use this contract; no driver or foreign ABI is claimed here.

Validation: the focused engine contract tests passed, along with three real-query handle integration tests and two shared-error identity tests. The full resumable test run passed; separate scheduler (18), host (15), and result-buffer (21) runs passed. Native workspace Check, WASM Check and incremental build passed. Full workspace Test exposed two scan cancellation/error-propagation failures: task-level cancellation dropped the coordinator before its worker-drain cleanup. The approved correction keeps cancellation at source waits, including checkpoint replay. All three multi-file scan tests and eight query-scope tests passed after that correction; the workspace rerun reported 1,568 passed, zero failures, and no unfinished tests across 20 suite summaries.

Migration boundary: `LocalQuery::start` adapts the existing production execution path. CLI/WASM adoption, production operator migration, and old API deletion remain their explicitly planned later tasks; T10 does not claim those migrations are finished.

Final regression query: Q12 returned MAIL (9,883 / 14,894) and SHIP (9,953 / 14,906), two rows in 223 ms. WASM Check passed again after the cancellation correction. This validates existing production behavior, not a resumable-scheduler speedup.

### T11 — Migrate generic scans and connector execution
- [ ] Replace run-to-completion source pushing with incremental source state/cursors and retained I/O readiness.
- Preserve provider-defined morsels, projection, pruning, runtime filters, shared metadata, row coverage and scan statistics.
- Done when file-format-neutral provider tests, slow I/O, early LIMIT, cancellation and Parquet regressions pass without preloading data.

### T12 — Migrate expression and LIMIT operators
- [ ] Remove downstream ownership from expression and LIMIT execution; return output/status to the pipeline executor.
- Reuse expression kernels and the functions crate; preserve mapped inputs, selection-only filtering and computed-column ownership.
- Done when custom functions/kernels, NULLs, casts, empty output and early completion pass with unchanged source buffer identity.

### T13 — Migrate join build and probe execution
- [ ] Separate build, publish/combine, probe and unmatched-output phases; adapt the existing bounded hash-probe machinery.
- Persist cursor position, accepted matches, residual state and output mappings safely without self-referential borrowed state.
- Done when inner/outer/semi/anti/single/cross joins resume correctly, preserve multiplicity and emit unmatched build rows once.

### T14 — Preserve runtime-filter lifecycle and placement
- [ ] Publish exact build feedback and safe runtime filters before dependent probe work becomes runnable.
- Preserve legality across join kinds, source-column lineage, provider pushdown and filter memory ownership.
- Done when restart/suspension does not rebuild filters unnecessarily or move them across unsafe semantic boundaries.

### T15 — Migrate aggregation and DISTINCT
- [ ] Separate accumulation, local combination, distinct finalization and bounded output cursor state.
- Reuse registered accumulators and grouping machinery; retain source-backed group keys and exact result types.
- Done when empty/scalar/grouped aggregates, grouping sets, dictionary NULLs, COUNT(DISTINCT) and numeric-width regressions pass.

### T16 — Migrate sort and top-N
- [ ] Separate input retention, ordering finalization and bounded sorted-output cursors; preserve helper-column projection.
- Identify expensive indivisible CPU work and split required phases so execution budgets are meaningful during finalization.
- Done when Q10-sized output, ties, NULL ordering, top-N, early stop and repeated suspension preserve correct ordering.

### T17 — Migrate spill execution
- [ ] Make join repartitioning, aggregate partition finalization and sort merge state resumable without duplicating algorithms.
- Preserve memory/disk accounting, bounded workspace, RAII ownership and cleanup on all exit paths.
- Done when low-budget tests suspend during spill phases, resume exactly once and leave no leaked files or reservations.

### T18 — Migrate storage and discard sinks
- [ ] Adapt existing WriteStorage/WriteTransaction and discard behavior to the new sink/finalization lifecycle.
- Preserve atomic commit receipts and abort behavior; result streaming must not report successful writes before commit.
- Done when retries/resumption cannot duplicate writes or commits, and errors/cancellation clean up pending transactions.

### T19 — Preserve checkpoints and adaptive continuations
- [ ] Execute existing checkpoint producers/replay sources through the same pipeline runtime and query resource scope.
- Preserve schema/lineage, statistics coverage and replacement of only safe, unexecuted continuation work.
- Done when correlated-domain and adaptive regression tests pass without a second interpreter or duplicated checkpoint statistics.

### T20 — Separate encoding from execution and transport
- [ ] Wrap the DataChunkView result stream with incremental format encoders; reuse CLI JSON and WASM Arrow IPC implementations.
- Keep encoding synchronous CPU work and transport readiness separate; share reusable codecs without importing browser types into exec.
- Done when empty results, schemas, sparse mappings, dictionaries, large values and fragmented transport round-trip correctly.

### T21 — Adapt CLI and prove the native-client boundary
- [ ] Consume the shared query handle for CLI display, JSON streaming and writes without retaining unused output.
- Test a native row-cursor consumer suitable for a future JDBC adapter: batch fetch, metadata, NULL access, cancellation and close.
- Done when clients require no operator changes; this task does not claim to ship a JDBC driver or stable foreign ABI.

### T22 — Adapt WASM and the Worker lifecycle
- [ ] Expose bounded execution/fetch operations that release engine borrows between scheduling turns and retain the browser session.
- Connect ACKs to fetch demand, cancellation to query ownership and stale messages to execution identity; reserve termination for recovery.
- Done when Q10 above 16,384 rows completes and repeat queries, slow ACKs, cancellation and errors work in Chromium and WebKit.

### T23 — Bound browser result retention and fix control ownership
- [ ] Distinguish streamed consumption from explicit collection; account active decoded results and release them on failure/cancel.
- Anchor Stop/progress to execution identity rather than editable text; handle owner-tab closure and directory changes explicitly.
- Done when editing/closing a running tab cannot orphan execution and consumers cannot silently accumulate unlimited result memory.

### T24 — Add execution visibility without payload logging
- [ ] Report query/pipeline/operator IDs, phases, rows, retained bytes, pending bytes, yields, block reasons and wake counts.
- Separate compute, I/O wait, consumer wait, encoding and transport timings; avoid double-counting nested wall time as CPU time.
- Done when Q10 logs explain progress and memory ownership using bounded structured fields without printing data or giant plans.

### T25 — Complete semantic and lifecycle regression coverage
- [ ] Add deterministic tests for one-thread saturation, repeated block/resume, lost-wake races, early close and failure propagation.
- Cover ordering, duplicates, outer NULL extension, empty/zero-column results, schema drift, spills and retained-view validity after close.
- Done when the same runtime tests pass for native/cooperative drivers and all ownership assertions remain valid.

### T26 — Remove superseded code and enforce module boundaries
- [ ] Delete the recursive production driver, downstream-owned chains and superseded async delivery adapters after their replacements pass.
- Remove materializing streaming fallbacks, obsolete wrappers/tests and stale comments; preserve algorithms and unrelated worktree changes.
- Done when searches show one active execution model, no legacy dispatch flags and all affected modules meet the 200-line limit.

### T27 — Final correctness, performance and release validation
- [ ] Run incremental build_project plus existing workspace Test, Check and Build; validate WASM and browser packaging.
- Run Q1–Q22 one by one against DuckDB with complete value/type/order semantics, documented numeric tolerances and identical inputs.
- Compare preparation, first-batch latency, execution, drain time, peak memory and bytes copied under stated cache/thread/build settings.
- Done when regressions are resolved, Q10 no longer hangs and measured results are recorded without claiming unproven DuckDB parity.

## Dependencies and completion policy

- T00 → T01–T03 → T04–T06 establishes the execution contract and scheduler.
- T07–T10 establishes output, host scheduling and query lifetime; integrate these before claiming bounded streaming.
- T11–T19 migrates all existing operator paths; dependencies such as probe-before-build remain enforced by T04–T06.
- T20–T24 completes client delivery, retention and visibility; T25 runs throughout every migration task.
- T26 follows successful operator/client migration; T27 is the final gate.
- Do not keep both old/new production runtimes as a permanent compatibility design.
- No task is complete solely because it compiles; document tests, remaining limits and actual measurements.
- Full JDBC driver, remote protocols, distributed execution and browser persistent spill implementation are separate deliverables.
- Browser spill capability must remain explicit: a missing backend produces a resource/capability error, never unlimited buffering.

## Source anchors

- Vein contracts: crates/exec/src/pipeline/node.rs; canonical plan: crates/common/src/protocol/task/mod.rs.
- Vein graph: crates/common/src/protocol/pipeline_graph/mod.rs; current driver: crates/exec/src/runtime/recursive/mod.rs.
- Vein client scope: crates/engine/src/local/query/mod.rs; delivery: crates/engine/src/local/stream.rs.
- DuckDB scheduling: reference_code/duckdb/src/parallel/pipeline_executor.cpp and pipeline.cpp.
- DuckDB backpressure: reference_code/duckdb/src/main/buffered_data/simple_buffered_data.cpp and batched_buffered_data.cpp.
- DuckDB result lifecycle: reference_code/duckdb/src/main/query_result_stream.cpp and execution/operator/helper/physical_result_sink.cpp.
