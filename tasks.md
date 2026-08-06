# Vein Database: Comprehensive Production Task List & Security Audit

This document is the absolute ground-truth roadmap for taking Vein to a highly robust, secure, and advanced production state. It merges our structural analysis with a deep security/soundness audit and the requirement to implement cutting-edge academic database algorithms (WCOJ, ADOPT, Factorization).

## 1. CRITICAL: Security, Soundness, and Undefined Behavior (UB)
These must be fixed before the system processes a single byte of untrusted input.
*   **[ ] 1.1 Unchecked Rkyv Deserialization:** Add `#[derive(rkyv::bytecheck::CheckBytes)]` to all protocol structs (`VeinControlEvent`, `VfsRequest`, etc.) and use validating archive strategies to prevent out-of-bounds memory reads (UB).
*   **[ ] 1.2 `&'static transmute` in Mmap:** Remove the `unsafe { std::mem::transmute(&mmap[..]) }` in `mmap_manager.rs`. Map the file and use owned buffers or pinned Mmap closures to prevent use-after-free UB.
*   **[ ] 1.3 Global Block-ID Collisions:** Replace the naive 16-bit node-hash/sequence block ID generator in `mmap_manager.rs` with cluster-unique UUID v7 + a monotonic persisted counter to prevent silent data overwrites.
*   **[ ] 1.4 Unbounded Network Allocations:** Add hard limits to `buffer.extend_from_slice()` and `read_to_end()` in `router.rs` and `server.rs`. Add read timeouts to all QUIC streams to prevent Slowloris OOM attacks.
*   **[ ] 1.5 Cluster Identity Authentication:** Implement mTLS with client-cert verification in the QUIC listener. Reject consensus votes from unauthenticated IP addresses to prevent quorum forging.

## 2. Advanced Execution Engine (Data Plane)
Upgrading the executor to support state-of-the-art graph/relational hybrid execution.
*   **[ ] 2.1 Kuzu-Style Factorized Vectorized Execution:** Complete the Epic 3 implementation. Update `Filter` and `Project` operators to natively process `VeinBatch::Factorized` chunks instead of panicking or silently skipping.
*   **[ ] 2.2 Worst-Case Optimal Joins (WCOJ):** Implement Generic Join / Leapfrog Triejoin algorithms for multi-way cyclic joins, drastically reducing intermediate memory blowup compared to standard binary Hash Joins.
*   **[ ] 2.3 Arrow IPC Data Transit:** Replace the mocked `VfsSink.flush` (`.clear()`) with actual `arrow::ipc::writer::FileWriter` serialization.
*   **[ ] 2.4 Strict Operator Validation:** Remove silent fallbacks. If an Executor receives an unsupported operator or expression, it must loudly fail the task rather than returning incorrect query results.
*   **[ ] 2.5 Streaming Parquet Scan:** Replace the eager, blocking `collect::<Vec<_>>` memory bomb in the legacy parquet scanner with the true async OpenDAL stream reader.

## 3. Distributed Scheduling & Query Optimization
Transforming the Master node into an intelligent, adaptive coordinator.
*   **[ ] 3.1 ADOPT (RL-Based Adaptive Join Optimization):** Implement Reinforcement Learning-based join ordering in the CBO/Planner to dynamically adjust join trees based on real-time execution statistics, replacing the mocked CBO memo.
*   **[ ] 3.2 Hybrid Morsel Dispatching:** Implement dynamic task partitioning in `Scheduler::execute_batch_waves`. Instead of 1 task per stage, split stages into micro-morsels and dispatch them dynamically to Executors based on CPU telemetry.
*   **[ ] 3.3 The QuicTaskDispatcher:** Implement the actual network transmission layer (`QuicTaskDispatcher`) so the Master can send `MorselTask`s to Executor nodes.
*   **[ ] 3.4 Dynamic Retry & Failure Isolation:** Fix the scheduler bug where retries are pinned to `executors[0]`. Implement dynamic reassignment for failed tasks.

## 4. SQL Parser & Logical Binder
Fixing semantic correctness bugs in the custom pipe-SQL frontend.
*   **[ ] 4.1 Parser Truncation Bugs:** Fix postfix ops (`IN`, `BETWEEN`, `LIKE`) that return early and truncate clauses (e.g., `WHERE x IN (1,2) AND y > 3` drops the `AND`).
*   **[ ] 4.2 Literal & Type Correctness:** Fix string literals keeping their quotes (`'foo' != 'foo'`), fix integer overflow silently wrapping to `0`, and fix `NULLS FIRST` defaulting on `ASC` sorts.
*   **[ ] 4.3 Implicit Casts:** Update `binder/expr.rs` to insert implicit casts (e.g., comparing string to int) so queries don't panic at runtime.
*   **[ ] 4.4 Complete Physical Translation:** Implement `SqlPlanner::plan` to fully translate Logical Plans into Physical DAGs.

## 5. Storage Durability & Consensus (VFS Archipelago)
Ensuring zero data loss and absolute cluster consistency.
*   **[ ] 5.1 True Consensus Semantics:** The Leaderless SMR `ArchipelagoState` currently commits a placeholder `AppendExtent` before the data is written. Move data flush into the commit critical section. Implement vector clocks to resolve concurrent write collisions.
*   **[ ] 5.2 Sequence Persistence Race:** Fix the `fetch_add` race condition in `router.rs` by persisting the exact CAS return value before using it for a Proposal ID.
*   **[ ] 5.3 Silent Zero-Reads:** Fix `read_extent` on unsealed blocks. Track sealed ranges and return a "Retry" or "404" instead of silently returning arrays of zeros for data that hasn't synced yet.
*   **[ ] 5.4 Upload Channel Backpressure:** Fix the `upload_tx.send` panic/block. If the upload channel to S3 is full, apply backpressure correctly rather than stalling all appends or silently losing blocks.
*   **[ ] 5.5 Ingestion Buffer WAL & Cap:** Add a Write-Ahead Log to the 64MB ingestion buffer. Enforce a global `total_memory_usage` cap to prevent unbounded DashMap OOMs.
*   **[ ] 5.6 SipHash24 Collision Fix:** Replace the fixed-key SipHash24 for block metadata with Blake3, and store the actual path to prevent catastrophic file interleaving.
*   **[ ] 5.7 Anti-Entropy Merkle Healing:** Replace the stubbed `HealDaemon` flat array with a true Radix/Merkle Tree for logarithmic diffing, and implement the P2P missing-block fetch protocol.
