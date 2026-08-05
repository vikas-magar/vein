# Vein Ultimate Architecture & Master Development Plan
**Version**: 5.1 (Final Integration)
**Date**: 2026-08-05

This document represents the complete, end-to-end architectural vision and implementation plan for Vein. It synthesizes the exact internal implementations of Kuzu and LadybugDB (Factorized execution, Morsels, WCOJ) and maps them into Vein's distributed, cloud-native Rust architecture.

---

## Part 1: Deep Architecture & Execution Model

### 1.1 Vectorization & Selection Vectors (The Pipeline Engine)
Both Kuzu and LadybugDB rely heavily on vectorized execution to utilize SIMD instructions and avoid branching.
*   **The DataChunk**: Instead of passing raw `RecordBatch`es directly, Vein will process `DataChunk`s consisting of `ValueVector`s. 
*   **Selection Vectors**: Vein will adopt the `SelectionVector` pattern. When a `Filter` operator processes a chunk, it **does not copy data**. It simply updates a bitmask/array (`SelectionView`) marking which row indices survived the filter. Subsequent operators in the pipeline only iterate over valid indices in the `SelectionVector`.

### 1.2 Morsel-Driven Scheduling (Type 4 to Type 5 Evolution)
Unlike traditional Volcano iterators, Vein uses a pipeline-breaker architecture. A pipeline executes until it hits a blocking operator (e.g., `HashJoinBuild`, `OrderByMerge`).
*   **Type 4 Schedulers (DuckDB/LadybugDB Style)**: Vein's `TaskScheduler` splits pipeline execution into Morsels.
    *   *Implementation*: A `ScanNode` has a `SharedState` object containing a counter (`currentCommittedGroupIdx`). When an idle thread inside our `WorkerPool` needs work, it calls `nextMorsel()`, takes a short-lived `std::sync::Mutex`, atomically claims a `nodeGroupIdx` (e.g., a Parquet row group), increments the counter, releases the lock, and processes the chunk.
*   **Type 5 Schedulers (Kuzu Hybrid/Elastic Evolution)**: Type 4 fails under extreme data skew (one morsel takes 60 seconds, stranding the query).
    *   *Implementation*: Vein's `Master` actively tracks the output sizes of morsels across the network. If a single VFS block or partition yields 40x more data than others, the Master cancels the long-running task, slices the remaining byte boundaries of that morsel into dozens of micro-morsels, and re-queues them. This elasticity guarantees uniform completion times.

### 1.3 Factorized Execution (The Join Revolution)
Materializing flat rows during multi-way joins causes an O(N³) memory explosion. Vein adopts Kuzu's `FactorizedTable`.
*   *Implementation*: Vein will wrap Arrow structures in a `VeinBatch { Flat, Factorized }` enum. Instead of flattening data during a 1-to-many join, the Hash Table stores collections of multi-valued fields as nested lists (`overflow_value_t`).
*   When a Worker pushes data to the VFS during the shuffle phase, it serializes these *compressed, un-flattened* structures, reducing network and disk shuffle I/O by 10x-50x.

### 1.4 Worst-Case Optimal Joins (WCOJ)
Standard binary Hash Joins are catastrophic for cyclic queries (e.g., finding triangles).
*   *Implementation*: The Vein Planner will inject `IntersectBuild` operators. Following Kuzu's `Intersect::intersectLists`, Vein will collect adjacency lists for identical node IDs. It performs iterative `twoWayIntersect()` calls, using synchronized `SelectionVector`s to progressively shrink the valid mask across all lists simultaneously without moving memory.

### 1.5 Distributed VFS Shuffle (The Vein Advantage)
LadybugDB and DuckDB are strictly single-node, embedded databases. Vein takes their world-class vertical execution and distributes it horizontally.
*   Compute nodes are stateless. Morsel outputs bounded by pipeline breakers are pushed directly into the Virtual File System (VFS) via AF_XDP/s2n-quic.

---

## Part 2: Epics and Implementation Plan

### Epic 1: The Vectorized Type 4 Execution Engine (Foundation)
*Rebuilding the core pipeline to support Morsels and Selection Vectors.*

- [ ] **Task 1.1: Selection Vectors & DataChunks**
  - Create `SelectionVector` struct in `crates/common/src/`.
  - Refactor `PipelineNode::push()` to accept a `DataChunk` containing `ValueVector`s and a `SelectionVector` rather than a flat Arrow `RecordBatch`.
- [ ] **Task 1.2: The Pipeline Breaker Architecture**
  - Implement `Pipeline` and `Task` structs in `crates/exec/`. Define operators that block execution (e.g., `HashJoinBuild`).
- [ ] **Task 1.3: Type 4 Morsel Dispatcher (`SharedState`)**
  - Modify `ScanSource` in `crates/exec/src/operators/source/`. 
  - Implement `SharedState` with a Mutex-guarded `next_morsel()` function that hands out partition indices to idle workers in the `WorkerPool`.
- [ ] **Task 1.4: Dual-Pool Isolation**
  - Split `WorkerPool` into a CPU `LocalSet` pool and a standard `tokio` multi-threaded I/O pool to ensure kernel-bypass network I/O doesn't interrupt vector processing.

### Epic 2: The Type 5 Distributed Scheduler (Elasticity)
*Scaling the Type 4 single-node scheduler across the Vein cluster.*

- [ ] **Task 2.1: Execution Telemetry Loop**
  - Add `ExecutionFeedback` messages. Workers report morsel throughput to the Master.
- [ ] **Task 2.2: Type 5 Hybrid Elastic Splitting**
  - Update `crates/master/src/scheduler/dispatcher.rs`.
  - Add logic to detect skew: if a worker takes >200ms on a single morsel, cancel the task, split the remaining VFS block range into micro-morsels, and queue them to the `ExecutorRegistry`.
- [ ] **Task 2.3: Direct VFS Shuffle Writer**
  - Implement a `VfsSink` operator that writes pipelined `DataChunk`s directly to the VFS cluster over QUIC, fulfilling the disaggregated architecture.

### Epic 3: Factorization & Compression
*Implementing Kuzu's memory-saving innovations.*

- [ ] **Task 3.1: The `FactorizedTable` Abstraction**
  - Create `FactorizedTable` in `crates/exec/` to store `UNFLAT` vectors.
- [ ] **Task 3.2: Factorized Hash Join**
  - Refactor the Hash Join operator. When joining a primary key to a foreign key, output a `Factorized` chunk with `overflow` list pointers instead of taking the cartesian product.
- [ ] **Task 3.3: VFS Factorized Serialization**
  - Update the `VfsSink` to natively serialize factorized nested lists to Arrow IPC without flattening them first.

### Epic 4: WCOJ & Advanced Optimization (2026+)
*The bleeding edge of query planning.*

- [ ] **Task 4.1: The Intersect Operator (WCOJ)**
  - Implement `IntersectBuild` and `twoWayIntersect()` in Rust, ensuring `SelectionVector`s are sliced and synchronized accurately during execution.
- [ ] **Task 4.2: ADOPT RL Join Enumerator**
  - Replace the heuristic query planner with a Reinforcement Learning agent that dynamically routes morsels through different join orderings to discover the mathematically fastest path in real time.
- [ ] **Task 4.3: eBPF VFS Pushdown**
  - Compile simple filters to eBPF bytecode and push them to the VFS storage nodes so filtering happens *before* the network layer.

---

## Part 3: Kuzu Innovations for Vein (Innovation Matrix)

This section maps the specific innovations discovered during the research phase (`kuzu_innovations_for_vein.md`) to their target realization in Vein's pipeline.

| Innovation | Source | Implementation Phase | Estimated Performance Gain | Difficulty |
|---|---|---|---|---|
| **Hybrid Elastic Morsel Dispatcher** | Kuzu (Aug 2025 Paper) | Epic 2 | 2x-5x faster query time on skewed data | ⭐⭐⭐ |
| **Factorized Arrow Batches** | Kuzu (CIDR 2023) | Epic 3 | 3x-50x shuffle data size reduction | ⭐⭐⭐⭐ |
| **Worst-Case Optimal Joins (WCOJ)** | Kuzu + VLDB 2025 | Epic 4 | 3x-100x reduction in complexity for cyclic joins | ⭐⭐⭐⭐⭐ |
| **ADOPT RL Join Ordering** | VLDB 2023 / ADOPT | Epic 4 | Eliminates catastrophic plans natively | ⭐⭐⭐⭐ |
| **Intra-Query Elasticity** | TU Berlin 2025 | Epic 2 | Eliminates stage stragglers entirely | ⭐⭐⭐⭐ |
| **eBPF JIT Filter Pushdown** | Vein Original | Epic 4 | 10x-100x scan speed vs user-space | ⭐⭐⭐ |

---
**Status**: Ready for Execution. 
**Next Step**: Initiate Epic 1 (Task 1.1: Selection Vectors & DataChunks).
