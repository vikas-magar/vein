# Vein Ultimate Architecture & Master Development Plan
**Version**: 4.0 (Next-Generation Roadmap)
**Date**: 2026-08-05

This document represents the complete, end-to-end architectural vision and implementation plan for Vein. It synthesizes state-of-the-art research (Kuzu, Umbra, DuckDB, ADOPT) into a cloud-native, disaggregated OLAP engine.

---

## Part 1: Complete Architecture & Tech Spec

### 1.1 The Disaggregated Storage Layer (VFS)
Vein separates Compute and Storage to achieve infinite scale and fault tolerance.
*   **VFS as the Backbone**: The Virtual File System (VFS) handles all persistent data and acts as the **Distributed Shuffle Service**.
*   **Direct VFS Shuffle**: Compute nodes are stateless. Map tasks push intermediate Arrow data directly into VFS `IngestionBuffer`s, strictly bounding memory usage and preventing OOMs. Reduce tasks pull this data via QUIC.

### 1.2 The Storage-to-Compute Pipeline
*   **Disk Reads (`io_uring` + eBPF)**: Workers read from local NVMe via `io_uring`. Data enters the kernel page cache where it is intercepted by a custom `vein_jit` eBPF program. Non-matching rows are dropped in the kernel; only matching rows are copied to user-space.
*   **Network Reads (AF_XDP + s2n-quic)**: VFS shuffle blocks are transferred over QUIC running on top of AF_XDP raw sockets. This provides hardware-level kernel bypass (zero-copy networking) with reliable stream reassembly.

### 1.3 The Execution Engine (Morsel-Driven)
*   **Type 4 Work-Stealing (Phase 1)**: The `WorkerPool` uses Tokio `LocalSet`s pinned to CPU cores. Idle threads steal `MorselTask`s (chunks of 100k rows) from busy threads.
*   **Dual-Pool Isolation**: A dedicated thread pool handles async I/O (`io_uring`/QUIC), while the CPU pool exclusively runs pure compute pipelines, ensuring I/O never blocks processing.
*   **Factorized Execution (Phase 3)**: Pipelines process `VeinBatch`es, which can be either flat `RecordBatch`es or compressed `FactorizedBatch`es. This eliminates intermediate result explosion during multi-table joins.

### 1.4 The Intelligent Scheduler & Optimizer
*   **Type 5 Hybrid Elastic Morsels (Phase 3)**: The Master tracks morsel completion rates. If it detects data skew (e.g., a VFS block is 40x larger than expected), it dynamically splits the in-flight morsel across more workers.
*   **Intra-Query Elasticity**: The Master can dynamically add new compute nodes to a running Stage if the stage is falling behind its SLA.
*   **RL-Based Join Optimization (Phase 4)**: The `planner` uses ADOPT (Reinforcement Learning) to find the optimal join attribute order at runtime by testing different paths and gathering feedback, eliminating the need for a brittle Cost-Based Optimizer.
*   **Confidence-Bounded Early Stop (Phase 4)**: The Master halts cluster execution the millisecond an aggregation hits 99.9% statistical confidence, returning answers in milliseconds.

---

## Part 2: Implementation Plan & Epics

### Epic 1: The Disaggregated Pipeline Foundation
*Laying the groundwork for a stateless, high-throughput engine.*

- [ ] **Task 1.1: Dual-Pool Worker Architecture**
  - Refactor `WorkerPool` in `crates/exec/src/daemon/worker.rs`.
  - Implement a dedicated Tokio multi-threaded runtime for I/O, and `LocalSet`s for CPU.
- [ ] **Task 1.2: Introduce `VeinBatch` Pipeline Abstraction**
  - Modify `crates/exec/src/daemon/pipeline.rs`.
  - Create `VeinBatch { Flat, Factorized }` enum. Update `PipelineNode` trait.
- [ ] **Task 1.3: Direct VFS Shuffle Implementation**
  - Update `crates/exec/src/operators/sink/` to include a `VfsSink`.
  - Ensure Map tasks flush `RecordBatch` data directly to VFS via QUIC.
- [ ] **Task 1.4: eBPF JIT Compiler (`vein_jit`)**
  - Create `crates/vein_jit`. Implement AST-to-eBPF bytecode translator for simple filters.
  - Wire XDP hook in `common/src/xdp.rs` to load the compiled bytecode.

### Epic 2: Adaptive Execution & Master Intelligence
*Making the cluster resilient to stragglers and node failures.*

- [ ] **Task 2.1: Execution Telemetry Feedback Loop**
  - Update `common::protocol::ExecuteTask` with feedback channels.
  - Workers stream `rows_processed` and `bytes_produced` back to Master.
- [ ] **Task 2.2: Intra-Query Elasticity (Scale-Out)**
  - Modify `StageDag` and `ExecutorRegistry` in `crates/master/`.
  - Add logic to assign newly joined cluster nodes to currently running Stages.
- [ ] **Task 2.3: NUMA-Aware Task Dispatching**
  - Update Master's dispatcher to prefer routing VFS read tasks to Compute nodes that physically reside in the same rack/zone as the VFS data block.

### Epic 3: Advanced Morsels & Factorization (The Kuzu Integration)
*Achieving state-of-the-art processing density and eliminating data skew.*

- [ ] **Task 3.1: Type 5 Hybrid Elastic Morsel Splitting**
  - Update Master `scheduler/dispatcher.rs`.
  - Implement skew detection: If a `MorselTask` telemetry shows massive output generation, cancel it, split the remaining byte range, and queue as micro-morsels.
- [ ] **Task 3.2: Factorized Batch Execution**
  - Implement symbolic factorized structures in `VeinBatch::Factorized`.
  - Update the `HashJoin` operator to output `Factorized` batches instead of materializing the cartesian product.
- [ ] **Task 3.3: VFS Compression for Factorized Data**
  - Update `vfs/src/ingest/` to natively store factorized structures without flattening them, saving 10x-50x disk space during shuffles.

### Epic 4: Next-Gen Optimization (The 2026 Frontier)
*Pushing Vein beyond the capabilities of commercial cloud data warehouses.*

- [ ] **Task 4.1: Worst-Case Optimal Join (WCOJ) Operator**
  - Implement `LeapfrogTrieJoin` in `crates/exec/src/operators/`.
  - Build Trie indexes over Arrow dictionary columns.
- [ ] **Task 4.2: ADOPT RL Join Optimizer**
  - Remove static heuristics from `crates/planner/src/optimizer/`.
  - Implement an RL agent that tests attribute orders in "episodes" during query execution, learning the fastest path automatically.
- [ ] **Task 4.3: Confidence-Bounded Early Stop**
  - Update the Master's Aggregation coordinator.
  - Compute statistical confidence intervals on incoming morsel streams. Inject a global cancellation token when 99.9% confidence is reached for exploratory queries.

---
**Status**: Ready for Execution. 
**Next Step**: Subagent implementation of Epic 1, Task 1.1.
