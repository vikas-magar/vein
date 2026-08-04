# Vein Engine Architecture Design

**Date**: 2026-08-04
**Status**: Approved
**Context**: Re-architecting the distributed Vein data engine for cloud-native scale, prioritizing decoupled storage/compute and raw execution performance via eBPF.

## 1. Core Architectural Paradigm
Vein is a cloud-native, distributed OLAP data engine. It strictly adheres to a **Disaggregated Storage and Compute** architecture. 
- Compute nodes are completely stateless and do not hold persistent data.
- The **Virtual File System (VFS)** acts as the engine's distributed memory and persistent storage backbone.
- Data transfer over the network utilizes **QUIC** for multiplexed speed, while kernel-bypassing uses **AF_XDP**.

## 2. Component Design & Boundaries

### 2.1 The Execution Compute Layer (`crates/exec`, `crates/master`)
- **Master Node**: Orchestrates query Directed Acyclic Graphs (DAGs). It is responsible for partitioning work and assigning `MorselTask`s to idle worker nodes.
- **Worker Node (`WorkerPool`)**: Executes morsels using Tokio `LocalSet`s pinned to specific CPU cores (Cache-locality). 
- **Stateless Operation**: Workers do not hold massive memory buffers. They process data and immediately push it to the next boundary.

### 2.2 The Disaggregated Shuffle Layer (Direct VFS)
To solve the memory-bounding problem without Out-Of-Memory (OOM) crashes, Vein uses the VFS as a Remote Shuffle Service.
- **Map Phase**: When a Map task completes a chunk of work (e.g., filtering), it does NOT push to the Reducer. It writes the intermediate Arrow IPC chunk directly to a designated VFS path.
- **VFS Ingestion**: The VFS `IngestionBuffer` aggregates these chunks in memory up to 64MB, then flushes them to NVMe using `mmap_manager`. This guarantees strict memory limits on the cluster.
- **Reduce Phase**: Reduce tasks pull the 64MB blocks from VFS sequentially, ensuring fault-tolerant, memory-safe aggregations.

### 2.3 The eBPF JIT Compiler (`crates/vein_jit`)
To achieve zero-copy read speeds, query filters are pushed down into the Linux Kernel.
- **Trigger**: The `Planner` intercepts SQL `Filter` AST nodes (e.g., `WHERE age > 30`).
- **Translation**: The `vein_jit` module (a custom Rust compiler) translates the `Expr` AST directly into raw eBPF `u64` bytecode instructions. No heavy LLVM dependency is used.
- **Injection**: The Executor uses the `bpf()` syscall to attach this bytecode to the AF_XDP socket. 
- **Execution**: As packets arrive at the NIC, the eBPF program evaluates the filter directly on the raw Arrow IPC memory offsets. Only matching rows are passed to user-space.

## 3. Data Flow
1. **Query Submission**: User submits `SELECT x FROM t WHERE y > 10`.
2. **Planning**: `Planner` builds AST. Identifies `y > 10` for JIT pushdown.
3. **Compilation**: `vein_jit` generates eBPF bytecode for `y > 10`.
4. **Task Dispatch**: Master sends `ScanTask` (with eBPF payload) to Worker.
5. **Kernel Filtering**: Worker injects eBPF. NIC reads data, eBPF drops non-matching rows in kernel space.
6. **Shuffle**: Worker writes matching rows to VFS (Disaggregated Shuffle).
7. **Aggregation**: Reducer Worker pulls from VFS, aggregates, and returns to user.

## 4. Error Handling & Fault Tolerance
- **Compute Crashes**: If a Map or Reduce worker dies, the Master re-assigns the task. Because intermediate data is stored in VFS, no data is lost during Reducer crashes.
- **JIT Compilation Failures**: If `vein_jit` encounters an expression too complex for eBPF (e.g., exceeding the 512-byte stack limit), it gracefully falls back to generating a standard user-space Arrow compute filter.
- **VFS Quorum**: VFS uses Archipelago consensus. If a storage node goes offline, the fast-path quorum ensures writes and reads continue uninterrupted.

## 5. Testing Strategy
- **JIT Bytecode Verification**: Unit tests will compare `vein_jit` emitted bytecode against known-good `bpf` instruction outputs to ensure correctness before kernel injection.
- **Shuffle Integrity**: Integration tests will forcefully kill Reducer nodes mid-query to verify the Master can successfully recover the query by re-pulling from the VFS layer.
- **No `unwrap()`**: Strict enforcement of safe error propagation across all new components.
