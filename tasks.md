# Vein Database: Production Implementation Task List

This document contains a minute, highly detailed breakdown of every technical task required to upgrade Vein from an architectural skeleton into a robust, highly extensible, production-grade distributed database. (Note: eBPF pushdown is explicitly excluded from this list for now).

## 1. Networking & Transport (QUIC / RPC Layer)
The engine's structural bounds exist, but the actual daemon loops and serializers must be built.
*   **[ ] 1.1 Master QUIC Daemon Loop:** Implement the async `quinn::Endpoint` listener in `crates/server/src/main.rs` to continuously accept incoming connections from cluster nodes and clients.
*   **[ ] 1.2 Executor QUIC Daemon Loop:** Implement the counterpart listener on the Executor nodes to receive task dispatches.
*   **[ ] 1.3 Node Registration Handshake:** Build the protocol where Executors connect to the Master, authenticate, and are assigned a dynamic `NodeId` rather than hardcoding `1`.
*   **[ ] 1.4 QuicTaskDispatcher Implementation:** Implement the `TaskDispatcher` trait in `crates/master` to serialize `MorselTask`s and push them over the network to the assigned Executor.
*   **[ ] 1.5 Heartbeat & Telemetry Expansion:** Upgrade `NodeLoad` payloads to include fine-grained metrics (Disk I/O, Network I/O queue depth, CPU steal time) for the Elasticity Controller.

## 2. Query Parsing, Planning & Protocol (Frontend Layer)
The parser is custom, but it must be connected to the physical execution engine via strong types.
*   **[ ] 2.1 Strong Typed Protocol (AST):** Refactor `common::protocol::Operator::Filter` and `Project` to accept a serialized AST enum (e.g., via `bincode`) instead of a raw `String`.
*   **[ ] 2.2 Strong Typed Protocol (Schema):** Refactor `FieldSchema` to use native Apache Arrow `DataType` enums instead of `data_type: String`.
*   **[ ] 2.3 Logical Binder Completion:** Finish the `crates/planner` binder to handle all custom pipe-syntax operators (joins, multi-aggregations).
*   **[ ] 2.4 Physical DAG Translation:** Implement `SqlPlanner::plan` in `crates/master` to lower the `LogicalPlan` into a `StageDag` of physical tasks.
*   **[ ] 2.5 Dynamic Task Partitioning:** Update the Scheduler so that instead of dispatching 1 task per Stage, it dynamically calculates partition sizes and scatters `N` tasks across the cluster.

## 3. The Execution Engine (Data Plane)
The lock-free multithreading works, but the physical operators need production data-handling logic.
*   **[ ] 3.1 Arrow IPC Serialization:** Implement `arrow::ipc::writer::FileWriter` inside `VfsSink` and `NetworkSink` to seamlessly flatten `FactorizedChunk`s into zero-copy byte arrays.
*   **[ ] 3.2 Arrow IPC Deserialization:** Implement the reverse logic for the QUIC stream listener to feed byte streams directly into operator memory.
*   **[ ] 3.3 NetworkSink & NetworkSource:** Implement the operators required for intermediate Data Shuffles (e.g., sending data between executors for a distributed Group-By).
*   **[ ] 3.4 Physical Expression Evaluator:** Implement the logic inside the Executor to parse the incoming binary AST from the Master and dynamically construct Arrow compute kernels.
*   **[ ] 3.5 Hash Join Build Operator:** Implement `HashJoinBuild` to populate the `FactorizedTable` (currently only `HashJoinProbe` exists).
*   **[ ] 3.6 Distributed Aggregation:** Implement local and global aggregation operators (`Sum`, `Count`, `GroupBy`) utilizing partitioned, lock-free Hash Tables.
*   **[ ] 3.7 Graceful Task Cancellation:** Wire up the `CancellationToken` in the Executor control plane so the Master can instantly abort running pipelines.

## 4. The Storage Engine (VFS & Archipelago)
The Leaderless SMR architecture is brilliant, but it requires strict durability and conflict resolution.
*   **[ ] 4.1 Write-Ahead Log (WAL):** Implement a WAL for `ingest/buffer.rs` to guarantee that the 64MB memory buffer survives a kernel panic or power loss.
*   **[ ] 4.2 Durable Disk Syncing:** Inject `fsync()` / `msync()` calls (or use `O_DIRECT`) after critical block appends in `mmap_manager.rs` to bypass the OS page cache safely.
*   **[ ] 4.3 Redb Block Metadata Tracking:** Persist the `current_offset` of active `.bin` files into Redb so that upon crash recovery, unsealed blocks can be safely truncated without corruption.
*   **[ ] 4.4 SMR Conflict Resolution:** Upgrade the Leaderless `ArchipelagoState` quorum algorithm to include vector clocks or dependency graphs (like EPaxos) to deterministically resolve simultaneous write collisions.
*   **[ ] 4.5 True Merkle Radix Tree:** Replace the flat `Vec` of hashes with a real Merkle Radix tree for O(log N) cluster comparisons.
*   **[ ] 4.6 Anti-Entropy Healing Protocol:** Replace the `sleep(5)` loop in `HealDaemon` with the actual background P2P RPC logic that compares Merkle roots and fetches missing block extents from peers.
