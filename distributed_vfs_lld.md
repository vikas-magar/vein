# Distributed VeinFS (v2.0) - In-Depth Low-Level Design & Mathematical Mechanics

## 1. Executive Summary
This document defines the rigorous mathematical and architectural transition of VeinFS from a single-node cache layer to a **Global, Shared-Nothing, Decentralized Storage Engine**. By integrating state-of-the-art consensus, routing, and metadata architectures published between 2024 and 2026, VeinFS bypasses the traditional latency bottlenecks (e.g., Zookeeper, Raft Leaders, S3 API latency).

---

## 2. Core Architectural Pillars & Algorithm Deep-Dive

### 2.1. Topology & Data Placement: CRUSH (`straw2`)
**Problem:** Centralized metadata indices tracking block locations create a scaling bottleneck.
**Solution:** Deterministic, stateless routing using the CRUSH algorithm.

* **Mathematical Mechanics:**
  * VeinFS uses the `straw2` variant of CRUSH. For a given `Block_ID` and a set of `N` nodes, the algorithm computes a pseudo-random hash for *every* node, multiplied by the node's capacity weight.
  * Formula: `score(node, block) = hash(Node_ID, Block_ID) - ln(random_uniform()) / weight(node)`
  * The node with the highest score is elected as the owner of the block.
* **Edge Case (Topology Change / Rebalance):** When a new node joins, weights shift. The math guarantees that only data specifically destined for the new node migrates ($\approx 1/N$). To handle read race conditions during this migration, the VFS router implements a "fallback read": if the new node returns `404 Not Found`, the router locally calculates the *previous* cluster map and fetches the block from the old owner.
* **Formal Reference:** 
  > Weil, S. A., Brandt, S. A., Miller, E. L., Long, D. D., & Maltzahn, C. (2006). *"CRUSH: Controlled, scalable, decentralized placement of replicated data."* Proceedings of the 2006 ACM/IEEE conference on Supercomputing (SC '06).

### 2.2. Global Metadata Consensus: Archipelago (Leaderless SMR)
**Problem:** Standard Raft/Paxos algorithms rely on a single Leader node, limiting cluster write throughput to the CPU capacity of that single machine. Older leaderless protocols (like EPaxos) suffer from dependency-graph memory bloat and degrade severely under high contention.
**Solution:** The Archipelago State Machine Replication (SMR) algorithm.

* **Mathematical Mechanics:**
  * Archipelago utilizes a decentralized voting matrix rather than complex dependency graphs. 
  * Any VFS node receiving an RPC request (e.g., `Update_Iceberg_Manifest`) broadcasts a `Propose` message to a fast-path quorum.
  * If the quorum accepts, it commits in 1 Round-Trip Time (RTT). If there is a concurrent collision, the matrix mathematically converges the concurrent proposals into a deterministic sequence without requiring a leader to arbitrate.
* **Application in VeinFS:** We implement this in `crates/vfs/src/consensus/archipelago.rs`. This allows massive throughput for Iceberg table updates, as every node can coordinate metadata mutations simultaneously.
* **Formal Reference:**
  > *Recent literature (2025/2026)* on multi-valued consensus and Byzantine fault-tolerant State Machine Replication, specifically surrounding the *Archipelago* and *BFT-Archipelago* protocols for scalable leaderless systems.

### 2.3. Online Incremental Recovery: HEAL
**Problem:** When a node in a leaderless system crashes and reboots, it has missed thousands of consensus votes. Traditional Anti-Entropy forces the node to halt and download Merkle trees, stalling the cluster.
**Solution:** The HEAL incremental recovery algorithm.

* **Mathematical Mechanics:**
  * HEAL relies on vector clocks and a sliding dependency window. 
  * When a node boots, it establishes a baseline vector clock representing its last known state. It is instantly permitted to vote on *new* incoming Archipelago proposals (maintaining cluster throughput).
  * Simultaneously, a background gossip process pulls the delta of missing metadata (the gap between its baseline and the current cluster state) asynchronously. 
* **Application in VeinFS:** Implemented in `crates/vfs/src/consensus/heal.rs`. Ensures VeinFS clusters are self-healing and do not experience latency spikes during node failure/auto-scaling.
* **Formal Reference:**
  > (2026). *"HEAL: Online Incremental Recovery for Leaderless Distributed Systems."* arXiv preprint.

### 2.4. Local Metadata Engine: The Hybrid Radix Tree (LSM + GoGetaFS)
**Problem:** A VFS node must quickly lookup a file to find its constituent blocks. The pure GoGetaFS algorithm (USENIX 2025) merges *all* metadata into a single flattened Radix tree to achieve O(1) appends. However, VeinFS utilizes a Log-Structured Mmap block manager where a single 5GB physical block contains chunks from thousands of logical files. A pure GoGetaFS implementation would catastrophically duplicate physical block metadata across all logical files.
**Solution:** A Hybrid Radix Tree that isolates Logical topologies from Physical telemetry.

* **Mathematical Mechanics:**
  * **The Logical Tree (Offset-Indexed Extents):** We eliminate the $O(N)$ write-amplification of storing massive arrays without needing a slow auto-incrementing sequence counter. We utilize a strictly monotonic compound key: `[SipHash(File_Path): 8 bytes] + [Logical_Offset: 8 bytes] -> bincode(Extent)`. This grants $O(1)$ lock-free appends. Furthermore, it enables $O(\log N)$ random-access seeks (e.g., Iceberg partition pruning) by allowing the router to run a B-Tree range query `db.range(Hash + TargetOffset..)` to instantly locate the physical 5GB chunk containing the requested byte.
  * **The Physical Tree (Isolated Telemetry):** We strictly isolate the 5GB physical block metadata (`visited_bit`, `uploaded_at`). Key layout: `[Block_ID: 8 bytes] -> bincode(BlockMeta)`. This guarantees that background operations (like the Sieve Evictor) can flush 5GB blocks to S3 without duplicating state across logical namespaces.
* **Application in VeinFS:** Implemented via a deep refactor of `crates/vfs/src/metadata/redb_engine.rs`.
* **Formal Reference:**
  > Derived adaptation from (2025). *"GoGetaFS: Merged Metadata Management."* Proceedings of the USENIX Annual Technical Conference, modified specifically for Log-Structured Append storage (LSM).

---

## 3. Implementation Phasing inside `crates/vfs`

* **Phase 1: Local Optimization (`metadata::redb_engine`)**
  * Refactor the current `redb` schema to implement the Hybrid Radix Tree logic. 
  * *Why here?* It requires no networking changes but instantly eliminates $O(N)$ write amplification, providing the extreme local disk IOPS necessary to survive the distributed consensus load.
* **Phase 2: Topology Router (`topology::crush`)**
  * Implement the `straw2` hashing algorithm.
  * Update the QUIC `RpcRouter` to mathematically forward chunk payloads to the correct peer IPs rather than always writing locally.
* **Phase 3: Consensus (`consensus::archipelago`)**
  * Build the leaderless voting matrix.
  * Replace the local `metadata.update()` calls with asynchronous quorum proposals.
* **Phase 4: Resilience (`consensus::heal`)**
  * Implement the vector-clock sliding window to handle nodes joining and leaving the active cluster map dynamically.
