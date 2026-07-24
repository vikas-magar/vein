# Multi-Queue AF_XDP (RSS) Architecture Implementation Plan

To break past the 1.2 Gbps single-core barrier, we need to implement **Receive Side Scaling (RSS)**. This allows the AWS ENA hardware to distribute UDP packets across all available CPU cores, and our software will process them in parallel.

## 1. Remove the Hardware Queue Limitation
Currently, we restrict the AWS ENA adapter to a single combined queue using a hardware `ioctl`:
```rust
ch.combined_count = 1; 
libc::ioctl(sock, 0x8946, &mut req);
```
**Action:** We will remove this restriction. When a `t4g.micro` boots, it naturally uses 2 queues. When deployed on a `c7g.16xlarge`, it will naturally use 64 queues.

## 2. Multi-Socket XDP Initialization
AF_XDP sockets are explicitly bound to a **single hardware queue**. Because we are re-enabling multiple queues, we can no longer use a single XDP socket.

**Action:** We will query the NIC for its active queue count, and then loop to create an independent `AF_XDP` socket, UMEM (memory region), and Ring Buffers (Rx/Tx/Fill/Completion) for **each** hardware queue.

## 3. Update the eBPF Program (XSKMAP)
Our current eBPF program redirects all port 443 traffic into a single socket at index 0 of the BPF map. If traffic hits a different queue, it gets dropped.

**Action:** We will modify the eBPF C program to read the queue index of the incoming packet (`ctx->rx_queue_index`) and redirect the packet to the corresponding AF_XDP socket in the `XSKMAP`.
```c
// Pseudo eBPF
u32 queue_id = ctx->rx_queue_index;
return bpf_redirect_map(&xsk_map, queue_id, 0);
```

## 4. Multi-Threaded I/O Polling
Currently, `s2n-quic` runs its event loop on a single Tokio task polling our single XDP socket.

**Action:** We will utilize Tokio's multi-threaded runtime. We will spawn a dedicated thread for each CPU core, pin the thread to that specific core using `core_affinity`, and have each thread poll its corresponding AF_XDP socket. This ensures that memory never crosses NUMA nodes or CPU caches, providing perfect linear scaling.

## Next Steps
Once you approve this plan, I will begin modifying `receiver/src/main.rs` to initialize multiple sockets and deploy a new multi-queue benchmark!
