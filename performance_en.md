# ProxyLB Worker Core Logic Performance Analysis

This document analyzes the performance characteristics and optimizations implemented in the ProxyLB worker core logic. The design heavily prioritizes zero-copy data transfer, lock-free concurrency, and minimal heap allocations to support extreme concurrency workloads.

## 1. Thread Architecture & Core Affinity

ProxyLB employs a meticulously partitioned thread model to ensure the core data plane is never starved by control plane operations:

* **Current-Thread Tokio Runtimes:** Instead of a single multi-threaded Tokio runtime, the engine spawns multiple independent OS threads, each running a `tokio::runtime::Builder::new_current_thread()` event loop (see [`src/main.rs`](./src/main.rs)). This completely eliminates Tokio's work-stealing overhead and cross-thread synchronization on the hotpath.
* **Explicit Core Affinity:** Worker threads are pinned to specific CPU cores via `core_affinity`. This maximizes L1/L2 cache locality and reduces OS context-switching overhead (see [`src/main.rs`](./src/main.rs)).
* **Offloaded Ancillary Runtime:** Control plane tasks—such as background health checking, backend pool refilling, and metric aggregation—are executed on a dedicated `ancillary_runtime` thread.
* **Deferred FD Closing (`proxylb-fd-closer`):** TCP socket teardown and `close(2)` syscalls are offloaded to a dedicated OS thread via `defer_drop` using a lock-free `flume` channel. This ensures the worker event loop is never blocked by socket closure (see [`src/relay.rs`](./src/relay.rs)).

## 2. Lock-Free Routing & Scheduler Optimization

The routing logic ensures that selecting a backend candidate requires zero locks and zero heap allocations per connection:

* **Zero Compute Overhead:** The candidate selection logic executes in the worker core with virtually zero compute overhead. Complex operations like monitoring health and refilling pools are entirely decoupled, so the worker thread only performs simple array indexing or hash lookups to route traffic.
* **Thread-Local WRR Counters:** The Weighted Round Robin (WRR) scheduler leverages `std::cell::Cell<usize>` (`LOCAL_WRR_COUNTER`). By making the WRR index thread-local, the system avoids global atomic increment contention, eliminating cache line bouncing across CPU cores (see [`src/inbound/mod.rs`](./src/inbound/mod.rs)).
* **Accelerated Hashing:** Consistent hashing leverages `rustc_hash::FxHasher` (`fast-hash` feature). This non-cryptographic hash is significantly faster than the standard library's SipHash, enabling extremely fast mapping of target domains/IPs to hash ring nodes (see `hash_target_for_ch` in [`src/inbound/mod.rs`](./src/inbound/mod.rs)).
* **Lock-Free Routing Tables:** Global state changes (like config reloads or health status updates) are published using `arc_swap::ArcSwap`. Worker threads perform lock-free reads of the candidate lists, entirely bypassing `RwLock` read contention (see `BackendPool` in [`src/backend.rs`](./src/backend.rs)).

## 3. High-Performance Connection Pooling

* **Lock-Free Channel Extraction:** Pre-authenticated backend connections (e.g., SOCKS5, AnyTLS) are maintained in a `flume::bounded` channel (`pool_rx`). Worker threads simply pop ready-to-use streams from the channel, incurring near-zero latency (see `BackendEntry` in [`src/backend.rs`](./src/backend.rs)).
* **Background Refilling:** The `refill_pool_task` runs on the ancillary thread. Handshakes, DNS resolution, and authentication overhead never block or slow down the worker threads accepting client connections.

## 4. Zero-Copy & Zero-Allocation Relay

The relay loop is the most critical hotpath, heavily optimized to avoid data copying and memory allocation:

* **Zero-Copy `splice(2)` (Linux):** When relaying between two raw sockets (TCP or Unix Domain Sockets), ProxyLB utilizes the Linux `splice` syscall to move data directly in kernel space without copying it to user space (see `splice_bidirectional_with_pipes` in [`src/relay.rs`](./src/relay.rs)).
* **Preallocated 1MB Pipes:** To maximize `splice` throughput, the engine creates preallocated pipes and aggressively expands their capacity to 1MB using `fcntl` (`F_SETPIPE_SZ`). This reduces the frequency of `splice` syscalls (see `create_preallocated_pipes` in [`src/relay.rs`](./src/relay.rs)).
* **Lock-Free Buffer Pool (Fallback):** When `splice` cannot be used, a custom `LargeBidirectionalCopy` state machine is used. It draws 64KB transfer buffers from a lock-free `crossbeam_queue::ArrayQueue<Box<[u8]>>` (`BUFFER_POOL`). This prevents the system from doing two heap allocations per connection (see [`src/relay.rs`](./src/relay.rs)).
* **Stack-Allocated Counters (`StackVec`):** Traffic counter references are passed into the relay loop using a fixed-capacity, stack-allocated vector (`StackVec<'a, 4>`), dodging additional vector heap allocations (see [`src/relay.rs`](./src/relay.rs)).

## 5. Lock-Free Traffic Statistics

* **Relaxed Atomics:** `TrafficCounters` tracks bytes up/down and connection counts using `AtomicU64` and `AtomicI64`. These are incremented inside the relay loop using `Ordering::Relaxed`. 
* **Reference Sharing:** An `Arc<TrafficCounters>` is passed directly to the relay task, ensuring that live traffic updates do not require holding a global `RwLock` or `Mutex` over the backend pool (see [`src/backend.rs`](./src/backend.rs)).
