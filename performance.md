# ProxyLB Worker Core Logic 性能分析

本文档分析了 ProxyLB Worker 核心逻辑中实现的性能特性和优化方案。系统的整体设计高度侧重于零拷贝（Zero-Copy）数据传输、无锁（Lock-Free）并发以及最小化堆内存分配，从而完美支持极限并发负载。

## 1. 线程架构与 CPU 核心绑定 (Core Affinity)

ProxyLB 采用了精心划分的线程模型，确保核心数据面（Data Plane）永远不会因为控制面（Control Plane）的操作而发生饥饿：

* **Current-Thread Tokio 运行时:** 引擎没有使用单一的多线程 Tokio 运行时，而是生成了多个独立的操作系统线程，每个线程独立运行一个 `tokio::runtime::Builder::new_current_thread()` 事件循环（参考 [`src/main.rs`](./src/main.rs)）。这彻底消除了 Tokio 工作窃取（Work-Stealing）带来的开销，以及热路径（Hotpath）上的跨线程同步消耗。
* **显式的核心绑定 (Core Affinity):** Worker 线程通过 `core_affinity` 被直接绑定到特定的 CPU 核心上。这最大化了 L1/L2 缓存的局部性，并降低了操作系统的上下文切换开销（参考 [`src/main.rs`](./src/main.rs)）。
* **分离的辅助运行时 (Ancillary Runtime):** 控制面任务（如后台健康检查、后端连接池填充、指标聚合等）均在专门的 `ancillary_runtime` 线程上执行。
* **延迟的 FD 关闭 (`proxylb-fd-closer`):** TCP 套接字的销毁和 `close(2)` 系统调用通过无锁的 `flume` 通道和 `defer_drop` 机制被卸载到专门的 OS 线程中。这确保了 Worker 事件循环绝不会被套接字关闭过程阻塞（参考 [`src/relay.rs`](./src/relay.rs)）。

## 2. 无锁路由与调度器优化

路由逻辑确保在每次连接选择后端候选者时，实现“零锁竞争”和“零堆内存分配”：

* **零计算开销 (Zero Compute Overhead):** 候选者选择逻辑在 Worker 核心中执行时几乎没有任何计算开销。监控健康状态和填充连接池的繁重工作已被完全解耦，因此 Worker 线程只需执行简单的数组索引或哈希查找即可完成流量路由。
* **线程局部的 WRR 计数器:** 加权轮询 (WRR, Weighted Round Robin) 调度器使用了 `std::cell::Cell<usize>` (`LOCAL_WRR_COUNTER`)。通过将 WRR 索引设置为线程局部变量，系统避免了全局原子递增带来的竞争，消除了跨 CPU 核心的缓存行颠簸（Cache Line Bouncing）（参考 [`src/inbound/mod.rs`](./src/inbound/mod.rs)）。
* **加速哈希算法:** 一致性哈希采用了 `rustc_hash::FxHasher`（`fast-hash` feature）。这种非加密哈希算法比标准库中的 SipHash 快得多，能够将目标域名/IP 以极快的速度映射到哈希环节点上（参考 [`src/inbound/mod.rs`](./src/inbound/mod.rs) 中的 `hash_target_for_ch`）。
* **无锁路由表:** 全局状态的更改（如配置重载或健康状态更新）使用 `arc_swap::ArcSwap` 进行发布。Worker 线程可以对候选者列表进行无锁读取，完全避开了 `RwLock` 的读取竞争（参考 [`src/backend.rs`](./src/backend.rs) 中的 `BackendPool`）。

## 3. 高性能连接池

* **无锁通道提取:** 预认证的后端连接（如 SOCKS5、AnyTLS）保存在 `flume::bounded` 队列 (`pool_rx`) 中。Worker 线程只需从队列中弹出即用型流，几乎实现零延迟（参考 [`src/backend.rs`](./src/backend.rs) 中的 `BackendEntry`）。
* **后台填充 (Background Refilling):** `refill_pool_task` 运行在辅助线程上。握手、DNS 解析和身份验证的开销永远不会阻塞或减慢 Worker 线程接受客户端连接的速度。

## 4. 零拷贝 (Zero-Copy) 与零分配数据中继

中继 (Relay) 循环是系统最关键的热路径，经过了重度优化以避免数据复制和内存分配：

* **Linux 零拷贝 `splice(2)`:** 在两个原始套接字（TCP 或 Unix 域套接字 UDS）之间中继时，ProxyLB 利用 Linux 的 `splice` 系统调用直接在内核空间移动数据，无需将其复制到用户空间（参考 [`src/relay.rs`](./src/relay.rs) 中的 `splice_bidirectional_with_pipes`）。
* **预分配 1MB 管道:** 为了最大化 `splice` 的吞吐量，引擎使用 `fcntl` (`F_SETPIPE_SZ`) 创建并积极扩展预分配管道的容量至 1MB。这显著降低了 `splice` 系统调用的触发频率（参考 [`src/relay.rs`](./src/relay.rs) 中的 `create_preallocated_pipes`）。
* **无锁缓冲池 (回退机制):** 当无法使用 `splice` 时，系统将使用自定义的 `LargeBidirectionalCopy` 状态机。它会从无锁的 `crossbeam_queue::ArrayQueue<Box<[u8]>>` (`BUFFER_POOL`) 中提取 64KB 的传输缓冲区。这防止了系统在每个连接上产生两次堆分配（参考 [`src/relay.rs`](./src/relay.rs)）。
* **栈分配的计数器 (`StackVec`):** 流量计数器的引用通过固定容量的栈分配向量 (`StackVec<'a, 4>`) 传递到中继循环中，彻底避免了 Vector 带来的额外堆分配（参考 [`src/relay.rs`](./src/relay.rs)）。

## 5. 无锁流量统计

* **松散的原子操作 (Relaxed Atomics):** `TrafficCounters` 使用 `AtomicU64` 和 `AtomicI64` 追踪上下行字节数及连接数。在中继循环内部，它们通过 `Ordering::Relaxed` 进行增量操作，性能极高。
* **引用共享:** `Arc<TrafficCounters>` 被直接传递给中继任务，确保实时的流量更新不需要对后端池持有任何全局的 `RwLock` 或 `Mutex`（参考 [`src/backend.rs`](./src/backend.rs)）。
