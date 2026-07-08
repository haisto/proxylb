//! Backend pool management with health state tracking.
//!
//! Maintains an ordered list of SOCKS5h backends, their health status,
//! a ring-buffer of recent health check results, and cumulative traffic
//! counters updated atomically by the relay tasks.

use std::sync::atomic::{AtomicI64, AtomicU64};

use serde::Serialize;

/// Lock-free per-backend traffic counters.
///
/// Stored behind an `Arc` so inbound relay tasks can hold a clone without
/// acquiring the pool's `RwLock` on every byte count update.
#[derive(Debug, Default)]
pub struct TrafficCounters {
    /// Bytes relayed from client → backend (cumulative).
    pub bytes_up: AtomicU64,
    /// Bytes relayed from backend → client (cumulative).
    pub bytes_down: AtomicU64,
    /// Currently active relay sessions (signed so a race never wraps).
    pub active_connections: AtomicI64,
    /// Total connections ever accepted through this backend.
    pub total_connections: AtomicU64,
    // --- Connection pool stats ---
    /// Worker grabbed a pooled connection and it succeeded.
    pub pool_hits: AtomicU64,
    /// Pool was empty; worker fell back to a fresh on-demand connection.
    pub pool_misses: AtomicU64,
    /// Pooled connection was stale; worker transparently retried with a fresh one.
    pub pool_stale: AtomicU64,
}

pub struct InboundStats {
    pub name: String,
    pub listen: String,
    pub inbound_type: String,
    pub total_connections: AtomicU64,
    pub active_connections: AtomicI64,
    pub tx_bytes: AtomicU64,
    pub rx_bytes: AtomicU64,
}

#[derive(Debug, Clone, Serialize)]
pub struct InboundStatsView {
    pub name: String,
    pub listen: String,
    pub inbound_type: String,
    pub total_connections: u64,
    pub active_connections: i64,
    pub tx_bytes: u64,
    pub rx_bytes: u64,
}
