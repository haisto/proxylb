//! Backend pool management with health state tracking.
//!
//! Maintains an ordered list of SOCKS5h backends, their health status,
//! a ring-buffer of recent health check results, and cumulative traffic
//! counters updated atomically by the relay tasks.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use serde::Serialize;

/// Maximum number of health check history entries per backend.
pub const MAX_HISTORY: usize = 10;

/// A single health check result.
#[derive(Debug, Clone, Serialize)]
pub struct HealthCheckResult {
    pub timestamp: DateTime<Utc>,
    pub success: bool,
    pub latency_ms: Option<u64>,
    pub handshake_latency_ms: Option<u64>,
    pub error: Option<String>,
}

/// Current status of a backend.
#[derive(Debug, Clone)]
pub struct BackendStatus {
    pub healthy: bool,
    pub enabled: bool,
    pub last_check: Option<Instant>,
    pub last_latency: Option<Duration>,
    pub last_handshake_latency: Option<Duration>,
    pub consecutive_failures: u32,
    pub consecutive_successes: u32,
    pub history: VecDeque<HealthCheckResult>,
}

impl Default for BackendStatus {
    fn default() -> Self {
        Self {
            // Assume healthy initially — the first health check will confirm.
            healthy: true,
            enabled: true,
            last_check: None,
            last_latency: None,
            last_handshake_latency: None,
            consecutive_failures: 0,
            consecutive_successes: 0,
            history: VecDeque::with_capacity(MAX_HISTORY),
        }
    }
}
