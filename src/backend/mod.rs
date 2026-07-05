//! Backend pool management with health state tracking.
//!
//! Maintains an ordered list of SOCKS5h backends, their health status,
//! a ring-buffer of recent health check results, and cumulative traffic
//! counters updated atomically by the relay tasks.

pub mod health_status;
pub mod info;
pub mod pool;
pub mod stats;
pub use info::*;
pub use pool::*;
pub use stats::*;
