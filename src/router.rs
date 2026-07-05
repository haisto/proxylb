use crate::outbound::TargetAddr;
use std::sync::Arc;

thread_local! {
    pub(crate) static LOCAL_WRR_COUNTER: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Consolidates high-performance routing and load-balanced/failover backend connection establishment.
///
/// When `route` is `Some`, uses route-specific candidates (bound to a group or backend).
/// When `route` is `None`, uses the global failover order.

/// Extract the parent domain (e.g. "example.com" from "sub.example.com") without allocation.
/// Returns a `&str` slice into the input — zero heap allocation.
pub fn extract_parent_domain_str(host: &str) -> &str {
    let bytes = host.as_bytes();
    let mut dot_count = 0u8;
    for i in (0..bytes.len()).rev() {
        if bytes[i] == b'.' {
            dot_count += 1;
            if dot_count == 2 {
                return &host[i + 1..];
            }
        }
    }
    host // single-label or two-label domain — return as-is
}

/// Hash the routing key for consistent-hashing without heap allocation.
fn hash_target_for_ch(target: &TargetAddr) -> u64 {
    use std::hash::{Hash, Hasher};
    #[cfg(feature = "fast-hash")]
    let mut hasher = rustc_hash::FxHasher::default();
    #[cfg(not(feature = "fast-hash"))]
    let mut hasher = std::collections::hash_map::DefaultHasher::new();

    match target {
        TargetAddr::Ip(addr) => addr.ip().hash(&mut hasher),
        TargetAddr::Domain(host, _) => extract_parent_domain_str(host).hash(&mut hasher),
    }
    hasher.finish()
}

/// Compute the rotation start position for a subgroup (0 = no rotation).
fn compute_start(sg: &crate::scheduler::SubGroupCache, target: &TargetAddr) -> usize {
    let len = sg.healthy.len();
    if len == 0 {
        return 0;
    }

    match sg.strategy {
        crate::config::GroupStrategy::ConsistentHashing if !sg.hash_ring.is_empty() => {
            let h = hash_target_for_ch(target);
            let idx = match sg.hash_ring.binary_search_by_key(&h, |(k, _)| *k) {
                Ok(i) => i,
                Err(i) => {
                    if i == sg.hash_ring.len() {
                        0
                    } else {
                        i
                    }
                }
            };
            sg.hash_ring[idx].1 % len
        }
        crate::config::GroupStrategy::WeightedRoundRobin if !sg.wrr_choices.is_empty() => {
            let count = LOCAL_WRR_COUNTER.with(|c| {
                let v = c.get();
                c.set(v.wrapping_add(1));
                v
            });
            sg.wrr_choices[count % sg.wrr_choices.len()]
        }
        _ => 0,
    }
}

const MAX_INLINE_SUBGROUPS: usize = 16;

/// Zero-allocation iterator over candidates. Borrows from `CachedCandidates`
/// and yields `&(backend_idx, weight, Arc<BackendInfo>)` references — no
/// `Arc::clone()` per connection.
pub struct CandidateIter<'a> {
    subgroups: &'a [crate::scheduler::SubGroupCache],
    starts: [usize; MAX_INLINE_SUBGROUPS],
    sg_idx: usize,
    offset: usize,
}

impl<'a> CandidateIter<'a> {
    pub fn new(candidates: &'a crate::scheduler::CachedCandidates, target: &TargetAddr) -> Self {
        let count = candidates.subgroups.len().min(MAX_INLINE_SUBGROUPS);
        let mut starts = [0usize; MAX_INLINE_SUBGROUPS];
        for i in 0..count {
            starts[i] = compute_start(&candidates.subgroups[i], target);
        }
        CandidateIter {
            subgroups: &candidates.subgroups[..count],
            starts,
            sg_idx: 0,
            offset: 0,
        }
    }
}

impl<'a> Iterator for CandidateIter<'a> {
    type Item = &'a (usize, u32, Arc<crate::backend::BackendInfo>);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.sg_idx >= self.subgroups.len() {
                return None;
            }
            let sg = &self.subgroups[self.sg_idx];
            let len = sg.healthy.len();
            if len == 0 || self.offset >= len {
                self.sg_idx += 1;
                self.offset = 0;
                continue;
            }
            let pos = (self.starts[self.sg_idx] + self.offset) % len;
            self.offset += 1;
            return Some(&sg.healthy[pos]);
        }
    }
}

pub async fn route_and_connect(
    pool: &crate::backend::BackendPool,
    target: &TargetAddr,
    route_idx: Option<usize>,
) -> Result<
    (
        crate::outbound::BackendStream,
        Arc<crate::backend::TrafficCounters>,
    ),
    anyhow::Error,
> {
    use crate::outbound::BackendStream;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    let backend_timeout = Duration::from_secs(10);

    // Use route-specific candidates if a route is bound, otherwise global.
    let candidates = pool.get_route_candidates(route_idx);
    let unhealthy = &candidates.unhealthy;

    let mut backend_stream: Option<crate::outbound::BackendStream> = None;
    let mut chosen_traffic: Option<Arc<crate::backend::TrafficCounters>> = None;

    let iter = CandidateIter::new(&candidates, target);

    // First pass: try healthy backends.
    for (index, _, info) in iter {
        // Lock-free cache lookup: yields both the pooled stream (if any) and the traffic Arc.
        let pc = pool.get_pooled_connection(*index);
        let (pool_stream, traffic) = match pc {
            Some(pc) => (pc.stream, Some(pc.traffic)),
            None => (None, None), // OOB — never happens
        };

        let mut try_fresh = true;
        let mut conn_res: std::io::Result<BackendStream> =
            Err(std::io::Error::new(std::io::ErrorKind::Other, "no pool"));

        if let Some(stream) = pool_stream {
            match info.connect_target_pooled(stream, target).await {
                Ok(s) => {
                    tracing::debug!(backend = %info.name, "using pooled connection");
                    if let Some(ref tc) = traffic {
                        tc.pool_hits.fetch_add(1, Ordering::Relaxed);
                    }
                    try_fresh = false;
                    conn_res = Ok(s);
                }
                Err(e) => {
                    tracing::debug!(
                        backend = %info.name,
                        error = %e,
                        "pooled connection was stale, retrying with fresh connection"
                    );
                    if let Some(ref tc) = traffic {
                        tc.pool_stale.fetch_add(1, Ordering::Relaxed);
                    }
                    // try_fresh remains true
                }
            }
        } else if !info.is_direct() {
            if let Some(ref tc) = traffic {
                tc.pool_misses.fetch_add(1, Ordering::Relaxed);
            }
        }

        if try_fresh {
            conn_res = info.connect_target_fresh(target, backend_timeout).await;
        }

        match conn_res {
            Ok(stream) => {
                tracing::debug!(backend = %info.name, target = %target, "connected through backend");
                backend_stream = Some(stream);
                chosen_traffic = traffic;
                break;
            }
            Err(e) => {
                tracing::debug!(backend = %info.name, error = %e, "backend connect failed, trying next");
                pool.mark_unhealthy(*index, &format!("connect failed: {}", e));
            }
        }
    }

    // Second pass: try unhealthy backends as last resort.
    if backend_stream.is_none() {
        for (index, _, info) in unhealthy {
            let result = info.connect_target_fresh(target, backend_timeout).await;

            if let Ok(stream) = result {
                tracing::debug!(backend = %info.name, target = %target, "connected through unhealthy backend (fallback)");
                backend_stream = Some(stream);
                chosen_traffic = pool.get_traffic_counters(*index);
                break;
            }
        }
    }

    if let (Some(stream), Some(traffic)) = (backend_stream, chosen_traffic) {
        Ok((stream, traffic))
    } else {
        anyhow::bail!("all backends failed for {}", target)
    }
}

pub async fn route_and_connect_udp(
    pool: &crate::backend::BackendPool,
    target: &TargetAddr,
    route_idx: Option<usize>,
) -> Result<
    (
        crate::udp::UdpBackendSession,
        std::sync::Arc<crate::backend::TrafficCounters>,
    ),
    anyhow::Error,
> {
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    let backend_timeout = Duration::from_secs(10);
    let candidates = pool.get_route_candidates(route_idx);
    let iter = CandidateIter::new(&candidates, target);

    for (index, _, info) in iter {
        if !info.udp_enabled {
            continue;
        }

        let traffic = pool.get_traffic_counters(*index).unwrap_or_default();

        if info.is_direct() {
            if let Some(ref tc) = pool.get_traffic_counters(*index) {
                tc.pool_misses.fetch_add(1, Ordering::Relaxed);
            }
            if let Ok(socket) = crate::udp::create_tuned_udp_socket() {
                return Ok((crate::udp::UdpBackendSession::Direct { socket }, traffic));
            }
        } else if info.is_shadowsocks() {
            let ss_cfg = info.ss_config.as_ref().unwrap();
            let ss_ctx = info.ss_context.as_ref().unwrap().clone();
            if let Ok(socket) =
                ::shadowsocks::relay::udprelay::ProxySocket::connect(ss_ctx, ss_cfg.as_ref()).await
            {
                let server_addr = match ss_cfg.addr() {
                    ::shadowsocks::config::ServerAddr::SocketAddr(sa) => *sa,
                    ::shadowsocks::config::ServerAddr::DomainName(domain, port) => {
                        if let Ok(mut addrs) =
                            tokio::net::lookup_host((domain.as_str(), *port)).await
                        {
                            if let Some(addr) = addrs.next() {
                                addr
                            } else {
                                continue;
                            }
                        } else {
                            continue;
                        }
                    }
                };
                return Ok((
                    crate::udp::UdpBackendSession::Shadowsocks {
                        socket,
                        server_addr,
                    },
                    traffic,
                ));
            }
        } else {
            // SOCKS5
            if let Ok(tcp_stream) = crate::outbound::connect_endpoint(info, backend_timeout).await {
                if let Ok(tcp_stream) =
                    crate::outbound::socks5::socks5h_authenticate(tcp_stream, info).await
                {
                    if let Ok((tcp_stream, backend_relay_addr)) =
                        crate::outbound::socks5::socks5h_udp_associate(tcp_stream).await
                    {
                        if let Ok(socket) = crate::udp::create_tuned_udp_socket() {
                            return Ok((
                                crate::udp::UdpBackendSession::Socks5 {
                                    socket,
                                    backend_relay_addr,
                                    _tcp: tokio::sync::Mutex::new(
                                        crate::outbound::BackendStream::boxed(Box::pin(tcp_stream)),
                                    ),
                                },
                                traffic,
                            ));
                        }
                    }
                }
            }
        }
    }

    anyhow::bail!("all UDP backends failed for {}", target)
}
