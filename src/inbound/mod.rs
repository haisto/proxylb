pub mod http;
pub mod mtproto;
pub mod shadowsocks;
pub mod socks5;

use crate::outbound::TargetAddr;
use std::future::Future;
use std::sync::Arc;
use tokio::net::{TcpListener, UnixListener};
use tokio_util::sync::CancellationToken;

// ─── Unified listener abstraction ────────────────────────────────────────────

/// A bound listener that accepts either TCP or Unix-domain-socket connections.
pub enum BoundListener {
    Tcp(TcpListener),
    Unix(UnixListener),
}

impl BoundListener {
    /// Bind to `addr`. A `unix://…` prefix selects a UDS listener; anything
    /// else is treated as a TCP `host:port`.
    pub async fn bind(
        addr: &str,
        prebound_uds: Option<std::os::unix::net::UnixListener>,
    ) -> anyhow::Result<Self> {
        if let Some(uds) = prebound_uds {
            uds.set_nonblocking(true)?;
            return Ok(Self::Unix(UnixListener::from_std(uds)?));
        }

        if let Some(path) = addr.strip_prefix("unix://") {
            if let Some(parent) = std::path::Path::new(path).parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            if std::fs::metadata(path).is_ok() {
                let _ = std::fs::remove_file(path);
            }
            Ok(Self::Unix(UnixListener::bind(path)?))
        } else {
            let addr_parsed: std::net::SocketAddr = addr.parse()?;
            let socket = if addr_parsed.is_ipv6() {
                socket2::Socket::new(socket2::Domain::IPV6, socket2::Type::STREAM, None)?
            } else {
                socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::STREAM, None)?
            };

            socket.set_reuse_address(true)?;
            #[cfg(not(windows))]
            socket.set_reuse_port(true)?;

            socket.set_nonblocking(true)?;
            socket.bind(&addr_parsed.into())?;
            socket.listen(1024)?;

            let std_listener: std::net::TcpListener = socket.into();
            Ok(Self::Tcp(TcpListener::from_std(std_listener)?))
        }
    }

    /// Accept one connection and return the stream together with a display
    /// string for the remote address.
    pub async fn accept(&self) -> std::io::Result<(InboundStream, crate::stats::ClientId)> {
        match self {
            BoundListener::Tcp(l) => {
                let (s, addr) = l.accept().await?;
                let _ = s.set_nodelay(true);
                Ok((InboundStream::Tcp(s), crate::stats::ClientId::Ip(addr.ip())))
            }
            BoundListener::Unix(l) => {
                let (s, _addr) = l.accept().await?;
                Ok((InboundStream::Unix(s), crate::stats::ClientId::Unix))
            }
        }
    }
}

// ─── Unified inbound stream ───────────────────────────────────────────────────

/// A raw (pre-TLS) stream accepted from a `BoundListener`.
pub enum InboundStream {
    Tcp(tokio::net::TcpStream),
    Unix(tokio::net::UnixStream),
}

impl tokio::io::AsyncRead for InboundStream {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            InboundStream::Tcp(s) => std::pin::Pin::new(s).poll_read(cx, buf),
            InboundStream::Unix(s) => std::pin::Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl tokio::io::AsyncWrite for InboundStream {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match self.get_mut() {
            InboundStream::Tcp(s) => std::pin::Pin::new(s).poll_write(cx, buf),
            InboundStream::Unix(s) => std::pin::Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            InboundStream::Tcp(s) => std::pin::Pin::new(s).poll_flush(cx),
            InboundStream::Unix(s) => std::pin::Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            InboundStream::Tcp(s) => std::pin::Pin::new(s).poll_shutdown(cx),
            InboundStream::Unix(s) => std::pin::Pin::new(s).poll_shutdown(cx),
        }
    }
}

impl Unpin for InboundStream {}

#[cfg(target_os = "linux")]
impl crate::relay::AsRawStreamRef for InboundStream {
    fn as_raw_stream_ref(&self) -> Option<crate::relay::RawStreamRef<'_>> {
        match self {
            InboundStream::Tcp(s) => Some(crate::relay::RawStreamRef::Tcp(s)),
            InboundStream::Unix(s) => Some(crate::relay::RawStreamRef::Unix(s)),
        }
    }
}

// ─── Generic accept loop ──────────────────────────────────────────────────────

/// Drive `listener` until `cancel` fires, spawning `on_accept(stream, addr)`
/// for every incoming connection. The closure is called with a cloned context
/// each iteration (all captured `Arc`s are cheap to clone).
pub async fn run_accept_loop<F, Fut>(
    listener: BoundListener,
    cancel: CancellationToken,
    protocol: &'static str,
    on_accept: F,
) -> anyhow::Result<()>
where
    F: Fn(InboundStream, crate::stats::ClientId) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            res = listener.accept() => match res {
                Ok((stream, client_id)) => {
                    tokio::spawn(on_accept(stream, client_id));
                }
                Err(e) => { tracing::warn!(error = %e, "{protocol} accept error"); }
            }
        }
    }
    Ok(())
}

/// High-performance bidirectional relay with unified traffic counter tracking and clean stream shutdown.
///
/// After the relay completes, both streams are handed off to the fd-closer thread
/// (via [`crate::relay::defer_drop`]) so that `close(2)` / TCP teardown for both
/// the inbound client socket and the backend socket never runs on a tokio worker thread.
pub async fn relay_and_track<I>(
    mut inbound_stream: I,
    mut backend_stream: crate::outbound::BackendStream,
    traffic: Arc<crate::backend::TrafficCounters>,
    inbound_stats: Option<Arc<crate::backend::InboundStats>>,
    client_id: crate::stats::ClientId,
    target: &TargetAddr,
    protocol_name: &str,
    pool: &crate::backend::BackendPool,
) -> Result<(), anyhow::Error>
where
    I: tokio::io::AsyncRead
        + tokio::io::AsyncWrite
        + Unpin
        + crate::relay::AsRawStreamRef
        + Send
        + 'static,
{
    use std::sync::atomic::Ordering;
    use tokio::io::AsyncWriteExt;

    traffic.total_connections.fetch_add(1, Ordering::Relaxed);
    traffic.active_connections.fetch_add(1, Ordering::Relaxed);
    if let Some(ref stats) = inbound_stats {
        stats.total_connections.fetch_add(1, Ordering::Relaxed);
        stats.active_connections.fetch_add(1, Ordering::Relaxed);
    }

    let domain_stats = match target {
        TargetAddr::Domain(host, _) => Some(
            pool.domain_manager
                .get_or_create(crate::router::extract_parent_domain_str(host)),
        ),
        TargetAddr::Ip(_) => None,
    };
    let client_stats = pool.client_manager.get_or_create(&client_id);

    if let Some(ref ds) = domain_stats {
        ds.total_connections.fetch_add(1, Ordering::Relaxed);
    }
    client_stats
        .total_connections
        .fetch_add(1, Ordering::Relaxed);

    let mut up_counters = crate::relay::StackVec::<4>::new();
    up_counters.push(&traffic.bytes_up);
    if let Some(ref ds) = domain_stats {
        up_counters.push(&ds.tx_bytes);
    }
    up_counters.push(&client_stats.tx_bytes);

    let mut down_counters = crate::relay::StackVec::<4>::new();
    down_counters.push(&traffic.bytes_down);
    if let Some(ref ds) = domain_stats {
        down_counters.push(&ds.rx_bytes);
    }
    down_counters.push(&client_stats.rx_bytes);

    if let Some(ref stats) = inbound_stats {
        up_counters.push(&stats.tx_bytes);
        down_counters.push(&stats.rx_bytes);
    }

    match crate::relay::relay(
        &mut inbound_stream,
        &mut backend_stream,
        up_counters,
        down_counters,
    )
    .await
    {
        Ok((up, down)) => {
            tracing::debug!(
                target = %target,
                up_bytes = up,
                down_bytes = down,
                "{} relay complete",
                protocol_name
            );
            // Counters were already incrementally updated in real-time by the relay.
        }
        Err(e) => {
            tracing::debug!(
                target = %target,
                error = %e,
                "{} relay error",
                protocol_name
            );
        }
    }

    traffic.active_connections.fetch_sub(1, Ordering::Relaxed);
    if let Some(ref stats) = inbound_stats {
        stats.active_connections.fetch_sub(1, Ordering::Relaxed);
    }

    // Flush the write-halves so peers see a clean EOF before we release the FDs.
    let _ = inbound_stream.shutdown().await;
    let _ = backend_stream.shutdown().await;

    // Defer both close(2) calls to the fd-closer thread — off the worker runtime.
    crate::relay::defer_drop(inbound_stream);
    crate::relay::defer_drop(backend_stream);

    Ok(())
}

/// Helper function to check if a target address points to a private/loopback address.
#[allow(dead_code)]
pub fn is_private_target_sync(target: &crate::outbound::TargetAddr) -> bool {
    match target {
        crate::outbound::TargetAddr::Ip(addr) => is_private_ip(addr.ip()),
        crate::outbound::TargetAddr::Domain(host, _port) => {
            let host_lower = host.to_lowercase();
            host_lower == "localhost" || host_lower.ends_with(".local")
        }
    }
}

/// Helper function to check if an IpAddr is private or loopback/local.
#[allow(dead_code)]
fn is_private_ip(ip: std::net::IpAddr) -> bool {
    match ip {
        std::net::IpAddr::V4(ipv4) => {
            ipv4.is_loopback()
                || ipv4.is_private()
                || ipv4.is_link_local()
                || ipv4.is_unspecified()
                || ipv4.is_broadcast()
        }
        std::net::IpAddr::V6(ipv6) => {
            ipv6.is_loopback()
                || ipv6.is_unspecified()
                || is_ipv6_unique_local(&ipv6)
                || is_ipv6_link_local(&ipv6)
        }
    }
}

#[allow(dead_code)]
fn is_ipv6_unique_local(ipv6: &std::net::Ipv6Addr) -> bool {
    (ipv6.octets()[0] & 0xfe) == 0xfc
}

#[allow(dead_code)]
fn is_ipv6_link_local(ipv6: &std::net::Ipv6Addr) -> bool {
    (ipv6.octets()[0] == 0xfe) && ((ipv6.octets()[1] & 0xc0) == 0x80)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

    #[test]
    fn test_is_private_ip() {
        assert!(is_private_ip(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))));
        assert!(is_private_ip(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))));
        assert!(is_private_ip(IpAddr::V4(Ipv4Addr::new(172, 16, 0, 1))));
        assert!(is_private_ip(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1))));
        assert!(is_private_ip(IpAddr::V4(Ipv4Addr::new(169, 254, 1, 1))));
        assert!(is_private_ip(IpAddr::V6(Ipv6Addr::new(
            0, 0, 0, 0, 0, 0, 0, 1
        )))); // ::1
        assert!(is_private_ip(IpAddr::V6(Ipv6Addr::new(
            0xfc00, 0, 0, 0, 0, 0, 0, 1
        )))); // fc00::1

        assert!(!is_private_ip(IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8))));
        assert!(!is_private_ip(IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1))));
    }

    #[test]
    fn test_is_private_target() {
        let t1 = TargetAddr::Ip(SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 80));
        assert!(is_private_target_sync(&t1));

        let t2 = TargetAddr::Domain("localhost".to_string(), 80);
        assert!(is_private_target_sync(&t2));

        let t3 = TargetAddr::Domain("some-service.local".to_string(), 80);
        assert!(is_private_target_sync(&t3));

        let t4 = TargetAddr::Ip(SocketAddr::new(IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8)), 80));
        assert!(!is_private_target_sync(&t4));
    }
}
