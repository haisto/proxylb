//! Backend pool management with health state tracking.
//!
//! Maintains an ordered list of SOCKS5h backends, their health status,
//! a ring-buffer of recent health check results, and cumulative traffic
//! counters updated atomically by the relay tasks.

use std::sync::Arc;

use shadowsocks::config::ServerConfig as SsServerConfig;
use shadowsocks::context::{Context as SsContext, SharedContext};

use crate::config::BackendConfig;
use tokio_rustls::TlsConnector;

/// How the load balancer connects to a SOCKS5 backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackendEndpoint {
    /// Classic TCP connection to `host:port`.
    Tcp { host: String, port: u16 },
    /// Unix domain socket at the given filesystem path.
    Unix { path: String },
    /// Direct connection outbound.
    Direct,
}

impl BackendEndpoint {
    /// Human-readable address string (used in the web dashboard).
    pub fn display(&self) -> String {
        match self {
            BackendEndpoint::Tcp { host, port } => format!("{}:{}", host, port),
            BackendEndpoint::Unix { path } => format!("unix:{}", path),
            BackendEndpoint::Direct => "direct".to_string(),
        }
    }
}

/// Parsed backend information.
#[derive(Clone)]
pub struct BackendInfo {
    pub name: String,
    pub endpoint: BackendEndpoint,
    pub username: Option<String>,
    pub password: Option<String>,
    pub pool_size: usize,
    /// Set when this backend is a Shadowsocks server.
    /// Contains the pre-derived key material (cheap to clone — inner is `Arc`).
    pub ss_config: Option<Arc<SsServerConfig>>,
    /// Shared Shadowsocks context — one per backend, reused across pool connections.
    pub ss_context: Option<SharedContext>,
    pub bind_interface: Option<String>,
    pub tls_connector: Option<TlsConnector>,
    pub server_name: Option<String>,
    pub enabled: Option<bool>,
    pub force_healthy: bool,
    pub unhealthy_threshold: u32,
    pub recover_threshold: u32,
    pub udp_enabled: bool,
    pub anytls_manager: Option<Arc<crate::outbound::anytls::AnytlsManager>>,
    pub tcp_congestion: Option<String>,
    pub hooks: Vec<String>,
}

impl std::fmt::Debug for BackendInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BackendInfo")
            .field("name", &self.name)
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
    }
}

impl BackendInfo {
    pub fn from_config(
        cfg: &BackendConfig,
        index: usize,
        global_bind_interface: Option<&str>,
    ) -> anyhow::Result<Self> {
        let is_direct = cfg.backend_type == "direct";

        let endpoint = match cfg.backend_type.as_str() {
            "direct" => BackendEndpoint::Direct,
            "socks5" | "ss" | "shadowsocks" | "uds" | "anytls" => {
                let addr = cfg
                    .address
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("backend must specify address"))?;
                if addr.starts_with("unix://") {
                    let path = addr.strip_prefix("unix://").unwrap().to_string();
                    BackendEndpoint::Unix { path }
                } else if addr.starts_with('/')
                    || addr.starts_with("./")
                    || addr.starts_with("../")
                    || cfg.backend_type == "uds"
                {
                    let path = if let Some(stripped) = addr.strip_prefix("unix://") {
                        stripped.to_string()
                    } else {
                        addr.clone()
                    };
                    BackendEndpoint::Unix { path }
                } else {
                    // Parse "host:port"
                    if let Some(pos) = addr.rfind(':') {
                        let host = addr[..pos].to_string();
                        let port: u16 = addr[pos + 1..].parse().map_err(|_| {
                            anyhow::anyhow!("invalid port in backend address: {}", addr)
                        })?;
                        BackendEndpoint::Tcp { host, port }
                    } else {
                        anyhow::bail!(
                            "backend address must be in host:port or unix://path format: {}",
                            addr
                        );
                    }
                }
            }
            other => anyhow::bail!("unknown backend type: {}", other),
        };

        let name = cfg.name.clone().unwrap_or_else(|| {
            if is_direct {
                format!("direct-{}", index)
            } else {
                format!("backend-{}", index)
            }
        });

        // Build Shadowsocks config if this is an SS backend.
        let (ss_config, ss_context) =
            if cfg.backend_type == "ss" || cfg.backend_type == "shadowsocks" {
                let method_str = cfg.username.as_deref().ok_or_else(|| {
                    anyhow::anyhow!("ss backend must specify method in username field")
                })?;
                let pass = cfg.password.as_deref().ok_or_else(|| {
                    anyhow::anyhow!("ss backend must specify password in password field")
                })?;
                let addr = cfg
                    .address
                    .as_deref()
                    .expect("ss backend must have address");
                let dummy_sock: std::net::SocketAddr = if addr.starts_with("unix://")
                    || addr.starts_with('/')
                    || addr.starts_with("./")
                    || addr.starts_with("../")
                {
                    "0.0.0.0:0".parse().unwrap()
                } else {
                    addr.parse()
                        .or_else(|_| {
                            if let Some(pos) = addr.rfind(':') {
                                let port: u16 = addr[pos + 1..].parse()?;
                                let host_str = &addr[..pos];
                                let ip: std::net::IpAddr = host_str.parse().unwrap_or(
                                    std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED),
                                );
                                Ok(std::net::SocketAddr::new(ip, port))
                            } else {
                                Err(anyhow::anyhow!("invalid address"))
                            }
                        })
                        .unwrap_or_else(|_| "0.0.0.0:0".parse().unwrap())
                };

                let method: shadowsocks::crypto::CipherKind = method_str
                    .parse()
                    .map_err(|_| anyhow::anyhow!("unsupported ss_method: {}", method_str))?;

                let ss_cfg = SsServerConfig::new(dummy_sock, pass, method).map_err(|e| {
                    anyhow::anyhow!("shadowsocks config error for backend '{}': {}", name, e)
                })?;

                let ctx = SsContext::new_shared(shadowsocks::config::ServerType::Local);
                (Some(Arc::new(ss_cfg)), Some(ctx))
            } else {
                (None, None)
            };

        let pool_size = if is_direct { 0 } else { cfg.pool_size };
        let bind_interface = cfg
            .bind_interface
            .clone()
            .or_else(|| global_bind_interface.map(String::from));

        let tls_connector = if let Some(tls) = &cfg.tls {
            Some(crate::tls::create_tls_connector(tls.insecure)?)
        } else {
            None
        };

        let server_name = cfg.tls.as_ref().and_then(|tls| tls.server_name.clone());

        let anytls_manager = if cfg.backend_type == "anytls" {
            let password = cfg.password.as_deref().unwrap_or("").to_string();
            Some(Arc::new(crate::outbound::anytls::AnytlsManager::new(
                password,
                if pool_size > 0 { pool_size } else { 1 },
            )))
        } else {
            None
        };

        Ok(Self {
            name,
            endpoint,
            username: cfg.username.clone(),
            password: cfg.password.clone(),
            pool_size,
            ss_config,
            ss_context,
            bind_interface,
            tls_connector,
            server_name,
            enabled: cfg.enabled,
            force_healthy: cfg.force_healthy,
            unhealthy_threshold: cfg.unhealthy_threshold,
            recover_threshold: cfg.recover_threshold,
            udp_enabled: cfg.network.as_deref() == Some("tcp_udp"),
            anytls_manager,
            tcp_congestion: cfg.tcp.as_ref().and_then(|t| t.congestion.clone()),
            hooks: cfg.hooks.clone(),
        })
    }

    /// Returns `true` if this is a Shadowsocks backend.
    pub fn is_shadowsocks(&self) -> bool {
        self.ss_config.is_some()
    }

    /// Returns `true` if this is a direct connection outbound backend.
    pub fn is_direct(&self) -> bool {
        matches!(self.endpoint, BackendEndpoint::Direct)
    }

    /// Returns `true` if this is an AnyTLS outbound backend.
    pub fn is_anytls(&self) -> bool {
        self.anytls_manager.is_some()
    }

    /// Returns true if this backend requires SOCKS5 authentication.
    pub fn requires_auth(&self) -> bool {
        self.username.is_some() && self.password.is_some()
    }

    pub async fn connect_target_pooled(
        &self,
        stream: crate::outbound::BackendStream,
        target: &crate::outbound::TargetAddr,
    ) -> std::io::Result<crate::outbound::BackendStream> {
        if self.is_shadowsocks() {
            let ss_cfg = self.ss_config.as_ref().unwrap();
            let ss_ctx = self.ss_context.as_ref().unwrap().clone();
            Ok(crate::outbound::ss_connect_pooled(
                stream, ss_cfg, ss_ctx, target,
            ))
        } else if self.is_anytls() {
            crate::outbound::anytls::anytls_connect_target(stream, target).await
        } else if self.is_direct() {
            unreachable!("Direct backend should not have pooled streams")
        } else {
            crate::outbound::socks5h_connect_target(stream, target).await
        }
    }

    pub async fn connect_target_fresh(
        &self,
        target: &crate::outbound::TargetAddr,
        timeout: std::time::Duration,
    ) -> std::io::Result<crate::outbound::BackendStream> {
        if self.is_direct() {
            crate::outbound::direct_connect(target, timeout, self.bind_interface.as_deref()).await
        } else if self.is_shadowsocks() {
            let ss_cfg = self.ss_config.as_ref().unwrap();
            let ss_ctx = self.ss_context.as_ref().unwrap().clone();
            crate::outbound::ss_connect_fresh(self, ss_cfg, ss_ctx, target, timeout).await
        } else if self.is_anytls() {
            let s = crate::outbound::anytls::anytls_connect_fresh(self).await?;
            crate::outbound::anytls::anytls_connect_target(s, target).await
        } else {
            crate::outbound::socks5h_connect(self, target, timeout).await
        }
    }
}
