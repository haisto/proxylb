//! Web status RESTful API for backend health monitoring.
//!
//! Provides JSON API endpoints for real-time status query and enabling/disabling backends.

use axum::http::{StatusCode, Uri, header};
use axum::{
    Router,
    extract::State,
    response::{IntoResponse, Json},
    routing::get,
};
use memory_stats::memory_stats;
use mime_guess::from_path;
use rust_embed::RustEmbed;
use serde::Serialize;
use tower_http::cors::{Any, CorsLayer};

#[derive(RustEmbed)]
#[folder = "web/dist/"]
struct Assets;

use crate::backend::BackendPool;

/// Memory statistics.
#[derive(Serialize, Clone, Copy, Debug)]
pub struct MemStats {
    pub rss: usize,
    pub vmsize: usize,
}

pub fn get_memory_usage() -> MemStats {
    if let Some(usage) = memory_stats() {
        MemStats {
            rss: usage.physical_mem,
            vmsize: usage.virtual_mem,
        }
    } else {
        MemStats { rss: 0, vmsize: 0 }
    }
}

/// CPU usage statistics.
/// Returns cumulative CPU times so the frontend can compute instantaneous CPU%
/// by diffing successive samples against wall-clock time.
#[derive(Serialize, Clone, Copy, Debug)]
pub struct CpuStats {
    /// Cumulative user-mode CPU time in microseconds.
    pub user_time_us: u64,
    /// Cumulative system (kernel) CPU time in microseconds.
    pub system_time_us: u64,
    /// Number of logical CPUs (for normalizing to 0-100%).
    pub num_cpus: usize,
}

pub fn get_cpu_usage() -> CpuStats {
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe {
        libc::getrusage(libc::RUSAGE_SELF, &mut usage);
    }
    let user_us = usage.ru_utime.tv_sec as u64 * 1_000_000 + usage.ru_utime.tv_usec as u64;
    let sys_us = usage.ru_stime.tv_sec as u64 * 1_000_000 + usage.ru_stime.tv_usec as u64;
    let cpus = get_effective_cpu_count();
    CpuStats {
        user_time_us: user_us,
        system_time_us: sys_us,
        num_cpus: cpus,
    }
}

/// Returns the number of CPUs effectively available to this process.
///
/// On Linux, checks cgroup v2/v1 for cpuset and cpu quota limits (Docker
/// `--cpuset-cpus`, `--cpus`). Falls back to `sysconf(_SC_NPROCESSORS_ONLN)`
/// which gives the system-wide count — correct for bare-metal and macOS
/// (even when individual threads are pinned to specific cores).
fn get_effective_cpu_count() -> usize {
    #[cfg(target_os = "linux")]
    {
        if let Some(n) = cgroup_cpu_count() {
            return n;
        }
    }
    let n = unsafe { libc::sysconf(libc::_SC_NPROCESSORS_ONLN) };
    if n > 0 { n as usize } else { 1 }
}

#[cfg(target_os = "linux")]
fn cgroup_cpu_count() -> Option<usize> {
    // cgroup v2 cpuset (e.g. --cpuset-cpus=0,1)
    if let Ok(content) = std::fs::read_to_string("/sys/fs/cgroup/cpuset.cpus.effective") {
        if let Some(n) = parse_cpuset_count(content.trim()) {
            return Some(n);
        }
    }
    // cgroup v2 cpu.max quota (e.g. --cpus=2)
    if let Ok(content) = std::fs::read_to_string("/sys/fs/cgroup/cpu.max") {
        let parts: Vec<&str> = content.trim().split_whitespace().collect();
        if parts.len() == 2 && parts[0] != "max" {
            if let (Ok(quota), Ok(period)) = (parts[0].parse::<u64>(), parts[1].parse::<u64>()) {
                if period > 0 {
                    let cpus = (quota as f64 / period as f64).ceil() as usize;
                    if cpus > 0 {
                        return Some(cpus);
                    }
                }
            }
        }
    }
    // cgroup v1 cpuset
    if let Ok(content) = std::fs::read_to_string("/sys/fs/cgroup/cpuset/cpuset.cpus") {
        if let Some(n) = parse_cpuset_count(content.trim()) {
            return Some(n);
        }
    }
    // cgroup v1 cpu quota
    if let (Ok(quota_s), Ok(period_s)) = (
        std::fs::read_to_string("/sys/fs/cgroup/cpu/cpu.cfs_quota_us"),
        std::fs::read_to_string("/sys/fs/cgroup/cpu/cpu.cfs_period_us"),
    ) {
        if let (Ok(quota), Ok(period)) = (
            quota_s.trim().parse::<i64>(),
            period_s.trim().parse::<u64>(),
        ) {
            if quota > 0 && period > 0 {
                let cpus = (quota as f64 / period as f64).ceil() as usize;
                if cpus > 0 {
                    return Some(cpus);
                }
            }
        }
    }
    None
}

#[cfg(target_os = "linux")]
fn parse_cpuset_count(s: &str) -> Option<usize> {
    // Parses "0-3,5,7-9" format → counts individual CPUs (e.g. 8)
    let mut count = 0usize;
    for part in s.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        if let Some((start, end)) = part.split_once('-') {
            if let (Ok(s), Ok(e)) = (start.parse::<usize>(), end.parse::<usize>()) {
                count += e - s + 1;
            }
        } else if part.parse::<usize>().is_ok() {
            count += 1;
        }
    }
    if count > 0 { Some(count) } else { None }
}

#[derive(Serialize)]
struct AdBlockStatusView {
    enabled: bool,
    block_rules_count: usize,
    allow_rules_count: usize,
    blocked_requests: u64,
}

/// JSON API response.
#[derive(Serialize)]
struct ApiResponse {
    backends: Vec<crate::backend::BackendStatusView>,
    tree: Vec<crate::backend::TreeItem>,
    memory: MemStats,
    cpu: CpuStats,
    inbounds: Vec<crate::backend::InboundStatsView>,
    clients: Vec<crate::stats::ClientStatsView>,
    domains: Vec<crate::stats::DomainStatsView>,
    blocked_domains: Vec<crate::stats::DomainStatsView>,
    adblock: AdBlockStatusView,
}

/// Create the axum router.
pub fn create_router(pool: BackendPool) -> Router {
    use axum::routing::post;
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any)
        .allow_private_network(true);

    Router::new()
        .route("/api", get(api_info))
        .route("/api/status", get(api_status))
        .route("/api/backends/{name}/enable", post(api_enable_backend))
        .route("/api/backends/{name}/disable", post(api_disable_backend))
        .route(
            "/api/filter/items",
            axum::routing::get(api_get_items)
                .post(api_add_item)
                .delete(api_delete_item),
        )
        .route("/api/filter/settings", post(api_set_filter_settings))
        .route("/api/filter/check", axum::routing::get(api_check_rule))
        .layer(cors)
        .fallback(get(static_handler))
        .with_state(pool)
}

async fn static_handler(uri: Uri) -> impl IntoResponse {
    let path = uri.path().trim_start_matches('/').to_string();

    if path.is_empty() {
        return axum::response::Redirect::temporary("/index.html").into_response();
    }

    match Assets::get(path.as_str()) {
        Some(content) => {
            let mime = from_path(path).first_or_octet_stream();
            ([(header::CONTENT_TYPE, mime.as_ref())], content.data).into_response()
        }
        None => {
            if let Some(index) = Assets::get("index.html") {
                let mime = from_path("index.html").first_or_octet_stream();
                ([(header::CONTENT_TYPE, mime.as_ref())], index.data).into_response()
            } else {
                (StatusCode::NOT_FOUND, "404 Not Found").into_response()
            }
        }
    }
}

/// GET /api
async fn api_info() -> impl IntoResponse {
    Json(serde_json::json!({
        "status": "ok",
        "name": "ProxyLB API",
        "version": concat!(env!("CARGO_PKG_VERSION"), "-", env!("GIT_HASH"))
    }))
}

/// POST /api/backends/:name/enable
async fn api_enable_backend(
    State(pool): State<BackendPool>,
    axum::extract::Path(name): axum::extract::Path<String>,
) -> impl IntoResponse {
    match pool.set_backend_enabled(&name, true).await {
        Ok(found) => {
            if found {
                axum::http::StatusCode::OK
            } else {
                axum::http::StatusCode::NOT_FOUND
            }
        }
        Err(_) => axum::http::StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// POST /api/backends/:name/disable
async fn api_disable_backend(
    State(pool): State<BackendPool>,
    axum::extract::Path(name): axum::extract::Path<String>,
) -> impl IntoResponse {
    match pool.set_backend_enabled(&name, false).await {
        Ok(found) => {
            if found {
                axum::http::StatusCode::OK
            } else {
                axum::http::StatusCode::NOT_FOUND
            }
        }
        Err(_) => axum::http::StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// Run the web status server.
pub async fn run_web_server(listen_addr: String, pool: BackendPool) -> anyhow::Result<()> {
    let app = create_router(pool);
    let uds_path = listen_addr
        .strip_prefix("unix://")
        .or_else(|| listen_addr.strip_prefix("unix:"));

    if let Some(raw) = uds_path {
        let mut path = raw.to_string();
        if !path.starts_with('/') {
            path = format!("/{}", path);
        }
        if let Some(parent) = std::path::Path::new(&path).parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let _ = tokio::fs::remove_file(&path).await;
        let listener = tokio::net::UnixListener::bind(&path)?;
        tracing::info!(listen = %path, "web status REST API started (UDS)");
        axum::serve(listener, app).await?;
    } else {
        let listener = tokio::net::TcpListener::bind(&listen_addr).await?;
        tracing::info!(listen = %listen_addr, "web status REST API started");
        axum::serve(listener, app).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::BackendPool;
    use crate::config::FilterConfig;
    use tokio::sync::watch;

    #[tokio::test]
    async fn test_run_web_server_uds() {
        let (_tx, rx) = watch::channel(0u64);
        let pool = BackendPool::new(
            &[],
            &[],
            None,
            None,
            rx,
            &FilterConfig::default(),
            tokio::runtime::Handle::current(),
            std::collections::HashSet::new(),
        )
        .unwrap();

        let socket_path = "/tmp/test_api_web.sock";
        let listen_addr = format!("unix://{}", socket_path);

        // Spawn the server in the background
        let pool_clone = pool.clone();
        let handle = tokio::spawn(async move {
            let _ = run_web_server(listen_addr, pool_clone).await;
        });

        // Wait a bit for the server to start
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Verify socket file exists
        assert!(std::path::Path::new(socket_path).exists());

        // Cleanup
        handle.abort();
        let _ = tokio::fs::remove_file(socket_path).await;
    }

    #[tokio::test]
    async fn test_run_web_server_uds_relative() {
        let (_tx, rx) = watch::channel(0u64);
        let pool = BackendPool::new(
            &[],
            &[],
            None,
            None,
            rx,
            &FilterConfig::default(),
            tokio::runtime::Handle::current(),
            std::collections::HashSet::new(),
        )
        .unwrap();

        let listen_addr = "unix://tmp/proxylb_api.sock".to_string();

        // Spawn the server in the background
        let pool_clone = pool.clone();
        let handle = tokio::spawn(async move {
            let _ = run_web_server(listen_addr, pool_clone).await;
        });

        // Wait a bit for the server to start
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Verify socket file exists
        assert!(std::path::Path::new("/tmp/proxylb_api.sock").exists());

        // Cleanup
        handle.abort();
        let _ = tokio::fs::remove_file("/tmp/proxylb_api.sock").await;
    }
}

/// JSON API endpoint: GET /api/status
async fn api_status(State(pool): State<BackendPool>) -> Json<ApiResponse> {
    let backends = pool.status_views().await;
    let tree = pool.status_tree().await;
    let memory = get_memory_usage();
    let cpu = get_cpu_usage();
    let inbounds = pool.get_inbound_stats();
    let clients = pool.client_manager.get_views();

    let engine = pool.filter_manager.engine.load();
    let adblock = AdBlockStatusView {
        enabled: pool.filter_manager.is_enabled(),
        block_rules_count: engine.block_rules_count,
        allow_rules_count: 0,
        blocked_requests: pool
            .filter_manager
            .blocked_requests
            .load(std::sync::atomic::Ordering::Relaxed),
    };

    let domains = pool.domain_manager.get_views();
    let blocked_domains = pool.filter_manager.blocked_domains_manager.get_views();

    Json(ApiResponse {
        backends,
        tree,
        memory,
        cpu,
        inbounds,
        clients,
        domains,
        blocked_domains,
        adblock,
    })
}

#[derive(serde::Deserialize)]
#[serde(tag = "type")]
pub enum FilterItemRequest {
    #[serde(rename = "rule")]
    Rule { rules: Vec<String> },
    #[serde(rename = "url")]
    Url {
        url: String,
        #[serde(default)]
        tag: String,
    },
}

#[derive(serde::Serialize)]
pub struct FilterItemsResponse {
    pub rules: Vec<String>,
    pub urls: Vec<crate::filter::FilterUrl>,
}

#[derive(serde::Deserialize)]
pub struct FilterSettingsRequest {
    pub enabled: bool,
    pub block_private_addresses: bool,
}

async fn api_get_items(State(pool): State<BackendPool>) -> impl IntoResponse {
    let rules = pool.filter_manager.get_rules();
    let mut urls = pool.filter_manager.get_urls();
    let cache = pool.filter_manager.cached_remote_contents.read().await;
    for u in &mut urls {
        if let Some(content) = cache.get(&u.url) {
            u.rule_count = content
                .lines()
                .filter_map(crate::filter::parse_rule_line)
                .count();
        }
    }
    Json(FilterItemsResponse { rules, urls })
}

async fn api_add_item(
    State(pool): State<BackendPool>,
    Json(req): Json<FilterItemRequest>,
) -> impl IntoResponse {
    match req {
        FilterItemRequest::Rule { rules } => {
            if pool.filter_manager.add_rules(&rules).await.is_ok() {
                axum::http::StatusCode::OK
            } else {
                axum::http::StatusCode::INTERNAL_SERVER_ERROR
            }
        }
        FilterItemRequest::Url { url, tag } => {
            match crate::filter::download_url(&pool, pool.filter_manager.backend.as_deref(), &url)
                .await
            {
                Ok(content) => {
                    if pool
                        .filter_manager
                        .add_url(&url, &tag, content)
                        .await
                        .is_ok()
                    {
                        axum::http::StatusCode::OK
                    } else {
                        axum::http::StatusCode::INTERNAL_SERVER_ERROR
                    }
                }
                Err(e) => {
                    tracing::error!(url = %url, error = %e, "failed to download filter list from web UI");
                    axum::http::StatusCode::BAD_REQUEST
                }
            }
        }
    }
}

async fn api_delete_item(
    State(pool): State<BackendPool>,
    Json(req): Json<FilterItemRequest>,
) -> impl IntoResponse {
    match req {
        FilterItemRequest::Rule { rules } => {
            if pool.filter_manager.delete_rules(&rules).await.is_ok() {
                axum::http::StatusCode::OK
            } else {
                axum::http::StatusCode::INTERNAL_SERVER_ERROR
            }
        }
        FilterItemRequest::Url { url, tag: _ } => {
            if pool.filter_manager.delete_url(&url).await.is_ok() {
                axum::http::StatusCode::OK
            } else {
                axum::http::StatusCode::INTERNAL_SERVER_ERROR
            }
        }
    }
}

async fn api_set_filter_settings(
    State(pool): State<BackendPool>,
    Json(req): Json<FilterSettingsRequest>,
) -> impl IntoResponse {
    pool.filter_manager.set_enabled(req.enabled);
    pool.filter_manager
        .set_block_private(req.block_private_addresses);
    axum::http::StatusCode::OK
}

#[derive(serde::Deserialize)]
pub struct CheckRequest {
    pub target: String,
}

#[derive(serde::Serialize)]
pub struct CheckResponse {
    pub blocked: bool,
}

async fn api_check_rule(
    State(pool): State<BackendPool>,
    axum::extract::Query(req): axum::extract::Query<CheckRequest>,
) -> impl IntoResponse {
    let target_addr = if let Ok(ip) = req.target.parse::<std::net::IpAddr>() {
        crate::outbound::TargetAddr::Ip(std::net::SocketAddr::new(ip, 0))
    } else {
        crate::outbound::TargetAddr::Domain(req.target.clone(), 0)
    };
    let blocked = pool.filter_manager.is_blocked(&target_addr);
    Json(CheckResponse { blocked })
}
