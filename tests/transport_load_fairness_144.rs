//! Phase 4 transport load, fairness, cancellation, and regression baseline
//! suite for DecapodLabs/dactyl#144.
//!
//! Evidence classes:
//! - `loopback_http`: real TCP/HTTP keep-alive loopback servers exercising
//!   `NeonAdapter`, `SupabaseAdapter`, `CloudConnector`, and `SharedHttpClient`.
//! - `local_postgres`, `hosted_supabase`, `live_propodus`: unavailable here and
//!   never claimed as passed.
#![cfg(any(feature = "neon", feature = "supabase"))]

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use dactyl_db::ResourceBudget;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

static TEST_SUITE_GATE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[cfg(all(feature = "neon", feature = "supabase"))]
#[derive(Clone, Debug)]
struct SplitMix64(u64);

#[cfg(all(feature = "neon", feature = "supabase"))]
impl SplitMix64 {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn next_range(&mut self, upper_exclusive: u64) -> u64 {
        if upper_exclusive == 0 {
            0
        } else {
            self.next_u64() % upper_exclusive
        }
    }
}

#[derive(Clone)]
struct ServerStats {
    accepted: Arc<AtomicUsize>,
    open_connections: Arc<AtomicUsize>,
    peak_open_connections: Arc<AtomicUsize>,
    requests: Arc<AtomicUsize>,
    in_flight: Arc<AtomicUsize>,
    peak_in_flight: Arc<AtomicUsize>,
    peak_idle_connections: Arc<AtomicUsize>,
    response_bytes: Arc<AtomicU64>,
    service_us_by_seq: Arc<Vec<AtomicU64>>,
    shutdown: Arc<AtomicBool>,
}

impl ServerStats {
    fn new(max_seq: usize) -> Self {
        let mut slots = Vec::with_capacity(max_seq);
        for _ in 0..max_seq {
            slots.push(AtomicU64::new(0));
        }
        Self {
            accepted: Arc::new(AtomicUsize::new(0)),
            open_connections: Arc::new(AtomicUsize::new(0)),
            peak_open_connections: Arc::new(AtomicUsize::new(0)),
            requests: Arc::new(AtomicUsize::new(0)),
            in_flight: Arc::new(AtomicUsize::new(0)),
            peak_in_flight: Arc::new(AtomicUsize::new(0)),
            peak_idle_connections: Arc::new(AtomicUsize::new(0)),
            response_bytes: Arc::new(AtomicU64::new(0)),
            service_us_by_seq: Arc::new(slots),
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    fn record_service_us(&self, seq: usize, us: u64) {
        if let Some(slot) = self.service_us_by_seq.get(seq) {
            slot.store(us, Ordering::Release);
        }
    }

    #[cfg(all(feature = "neon", feature = "supabase"))]
    fn service_us(&self, seq: usize) -> u64 {
        self.service_us_by_seq
            .get(seq)
            .map_or(0, |slot| slot.load(Ordering::Acquire))
    }

    fn current_idle(&self) -> usize {
        let open = self.open_connections.load(Ordering::SeqCst);
        let active = self.in_flight.load(Ordering::SeqCst);
        open.saturating_sub(active)
    }

    fn sample_idle(&self) {
        let idle = self.current_idle();
        self.peak_idle_connections.fetch_max(idle, Ordering::SeqCst);
    }
}

#[cfg_attr(not(all(feature = "neon", feature = "supabase")), allow(dead_code))]
enum ServerAction {
    Json {
        status: u16,
        keep_alive: bool,
        body: Vec<u8>,
        seq: Option<usize>,
    },
    /// Send 200 OK headers immediately, then stall before closing/writing body.
    StallAfterHeaders {
        header_delay: Duration,
        body_delay: Duration,
        body: Vec<u8>,
    },
    /// Stall before sending headers.
    StallBeforeHeaders {
        delay: Duration,
        status: u16,
        body: Vec<u8>,
    },
    /// Send partial headers/body or drop socket abruptly without completing response.
    AbortConnection { send_headers_first: bool },
    /// Stream an unframed response exceeding `max_wire_bytes`.
    UnframedOversized { total_bytes: usize },
}

async fn read_http_request(
    stream: &mut tokio::net::TcpStream,
    buf: &mut Vec<u8>,
) -> Option<(String, String, Vec<u8>)> {
    loop {
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            let head_end = pos + 4;
            let head_str = String::from_utf8_lossy(&buf[..head_end]).into_owned();
            let mut content_length = 0usize;
            for line in head_str.lines().skip(1) {
                if let Some((name, val)) = line.split_once(':') {
                    if name.trim().eq_ignore_ascii_case("content-length") {
                        content_length = val.trim().parse::<usize>().unwrap_or(0);
                    }
                }
            }
            while buf.len() < head_end + content_length {
                let mut chunk = [0u8; 4096];
                let n = stream.read(&mut chunk).await.ok()?;
                if n == 0 {
                    return None;
                }
                buf.extend_from_slice(&chunk[..n]);
            }
            let body = buf[head_end..head_end + content_length].to_vec();
            buf.drain(..head_end + content_length);
            let request_line = head_str.lines().next().unwrap_or("");
            let mut parts = request_line.split_whitespace();
            let method = parts.next().unwrap_or("").to_owned();
            let path = parts.next().unwrap_or("").to_owned();
            return Some((head_str, format!("{method} {path}"), body));
        }
        let mut chunk = [0u8; 4096];
        let n = stream.read(&mut chunk).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&chunk[..n]);
    }
}

async fn spawn_instrumented_server<F, Fut>(max_seq: usize, handler: F) -> (String, ServerStats)
where
    F: Fn(String, String, Vec<u8>) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = ServerAction> + Send + 'static,
{
    let stats = ServerStats::new(max_seq);
    let stats_accept = stats.clone();
    let handler = Arc::new(handler);
    let (tx, rx) = std::sync::mpsc::sync_channel(1);

    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let origin = format!("http://{}", listener.local_addr().unwrap());
            let _ = tx.send(origin);
            while !stats_accept.shutdown.load(Ordering::Relaxed) {
                let Ok(accept_res) =
                    tokio::time::timeout(Duration::from_millis(200), listener.accept()).await
                else {
                    continue;
                };
                let Ok((mut stream, _)) = accept_res else {
                    break;
                };
                stats_accept.accepted.fetch_add(1, Ordering::SeqCst);
                let open = stats_accept.open_connections.fetch_add(1, Ordering::SeqCst) + 1;
                stats_accept
                    .peak_open_connections
                    .fetch_max(open, Ordering::SeqCst);
                let stats_conn = stats_accept.clone();
                let handler_conn = handler.clone();
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    while let Some((head, target, body)) =
                        read_http_request(&mut stream, &mut buf).await
                    {
                        stats_conn.requests.fetch_add(1, Ordering::SeqCst);
                        let active = stats_conn.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                        stats_conn
                            .peak_in_flight
                            .fetch_max(active, Ordering::SeqCst);
                        let started = Instant::now();
                        let action = handler_conn(head, target, body).await;
                        let service_us = started.elapsed().as_micros() as u64;
                        let keep_alive_conn = match action {
                            ServerAction::Json {
                                status,
                                keep_alive,
                                body: resp_body,
                                seq,
                            } => {
                                if let Some(seq) = seq {
                                    stats_conn.record_service_us(seq, service_us.max(1));
                                }
                                let conn_hdr = if keep_alive { "keep-alive" } else { "close" };
                                let header = format!(
                                    "HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: {conn_hdr}\r\n\r\n",
                                    resp_body.len()
                                );
                                stats_conn
                                    .response_bytes
                                    .fetch_add(resp_body.len() as u64, Ordering::SeqCst);
                                let failed = stream.write_all(header.as_bytes()).await.is_err()
                                    || stream.write_all(&resp_body).await.is_err()
                                    || stream.flush().await.is_err();
                                keep_alive && !failed
                            }
                            ServerAction::StallBeforeHeaders {
                                delay,
                                status,
                                body: resp_body,
                            } => {
                                tokio::time::sleep(delay).await;
                                let header = format!(
                                    "HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                                    resp_body.len()
                                );
                                let _ = stream.write_all(header.as_bytes()).await;
                                let _ = stream.write_all(&resp_body).await;
                                let _ = stream.flush().await;
                                false
                            }
                            ServerAction::StallAfterHeaders {
                                header_delay,
                                body_delay,
                                body: resp_body,
                            } => {
                                tokio::time::sleep(header_delay).await;
                                let header = format!(
                                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                                    resp_body.len()
                                );
                                if stream.write_all(header.as_bytes()).await.is_ok() {
                                    let _ = stream.flush().await;
                                    tokio::time::sleep(body_delay).await;
                                    let _ = stream.write_all(&resp_body).await;
                                    let _ = stream.flush().await;
                                }
                                false
                            }
                            ServerAction::AbortConnection { send_headers_first } => {
                                if send_headers_first {
                                    let _ = stream
                                        .write_all(
                                            b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 128\r\nConnection: close\r\n\r\n{\"columns\":",
                                        )
                                        .await;
                                    let _ = stream.flush().await;
                                }
                                false
                            }
                            ServerAction::UnframedOversized { total_bytes } => {
                                let _ = stream
                                    .write_all(
                                        b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{\"columns\":[],\"rows\":[],\"pad\":\"",
                                    )
                                    .await;
                                let chunk = vec![b'x'; 2048.min(total_bytes)];
                                let mut sent = 0usize;
                                while sent < total_bytes {
                                    let slice_len = (total_bytes - sent).min(chunk.len());
                                    if stream.write_all(&chunk[..slice_len]).await.is_err() {
                                        break;
                                    }
                                    sent += slice_len;
                                }
                                let _ = stream.write_all(b"\"}").await;
                                let _ = stream.flush().await;
                                false
                            }
                        };
                        stats_conn.in_flight.fetch_sub(1, Ordering::SeqCst);
                        stats_conn.sample_idle();
                        if !keep_alive_conn {
                            break;
                        }
                    }
                    stats_conn.open_connections.fetch_sub(1, Ordering::SeqCst);
                });
            }
        });
    });

    (rx.recv().unwrap(), stats)
}

#[cfg(all(feature = "neon", feature = "supabase"))]
fn open_fd_count() -> Option<usize> {
    #[cfg(target_os = "linux")]
    {
        std::fs::read_dir("/proc/self/fd")
            .ok()
            .map(|entries| entries.count())
    }
    #[cfg(target_os = "macos")]
    {
        std::fs::read_dir("/dev/fd")
            .ok()
            .map(|entries| entries.count())
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        None
    }
}

#[cfg(all(feature = "neon", feature = "supabase"))]
fn process_rss_bytes() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let status = std::fs::read_to_string("/proc/self/status").ok()?;
        for line in status.lines() {
            if let Some(rest) = line.strip_prefix("VmRSS:") {
                let kb = rest.split_whitespace().next()?.parse::<u64>().ok()?;
                return Some(kb * 1024);
            }
        }
        None
    }
    #[cfg(target_os = "macos")]
    {
        let out = std::process::Command::new("ps")
            .args(["-o", "rss=", "-p", &std::process::id().to_string()])
            .output()
            .ok()?;
        if !out.status.success() {
            return None;
        }
        let kb = String::from_utf8_lossy(&out.stdout)
            .trim()
            .parse::<u64>()
            .ok()?;
        Some(kb * 1024)
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        None
    }
}

#[cfg(all(feature = "neon", feature = "supabase"))]
fn percentile(sorted: &[u64], pct: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() - 1) * pct) / 100;
    sorted[idx]
}

#[cfg(all(feature = "neon", feature = "supabase"))]
#[derive(Default)]
struct LatencySampleCollector {
    total_us: Vec<u64>,
    service_us: Vec<u64>,
    queue_us: Vec<u64>,
    result_bytes: u64,
}

#[cfg(all(feature = "neon", feature = "supabase"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sustained_mixed_sync_async_hot_cold_multi_origin_fairness_and_baseline() {
    use dactyl_db::{
        AccessMode, AccountOperation, CloudConnector, CloudResponseMetadata, Connection,
        DatastoreRoute, OpenOptions, OperationAttempt, Parameter, StorageContext,
        SupabaseRpcConfig,
    };

    let _gate = TEST_SUITE_GATE.lock().await;
    let total_ops: usize = 240;

    let make_handler = || {
        |_head: String, target: String, body: Vec<u8>| async move {
            let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
            let seq = parsed
                .get("params")
                .and_then(|p| p.as_array())
                .and_then(|arr| arr.first())
                .and_then(|v| v.as_u64())
                .or_else(|| parsed.get("seq").and_then(|v| v.as_u64()))
                .unwrap_or(0) as usize;
            let delay_ms = 3 + (seq as u64 % 5);
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            if target.ends_with("/query") {
                let resp = serde_json::to_vec(&serde_json::json!({
                    "columns": ["seq", "payload"],
                    "rows": [{"seq": seq as i64, "payload": "ok-payload-0123456789abcdef"}],
                    "affected_rows": 1,
                    "generated_keys": []
                }))
                .unwrap();
                ServerAction::Json {
                    status: 200,
                    keep_alive: true,
                    body: resp,
                    seq: Some(seq),
                }
            } else {
                let resp = serde_json::to_vec(&serde_json::json!({
                    "status": "active",
                    "seq": seq
                }))
                .unwrap();
                ServerAction::Json {
                    status: 200,
                    keep_alive: true,
                    body: resp,
                    seq: Some(seq),
                }
            }
        }
    };

    let (neon_origin, neon_stats) = spawn_instrumented_server(total_ops + 32, make_handler()).await;
    let (supa_origin, supa_stats) = spawn_instrumented_server(total_ops + 32, make_handler()).await;
    let (cloud_origin, cloud_stats) =
        spawn_instrumented_server(total_ops + 32, make_handler()).await;

    // Pool A (max_in_flight=6) shared across Supabase + CloudConnector origins.
    let budget_a = ResourceBudget::v1()
        .with_http_pool_limits(6, 6, Duration::from_secs(5))
        .with_request_timeout(Duration::from_secs(4));
    // Pool B (distinct PoolSettings: max_in_flight=4, idle=4, idle_timeout=6s) for Neon origin.
    let budget_b = ResourceBudget::v1()
        .with_http_pool_limits(4, 4, Duration::from_secs(6))
        .with_request_timeout(Duration::from_secs(4));

    let ctx = StorageContext::new(1, serde_json::json!({"org_id": "org-load"})).unwrap();
    let neon_conn = Arc::new(
        Connection::open_with_budget(
            DatastoreRoute::neon(format!("{neon_origin}/sql"), Some("neon-tok".into())),
            OpenOptions {
                access_mode: AccessMode::ReadWrite,
                ..OpenOptions::default()
            },
            Some(ctx.clone()),
            budget_b,
        )
        .unwrap(),
    );
    let supa_conn = Arc::new(
        Connection::open_with_budget(
            DatastoreRoute::supabase(&supa_origin, Some("supa-tok".into())),
            OpenOptions {
                access_mode: AccessMode::ReadWrite,
                ..OpenOptions::default()
            },
            Some(ctx),
            budget_a,
        )
        .unwrap(),
    );
    let cloud_conn = Arc::new(
        CloudConnector::with_config(SupabaseRpcConfig::v1(&cloud_origin, "sb_publishable_load"))
            .unwrap()
            .with_http_budget(budget_a)
            .unwrap(),
    );

    // Warm up one call per adapter so lazy client initialization doesn't skew FD/RSS delta.
    let _ = neon_conn
        .read_async("SELECT $1", &[Parameter::Integer(0)])
        .await
        .unwrap();
    let _ = supa_conn
        .read_async("SELECT $1", &[Parameter::Integer(0)])
        .await
        .unwrap();
    {
        let mut meta = CloudResponseMetadata::default();
        let _ = cloud_conn
            .account_request_with_metadata_async(
                AccountOperation::Status,
                &serde_json::json!({"version": 1, "seq": 0}),
                "bearer-warm",
                None,
                None,
                &mut meta,
            )
            .await
            .unwrap();
    }

    let fd_before = open_fd_count();
    let rss_before = process_rss_bytes();
    let fd_peak = Arc::new(AtomicUsize::new(fd_before.unwrap_or(0)));
    let rss_peak = Arc::new(AtomicU64::new(rss_before.unwrap_or(0)));
    let stop_sampler = Arc::new(AtomicBool::new(false));

    let sampler = {
        let stop = stop_sampler.clone();
        let fd_peak = fd_peak.clone();
        let rss_peak = rss_peak.clone();
        let neon_s = neon_stats.clone();
        let supa_s = supa_stats.clone();
        let cloud_s = cloud_stats.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(15));
            while !stop.load(Ordering::Relaxed) {
                interval.tick().await;
                neon_s.sample_idle();
                supa_s.sample_idle();
                cloud_s.sample_idle();
                if let Some(fd) = open_fd_count() {
                    fd_peak.fetch_max(fd, Ordering::SeqCst);
                }
                if let Some(rss) = process_rss_bytes() {
                    rss_peak.fetch_max(rss, Ordering::SeqCst);
                }
            }
        })
    };

    let collector = Arc::new(std::sync::Mutex::new(LatencySampleCollector::default()));
    let mut prng = SplitMix64::new(0xD4C7_1440_2026_0001);
    let mut async_tasks = Vec::new();
    let mut sync_threads = Vec::new();
    let cold_sync_max_us = Arc::new(AtomicU64::new(0));
    let cold_async_max_us = Arc::new(AtomicU64::new(0));

    // Hot async workers hammering Neon, Supabase, and CloudConnector across 180 ops.
    for seq in 1..=180usize {
        let choice = prng.next_range(3);
        let neon_c = neon_conn.clone();
        let supa_c = supa_conn.clone();
        let cloud_c = cloud_conn.clone();
        let neon_s = neon_stats.clone();
        let supa_s = supa_stats.clone();
        let cloud_s = cloud_stats.clone();
        let col = collector.clone();
        async_tasks.push(tokio::spawn(async move {
            let start = Instant::now();
            let (svc_us, res_bytes) = match choice {
                0 => {
                    let rows = neon_c
                        .read_async("SELECT $1", &[Parameter::Integer(seq as i64)])
                        .await
                        .unwrap();
                    assert_eq!(rows.len(), 1);
                    let payload = rows.as_slice()[0].get_str("payload").unwrap();
                    (neon_s.service_us(seq), payload.len() as u64 + 8)
                }
                1 => {
                    let attempt = OperationAttempt::new(format!("load-supa-{seq}")).unwrap();
                    let mut meta = CloudResponseMetadata::default();
                    let write = supa_c
                        .write_with_attempt_async(
                            "UPDATE items SET v = $1",
                            &[Parameter::Integer(seq as i64)],
                            &attempt,
                            &mut meta,
                        )
                        .await
                        .unwrap();
                    assert_eq!(write.affected_rows, 1);
                    (supa_s.service_us(seq), 16)
                }
                _ => {
                    let mut meta = CloudResponseMetadata::default();
                    let out = cloud_c
                        .account_request_with_metadata_async(
                            AccountOperation::Status,
                            &serde_json::json!({"version": 1, "seq": seq}),
                            "bearer-hot",
                            None,
                            None,
                            &mut meta,
                        )
                        .await
                        .unwrap();
                    assert_eq!(out["status"], "active");
                    (cloud_s.service_us(seq), 32)
                }
            };
            let tot_us = start.elapsed().as_micros() as u64;
            let q_us = tot_us.saturating_sub(svc_us);
            let mut guard = col.lock().unwrap();
            guard.total_us.push(tot_us);
            guard.service_us.push(svc_us);
            guard.queue_us.push(q_us);
            guard.result_bytes += res_bytes;
        }));
    }

    // Simultaneous sync OS-thread workers (40 ops) + cold sync & cold async callers (20 ops).
    for seq in 181..=220usize {
        let neon_c = neon_conn.clone();
        let supa_c = supa_conn.clone();
        let neon_s = neon_stats.clone();
        let supa_s = supa_stats.clone();
        let col = collector.clone();
        let use_neon = prng.next_range(2) == 0;
        sync_threads.push(std::thread::spawn(move || {
            let start = Instant::now();
            let svc_us = if use_neon {
                let rows = neon_c
                    .read("SELECT $1", &[Parameter::Integer(seq as i64)])
                    .unwrap();
                assert_eq!(rows.len(), 1);
                neon_s.service_us(seq)
            } else {
                let rows = supa_c
                    .read("SELECT $1", &[Parameter::Integer(seq as i64)])
                    .unwrap();
                assert_eq!(rows.len(), 1);
                supa_s.service_us(seq)
            };
            let tot_us = start.elapsed().as_micros() as u64;
            let mut guard = col.lock().unwrap();
            guard.total_us.push(tot_us);
            guard.service_us.push(svc_us);
            guard.queue_us.push(tot_us.saturating_sub(svc_us));
            guard.result_bytes += 32;
        }));
    }

    // Periodic cold callers arriving while hot sync+async load runs.
    for seq in 221..=240usize {
        let supa_c = supa_conn.clone();
        let neon_c = neon_conn.clone();
        let supa_s = supa_stats.clone();
        let neon_s = neon_stats.clone();
        let col = collector.clone();
        let cold_sync_max = cold_sync_max_us.clone();
        let cold_async_max = cold_async_max_us.clone();
        if seq % 2 == 0 {
            async_tasks.push(tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(((seq - 220) * 2) as u64)).await;
                let start = Instant::now();
                let rows = supa_c
                    .read_async("SELECT $1", &[Parameter::Integer(seq as i64)])
                    .await
                    .unwrap();
                assert_eq!(rows.len(), 1);
                let tot_us = start.elapsed().as_micros() as u64;
                cold_async_max.fetch_max(tot_us, Ordering::SeqCst);
                let svc_us = supa_s.service_us(seq);
                let mut guard = col.lock().unwrap();
                guard.total_us.push(tot_us);
                guard.service_us.push(svc_us);
                guard.queue_us.push(tot_us.saturating_sub(svc_us));
                guard.result_bytes += 32;
            }));
        } else {
            sync_threads.push(std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(((seq - 220) * 2) as u64));
                let start = Instant::now();
                let rows = neon_c
                    .read("SELECT $1", &[Parameter::Integer(seq as i64)])
                    .unwrap();
                assert_eq!(rows.len(), 1);
                let tot_us = start.elapsed().as_micros() as u64;
                cold_sync_max.fetch_max(tot_us, Ordering::SeqCst);
                let svc_us = neon_s.service_us(seq);
                let mut guard = col.lock().unwrap();
                guard.total_us.push(tot_us);
                guard.service_us.push(svc_us);
                guard.queue_us.push(tot_us.saturating_sub(svc_us));
                guard.result_bytes += 32;
            }));
        }
    }

    for task in async_tasks {
        task.await.unwrap();
    }
    for handle in sync_threads {
        handle.join().unwrap();
    }
    stop_sampler.store(true, Ordering::Relaxed);
    sampler.await.unwrap();

    let fd_with_keepalive = open_fd_count();
    let rss_after = process_rss_bytes();

    // Assert aggregate pool ceilings per budget pool!
    let neon_peak = neon_stats.peak_in_flight.load(Ordering::SeqCst);
    let supa_peak = supa_stats.peak_in_flight.load(Ordering::SeqCst);
    let cloud_peak = cloud_stats.peak_in_flight.load(Ordering::SeqCst);
    assert!(neon_peak <= 4, "Pool B peak {neon_peak} exceeded limit 4");
    assert!(
        supa_peak <= 6 && cloud_peak <= 6,
        "Pool A peak supa={supa_peak}, cloud={cloud_peak} exceeded limit 6"
    );

    // Assert keep-alive connection reuse across all 3 origins.
    let total_server_reqs = neon_stats.requests.load(Ordering::SeqCst)
        + supa_stats.requests.load(Ordering::SeqCst)
        + cloud_stats.requests.load(Ordering::SeqCst);
    let total_accepted = neon_stats.accepted.load(Ordering::SeqCst)
        + supa_stats.accepted.load(Ordering::SeqCst)
        + cloud_stats.accepted.load(Ordering::SeqCst);
    assert_eq!(total_server_reqs, 243);
    assert!(
        total_accepted <= 48,
        "expected keep-alive pooling reuse across 243 requests; accepted={total_accepted}"
    );

    drop((neon_conn, supa_conn, cloud_conn));
    neon_stats.shutdown.store(true, Ordering::Relaxed);
    supa_stats.shutdown.store(true, Ordering::Relaxed);
    cloud_stats.shutdown.store(true, Ordering::Relaxed);
    tokio::time::sleep(Duration::from_millis(250)).await;
    let fd_after_drop = open_fd_count();

    let mut col = collector.lock().unwrap();
    col.total_us.sort_unstable();
    col.service_us.sort_unstable();
    col.queue_us.sort_unstable();

    let total_p50_ms = percentile(&col.total_us, 50) as f64 / 1000.0;
    let total_p95_ms = percentile(&col.total_us, 95) as f64 / 1000.0;
    let total_p99_ms = percentile(&col.total_us, 99) as f64 / 1000.0;
    let svc_p50_ms = percentile(&col.service_us, 50) as f64 / 1000.0;
    let svc_p95_ms = percentile(&col.service_us, 95) as f64 / 1000.0;
    let svc_p99_ms = percentile(&col.service_us, 99) as f64 / 1000.0;
    let q_p50_ms = percentile(&col.queue_us, 50) as f64 / 1000.0;
    let q_p95_ms = percentile(&col.queue_us, 95) as f64 / 1000.0;
    let q_p99_ms = percentile(&col.queue_us, 99) as f64 / 1000.0;
    let total_wire_resp_bytes = neon_stats.response_bytes.load(Ordering::SeqCst)
        + supa_stats.response_bytes.load(Ordering::SeqCst)
        + cloud_stats.response_bytes.load(Ordering::SeqCst);
    let peak_idle = neon_stats.peak_idle_connections.load(Ordering::SeqCst)
        + supa_stats.peak_idle_connections.load(Ordering::SeqCst)
        + cloud_stats.peak_idle_connections.load(Ordering::SeqCst);
    let fd_delta_keepalive = match (fd_before, fd_with_keepalive) {
        (Some(b), Some(a)) => Some(a as isize - b as isize),
        _ => None,
    };
    let fd_delta_after_drop = match (fd_before, fd_after_drop) {
        (Some(b), Some(a)) => Some(a as isize - b as isize),
        _ => None,
    };
    let rss_delta_bytes = match (rss_before, rss_after) {
        (Some(b), Some(a)) => Some(a as i64 - b as i64),
        _ => None,
    };

    println!(
        "BASELINE_SUSTAINED_144={{\"ops\":{},\"total_ms\":{{\"p50\":{:.2},\"p95\":{:.2},\"p99\":{:.2}}},\"service_ms\":{{\"p50\":{:.2},\"p95\":{:.2},\"p99\":{:.2}}},\"queue_ms\":{{\"p50\":{:.2},\"p95\":{:.2},\"p99\":{:.2}}},\"cold_sync_max_ms\":{:.2},\"cold_async_max_ms\":{:.2},\"peak_in_flight\":{{\"neon\":{},\"supabase\":{},\"cloud\":{}}},\"accepted_conns\":{},\"peak_idle_conns\":{},\"fd_before\":{:?},\"fd_peak\":{},\"fd_keepalive\":{:?},\"fd_after_drop\":{:?},\"fd_delta_keepalive\":{:?},\"fd_delta_after_drop\":{:?},\"rss_before_bytes\":{:?},\"rss_peak_bytes\":{},\"rss_after_bytes\":{:?},\"rss_delta_bytes\":{:?},\"result_bytes\":{},\"wire_resp_bytes\":{}}}",
        col.total_us.len(),
        total_p50_ms,
        total_p95_ms,
        total_p99_ms,
        svc_p50_ms,
        svc_p95_ms,
        svc_p99_ms,
        q_p50_ms,
        q_p95_ms,
        q_p99_ms,
        cold_sync_max_us.load(Ordering::SeqCst) as f64 / 1000.0,
        cold_async_max_us.load(Ordering::SeqCst) as f64 / 1000.0,
        neon_peak,
        supa_peak,
        cloud_peak,
        total_accepted,
        peak_idle,
        fd_before,
        fd_peak.load(Ordering::SeqCst),
        fd_with_keepalive,
        fd_after_drop,
        fd_delta_keepalive,
        fd_delta_after_drop,
        rss_before,
        rss_peak.load(Ordering::SeqCst),
        rss_after,
        rss_delta_bytes,
        col.result_bytes,
        total_wire_resp_bytes,
    );

    // Regression thresholds (documented in docs/transport-load-baseline.md).
    assert!(
        total_p99_ms < 1500.0,
        "sustained p99 latency {total_p99_ms:.2}ms regressed past 1500ms threshold"
    );
    assert!(
        q_p99_ms < 1400.0,
        "sustained p99 queue latency {q_p99_ms:.2}ms regressed past 1400ms threshold"
    );
    if let Some(delta) = fd_delta_keepalive {
        assert!(
            delta <= 64,
            "keep-alive FD delta {delta} exceeded sustained threshold 64"
        );
    }
    if let Some(delta) = fd_delta_after_drop {
        assert!(
            delta <= 8,
            "post-drop leaked FD delta {delta} exceeded threshold 8"
        );
    }
    if let Some(rss_delta) = rss_delta_bytes {
        assert!(
            rss_delta <= 64 * 1024 * 1024,
            "RSS delta {rss_delta} bytes exceeded 64 MiB regression threshold"
        );
    }
}

#[cfg(all(feature = "neon", feature = "supabase"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn burst_fault_cancellation_oversized_churn_and_timeout_semantics_baseline() {
    use dactyl_db::{
        AccessMode, AdapterErrorKind, Connection, DatastoreRoute, FailureOrigin, MutationOutcome,
        OpenOptions, Parameter, StorageContext,
    };

    let _gate = TEST_SUITE_GATE.lock().await;
    let burst_ops: usize = 160;

    let (origin, stats) = spawn_instrumented_server(burst_ops + 16, |_head, _target, body| async move {
        let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
        let seq = parsed
            .get("params")
            .and_then(|p| p.as_array())
            .and_then(|arr| arr.first())
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as usize;
        match seq % 10 {
            1 => ServerAction::StallBeforeHeaders {
                delay: Duration::from_millis(180),
                status: 200,
                body: serde_json::to_vec(&serde_json::json!({
                    "columns": ["ok"], "rows": [{"ok": 1}], "affected_rows": 1, "generated_keys": []
                }))
                .unwrap(),
            },
            2 => ServerAction::StallAfterHeaders {
                header_delay: Duration::from_millis(5),
                body_delay: Duration::from_millis(180),
                body: serde_json::to_vec(&serde_json::json!({
                    "columns": ["ok"], "rows": [{"ok": 1}], "affected_rows": 1, "generated_keys": []
                }))
                .unwrap(),
            },
            3 => ServerAction::AbortConnection {
                send_headers_first: true,
            },
            4 => ServerAction::UnframedOversized { total_bytes: 16_384 },
            _ => {
                tokio::time::sleep(Duration::from_millis(4)).await;
                let resp = serde_json::to_vec(&serde_json::json!({
                    "columns": ["seq"], "rows": [{"seq": seq as i64}], "affected_rows": 1, "generated_keys": []
                }))
                .unwrap();
                ServerAction::Json {
                    status: 200,
                    keep_alive: true,
                    body: resp,
                    seq: Some(seq),
                }
            }
        }
    })
    .await;

    let pooled_budget = ResourceBudget::v1()
        .with_http_pool_limits(6, 6, Duration::from_secs(5))
        .with_max_wire_bytes(2048)
        .with_request_timeout(Duration::from_millis(95));
    let churn_budget = ResourceBudget::v1()
        .with_http_pool_limits(4, 0, Duration::from_secs(5))
        .with_max_wire_bytes(2048)
        .with_request_timeout(Duration::from_millis(95));

    let ctx = StorageContext::new(1, serde_json::json!({"org_id": "burst"})).unwrap();
    let neon_pooled = Arc::new(
        Connection::open_with_budget(
            DatastoreRoute::neon(format!("{origin}/sql"), Some("tok-p".into())),
            OpenOptions {
                access_mode: AccessMode::ReadWrite,
                ..OpenOptions::default()
            },
            Some(ctx.clone()),
            pooled_budget,
        )
        .unwrap(),
    );
    let supa_churn = Arc::new(
        Connection::open_with_budget(
            DatastoreRoute::supabase(&origin, Some("tok-c".into())),
            OpenOptions {
                access_mode: AccessMode::ReadWrite,
                ..OpenOptions::default()
            },
            Some(ctx.clone()),
            churn_budget,
        )
        .unwrap(),
    );

    let fd_before = open_fd_count();
    let rss_before = process_rss_bytes();
    let mut prng = SplitMix64::new(0xD4C7_1440_2026_0002);

    let mut handles = Vec::with_capacity(burst_ops);
    let ok_count = Arc::new(AtomicUsize::new(0));
    let cancelled_count = Arc::new(AtomicUsize::new(0));
    let typed_fault_count = Arc::new(AtomicUsize::new(0));

    for seq in 1..=burst_ops {
        let neon = neon_pooled.clone();
        let supa = supa_churn.clone();
        let ok_c = ok_count.clone();
        let can_c = cancelled_count.clone();
        let fault_c = typed_fault_count.clone();
        let cancel_me = seq % 10 == 5;
        let use_sync = seq % 10 == 6;
        let use_churn = prng.next_range(4) == 0;

        if use_sync {
            handles.push(tokio::task::spawn_blocking(move || {
                let res = neon.write("UPDATE t SET v = $1", &[Parameter::Integer(seq as i64)]);
                match res {
                    Ok(_) => {
                        ok_c.fetch_add(1, Ordering::SeqCst);
                    }
                    Err(err) => {
                        assert!(matches!(
                            err.adapter_kind(),
                            Some(
                                AdapterErrorKind::Timeout
                                    | AdapterErrorKind::Transport
                                    | AdapterErrorKind::Protocol
                            )
                        ));
                        fault_c.fetch_add(1, Ordering::SeqCst);
                    }
                }
            }));
            continue;
        }

        handles.push(tokio::spawn(async move {
            if cancel_me {
                // Drop future abruptly after 12ms (cancels either queued permit or in-flight HTTP).
                let params = [Parameter::Integer(seq as i64)];
                let fut = neon.write_async("UPDATE t SET v = $1", &params);
                if tokio::time::timeout(Duration::from_millis(12), fut)
                    .await
                    .is_err()
                {
                    can_c.fetch_add(1, Ordering::SeqCst);
                } else {
                    ok_c.fetch_add(1, Ordering::SeqCst);
                }
                return;
            }

            let conn = if use_churn { &supa } else { &neon };
            let res = conn
                .write_async("UPDATE t SET v = $1", &[Parameter::Integer(seq as i64)])
                .await;
            match (seq % 10, res) {
                (1 | 2, Err(err)) => {
                    assert_eq!(err.adapter_kind(), Some(AdapterErrorKind::Timeout));
                    // Could be NotAttempted if deadline expired in queue, or Uncertain if post-send.
                    assert!(matches!(
                        err.mutation_outcome(),
                        MutationOutcome::Uncertain | MutationOutcome::NotAttempted
                    ));
                    fault_c.fetch_add(1, Ordering::SeqCst);
                }
                (3, Err(err)) => {
                    assert!(matches!(
                        err.adapter_kind(),
                        Some(AdapterErrorKind::Transport | AdapterErrorKind::Timeout)
                    ));
                    assert!(matches!(
                        err.mutation_outcome(),
                        MutationOutcome::Uncertain | MutationOutcome::NotAttempted
                    ));
                    fault_c.fetch_add(1, Ordering::SeqCst);
                }
                (4, Err(err)) => {
                    if err.adapter_kind() == Some(AdapterErrorKind::Protocol) {
                        assert_eq!(err.adapter_code(), Some("response_too_large"));
                        assert_eq!(err.failure_origin(), FailureOrigin::MalformedResponse);
                        assert_eq!(err.mutation_outcome(), MutationOutcome::Uncertain);
                    } else {
                        assert_eq!(err.adapter_kind(), Some(AdapterErrorKind::Timeout));
                        assert!(matches!(
                            err.mutation_outcome(),
                            MutationOutcome::NotAttempted | MutationOutcome::Uncertain
                        ));
                    }
                    fault_c.fetch_add(1, Ordering::SeqCst);
                }
                (_, Ok(_)) => {
                    ok_c.fetch_add(1, Ordering::SeqCst);
                }
                (_, Err(err)) => {
                    // Burst contention may legitimately cause queue timeout.
                    assert_eq!(err.adapter_kind(), Some(AdapterErrorKind::Timeout));
                    fault_c.fetch_add(1, Ordering::SeqCst);
                }
            }
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    // Allow abrupt TCP closes and dropped reqwest tasks a brief moment to settle.
    tokio::time::sleep(Duration::from_millis(80)).await;

    // Post-burst health probe: verify ALL 6 slots of pooled_budget & ALL 4 slots of churn_budget
    // are completely released (no leaked slots or deadlocks after cancellations/errors!).
    let healthy_budget = pooled_budget.with_request_timeout(Duration::from_secs(2));
    let probe_conn = Arc::new(
        Connection::open_with_budget(
            DatastoreRoute::neon(format!("{origin}/sql"), Some("tok-probe".into())),
            OpenOptions::default(),
            Some(ctx),
            healthy_budget,
        )
        .unwrap(),
    );
    let mut probe_tasks = Vec::new();
    for seq in 0..12usize {
        let p = probe_conn.clone();
        probe_tasks.push(tokio::spawn(async move {
            let rows = p
                .read_async("SELECT $1", &[Parameter::Integer((seq * 10) as i64)])
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
        }));
    }
    for task in probe_tasks {
        task.await.unwrap();
    }

    drop((neon_pooled, supa_churn, probe_conn));
    stats.shutdown.store(true, Ordering::Relaxed);
    tokio::time::sleep(Duration::from_millis(250)).await;

    let fd_after = open_fd_count();
    let rss_after = process_rss_bytes();
    let fd_delta = match (fd_before, fd_after) {
        (Some(b), Some(a)) => Some(a as isize - b as isize),
        _ => None,
    };

    println!(
        "BASELINE_BURST_144={{\"burst_ops\":{},\"ok\":{},\"cancelled\":{},\"typed_faults\":{},\"accepted_conns\":{},\"open_conns_after\":{},\"fd_before\":{:?},\"fd_after_drop\":{:?},\"fd_delta\":{:?},\"rss_before_bytes\":{:?},\"rss_after_bytes\":{:?}}}",
        burst_ops,
        ok_count.load(Ordering::SeqCst),
        cancelled_count.load(Ordering::SeqCst),
        typed_fault_count.load(Ordering::SeqCst),
        stats.accepted.load(Ordering::SeqCst),
        stats.open_connections.load(Ordering::SeqCst),
        fd_before,
        fd_after,
        fd_delta,
        rss_before,
        rss_after,
    );

    assert!(ok_count.load(Ordering::SeqCst) > 0);
    assert!(cancelled_count.load(Ordering::SeqCst) > 0);
    assert!(typed_fault_count.load(Ordering::SeqCst) > 0);
    if let Some(delta) = fd_delta {
        assert!(
            delta <= 16,
            "burst post-drop open FD delta {delta} exceeded regression threshold 16"
        );
    }
}

#[cfg(all(feature = "neon", feature = "supabase"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deadlines_cover_queue_plus_execution_and_presend_timeouts_stay_not_attempted() {
    use dactyl_db::{
        AccessMode, AccountOperation, AdapterErrorKind, CloudConnector, CloudResponseMetadata,
        Connection, DatastoreRoute, EffectStatus, MutationOutcome, ObservationSource, OpenOptions,
        OperationAttempt, Parameter, StorageContext, SupabaseRpcConfig,
    };

    let _gate = TEST_SUITE_GATE.lock().await;
    let (origin, stats) = spawn_instrumented_server(32, |_head, target, _body| async move {
        tokio::time::sleep(Duration::from_millis(90)).await;
        if target.starts_with("POST /query") {
            ServerAction::Json {
                status: 200,
                keep_alive: true,
                body: serde_json::to_vec(&serde_json::json!({
                    "columns": ["ok"],
                    "rows": [{"ok": 1}],
                    "affected_rows": 1,
                    "generated_keys": []
                }))
                .unwrap(),
                seq: None,
            }
        } else {
            ServerAction::Json {
                status: 200,
                keep_alive: true,
                body: serde_json::to_vec(&serde_json::json!({ "status": "ok" })).unwrap(),
                seq: None,
            }
        }
    })
    .await;

    let slow_budget = ResourceBudget::v1()
        .with_http_pool_limits(1, 1, Duration::from_secs(5))
        .with_request_timeout(Duration::from_millis(500));
    let tight_budget = ResourceBudget::v1()
        .with_http_pool_limits(1, 1, Duration::from_secs(5))
        .with_request_timeout(Duration::from_millis(35));
    // Queue (~90ms) + execution (~90ms) = ~180ms > combined_budget (135ms)!
    let combined_budget = ResourceBudget::v1()
        .with_http_pool_limits(1, 1, Duration::from_secs(5))
        .with_request_timeout(Duration::from_millis(135));

    let ctx = StorageContext::new(1, serde_json::json!({"org_id": "deadline"})).unwrap();
    let holder = Connection::open_with_budget(
        DatastoreRoute::neon(format!("{origin}/sql"), Some("tok".into())),
        OpenOptions {
            access_mode: AccessMode::ReadWrite,
            ..OpenOptions::default()
        },
        Some(ctx.clone()),
        slow_budget,
    )
    .unwrap();
    let neon_tight = Connection::open_with_budget(
        DatastoreRoute::neon(format!("{origin}/sql"), Some("tok".into())),
        OpenOptions {
            access_mode: AccessMode::ReadWrite,
            ..OpenOptions::default()
        },
        Some(ctx.clone()),
        tight_budget,
    )
    .unwrap();
    let supa_tight = Connection::open_with_budget(
        DatastoreRoute::supabase(&origin, Some("tok".into())),
        OpenOptions {
            access_mode: AccessMode::ReadWrite,
            ..OpenOptions::default()
        },
        Some(ctx.clone()),
        tight_budget,
    )
    .unwrap();
    let cloud_tight =
        CloudConnector::with_config(SupabaseRpcConfig::v1(&origin, "sb_publishable_t"))
            .unwrap()
            .with_http_budget(tight_budget)
            .unwrap();
    let neon_combined = Connection::open_with_budget(
        DatastoreRoute::neon(format!("{origin}/sql"), Some("tok".into())),
        OpenOptions {
            access_mode: AccessMode::ReadWrite,
            ..OpenOptions::default()
        },
        Some(ctx),
        combined_budget,
    )
    .unwrap();

    // Hold the single shared slot for ~90ms while all 3 tight-timeout callers queue concurrently.
    let hold_task = tokio::spawn(async move {
        holder
            .write_async("UPDATE t SET v = $1", &[Parameter::Integer(1)])
            .await
            .unwrap()
    });
    tokio::time::sleep(Duration::from_millis(10)).await;

    // 1. Pre-send queue timeouts across Neon, Supabase, and CloudConnector MUST be NotAttempted.
    let att_supa = OperationAttempt::new("deadline-supa").unwrap();
    let mut meta_supa = CloudResponseMetadata::default();
    let att_cloud = OperationAttempt::new("deadline-cloud").unwrap();
    let mut meta_cloud = CloudResponseMetadata::default();
    let checkout_body = serde_json::json!({"version": 1});

    let (neon_res, supa_res, cloud_res) = tokio::join!(
        neon_tight.write_async("UPDATE t SET v = $1", &[Parameter::Integer(2)]),
        supa_tight.write_with_attempt_async(
            "UPDATE t SET v = $1",
            &[Parameter::Integer(3)],
            &att_supa,
            &mut meta_supa,
        ),
        cloud_tight.account_request_with_attempt_async(
            AccountOperation::Checkout,
            &checkout_body,
            "bearer-t",
            None,
            None,
            &att_cloud,
            &mut meta_cloud,
        ),
    );

    let neon_err = neon_res.unwrap_err();
    assert_eq!(neon_err.adapter_kind(), Some(AdapterErrorKind::Timeout));
    assert_eq!(neon_err.mutation_outcome(), MutationOutcome::NotAttempted);

    let supa_err = supa_res.unwrap_err();
    assert_eq!(supa_err.adapter_kind(), Some(AdapterErrorKind::Timeout));
    assert_eq!(supa_err.mutation_outcome(), MutationOutcome::NotAttempted);
    let prov_supa = meta_supa.provenance().unwrap();
    assert_eq!(prov_supa.status(), EffectStatus::NotAttempted);
    assert_eq!(
        prov_supa.observation_source(),
        ObservationSource::LocalPreflight
    );

    let cloud_err = cloud_res.unwrap_err();
    assert_eq!(cloud_err.adapter_kind(), Some(AdapterErrorKind::Timeout));
    assert_eq!(cloud_err.mutation_outcome(), MutationOutcome::NotAttempted);
    let prov_cloud = meta_cloud.provenance().unwrap();
    assert_eq!(prov_cloud.status(), EffectStatus::NotAttempted);
    assert_eq!(
        prov_cloud.observation_source(),
        ObservationSource::LocalPreflight
    );

    hold_task.await.unwrap();
    // Only the holder request reached the server (3 queue timeouts never dispatched!).
    assert_eq!(stats.requests.load(Ordering::SeqCst), 1);

    // 2. Combined queue + execution budget proof:
    // Holder runs 90ms; combined_budget (135ms) enters queue at t=10ms, acquires slot at t=90ms
    // with ~55ms left, dispatches to server (which takes 90ms), and therefore times out POST-SEND
    // with MutationOutcome::Uncertain!
    let holder2 = Connection::open_with_budget(
        DatastoreRoute::neon(format!("{origin}/sql"), Some("tok".into())),
        OpenOptions {
            access_mode: AccessMode::ReadWrite,
            ..OpenOptions::default()
        },
        Some(StorageContext::new(1, serde_json::json!({"org_id": "deadline2"})).unwrap()),
        slow_budget,
    )
    .unwrap();
    let hold2 = tokio::spawn(async move {
        holder2
            .write_async("UPDATE t SET v = $1", &[Parameter::Integer(4)])
            .await
            .unwrap()
    });
    tokio::time::sleep(Duration::from_millis(10)).await;
    let combined_err = neon_combined
        .write_async("UPDATE t SET v = $1", &[Parameter::Integer(5)])
        .await
        .unwrap_err();
    assert_eq!(combined_err.adapter_kind(), Some(AdapterErrorKind::Timeout));
    assert_eq!(combined_err.mutation_outcome(), MutationOutcome::Uncertain);
    hold2.await.unwrap();
    assert_eq!(stats.requests.load(Ordering::SeqCst), 3);

    stats.shutdown.store(true, Ordering::Relaxed);
}

#[cfg(any(feature = "neon", feature = "supabase"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn aggregate_process_ceiling_and_distinct_budget_pool_bounds_under_mixed_load() {
    use dactyl_db::{
        AdapterErrorKind, Connection, DatastoreRoute, MutationOutcome, OpenOptions, Parameter,
        StorageContext,
    };

    let make_route = |origin: &str| {
        #[cfg(feature = "neon")]
        {
            DatastoreRoute::neon(format!("{origin}/sql"), Some("tok".into()))
        }
        #[cfg(not(feature = "neon"))]
        {
            DatastoreRoute::supabase(origin, Some("tok".into()))
        }
    };

    let _exclusive_gate = TEST_SUITE_GATE.lock().await;

    let (origin, stats) = spawn_instrumented_server(256, |_head, _target, _body| async move {
        tokio::time::sleep(Duration::from_millis(60)).await;
        ServerAction::Json {
            status: 200,
            keep_alive: true,
            body: serde_json::to_vec(&serde_json::json!({
                "columns": ["ok"],
                "rows": [{"ok": 1}],
                "affected_rows": 0,
                "generated_keys": []
            }))
            .unwrap(),
            seq: None,
        }
    })
    .await;

    let ctx = StorageContext::new(1, serde_json::json!({"org_id": "process-cap"})).unwrap();

    // 1. Live distinct budget pool limit = 64.
    let mut conns = Vec::with_capacity(64);
    for i in 0..64u64 {
        let budget = ResourceBudget::v1()
            .with_http_pool_limits(2, 1, Duration::from_millis(100 + i))
            .with_request_timeout(Duration::from_secs(2));
        conns.push(Arc::new(
            Connection::open_with_budget(
                make_route(&origin),
                OpenOptions::default(),
                Some(ctx.clone()),
                budget,
            )
            .unwrap(),
        ));
    }
    let overflow = Connection::open_with_budget(
        make_route(&origin),
        OpenOptions::default(),
        Some(ctx.clone()),
        ResourceBudget::v1().with_http_pool_limits(2, 1, Duration::from_millis(999)),
    )
    .err()
    .expect("65th live distinct budget pool must fail closed");
    assert_eq!(overflow.adapter_kind(), Some(AdapterErrorKind::Unavailable));
    assert_eq!(overflow.mutation_outcome(), MutationOutcome::NotAttempted);

    // 2. Drop 1 pool, verify recovery, then launch 96 concurrent requests across the 64 distinct
    //    pools (each pool has capacity 2, total scoped capacity = 128) and verify server
    //    `peak_in_flight <= 64` (MAX_PROCESS_IN_FLIGHT).
    conns.pop();
    conns.push(Arc::new(
        Connection::open_with_budget(
            make_route(&origin),
            OpenOptions::default(),
            Some(ctx),
            ResourceBudget::v1()
                .with_http_pool_limits(2, 1, Duration::from_millis(999))
                .with_request_timeout(Duration::from_secs(2)),
        )
        .unwrap(),
    ));

    let mut tasks = Vec::with_capacity(96);
    for idx in 0..96usize {
        let conn = conns[idx % 64].clone();
        tasks.push(tokio::spawn(async move {
            let rows = conn
                .read_async("SELECT $1", &[Parameter::Integer(idx as i64)])
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }

    let peak = stats.peak_in_flight.load(Ordering::SeqCst);
    assert_eq!(stats.requests.load(Ordering::SeqCst), 96);
    assert!(
        peak <= 64,
        "process-wide peak in-flight {peak} exceeded MAX_PROCESS_IN_FLIGHT = 64"
    );

    stats.shutdown.store(true, Ordering::Relaxed);
}
