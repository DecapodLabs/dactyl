//! Conformance and regression tests for Dactyl #134: runtime-aware HTTP
//! connection pooling, `ResourceBudget` concurrency/keep-alive limits, and
//! high-concurrency async fanout across `Neon`, `Supabase`, and `CloudConnector`.
#![cfg(any(feature = "neon", feature = "supabase"))]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use dactyl_db::ResourceBudget;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[derive(Clone)]
struct PoolStats {
    accepted: Arc<AtomicUsize>,
    requests: Arc<AtomicUsize>,
    in_flight: Arc<AtomicUsize>,
    peak_in_flight: Arc<AtomicUsize>,
}

impl PoolStats {
    fn new() -> Self {
        Self {
            accepted: Arc::new(AtomicUsize::new(0)),
            requests: Arc::new(AtomicUsize::new(0)),
            in_flight: Arc::new(AtomicUsize::new(0)),
            peak_in_flight: Arc::new(AtomicUsize::new(0)),
        }
    }
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

async fn spawn_keepalive_server<F, Fut>(handler: F) -> (String, PoolStats)
where
    F: Fn(String, String, Vec<u8>) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = (u16, String, Vec<u8>)> + Send + 'static,
{
    let stats = PoolStats::new();
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
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                stats_accept.accepted.fetch_add(1, Ordering::SeqCst);
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
                        let (status, extra_headers, resp_body) =
                            handler_conn(head, target, body).await;
                        stats_conn.in_flight.fetch_sub(1, Ordering::SeqCst);
                        let header = format!(
                            "HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: keep-alive\r\n{extra_headers}\r\n",
                            resp_body.len()
                        );
                        if stream.write_all(header.as_bytes()).await.is_err() {
                            break;
                        }
                        if stream.write_all(&resp_body).await.is_err() {
                            break;
                        }
                        let _ = stream.flush().await;
                    }
                });
            }
        });
    });

    (rx.recv().unwrap(), stats)
}

#[test]
fn http_pool_budget_rejects_invalid_limits_and_preserves_defaults() {
    let default = ResourceBudget::v1();
    assert_eq!(default.http_max_in_flight(), 16);
    assert_eq!(default.http_max_idle_per_host(), 32);
    assert_eq!(default.http_idle_timeout(), Duration::from_secs(15));
    assert!(default.validate().is_ok());

    let min_valid = ResourceBudget::v1().with_http_pool_limits(1, 0, Duration::from_millis(1));
    assert!(min_valid.validate().is_ok());

    let max_valid = ResourceBudget::v1().with_http_pool_limits(64, 64, Duration::from_secs(30));
    assert!(max_valid.validate().is_ok());

    for invalid in [
        ResourceBudget::v1().with_http_pool_limits(0, 8, Duration::from_secs(5)),
        ResourceBudget::v1().with_http_pool_limits(65, 8, Duration::from_secs(5)),
        ResourceBudget::v1().with_http_pool_limits(8, 65, Duration::from_secs(5)),
        ResourceBudget::v1().with_http_pool_limits(8, 8, Duration::ZERO),
    ] {
        assert!(invalid.validate().is_err());
    }
}

#[cfg(feature = "neon")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn neon_async_and_sync_share_runtime_aware_pool_and_enforce_in_flight_limit() {
    use dactyl_db::{
        AccessMode, Connection, DatastoreRoute, OpenOptions, Operation, Parameter, StorageContext,
    };

    let (origin, stats) = spawn_keepalive_server(|_head, _target, body| async move {
        tokio::time::sleep(Duration::from_millis(10)).await;
        let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
        if let Some(statements) = parsed.get("operations").and_then(|v| v.as_array()) {
            let results: Vec<_> = statements
                .iter()
                .map(|_| {
                    serde_json::json!({
                        "columns": ["ok"],
                        "rows": [{"ok": "1"}],
                        "affected_rows": 1,
                        "generated_keys": []
                    })
                })
                .collect();
            return (
                200,
                String::new(),
                serde_json::to_vec(&serde_json::json!({ "results": results })).unwrap(),
            );
        }
        (
            200,
            String::new(),
            serde_json::to_vec(&serde_json::json!({
                "columns": ["id"],
                "rows": [{"id": "42"}],
                "affected_rows": 1,
                "generated_keys": []
            }))
            .unwrap(),
        )
    })
    .await;

    let budget = ResourceBudget::v1()
        .with_http_pool_limits(4, 4, Duration::from_secs(5))
        .with_request_timeout(Duration::from_secs(5));
    let route = DatastoreRoute::neon(format!("{origin}/sql"), Some("neon-token".into()));
    let conn = Arc::new(
        Connection::open_with_budget(
            route,
            OpenOptions {
                access_mode: AccessMode::ReadWrite,
                ..OpenOptions::default()
            },
            Some(StorageContext::new(1, serde_json::json!({"org_id": "org"})).unwrap()),
            budget,
        )
        .unwrap(),
    );

    let mut handles = Vec::new();
    for i in 0..40usize {
        let conn = conn.clone();
        handles.push(tokio::spawn(async move {
            match i % 4 {
                0 => {
                    let rows = conn
                        .read_async("SELECT $1", &[Parameter::Integer(i as i64)])
                        .await
                        .unwrap();
                    assert_eq!(rows.len(), 1);
                }
                1 => {
                    let mut seen = 0u64;
                    let outcome = conn
                        .visit_rows_async("SELECT $1", &[Parameter::Integer(i as i64)], |_row| {
                            seen += 1;
                            Ok(true)
                        })
                        .await
                        .unwrap();
                    assert_eq!(outcome.rows, 1);
                    assert_eq!(seen, 1);
                }
                2 => {
                    let write = conn
                        .write_result_async(
                            "UPDATE items SET v = $1",
                            &[Parameter::Integer(i as i64)],
                        )
                        .await
                        .unwrap();
                    assert_eq!(write.affected_rows, 1);
                }
                _ => {
                    let atomic = conn
                        .atomic_async(&[Operation::write(
                            "UPDATE items SET v = $1",
                            vec![Parameter::Integer(i as i64)],
                        )])
                        .await
                        .unwrap();
                    assert_eq!(atomic.results.len(), 1);
                }
            }
        }));
    }
    for handle in handles {
        handle.await.unwrap();
    }

    assert_eq!(stats.requests.load(Ordering::SeqCst), 40);
    assert!(
        stats.peak_in_flight.load(Ordering::SeqCst) <= 4,
        "peak concurrent requests {} exceeded budget limit 4",
        stats.peak_in_flight.load(Ordering::SeqCst)
    );
    assert!(
        stats.accepted.load(Ordering::SeqCst) < 40,
        "expected HTTP keep-alive connection reuse, accepted {}",
        stats.accepted.load(Ordering::SeqCst)
    );
}

#[cfg(feature = "neon")]
#[tokio::test(flavor = "current_thread")]
async fn neon_sync_facade_works_inside_current_thread_tokio_runtime() {
    use dactyl_db::{AccessMode, Connection, DatastoreRoute, OpenOptions, StorageContext};

    let (origin, stats) = spawn_keepalive_server(|_head, _target, _body| async move {
        (
            200,
            String::new(),
            serde_json::to_vec(&serde_json::json!({
                "columns": ["ok"],
                "rows": [{"ok": "1"}],
                "affected_rows": 0,
                "generated_keys": []
            }))
            .unwrap(),
        )
    })
    .await;

    let route = DatastoreRoute::neon(format!("{origin}/sql"), Some("neon-token".into()));
    let conn = Connection::open_with_budget(
        route,
        OpenOptions {
            access_mode: AccessMode::ReadOnly,
            ..OpenOptions::default()
        },
        Some(StorageContext::new(1, serde_json::json!({"org_id": "org"})).unwrap()),
        ResourceBudget::v1().with_http_pool_limits(2, 2, Duration::from_secs(5)),
    )
    .unwrap();

    let sync_rows = conn.read("SELECT 1", &[]).unwrap();
    assert_eq!(sync_rows.len(), 1);
    let async_rows = conn.read_async("SELECT 1", &[]).await.unwrap();
    assert_eq!(async_rows.len(), 1);
    assert_eq!(stats.requests.load(Ordering::SeqCst), 2);
}

#[cfg(feature = "supabase")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn supabase_and_cloud_async_250_fanout_avoids_fd_exhaustion_and_starvation() {
    use dactyl_db::{
        AccessMode, AccountOperation, CloudResponseMetadata, Connection, DatastoreRoute,
        LogicalConnector, OpenOptions, OperationAttempt, OperationKind, Parameter, StorageContext,
        SupabaseRpcConfig,
    };

    let (origin, stats) = spawn_keepalive_server(|_head, target, body| async move {
        tokio::time::sleep(Duration::from_millis(6)).await;
        if target.starts_with("POST /query") {
            let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
            let org = parsed
                .get("context")
                .and_then(|c| c.get("payload"))
                .and_then(|p| p.get("org_id"))
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_owned();
            return (
                200,
                String::new(),
                serde_json::to_vec(&serde_json::json!({
                    "columns": ["org"],
                    "rows": [{"org": org}],
                    "affected_rows": 1,
                    "generated_keys": []
                }))
                .unwrap(),
            );
        }
        if target.starts_with("POST /rest/v1/rpc/dactyl_control") {
            return (
                200,
                String::new(),
                serde_json::to_vec(&serde_json::json!({ "ok": true })).unwrap(),
            );
        }
        (
            200,
            String::new(),
            serde_json::to_vec(&serde_json::json!({ "status": "active" })).unwrap(),
        )
    })
    .await;

    let budget = ResourceBudget::v1()
        .with_http_pool_limits(8, 8, Duration::from_secs(5))
        .with_request_timeout(Duration::from_secs(5));
    let rpc = SupabaseRpcConfig::v1(&origin, "sb_publishable_async250");
    let connector = Arc::new(
        LogicalConnector::cloud_with_config(rpc.clone())
            .unwrap()
            .with_http_budget(budget)
            .unwrap(),
    );

    let conn_a = Arc::new(
        Connection::open_with_budget(
            DatastoreRoute::supabase(&origin, Some("bearer-tenant-a".into())),
            OpenOptions {
                access_mode: AccessMode::ReadWrite,
                ..OpenOptions::default()
            },
            Some(
                StorageContext::new(
                    1,
                    serde_json::json!({"org_id": "org-alpha", "repo_id": "repo-alpha"}),
                )
                .unwrap(),
            ),
            budget,
        )
        .unwrap(),
    );
    let conn_b = Arc::new(
        Connection::open_with_budget(
            DatastoreRoute::supabase(&origin, Some("bearer-tenant-b".into())),
            OpenOptions {
                access_mode: AccessMode::ReadWrite,
                ..OpenOptions::default()
            },
            Some(
                StorageContext::new(
                    1,
                    serde_json::json!({"org_id": "org-beta", "repo_id": "repo-beta"}),
                )
                .unwrap(),
            ),
            budget,
        )
        .unwrap(),
    );

    let ticks = Arc::new(AtomicUsize::new(0));
    let ticks_bg = ticks.clone();
    let heartbeat = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(4));
        for _ in 0..10 {
            interval.tick().await;
            ticks_bg.fetch_add(1, Ordering::SeqCst);
        }
    });

    let mut tasks = Vec::with_capacity(250);
    for i in 0..250usize {
        let connector = connector.clone();
        let conn_a = conn_a.clone();
        let conn_b = conn_b.clone();
        tasks.push(tokio::spawn(async move {
            match i % 4 {
                0 => {
                    let rows = conn_a
                        .read_async(
                            "SELECT org FROM t WHERE id = $1",
                            &[Parameter::Integer(i as i64)],
                        )
                        .await
                        .unwrap();
                    assert_eq!(
                        rows.0.first().unwrap().get::<_, String>("org").unwrap(),
                        "org-alpha"
                    );
                }
                1 => {
                    let attempt = OperationAttempt::new(format!("fanout-att-{i}")).unwrap();
                    let mut meta = CloudResponseMetadata::default();
                    let write = conn_b
                        .write_with_attempt_async(
                            "INSERT INTO t VALUES ($1)",
                            &[Parameter::Integer(i as i64)],
                            &attempt,
                            &mut meta,
                        )
                        .await
                        .unwrap();
                    assert_eq!(write.affected_rows, 1);
                }
                2 => {
                    let mut meta = CloudResponseMetadata::default();
                    let out = connector
                        .account_request_with_metadata_async(
                            AccountOperation::Status,
                            &serde_json::json!({"version": 1}),
                            "bearer-account",
                            None,
                            None,
                            &mut meta,
                        )
                        .await
                        .unwrap();
                    assert_eq!(out["status"], "active");
                }
                _ => {
                    let attempt = OperationAttempt::new(format!("ctrl-att-{i}")).unwrap();
                    let mut meta = CloudResponseMetadata::default();
                    let out = connector
                        .control_request_with_attempt_async(
                            "dactyl_control",
                            &serde_json::json!({"idx": i}),
                            Some("bearer-ctrl"),
                            None,
                            OperationKind::Read,
                            budget,
                            &attempt,
                            &mut meta,
                        )
                        .await
                        .unwrap();
                    assert_eq!(out["ok"], true);
                }
            }
        }));
    }

    for task in tasks {
        task.await.unwrap();
    }
    heartbeat.await.unwrap();

    assert_eq!(stats.requests.load(Ordering::SeqCst), 250);
    assert!(
        ticks.load(Ordering::SeqCst) >= 5,
        "expected async heartbeat ticks without starvation"
    );
    assert!(
        stats.peak_in_flight.load(Ordering::SeqCst) <= 8,
        "peak concurrent requests {} exceeded configured http_max_in_flight=8",
        stats.peak_in_flight.load(Ordering::SeqCst)
    );
    assert!(
        stats.accepted.load(Ordering::SeqCst) <= 24,
        "expected aggressive connection pooling instead of 250 sockets; accepted={}",
        stats.accepted.load(Ordering::SeqCst)
    );
}

#[cfg(feature = "supabase")]
#[tokio::test(flavor = "current_thread")]
async fn cloud_connector_http_budget_validates_enforces_slot_saturation_and_runs_sync_safely() {
    use dactyl_db::{
        AccountOperation, AdapterErrorKind, CloudConnector, CloudResponseMetadata, Connection,
        DatastoreRoute, EffectStatus, LogicalConnector, ObservationSource, OpenOptions,
        OperationAttempt, StorageContext, SupabaseRpcConfig,
    };

    let config = SupabaseRpcConfig::v1("http://127.0.0.1:9", "sb_publishable_invalidbudget");
    let invalid_budget = ResourceBudget::v1().with_http_pool_limits(0, 8, Duration::from_secs(1));
    assert!(CloudConnector::with_config(config.clone())
        .unwrap()
        .with_http_budget(invalid_budget)
        .is_err());
    assert!(LogicalConnector::cloud_with_config(config)
        .unwrap()
        .with_http_budget(invalid_budget)
        .is_err());

    let (origin, stats) = spawn_keepalive_server(|_head, target, _body| async move {
        if target.starts_with("POST /query") {
            return (
                200,
                String::new(),
                serde_json::to_vec(&serde_json::json!({
                    "columns": ["ok"],
                    "rows": [{"ok": 1}],
                    "affected_rows": 0,
                    "generated_keys": []
                }))
                .unwrap(),
            );
        }
        tokio::time::sleep(Duration::from_millis(120)).await;
        (
            200,
            String::new(),
            serde_json::to_vec(&serde_json::json!({ "status": "ok" })).unwrap(),
        )
    })
    .await;

    // Verify sync Connection inside current_thread runtime still executes cleanly.
    let conn = Connection::open_with_budget(
        DatastoreRoute::supabase(&origin, Some("bearer-sync".into())),
        OpenOptions::default(),
        Some(
            StorageContext::new(1, serde_json::json!({"org_id": "org", "repo_id": "repo"}))
                .unwrap(),
        ),
        ResourceBudget::v1().with_http_pool_limits(2, 2, Duration::from_secs(5)),
    )
    .unwrap();
    assert_eq!(conn.read("SELECT 1", &[]).unwrap().len(), 1);
    assert_eq!(conn.read_async("SELECT 1", &[]).await.unwrap().len(), 1);

    // Saturate http_max_in_flight = 1 and assert the second request fails locally
    // before sending (EffectStatus::NotAttempted / LocalPreflight).
    let tight_budget = ResourceBudget::v1()
        .with_http_pool_limits(1, 1, Duration::from_secs(5))
        .with_request_timeout(Duration::from_millis(45));
    let slow_budget = ResourceBudget::v1()
        .with_http_pool_limits(1, 1, Duration::from_secs(5))
        .with_request_timeout(Duration::from_millis(500));

    let rpc = SupabaseRpcConfig::v1(&origin, "sb_publishable_saturated");
    let first_conn = CloudConnector::with_config(rpc.clone())
        .unwrap()
        .with_http_budget(slow_budget)
        .unwrap();
    let second_conn = CloudConnector::with_config(rpc)
        .unwrap()
        .with_http_budget(tight_budget)
        .unwrap();
    let conn_saturated = Arc::new(
        CloudConnector::with_config(SupabaseRpcConfig::v1(&origin, "sb_publishable_saturated"))
            .unwrap()
            .with_http_budget(tight_budget)
            .unwrap(),
    );
    let _ = second_conn;

    let first_task = tokio::spawn(async move {
        let mut meta = CloudResponseMetadata::default();
        first_conn
            .account_request_with_metadata_async(
                AccountOperation::Status,
                &serde_json::json!({"version": 1}),
                "bearer-slow",
                None,
                None,
                &mut meta,
            )
            .await
            .unwrap()
    });
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Use the exact same PoolSettings budget (via conn_saturated's cloned budget
    // inside one task holding slot) — note slow_budget and tight_budget differ in
    // PoolSettings ONLY by pool limits, NOT request_timeout! So first_conn and
    // conn_saturated SHARE the same 1-slot SharedHttpClient!
    let attempt = OperationAttempt::new("att-saturated").unwrap();
    let mut meta = CloudResponseMetadata::default();
    let err = conn_saturated
        .account_request_with_attempt_async(
            AccountOperation::Checkout,
            &serde_json::json!({"version": 1}),
            "bearer-sat",
            None,
            None,
            &attempt,
            &mut meta,
        )
        .await
        .unwrap_err();
    assert_eq!(err.adapter_kind(), Some(AdapterErrorKind::Timeout));
    let prov = meta
        .provenance()
        .expect("local preflight timeout provenance");
    assert_eq!(prov.status(), EffectStatus::NotAttempted);
    assert_eq!(prov.observation_source(), ObservationSource::LocalPreflight);
    assert!(!prov.origin_bound());

    let res = first_task.await.unwrap();
    assert_eq!(res["status"], "ok");
    // 2 reads + 1 slow account request; the saturated Checkout timed out before send!
    assert_eq!(stats.requests.load(Ordering::SeqCst), 3);
}
