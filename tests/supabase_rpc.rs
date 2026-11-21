//! Synthetic native RPC evidence only; no PostgreSQL or hosted proof is implied.
#![cfg(feature = "supabase")]
use dactyl_db::{
    AccessMode, AdapterErrorKind, Connection, DatastoreRoute, GeneratedKey, OpenOptions, Operation,
    OperationResult, Parameter, StorageContext, SupabaseRpcConfig,
};
use serde_json::{json, Value};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;
const KEY: &str = "sb_publishable_fixture";
const BEARER: &str = "synthetic-user";
fn context() -> StorageContext {
    StorageContext::new(17, json!({"future":{"opaque":[1,false,"value"]}})).unwrap()
}
fn route(origin: &str) -> DatastoreRoute {
    DatastoreRoute::supabase_rpc(SupabaseRpcConfig::v1(origin, KEY), Some(BEARER.into()))
}
fn open(origin: &str, access_mode: AccessMode) -> Connection {
    Connection::open_with_options_and_context(
        route(origin),
        OpenOptions {
            access_mode,
            ..OpenOptions::default()
        },
        Some(context()),
    )
    .unwrap()
}
/// A single-request server returning a fixed response, never a database.
fn fixture(
    status: u16,
    body: Value,
) -> (
    String,
    mpsc::Receiver<(String, Value)>,
    thread::JoinHandle<()>,
) {
    fixture_raw(status, body.to_string(), "")
}

fn fixture_raw(
    status: u16,
    body: String,
    extra_headers: &str,
) -> (
    String,
    mpsc::Receiver<(String, Value)>,
    thread::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let (sender, receiver) = mpsc::channel();
    let extra_headers = extra_headers.to_owned();
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut bytes = Vec::new();
        let mut chunk = [0; 4096];
        let (headers, body_start, length) = loop {
            let n = stream.read(&mut chunk).unwrap();
            assert!(n > 0);
            bytes.extend_from_slice(&chunk[..n]);
            if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                let headers = String::from_utf8(bytes[..end].to_vec()).unwrap();
                let length = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().unwrap())
                    })
                    .unwrap();
                break (headers, end + 4, length);
            }
        };
        while bytes.len() - body_start < length {
            let n = stream.read(&mut chunk).unwrap();
            assert!(n > 0);
            bytes.extend_from_slice(&chunk[..n]);
        }
        let payload = serde_json::from_slice(&bytes[body_start..body_start + length]).unwrap();
        sender.send((headers, payload)).unwrap();
        if status == 0 {
            drop(stream);
            listener.set_nonblocking(true).unwrap();
            thread::sleep(Duration::from_millis(150));
            assert!(
                listener.accept().is_err(),
                "mutation was replayed after response loss"
            );
            return;
        }
        write!(stream, "HTTP/1.1 {status} Fixture\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n{extra_headers}\r\n{body}", body.len()).ok();
    });
    (endpoint, receiver, handle)
}

fn empty_result(affected: u64) -> Value {
    json!({"columns":[],"rows":[],"affected_rows":affected,"generated_keys":[]})
}

#[test]
fn headers_named_envelope_and_opaque_context() {
    let (origin, received, server) = fixture(
        200,
        json!({"columns":["n","b","i","r","s","blob"],"rows":[{"n":null,"b":true,"i":-9223372036854775808_i64,"r":1.25,"s":"text","blob":[0,255]}],"affected_rows":0,"generated_keys":[]}),
    );
    let params = vec![
        Parameter::Null,
        Parameter::Bool(true),
        Parameter::Integer(i64::MIN),
        Parameter::Real(1.25),
        Parameter::Text("text".into()),
        Parameter::Blob(vec![0, 255]),
    ];
    let rows = open(&origin, AccessMode::ReadWrite)
        .read("fixture", &params)
        .unwrap();
    let (headers, body) = received.recv().unwrap();
    server.join().unwrap();
    let mut lines = headers.lines();
    assert_eq!(
        lines.next(),
        Some("POST /rest/v1/rpc/dactyl_query_v1 HTTP/1.1")
    );
    let headers: std::collections::HashMap<_, _> = lines
        .filter_map(|line| {
            let (n, v) = line.split_once(':')?;
            Some((n.to_ascii_lowercase(), v.trim().to_owned()))
        })
        .collect();
    assert_eq!(
        headers.get("authorization"),
        Some(&format!("Bearer {BEARER}"))
    );
    assert_eq!(headers.get("apikey").map(String::as_str), Some(KEY));
    for (name, value) in [
        ("accept", "application/json"),
        ("content-type", "application/json"),
        ("content-profile", "dactyl_api"),
        ("accept-profile", "dactyl_api"),
        ("dactyl-contract-version", "1"),
    ] {
        assert_eq!(headers.get(name).map(String::as_str), Some(value));
    }
    assert_eq!(
        body,
        json!({"request":{"version":1,"kind":"read","sql":"fixture","params":params,"access_mode":"read_only","context":context()}})
    );
    assert_eq!(rows.as_slice()[0].get_int("i").unwrap(), i64::MIN);
    assert_eq!(rows.as_slice()[0].get_blob("blob").unwrap(), vec![0, 255]);
}
#[test]
fn one_batch_retains_order_zero_cas_and_explicit_keys() {
    let mut first = empty_result(1);
    first["generated_keys"] = json!(["created", 42]);
    let (origin, received, server) = fixture(
        200,
        json!({"results":[first,empty_result(0),empty_result(0)]}),
    );
    let operations = vec![
        Operation::write("insert", vec![Parameter::Text("bound".into())]),
        Operation::write("cas", vec![]),
        Operation::read("observe", vec![]),
    ];
    let result = open(&origin, AccessMode::ReadWrite)
        .atomic(&operations)
        .unwrap();
    assert_eq!(result.results.len(), 3);
    match &result.results[0] {
        OperationResult::Write(w) => assert_eq!(
            w.generated_keys,
            vec![
                GeneratedKey::Text("created".into()),
                GeneratedKey::Integer(42)
            ]
        ),
        _ => panic!("write shape"),
    }
    match &result.results[1] {
        OperationResult::Write(w) => assert_eq!(w.affected_rows, 0),
        _ => panic!("CAS shape"),
    }
    assert!(matches!(result.results[2], OperationResult::Rows(_)));
    let (h, b) = received.recv().unwrap();
    server.join().unwrap();
    assert!(h.starts_with("POST /rest/v1/rpc/dactyl_batch_v1 "));
    assert_eq!(
        b,
        json!({"request":{"version":1,"operations":operations,"access_mode":"read_write","context":context()}})
    );
}

#[test]
fn read_only_native_batch_uses_the_physical_read_transaction_boundary() {
    let (origin, received, server) =
        fixture(200, json!({"results":[empty_result(0),empty_result(0)]}));
    let operations = vec![
        Operation::read("first", vec![]),
        Operation::read("second", vec![]),
    ];
    let result = open(&origin, AccessMode::ReadOnly)
        .atomic(&operations)
        .unwrap();
    assert_eq!(result.results.len(), 2);
    let (headers, body) = received.recv().unwrap();
    server.join().unwrap();
    assert!(headers.starts_with("POST /rest/v1/rpc/dactyl_read_batch_v1 "));
    assert_eq!(body["request"]["access_mode"], "read_only");
    assert_eq!(body["request"]["operations"], json!(operations));
}
#[test]
fn native_configuration_is_explicit_and_secure() {
    for origin in [
        "https://project.example/rest/v1",
        "https://user:password@project.example",
        "https://project.example?secret=1",
        "https://project.example#fragment",
        "http://localhost:1",
        "http://remote.example",
        "postgres://db/project",
    ] {
        assert!(route(origin).validate().is_err());
    }
    for key in [
        "",
        "sb_secret_PRIVATE",
        "eyJhbGci.legacy.anon",
        "sb_publishable_",
        "sb_publishable_bad\r\n",
    ] {
        assert!(DatastoreRoute::supabase_rpc(
            SupabaseRpcConfig::v1("https://project.example", key),
            None
        )
        .validate()
        .is_err());
    }
    for schema in ["", "9schema", "schema,path", "schema/path"] {
        assert!(DatastoreRoute::supabase_rpc(
            SupabaseRpcConfig::v1("https://project.example", KEY).with_schema(schema),
            None
        )
        .validate()
        .is_err());
    }
    let config =
        SupabaseRpcConfig::v1("https://private-origin.example", KEY).with_schema("private_schema");
    let r = DatastoreRoute::supabase_rpc(config.clone(), Some(BEARER.into()));
    let debug = format!("{config:?} {r:?}");
    for private in ["private-origin", KEY, "private_schema", BEARER] {
        assert!(!debug.contains(private));
    }
    assert_eq!(config.contract_version(), 1);
    assert!(r.validate().is_ok());
    assert!(Connection::open(r).is_err());
    assert!(Connection::open_with_context(
        DatastoreRoute::supabase_rpc(config, None),
        Some(context())
    )
    .is_err());
    assert!(
        DatastoreRoute::supabase("https://project.supabase.co", None)
            .supabase_rpc_config()
            .is_none()
    );
}
#[test]
fn read_only_empty_and_invalid_values_fail_without_network() {
    let db = open("http://127.0.0.1:1", AccessMode::ReadOnly);
    assert!(db.atomic(&[]).unwrap().results.is_empty());
    assert_eq!(
        db.write("write", &[]).unwrap_err().adapter_kind(),
        Some(AdapterErrorKind::ReadOnly)
    );
    assert_eq!(
        db.atomic(&[Operation::schema("ddl", vec![])])
            .unwrap_err()
            .adapter_kind(),
        Some(AdapterErrorKind::ReadOnly)
    );
    assert_eq!(
        db.read("read", &[Parameter::Real(f64::NAN)])
            .unwrap_err()
            .adapter_kind(),
        Some(AdapterErrorKind::Value)
    );
}
#[test]
fn postgrest_errors_are_typed_and_redacted() {
    for (status, code, kind) in [
        (403, "authorization_denied", AdapterErrorKind::Authorization),
        (422, "unsupported_query", AdapterErrorKind::Capability),
        (406, "PGRST106", AdapterErrorKind::Capability),
        (404, "PGRST202", AdapterErrorKind::Capability),
        (401, "PGRST301", AdapterErrorKind::Authentication),
        (409, "23505", AdapterErrorKind::Constraint),
        (409, "40001", AdapterErrorKind::Conflict),
        (500, "57014", AdapterErrorKind::Timeout),
        (400, "25006", AdapterErrorKind::ReadOnly),
        (403, "42501", AdapterErrorKind::Authorization),
        (502, "unknown-secret", AdapterErrorKind::Unavailable),
    ] {
        let (origin, received, server) = fixture(
            status,
            json!({"code":code,"message":"message-secret","details":"sql-secret","hint":"credential-secret"}),
        );
        let error = open(&origin, AccessMode::ReadWrite)
            .write("fixture", &[])
            .unwrap_err();
        received.recv().unwrap();
        server.join().unwrap();
        assert_eq!(error.adapter_kind(), Some(kind));
        assert!(!format!("{error:?}").contains("secret"));
        if code == "PGRST106" {
            assert_eq!(error.adapter_code(), Some("unsupported_schema"));
        }
    }
}
#[test]
fn malformed_native_results_are_explicit_failures() {
    for body in [
        json!({"results":[]}),
        json!({"results":[{"columns":[],"rows":[],"affected_rows":0}]}),
        json!({"results":[empty_result(0),empty_result(0)]}),
    ] {
        let (origin, received, server) = fixture(200, body);
        let error = open(&origin, AccessMode::ReadWrite)
            .atomic(&[Operation::read("fixture", vec![])])
            .unwrap_err();
        received.recv().unwrap();
        server.join().unwrap();
        assert_eq!(error.adapter_kind(), Some(AdapterErrorKind::Protocol));
    }
    let (origin, received, server) = fixture(
        200,
        json!({"columns":["value"],"rows":[{"value":{"unsupported":true}}],"affected_rows":1,"generated_keys":[]}),
    );
    assert_eq!(
        open(&origin, AccessMode::ReadWrite)
            .write("fixture", &[])
            .unwrap_err()
            .adapter_kind(),
        Some(AdapterErrorKind::Capability)
    );
    received.recv().unwrap();
    server.join().unwrap();
}
#[test]
fn redirect_never_forwards_the_credentials_or_retries_batch() {
    let destination = TcpListener::bind("127.0.0.1:0").unwrap();
    destination.set_nonblocking(true).unwrap();
    let (origin, received, server) = fixture_raw(
        307,
        "{}".into(),
        &format!("Location: http://{}\r\n", destination.local_addr().unwrap()),
    );
    assert_eq!(
        open(&origin, AccessMode::ReadWrite)
            .atomic(&[Operation::write("mutation", vec![])])
            .unwrap_err()
            .adapter_kind(),
        Some(AdapterErrorKind::Protocol)
    );
    received.recv().unwrap();
    server.join().unwrap();
    assert!(destination.accept().is_err());
}

#[test]
fn lost_write_response_is_not_retried() {
    let (origin, received, server) = fixture(0, empty_result(1));
    let error = open(&origin, AccessMode::ReadWrite)
        .atomic(&[Operation::write("mutation", vec![])])
        .unwrap_err();
    received.recv().unwrap();
    server.join().unwrap();
    assert_eq!(error.adapter_kind(), Some(AdapterErrorKind::Transport));
    assert_eq!(
        error.mutation_outcome(),
        dactyl_db::MutationOutcome::Uncertain
    );
}

#[test]
fn lease_fencing_cas_batches_and_compact_cursor_pagination_conform() {
    use dactyl_db::{FailureOrigin, MutationOutcome, ResourceBudget, ScanOutcome};

    // 1. Atomic lease-acquire -> audit event -> observation vs stale-fence zero-row CAS:
    let mut acquire_res = empty_result(1);
    acquire_res["generated_keys"] = json!(["lease_01"]);
    let mut event_res = empty_result(1);
    event_res["generated_keys"] = json!([101]);
    let observe_res = json!({
        "columns": ["lease_id", "holder", "fence_epoch"],
        "rows": [{"lease_id": "lease_01", "holder": "agent-a", "fence_epoch": 2}],
        "affected_rows": 0,
        "generated_keys": []
    });
    let (origin, received, server) = fixture(
        200,
        json!({"results": [acquire_res, event_res, observe_res]}),
    );
    let ops = vec![
        Operation::write(
            "UPDATE leases SET holder = $1, fence_epoch = fence_epoch + 1 WHERE lease_id = $2 AND fence_epoch = $3 RETURNING lease_id",
            vec![
                Parameter::Text("agent-a".into()),
                Parameter::Text("lease_01".into()),
                Parameter::Integer(1),
            ],
        ),
        Operation::write(
            "INSERT INTO lease_events (lease_id, fence_epoch, kind) VALUES ($1, $2, $3) RETURNING seq",
            vec![
                Parameter::Text("lease_01".into()),
                Parameter::Integer(2),
                Parameter::Text("acquired".into()),
            ],
        ),
        Operation::read(
            "SELECT lease_id, holder, fence_epoch FROM leases WHERE lease_id = $1",
            vec![Parameter::Text("lease_01".into())],
        ),
    ];
    let batch = open(&origin, AccessMode::ReadWrite).atomic(&ops).unwrap();
    assert_eq!(batch.results.len(), 3);
    assert!(matches!(
        &batch.results[0],
        OperationResult::Write(w) if w.affected_rows == 1 && w.generated_keys == vec![GeneratedKey::Text("lease_01".into())]
    ));
    assert!(matches!(
        &batch.results[1],
        OperationResult::Write(w) if w.affected_rows == 1 && w.generated_keys == vec![GeneratedKey::Integer(101)]
    ));
    match &batch.results[2] {
        OperationResult::Rows(rows) => {
            assert_eq!(rows.as_slice()[0].get_int("fence_epoch").unwrap(), 2);
        }
        _ => panic!("expected read observation"),
    }
    let (_, body) = received.recv().unwrap();
    server.join().unwrap();
    assert_eq!(body["request"]["operations"].as_array().unwrap().len(), 3);

    // Losing/stale fence CAS returns affected_rows = 0 without fabricating conflict policy.
    let (origin, received, server) =
        fixture(200, json!({"results": [empty_result(0), empty_result(0)]}));
    let stale_ops = vec![
        Operation::write(
            "UPDATE leases SET holder = $1, fence_epoch = fence_epoch + 1 WHERE lease_id = $2 AND fence_epoch = $3",
            vec![
                Parameter::Text("agent-b".into()),
                Parameter::Text("lease_01".into()),
                Parameter::Integer(1),
            ],
        ),
        Operation::read(
            "SELECT lease_id, holder, fence_epoch FROM leases WHERE lease_id = $1",
            vec![Parameter::Text("lease_01".into())],
        ),
    ];
    let stale = open(&origin, AccessMode::ReadWrite)
        .atomic(&stale_ops)
        .unwrap();
    assert!(matches!(
        &stale.results[0],
        OperationResult::Write(w) if w.affected_rows == 0
    ));
    received.recv().unwrap();
    server.join().unwrap();

    // 2. Bounded cursor pagination over compact non-todo operational store:
    let page = json!({
        "columns": ["seq", "stream_id", "content_sha256", "byte_len"],
        "rows": [
            {"seq": 10, "stream_id": "proof_ledger", "content_sha256": "sha256:aa", "byte_len": 128},
            {"seq": 11, "stream_id": "proof_ledger", "content_sha256": "sha256:bb", "byte_len": 256}
        ],
        "affected_rows": 0,
        "generated_keys": []
    });
    let (origin, received, server) = fixture(200, page.clone());
    let db = Connection::open_with_budget(
        route(&origin),
        OpenOptions {
            access_mode: AccessMode::ReadOnly,
            ..OpenOptions::default()
        },
        Some(context()),
        ResourceBudget::v1().with_max_rows(2),
    )
    .unwrap();
    let mut seqs = Vec::new();
    let outcome = db
        .visit_rows(
            "SELECT seq, stream_id, content_sha256, byte_len FROM operational_events WHERE seq > $1 ORDER BY seq ASC LIMIT $2",
            &[Parameter::Integer(9), Parameter::Integer(2)],
            |row| {
                seqs.push(row.get_int("seq")?);
                Ok(true)
            },
        )
        .unwrap();
    assert_eq!(
        outcome,
        dactyl_db::ScanOutcome {
            rows: 2,
            complete: true
        }
    );
    assert_eq!(seqs, vec![10, 11]);
    received.recv().unwrap();
    server.join().unwrap();

    // Budget overflow on compact cursor page fails with LocalBudget, never silent truncation.
    let (origin, received, server) = fixture(200, page);
    let tight = Connection::open_with_budget(
        route(&origin),
        OpenOptions {
            access_mode: AccessMode::ReadOnly,
            ..OpenOptions::default()
        },
        Some(context()),
        ResourceBudget::v1().with_max_rows(1),
    )
    .unwrap();
    let err = tight
        .visit_rows("SELECT seq FROM operational_events", &[], |_| Ok(true))
        .unwrap_err();
    assert_eq!(err.adapter_kind(), Some(AdapterErrorKind::Quota));
    assert_eq!(err.adapter_code(), Some("row_budget_exceeded"));
    assert_eq!(err.failure_origin(), FailureOrigin::LocalBudget);
    received.recv().unwrap();
    server.join().unwrap();

    // Early stop via visitor returning Ok(false) returns complete=false cleanly.
    let (origin, received, server) = fixture(
        200,
        json!({
            "columns": ["seq"],
            "rows": [{"seq": 1}, {"seq": 2}],
            "affected_rows": 0,
            "generated_keys": []
        }),
    );
    let stop_db = open(&origin, AccessMode::ReadOnly);
    let stopped = stop_db
        .visit_rows("SELECT seq FROM operational_events", &[], |row| {
            Ok(row.get_int("seq")? < 1)
        })
        .unwrap();
    assert_eq!(
        stopped,
        ScanOutcome {
            rows: 1,
            complete: false
        }
    );
    received.recv().unwrap();
    server.join().unwrap();

    // 3. Deterministic 401/403/409/413/429/5xx + truncated write error/outcome invariants:
    for (status, code, expected_kind, expected_origin, expected_uncertain) in [
        (
            401,
            "use_dpop_nonce",
            AdapterErrorKind::Authentication,
            FailureOrigin::Other,
            false,
        ),
        (
            403,
            "authorization_denied",
            AdapterErrorKind::Authorization,
            FailureOrigin::RemotePolicy,
            false,
        ),
        (
            409,
            "version_conflict",
            AdapterErrorKind::VersionConflict,
            FailureOrigin::Other,
            false,
        ),
        (
            413,
            "payload_too_large",
            AdapterErrorKind::Quota,
            FailureOrigin::RemotePolicy,
            false,
        ),
        (
            429,
            "rate_limited",
            AdapterErrorKind::RateLimited,
            FailureOrigin::RemotePolicy,
            false,
        ),
        (
            503,
            "storage_unavailable",
            AdapterErrorKind::Unavailable,
            FailureOrigin::Other,
            true,
        ),
    ] {
        let (origin, received, server) = fixture(
            status,
            json!({"code": code, "message": "secret-rpc-detail"}),
        );
        let err = open(&origin, AccessMode::ReadWrite)
            .write(
                "UPDATE leases SET holder = $1",
                &[Parameter::Text("a".into())],
            )
            .unwrap_err();
        assert_eq!(err.adapter_kind(), Some(expected_kind));
        assert_eq!(err.failure_origin(), expected_origin);
        assert_eq!(
            err.mutation_outcome() == MutationOutcome::Uncertain,
            expected_uncertain
        );
        assert!(!format!("{err} {err:?}").contains("secret-rpc-detail"));
        received.recv().unwrap();
        server.join().unwrap();
    }
}

#[test]
fn burst_250_concurrent_clients_reuse_connections_without_fd_leaks_and_enforce_timeouts() {
    use dactyl_db::{CloudConnector, ResourceBudget};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier};

    fn open_fd_count() -> Option<usize> {
        for path in ["/dev/fd", "/proc/self/fd"] {
            if let Ok(entries) = std::fs::read_dir(path) {
                return Some(entries.count());
            }
        }
        None
    }

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    listener.set_nonblocking(true).unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let accepted_connections = Arc::new(AtomicUsize::new(0));
    let handled_requests = Arc::new(AtomicUsize::new(0));

    let stop_srv = stop.clone();
    let accepted_srv = accepted_connections.clone();
    let handled_srv = handled_requests.clone();
    let server = thread::spawn(move || {
        let mut workers = Vec::new();
        while !stop_srv.load(Ordering::Relaxed) {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    accepted_srv.fetch_add(1, Ordering::SeqCst);
                    let stop_conn = stop_srv.clone();
                    let handled_conn = handled_srv.clone();
                    workers.push(thread::spawn(move || {
                        stream.set_nonblocking(false).unwrap();
                        stream
                            .set_read_timeout(Some(Duration::from_millis(200)))
                            .unwrap();
                        let mut buf = Vec::with_capacity(4096);
                        let mut chunk = [0_u8; 4096];
                        while !stop_conn.load(Ordering::Relaxed) {
                            let header_end = loop {
                                if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                                    break Some(pos);
                                }
                                match stream.read(&mut chunk) {
                                    Ok(0) => break None,
                                    Ok(n) => buf.extend_from_slice(&chunk[..n]),
                                    Err(e)
                                        if matches!(
                                            e.kind(),
                                            std::io::ErrorKind::WouldBlock
                                                | std::io::ErrorKind::TimedOut
                                        ) =>
                                    {
                                        if stop_conn.load(Ordering::Relaxed) {
                                            break None;
                                        }
                                    }
                                    Err(_) => break None,
                                }
                            };
                            let Some(end) = header_end else {
                                break;
                            };
                            let headers = std::str::from_utf8(&buf[..end]).unwrap();
                            let length = headers
                                .lines()
                                .find_map(|line| {
                                    let (k, v) = line.split_once(':')?;
                                    k.eq_ignore_ascii_case("content-length")
                                        .then(|| v.trim().parse::<usize>().unwrap())
                                })
                                .unwrap_or(0);
                            while buf.len() < end + 4 + length {
                                match stream.read(&mut chunk) {
                                    Ok(0) => break,
                                    Ok(n) => buf.extend_from_slice(&chunk[..n]),
                                    Err(_) => break,
                                }
                            }
                            if buf.len() < end + 4 + length {
                                break;
                            }
                            buf.drain(..end + 4 + length);
                            handled_conn.fetch_add(1, Ordering::SeqCst);
                            let resp = r#"{"columns":["ok"],"rows":[{"ok":true}],"affected_rows":0,"generated_keys":[]}"#;
                            if write!(
                                stream,
                                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{resp}",
                                resp.len()
                            )
                            .is_err()
                            {
                                break;
                            }
                        }
                    }));
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(5));
                }
                Err(_) => break,
            }
        }
        for w in workers {
            let _ = w.join();
        }
    });

    let fd_before = open_fd_count();
    {
        let connector =
            Arc::new(CloudConnector::with_config(SupabaseRpcConfig::v1(&origin, KEY)).unwrap());
        let barrier = Arc::new(Barrier::new(250));
        let mut clients = Vec::with_capacity(250);
        for _ in 0..250 {
            let connector = connector.clone();
            let barrier = barrier.clone();
            clients.push(thread::spawn(move || {
                let db = Connection::open_with_context(
                    connector.route(Some(BEARER.into())),
                    Some(context()),
                )
                .unwrap();
                barrier.wait();
                let rows = db.read("SELECT 1 AS ok", &[]).unwrap();
                assert!(rows.as_slice()[0].get_bool("ok").unwrap());
            }));
        }
        for handle in clients {
            handle.join().unwrap();
        }
    }
    stop.store(true, Ordering::SeqCst);
    server.join().unwrap();

    assert_eq!(handled_requests.load(Ordering::SeqCst), 250);
    let conns = accepted_connections.load(Ordering::SeqCst);
    assert!(
        (1..=32).contains(&conns),
        "expected bounded connection reuse (1..=32), observed {conns}"
    );
    if let (Some(before), Some(after)) = (fd_before, open_fd_count()) {
        // Bounded slack accommodates shared runtime / parallel test FDs in the
        // same test process while still catching any per-client FD leak across
        // the 250-way burst.
        assert!(
            after <= before + 24,
            "file descriptor count leaked across 250-way burst: before={before}, after={after}"
        );
    }

    // Parallel slow-server burst enforces strict ResourceBudget request_timeout across concurrent callers:
    let slow_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let slow_origin = format!("http://{}", slow_listener.local_addr().unwrap());
    slow_listener.set_nonblocking(true).unwrap();
    let slow_stop = Arc::new(AtomicBool::new(false));
    let slow_stop_srv = slow_stop.clone();
    let slow_srv = thread::spawn(move || {
        let mut held = Vec::new();
        while !slow_stop_srv.load(Ordering::Relaxed) {
            match slow_listener.accept() {
                Ok((stream, _)) => held.push(stream),
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(5));
                }
                Err(_) => break,
            }
        }
        thread::sleep(Duration::from_millis(120));
        drop(held);
    });

    let start = std::time::Instant::now();
    let slow_barrier = Arc::new(Barrier::new(48));
    let mut slow_clients = Vec::with_capacity(48);
    for _ in 0..48 {
        let origin = slow_origin.clone();
        let barrier = slow_barrier.clone();
        slow_clients.push(thread::spawn(move || {
            let db = Connection::open_with_budget(
                route(&origin),
                OpenOptions::default(),
                Some(context()),
                ResourceBudget::v1().with_request_timeout(Duration::from_millis(80)),
            )
            .unwrap();
            barrier.wait();
            let err = db.read("SELECT 1", &[]).unwrap_err();
            assert_eq!(err.adapter_kind(), Some(AdapterErrorKind::Timeout));
        }));
    }
    for handle in slow_clients {
        handle.join().unwrap();
    }
    assert!(
        start.elapsed() < Duration::from_secs(3),
        "concurrent timeout burst exceeded bounded deadline: {:?}",
        start.elapsed()
    );
    slow_stop.store(true, Ordering::SeqCst);
    slow_srv.join().unwrap();
}

fn gateway_seq_server(
    responses: Vec<(u16, String, Value)>,
) -> (
    String,
    mpsc::Receiver<(String, Value)>,
    thread::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let (tx, rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        for (status, extra_headers, body) in responses {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut bytes = Vec::new();
            let (headers, offset, length) = loop {
                let mut buf = [0; 4096];
                let n = stream.read(&mut buf).unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&buf[..n]);
                if let Some(end) = bytes.windows(4).position(|b| b == b"\r\n\r\n") {
                    let headers = String::from_utf8(bytes[..end].to_vec()).unwrap();
                    let length = headers
                        .lines()
                        .find_map(|line| {
                            let (k, v) = line.split_once(':')?;
                            k.eq_ignore_ascii_case("content-length")
                                .then(|| v.trim().parse::<usize>().unwrap())
                        })
                        .unwrap_or(0);
                    break (headers, end + 4, length);
                }
            };
            while bytes.len() - offset < length {
                let mut buf = [0; 4096];
                let n = stream.read(&mut buf).unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&buf[..n]);
            }
            let payload = if length == 0 {
                Value::Null
            } else {
                serde_json::from_slice(&bytes[offset..offset + length]).unwrap()
            };
            tx.send((headers, payload)).unwrap();
            if status == 0 {
                continue;
            }
            let raw = body.to_string();
            write!(
                stream,
                "HTTP/1.1 {status} Fixture\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n{extra_headers}\r\n{raw}",
                raw.len()
            )
            .ok();
        }
        listener.set_nonblocking(true).unwrap();
        thread::sleep(Duration::from_millis(80));
        assert!(
            listener.accept().is_err(),
            "unexpected extra HTTP request after responses exhausted"
        );
    });
    (origin, rx, handle)
}

fn rpc_header<'a>(headers: &'a str, name: &str) -> Option<&'a str> {
    headers.lines().find_map(|line| {
        let (key, value) = line.split_once(':')?;
        key.eq_ignore_ascii_case(name).then(|| value.trim())
    })
}

#[test]
fn device_gateway_routes_cloud_query_cloud_batch_and_retries_dpop_nonce_once() {
    use dactyl_db::{
        CloudConnector, CloudResponseMetadata, MutationOutcome, OperationKind, RequestProof,
        RequestProofInput, RequestProofSigner, ResourceBudget,
    };
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct Signer {
        calls: Mutex<Vec<(String, Option<String>)>>,
        observed: Mutex<Vec<String>>,
        bearer: bool,
    }
    impl RequestProofSigner for Signer {
        fn observe_nonce(&self, _canonical_url: &str, nonce: &str) {
            self.observed.lock().unwrap().push(nonce.to_owned());
        }
        fn sign_request(
            &self,
            input: &RequestProofInput<'_>,
        ) -> Result<RequestProof, dactyl_db::DactylError> {
            self.calls.lock().unwrap().push((
                input.canonical_url().to_owned(),
                input.nonce().map(str::to_owned),
            ));
            if self.bearer {
                return Ok(RequestProof::bearer());
            }
            RequestProof::dpop(format!("proof-{}", input.nonce().unwrap_or("none")))
        }
    }

    let signer = Arc::new(Signer::default());
    let (origin, rx, srv) = gateway_seq_server(vec![
        // 1. cloud-query hits 401 use_dpop_nonce -> single retry -> 200.
        (
            401,
            "DPoP-Nonce: rpc-nonce-1\r\nWWW-Authenticate: DPoP error=\"use_dpop_nonce\"\r\n".into(),
            json!({"error": {"code": "use_dpop_nonce", "message": "raw-nonce-detail"}}),
        ),
        (
            200,
            "DPoP-Nonce: rpc-nonce-2\r\n".into(),
            json!({"columns": ["ok"], "rows": [{"ok": true}], "affected_rows": 0, "generated_keys": []}),
        ),
        // 2. cloud-batch carries atomic event_params batch directly under VersionedRequest.
        (
            200,
            String::new(),
            json!({"results": [
                {"columns": [], "rows": [], "affected_rows": 1, "generated_keys": []},
                {"columns": [], "rows": [], "affected_rows": 1, "generated_keys": []}
            ]}),
        ),
        // 3. cloud-control hits 401 use_dpop_nonce -> single retry -> 429 overage_cap_exhausted + Retry-After.
        (
            401,
            "DPoP-Nonce: ctrl-nonce-1\r\n".into(),
            json!({"error": {"code": "use_dpop_nonce"}}),
        ),
        (
            429,
            "DPoP-Nonce: ctrl-nonce-2\r\nRetry-After: 90\r\n".into(),
            json!({"error": {"code": "overage_cap_exhausted", "message": "raw-cap-detail"}}),
        ),
    ]);

    let cfg = SupabaseRpcConfig::v1(&origin, KEY).with_device_gateway();
    assert!(cfg.uses_device_gateway());
    let route_gw = DatastoreRoute::supabase_rpc(cfg.clone(), Some("dpc_at_tok".into()))
        .with_proof_signer(signer.clone());
    let db = Connection::open_with_context(route_gw, Some(context())).unwrap();

    let rows = db.read("SELECT 1 AS ok", &[]).unwrap();
    assert!(rows.as_slice()[0].get_bool("ok").unwrap());

    let event_sql = "WITH event_params(event_id, event_ts, task_id, event_type, event_payload, event_actor) AS (VALUES ($1, $2, $3, $4, $5, $6)) INSERT INTO events (event_id, ts, seq, stream, subject_kind, subject_id, event_type, payload, actor) SELECT event_params.event_id, event_params.event_ts, 1, 'todo', 'task', event_params.task_id, event_params.event_type, event_params.event_payload, event_params.event_actor FROM event_params";
    let batch = db
        .atomic(&[
            Operation::write(
                "INSERT INTO tasks (id, updated_at, version) VALUES ($1, $2, 1)",
                vec!["task-9".into(), "2026-01-01T00:00:00Z".into()],
            ),
            Operation::write(
                event_sql,
                vec![
                    "evt-9".into(),
                    "2026-01-01T00:00:00Z".into(),
                    "task-9".into(),
                    "task.add".into(),
                    "{}".into(),
                    "agent-x".into(),
                ],
            ),
        ])
        .unwrap();
    assert_eq!(batch.results.len(), 2);

    let cloud = CloudConnector::with_config(cfg)
        .unwrap()
        .with_proof_signer(signer.clone());
    let mut ctrl_meta = CloudResponseMetadata::default();
    let ctrl_err = cloud
        .control_request_with_metadata(
            "propodus_repository_access_v1",
            &json!({"version": 1, "project_id": "org/repo"}),
            Some("dpc_at_tok"),
            None,
            OperationKind::Read,
            ResourceBudget::v1(),
            &mut ctrl_meta,
        )
        .unwrap_err();
    assert_eq!(ctrl_err.adapter_kind(), Some(AdapterErrorKind::Quota));
    assert_eq!(ctrl_err.adapter_code(), Some("overage_cap_exhausted"));
    assert_eq!(ctrl_meta.dpop_nonce(), Some("ctrl-nonce-2"));
    assert_eq!(ctrl_meta.retry_after(), Some("90"));
    assert!(!format!("{ctrl_err} {ctrl_err:?}").contains("raw-cap-detail"));

    let (h1, b1) = rx.recv().unwrap();
    assert!(h1.starts_with("POST /functions/v1/decapod-onboarding/cloud-query "));
    assert_eq!(rpc_header(&h1, "authorization"), Some("DPoP dpc_at_tok"));
    assert_eq!(rpc_header(&h1, "dpop"), Some("proof-none"));
    assert_eq!(b1["version"], 1);
    assert!(b1.get("request").is_none());
    assert_eq!(b1["sql"], "SELECT 1 AS ok");

    let (h2, _) = rx.recv().unwrap();
    assert!(h2.starts_with("POST /functions/v1/decapod-onboarding/cloud-query "));
    assert_eq!(rpc_header(&h2, "dpop"), Some("proof-rpc-nonce-1"));

    let (h3, b3) = rx.recv().unwrap();
    assert!(h3.starts_with("POST /functions/v1/decapod-onboarding/cloud-batch "));
    assert_eq!(b3["version"], 1);
    assert!(b3.get("request").is_none());
    assert_eq!(b3["operations"].as_array().unwrap().len(), 2);

    let (h4, b4) = rx.recv().unwrap();
    assert!(h4.starts_with("POST /functions/v1/decapod-onboarding/cloud-control "));
    assert_eq!(rpc_header(&h4, "dpop"), Some("proof-none"));
    assert_eq!(b4["version"], 1);
    assert_eq!(b4["function"], "propodus_repository_access_v1");

    let (h5, _) = rx.recv().unwrap();
    assert!(h5.starts_with("POST /functions/v1/decapod-onboarding/cloud-control "));
    assert_eq!(rpc_header(&h5, "dpop"), Some("proof-ctrl-nonce-1"));

    assert_eq!(
        signer.observed.lock().unwrap().as_slice(),
        ["rpc-nonce-1", "rpc-nonce-2", "ctrl-nonce-1", "ctrl-nonce-2"]
    );
    srv.join().unwrap();

    // Double challenge on cloud-query stops after 2 requests; unsigned or Bearer signer fails preflight.
    let double_signer = Arc::new(Signer::default());
    let (origin, rx, srv) = gateway_seq_server(vec![
        (
            401,
            "DPoP-Nonce: q-n1\r\n".into(),
            json!({"error": {"code": "use_dpop_nonce"}}),
        ),
        (
            401,
            "DPoP-Nonce: q-n2\r\n".into(),
            json!({"error": {"code": "use_dpop_nonce"}}),
        ),
    ]);
    let db_double = Connection::open_with_context(
        DatastoreRoute::supabase_rpc(
            SupabaseRpcConfig::v1(&origin, KEY).with_device_gateway(),
            Some("dpc_at_tok".into()),
        )
        .with_proof_signer(double_signer.clone()),
        Some(context()),
    )
    .unwrap();
    let double_err = db_double.read("SELECT 1", &[]).unwrap_err();
    assert_eq!(
        double_err.adapter_kind(),
        Some(AdapterErrorKind::Authentication)
    );
    assert_eq!(double_err.adapter_code(), Some("use_dpop_nonce"));
    assert_eq!(double_signer.calls.lock().unwrap().len(), 2);
    rx.recv().unwrap();
    rx.recv().unwrap();
    srv.join().unwrap();

    // Unsigned or Bearer-mode signer fails local preflight on gateway routes with zero HTTP traffic.
    let (unused_origin, _, unused_srv) = gateway_seq_server(vec![]);
    let unsigned_db = Connection::open_with_context(
        DatastoreRoute::supabase_rpc(
            SupabaseRpcConfig::v1(&unused_origin, KEY).with_device_gateway(),
            Some(BEARER.into()),
        ),
        Some(context()),
    )
    .unwrap();
    let unsigned_err = unsigned_db.read("SELECT 1", &[]).unwrap_err();
    assert_eq!(unsigned_err.adapter_code(), Some("device_proof_required"));
    assert_eq!(
        unsigned_err.mutation_outcome(),
        MutationOutcome::NotAttempted
    );

    let bearer_signer = Arc::new(Signer {
        bearer: true,
        ..Default::default()
    });
    let bearer_db = Connection::open_with_context(
        DatastoreRoute::supabase_rpc(
            SupabaseRpcConfig::v1(&unused_origin, KEY).with_device_gateway(),
            Some(BEARER.into()),
        )
        .with_proof_signer(bearer_signer),
        Some(context()),
    )
    .unwrap();
    let bearer_err = bearer_db.read("SELECT 1", &[]).unwrap_err();
    assert_eq!(bearer_err.adapter_code(), Some("device_proof_required"));
    assert_eq!(bearer_err.mutation_outcome(), MutationOutcome::NotAttempted);
    unused_srv.join().unwrap();

    // Mutating cloud-batch failure (503) marks outcome_unknown without retrying.
    let dpop_batch_signer = Arc::new(Signer::default());
    let (origin, rx, srv) = gateway_seq_server(vec![(
        503,
        "DPoP-Nonce: ignore-me\r\n".into(),
        json!({"error": {"code": "storage_unavailable"}}),
    )]);
    let batch_db = Connection::open_with_context(
        DatastoreRoute::supabase_rpc(
            SupabaseRpcConfig::v1(&origin, KEY).with_device_gateway(),
            Some("dpc_at_tok".into()),
        )
        .with_proof_signer(dpop_batch_signer.clone()),
        Some(context()),
    )
    .unwrap();
    let batch_err = batch_db
        .atomic(&[Operation::write(
            "INSERT INTO tasks (id) VALUES ($1)",
            vec!["t-1".into()],
        )])
        .unwrap_err();
    assert_eq!(batch_err.mutation_outcome(), MutationOutcome::Uncertain);
    assert_eq!(dpop_batch_signer.calls.lock().unwrap().len(), 1);
    rx.recv().unwrap();
    srv.join().unwrap();

    // New Wave 2 safe codes propagate through SupabaseRpc errors (provider_disabled, device_key_expired, quota_exceeded).
    for (status, code, kind) in [
        (403, "provider_disabled", AdapterErrorKind::Authorization),
        (401, "device_key_expired", AdapterErrorKind::Authentication),
        (429, "quota_exceeded", AdapterErrorKind::Quota),
    ] {
        let (origin, body) = (
            code,
            json!({"error": {"code": code, "message": "internal-rpc-secret"}}),
        );
        let (url, rx, srv) = fixture(status, body);
        let err = open(&url, AccessMode::ReadOnly)
            .read("SELECT 1", &[])
            .unwrap_err();
        assert_eq!(err.adapter_kind(), Some(kind));
        assert_eq!(err.adapter_code(), Some(origin));
        assert!(!format!("{err} {err:?}").contains("internal-rpc-secret"));
        rx.recv().unwrap();
        srv.join().unwrap();
    }
}
