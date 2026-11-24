//! Cross-layer transport fault injection, commit-ack loss reconciliation (#143),
//! and origin cutover resilience (#138) campaign.
//!
//! Evidence classes:
//! - `local_sqlite` + `synthetic_http`: instrumented loopback HTTP/Edge/RPC
//!   fault proxy backed by a real SQLite transaction & effect-ledger store,
//!   verified via an independent read-only observer connection and a pure-Rust
//!   reference model across deterministic seeds.
//! - `local_postgres`, `hosted_supabase`, `live_propodus`: unavailable in offline
//!   CI/local verification and never fabricated.

use dactyl_db::{
    AdapterErrorKind, LogicalConnector, LogicalTarget, MutationOutcome, OperationAttempt,
    DEFAULT_CLOUD_ORIGIN, FIRST_PARTY_CLOUD_ORIGIN,
};

#[test]
fn cutover_origin_constants_and_no_local_fallback_are_preserved_across_features() {
    assert_eq!(
        DEFAULT_CLOUD_ORIGIN,
        "https://goyxpcmxetztjlrrcwew.supabase.co"
    );
    assert_eq!(FIRST_PARTY_CLOUD_ORIGIN, "https://api.decapodlabs.ai");

    let dir = tempfile::tempdir().unwrap();
    let absent_sqlite = dir.path().join("must_never_be_created.sqlite");
    assert!(!absent_sqlite.exists());

    // Local connector never silently accepts cloud reconciliation or falls back.
    let local_path = dir.path().join("explicit_local.sqlite");
    let local =
        LogicalConnector::resolve(LogicalTarget::local(local_path.to_str().unwrap())).unwrap();
    let attempt = OperationAttempt::new("att-local-refuse").unwrap();
    let mut meta = dactyl_db::CloudResponseMetadata::default();
    let err = local
        .reconcile_operation(&attempt, "unused-token", None, None, &mut meta)
        .unwrap_err();
    assert_eq!(err.adapter_kind(), Some(AdapterErrorKind::Capability));
    assert_eq!(err.adapter_code(), Some("cloud_connector_required"));
    assert_eq!(err.mutation_outcome(), MutationOutcome::NotAttempted);
    assert!(!absent_sqlite.exists());
}

#[cfg(all(feature = "sqlite", feature = "supabase"))]
mod fault_reconciliation_campaign {
    use std::collections::{BTreeMap, HashMap, HashSet};
    use std::io::{Read, Write};
    use std::net::{Shutdown, TcpListener, TcpStream};
    use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::Duration;

    use dactyl_db::{
        AccessMode, AccountOperation, AdapterErrorKind, AuthScheme, CloudResponseMetadata,
        Connection, DactylError, DatastoreRoute, DecisionOutcome, EffectStatus, FailureOrigin,
        LogicalConnector, LogicalTarget, MutationOutcome, ObservationSource, OpenOptions,
        Operation, OperationAttempt, OperationKind, Parameter, ReconciliationObservation,
        RequestProof, RequestProofInput, RequestProofSigner, ResourceBudget, StorageContext,
        SupabaseRpcConfig, DEFAULT_CLOUD_ORIGIN, FIRST_PARTY_CLOUD_ORIGIN,
        PROVENANCE_CONTRACT_VERSION,
    };
    use serde_json::{json, Value};
    use tempfile::TempDir;

    const KEY: &str = "sb_publishable_fault_campaign_143";
    const HUMAN_BEARER: &str = "human-bearer-secret-143";
    const DEVICE_BEARER: &str = "dpc_at_device_secret_143";
    const MAX_RECONCILE_STEPS: usize = 3;

    #[derive(Clone)]
    struct Rng(u64);

    impl Rng {
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

        fn gen_range(&mut self, upper: usize) -> usize {
            (self.next_u64() as usize) % upper
        }
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    enum FaultMode {
        CleanCommit,
        BeforeCommitRollback,
        BeforeCommitNotAttempted,
        BeforeCommitUncertain500,
        AfterCommitDropAck,
        TruncatedAfterCommit,
        TimeoutLateCommit { delay: Duration },
        TimeoutBeforeCommit { delay: Duration },
        ConnectionResetBeforeEffect,
        AdmissionRefused { status: u16, code: &'static str },
        RateLimitedBeforeEffect,
        RateLimitedUncertainProvenance,
        DpopNonceChallengeOnce,
        DpopNonceChallengeTwice,
        DpopNonceChallengeAfterEffect,
        DuplicateConcatenatedJson,
        DuplicateProvenanceHeader,
        ReorderedWrongAttemptId,
        ReorderedWrongScopeRef,
        ReorderedWrongOperationKind,
        RedirectDuringWrite { location: String },
        UnadvertisedMissingGeneratedKeys200,
    }

    #[derive(Clone, Debug)]
    struct LedgerEntry {
        attempt_id: String,
        request_digest: String,
        request_id: String,
        policy_generation: u64,
        scope_ref: Option<String>,
        committed_value: Option<i64>,
        effect_status: &'static str,
        decision: &'static str,
        reason_code: Option<&'static str>,
    }

    #[derive(Clone, Debug)]
    struct RecordedWireRequest {
        method: String,
        path: String,
        headers: HashMap<String, String>,
    }

    impl RecordedWireRequest {
        fn header(&self, name: &str) -> Option<&str> {
            self.headers
                .get(&name.to_ascii_lowercase())
                .map(String::as_str)
        }
    }

    struct FaultProxyState {
        db_path: String,
        policy_generation: AtomicU64,
        revoked: AtomicBool,
        required_nonce: Mutex<Option<String>>,
        next_req_seq: AtomicU64,
        next_commit_seq: AtomicU64,
        fault_queue: Mutex<Vec<FaultMode>>,
        reconcile_fault_queue: Mutex<Vec<FaultMode>>,
        ledger: Mutex<HashMap<String, LedgerEntry>>,
        requests: Mutex<Vec<RecordedWireRequest>>,
        authorized_dispatches: AtomicUsize,
        unauthorized_dispatches: AtomicUsize,
        duplicate_commits_prevented: AtomicUsize,
        expected_origin_override: Mutex<Option<String>>,
    }

    struct FaultProxyServer {
        origin: String,
        state: Arc<FaultProxyState>,
        stop: Arc<AtomicBool>,
        handle: Option<thread::JoinHandle<()>>,
        _dir: TempDir,
    }

    impl FaultProxyServer {
        fn start() -> Self {
            let dir = TempDir::new().unwrap();
            let db_path = dir.path().join("proxy_ledger.sqlite");
            let db_path_str = db_path.to_string_lossy().into_owned();
            {
                let conn = Connection::open(DatastoreRoute::sqlite(&db_path_str)).unwrap();
                conn.atomic(&[Operation::schema(
                    "CREATE TABLE mutations (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        attempt_id TEXT UNIQUE NOT NULL,
                        request_digest TEXT NOT NULL,
                        request_id TEXT NOT NULL,
                        policy_generation INTEGER NOT NULL,
                        val INTEGER NOT NULL
                    );",
                    vec![],
                )])
                .unwrap();
            }

            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let origin = format!("http://{}", listener.local_addr().unwrap());

            let state = Arc::new(FaultProxyState {
                db_path: db_path_str.clone(),
                policy_generation: AtomicU64::new(1),
                revoked: AtomicBool::new(false),
                required_nonce: Mutex::new(None),
                next_req_seq: AtomicU64::new(1),
                next_commit_seq: AtomicU64::new(0),
                fault_queue: Mutex::new(Vec::new()),
                reconcile_fault_queue: Mutex::new(Vec::new()),
                ledger: Mutex::new(HashMap::new()),
                requests: Mutex::new(Vec::new()),
                authorized_dispatches: AtomicUsize::new(0),
                unauthorized_dispatches: AtomicUsize::new(0),
                duplicate_commits_prevented: AtomicUsize::new(0),
                expected_origin_override: Mutex::new(None),
            });

            let stop = Arc::new(AtomicBool::new(false));
            let stop_srv = stop.clone();
            let state_srv = state.clone();
            let origin_srv = origin.clone();

            let handle = thread::spawn(move || {
                let rw = Connection::open(DatastoreRoute::sqlite(&db_path_str)).unwrap();
                while !stop_srv.load(Ordering::Relaxed) {
                    let (mut stream, _) = match listener.accept() {
                        Ok(pair) => pair,
                        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(2));
                            continue;
                        }
                        Err(_) => break,
                    };
                    stream.set_nonblocking(false).unwrap();
                    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
                    handle_connection(&mut stream, &origin_srv, &state_srv, &rw);
                }
            });

            Self {
                origin,
                state,
                stop,
                handle: Some(handle),
                _dir: dir,
            }
        }

        fn push_fault(&self, mode: FaultMode) {
            self.state.fault_queue.lock().unwrap().push(mode);
        }

        fn push_reconcile_fault(&self, mode: FaultMode) {
            self.state.reconcile_fault_queue.lock().unwrap().push(mode);
        }

        fn observer(&self) -> Connection {
            Connection::open_with_options(
                DatastoreRoute::sqlite(&self.state.db_path),
                OpenOptions {
                    access_mode: AccessMode::ReadOnly,
                    ..OpenOptions::default()
                },
            )
            .unwrap()
        }

        fn persisted_attempts(&self) -> BTreeMap<String, (String, String, i64, i64)> {
            let obs = self.observer();
            let rows = obs
                .read(
                    "SELECT attempt_id, request_digest, request_id, policy_generation, val FROM mutations ORDER BY id",
                    &[],
                )
                .unwrap();
            let mut out = BTreeMap::new();
            for row in rows.iter() {
                let aid = row.get_str("attempt_id").unwrap();
                let dig = row.get_str("request_digest").unwrap();
                let rid = row.get_str("request_id").unwrap();
                let gen = row.get_int("policy_generation").unwrap();
                let val = row.get_int("val").unwrap();
                out.insert(aid, (dig, rid, gen, val));
            }
            out
        }
    }

    impl Drop for FaultProxyServer {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::SeqCst);
            if let Some(handle) = self.handle.take() {
                let _ = handle.join();
            }
        }
    }

    fn read_http_request(stream: &mut TcpStream) -> Option<(RecordedWireRequest, Vec<u8>)> {
        let mut buf = Vec::new();
        let mut chunk = [0_u8; 4096];
        let (head_str, head_end, content_length) = loop {
            match stream.read(&mut chunk) {
                Ok(0) => return None,
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
                Err(_) => return None,
            }
            if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                let head = String::from_utf8_lossy(&buf[..pos]).into_owned();
                let len = head
                    .lines()
                    .find_map(|line| {
                        let (k, v) = line.split_once(':')?;
                        k.eq_ignore_ascii_case("content-length")
                            .then(|| v.trim().parse::<usize>().ok())
                            .flatten()
                    })
                    .unwrap_or(0);
                break (head, pos + 4, len);
            }
        };
        while buf.len() < head_end + content_length {
            match stream.read(&mut chunk) {
                Ok(0) | Err(_) => return None,
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
            }
        }
        let mut lines = head_str.lines();
        let req_line = lines.next().unwrap_or("");
        let mut parts = req_line.split_whitespace();
        let method = parts.next().unwrap_or("").to_owned();
        let path = parts.next().unwrap_or("").to_owned();
        let mut headers = HashMap::new();
        for line in lines {
            if let Some((k, v)) = line.split_once(':') {
                headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_owned());
            }
        }
        let body = buf[head_end..head_end + content_length].to_vec();
        Some((
            RecordedWireRequest {
                method,
                path,
                headers,
            },
            body,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    fn build_provenance_headers(
        origin: &str,
        attempt_id: Option<&str>,
        request_digest: Option<&str>,
        scope_ref: Option<&str>,
        request_id: &str,
        effect_status: &str,
        decision: &str,
        operation_kind: &str,
        reason_code: Option<&str>,
        decision_ref: Option<&str>,
        auth_scheme: &str,
        policy_generation: u64,
    ) -> Vec<(String, String)> {
        let mut hdrs = vec![
            (
                "Decapod-Cloud-Provenance-Version".into(),
                PROVENANCE_CONTRACT_VERSION.to_string(),
            ),
            ("Decapod-Cloud-Request-Id".into(), request_id.to_owned()),
            (
                "Decapod-Cloud-Effect-Status".into(),
                effect_status.to_owned(),
            ),
            ("Decapod-Cloud-Decision".into(), decision.to_owned()),
            (
                "Decapod-Cloud-Operation-Kind".into(),
                operation_kind.to_owned(),
            ),
            ("Decapod-Cloud-Origin".into(), origin.to_owned()),
            ("Decapod-Cloud-Auth-Scheme".into(), auth_scheme.to_owned()),
            (
                "Decapod-Cloud-Policy-Generation".into(),
                policy_generation.to_string(),
            ),
        ];
        if let Some(aid) = attempt_id {
            hdrs.push(("Decapod-Cloud-Attempt-Id".into(), aid.to_owned()));
        }
        if let Some(dig) = request_digest {
            hdrs.push(("Decapod-Cloud-Request-Digest".into(), dig.to_owned()));
        }
        if let Some(scope) = scope_ref {
            hdrs.push(("Decapod-Cloud-Scope-Ref".into(), scope.to_owned()));
        }
        if let Some(code) = reason_code {
            hdrs.push(("Decapod-Cloud-Reason-Code".into(), code.to_owned()));
        }
        if let Some(dref) = decision_ref {
            hdrs.push(("Decapod-Cloud-Decision-Ref".into(), dref.to_owned()));
        }
        hdrs
    }

    fn send_http(stream: &mut TcpStream, status: u16, headers: &[(String, String)], body: &[u8]) {
        let mut raw = format!(
            "HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n",
            body.len()
        );
        for (k, v) in headers {
            raw.push_str(&format!("{k}: {v}\r\n"));
        }
        raw.push_str("\r\n");
        let _ = stream.write_all(raw.as_bytes());
        let _ = stream.write_all(body);
        let _ = stream.flush();
    }

    #[allow(clippy::too_many_arguments)]
    fn commit_mutation(
        rw: &Connection,
        state: &FaultProxyState,
        attempt_id: &str,
        request_digest: &str,
        request_id: &str,
        policy_gen: u64,
        scope_ref: Option<&str>,
        val: i64,
    ) -> Result<LedgerEntry, ()> {
        let mut ledger = state.ledger.lock().unwrap();
        if let Some(existing) = ledger.get(attempt_id) {
            if existing.request_digest != request_digest {
                return Err(());
            }
            if existing.effect_status == "committed_observed" {
                state
                    .duplicate_commits_prevented
                    .fetch_add(1, Ordering::SeqCst);
            }
            return Ok(existing.clone());
        }
        let seq = state.next_commit_seq.fetch_add(1, Ordering::SeqCst) + 1;
        let actual_val = if val == 0 { seq as i64 } else { val };
        rw.write(
            "INSERT INTO mutations (attempt_id, request_digest, request_id, policy_generation, val) VALUES (?1, ?2, ?3, ?4, ?5)",
            &[
                Parameter::Text(attempt_id.to_owned()),
                Parameter::Text(request_digest.to_owned()),
                Parameter::Text(request_id.to_owned()),
                Parameter::Integer(policy_gen as i64),
                Parameter::Integer(actual_val),
            ],
        )
        .unwrap();
        let entry = LedgerEntry {
            attempt_id: attempt_id.to_owned(),
            request_digest: request_digest.to_owned(),
            request_id: request_id.to_owned(),
            policy_generation: policy_gen,
            scope_ref: scope_ref.map(str::to_owned),
            committed_value: Some(actual_val),
            effect_status: "committed_observed",
            decision: "allowed",
            reason_code: None,
        };
        ledger.insert(attempt_id.to_owned(), entry.clone());
        Ok(entry)
    }

    fn success_body_for_path(path: &str, val: i64) -> Vec<u8> {
        if path.contains("dactyl_batch_v1") || path.contains("cloud-batch") {
            serde_json::to_vec(&json!({
                "results": [{
                    "columns": [],
                    "rows": [],
                    "affected_rows": 1,
                    "generated_keys": [val]
                }]
            }))
            .unwrap()
        } else if path.contains("dactyl_query_v1") || path.contains("cloud-query") {
            serde_json::to_vec(&json!({
                "columns": [],
                "rows": [],
                "affected_rows": 1,
                "generated_keys": [val]
            }))
            .unwrap()
        } else {
            serde_json::to_vec(&json!({
                "ok": true,
                "committed_val": val
            }))
            .unwrap()
        }
    }

    fn handle_connection(
        stream: &mut TcpStream,
        origin: &str,
        state: &FaultProxyState,
        rw: &Connection,
    ) {
        let Some((req, body)) = read_http_request(stream) else {
            return;
        };
        state.requests.lock().unwrap().push(req.clone());

        let req_seq = state.next_req_seq.fetch_add(1, Ordering::SeqCst);
        let request_id = format!("req-143-{req_seq:05}");
        let policy_gen = state.policy_generation.load(Ordering::SeqCst);

        let auth_hdr = req.header("authorization").unwrap_or("");
        let auth_scheme = if auth_hdr.starts_with("DPoP ") || req.header("dpop").is_some() {
            "dpop"
        } else {
            "bearer"
        };

        // Discovery endpoint
        if req.method == "GET" && req.path.ends_with("/config") {
            let advertised = state
                .expected_origin_override
                .lock()
                .unwrap()
                .clone()
                .unwrap_or_else(|| origin.to_owned());
            let body = serde_json::to_vec(&json!({
                "version": 1,
                "project_origin": advertised,
                "publishable_key": KEY,
                "schema": "dactyl_api"
            }))
            .unwrap();
            send_http(stream, 200, &[], &body);
            return;
        }

        let attempt_id = req.header("decapod-cloud-attempt-id");
        let request_digest = req.header("decapod-cloud-request-digest");
        let scope_ref = req.header("decapod-cloud-scope-ref");

        // Enforce DPoP htu validation if DPoP proof carries htu=...
        if let Some(dpop_hdr) = req.header("dpop") {
            if let Some(htu_part) = dpop_hdr.split(';').find_map(|p| p.strip_prefix("htu=")) {
                let expected_htu = format!("{origin}{}", req.path.split('?').next().unwrap_or(""));
                if htu_part != expected_htu {
                    state.unauthorized_dispatches.fetch_add(1, Ordering::SeqCst);
                    let hdrs = build_provenance_headers(
                        origin,
                        attempt_id,
                        request_digest,
                        scope_ref,
                        &request_id,
                        "refused",
                        "refused",
                        if req.path.contains("operation-status") || req.path.contains("ctrl_status")
                        {
                            "read"
                        } else {
                            "write"
                        },
                        Some("dpop_htu_mismatch"),
                        Some("dec-htu-mismatch"),
                        auth_scheme,
                        policy_gen,
                    );
                    let err_body = serde_json::to_vec(&json!({
                        "error": {"code": "dpop_htu_mismatch", "message": "raw-htu-secret"}
                    }))
                    .unwrap();
                    send_http(stream, 401, &hdrs, &err_body);
                    return;
                }
            }
        }

        // Reconciliation readback path
        let is_reconcile = req.path.contains("operation-status")
            || req.path.contains("dactyl_ctrl_status")
            || req.header("decapod-cloud-operation-kind") == Some("read");
        if is_reconcile {
            let fault = {
                let mut q = state.reconcile_fault_queue.lock().unwrap();
                if q.is_empty() {
                    FaultMode::CleanCommit
                } else {
                    q.remove(0)
                }
            };
            match fault {
                FaultMode::ConnectionResetBeforeEffect => {
                    let _ = stream.shutdown(Shutdown::Both);
                    return;
                }
                FaultMode::TimeoutBeforeCommit { delay } => {
                    thread::sleep(delay);
                    let body = serde_json::to_vec(&json!({"status": "delayed"})).unwrap();
                    send_http(stream, 503, &[], &body);
                    return;
                }
                _ => {}
            }

            let parsed: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
            let lookup_aid = attempt_id
                .or_else(|| parsed.get("attempt_id").and_then(Value::as_str))
                .unwrap_or("");
            let ledger = state.ledger.lock().unwrap();
            if let Some(entry) = ledger.get(lookup_aid) {
                let hdrs = build_provenance_headers(
                    origin,
                    Some(&entry.attempt_id),
                    Some(&entry.request_digest),
                    entry.scope_ref.as_deref(),
                    &entry.request_id,
                    entry.effect_status,
                    entry.decision,
                    "read",
                    entry.reason_code,
                    Some("dec-reconcile"),
                    auth_scheme,
                    entry.policy_generation,
                );
                let resp_body = serde_json::to_vec(&json!({
                    "found": true,
                    "request_id": entry.request_id,
                    "policy_generation": entry.policy_generation,
                    "effect_status": entry.effect_status,
                    "committed_val": entry.committed_value
                }))
                .unwrap();
                send_http(stream, 200, &hdrs, &resp_body);
            } else {
                let hdrs = build_provenance_headers(
                    origin,
                    Some(lookup_aid),
                    request_digest,
                    scope_ref,
                    &request_id,
                    "not_attempted",
                    "allowed",
                    "read",
                    Some("not_attempted"),
                    None,
                    auth_scheme,
                    policy_gen,
                );
                let resp_body = serde_json::to_vec(&json!({
                    "found": false,
                    "effect_status": "not_attempted"
                }))
                .unwrap();
                send_http(stream, 200, &hdrs, &resp_body);
            }
            return;
        }

        // Check mid-flight revocation before admission
        if state.revoked.load(Ordering::SeqCst) {
            state.unauthorized_dispatches.fetch_add(1, Ordering::SeqCst);
            if let Some(aid) = attempt_id {
                state
                    .ledger
                    .lock()
                    .unwrap()
                    .entry(aid.to_owned())
                    .or_insert_with(|| LedgerEntry {
                        attempt_id: aid.to_owned(),
                        request_digest: request_digest.unwrap_or("none").to_owned(),
                        request_id: request_id.clone(),
                        policy_generation: policy_gen,
                        scope_ref: scope_ref.map(str::to_owned),
                        committed_value: None,
                        effect_status: "refused",
                        decision: "refused",
                        reason_code: Some("grant_revoked"),
                    });
            }
            let hdrs = build_provenance_headers(
                origin,
                attempt_id,
                request_digest,
                scope_ref,
                &request_id,
                "refused",
                "refused",
                "write",
                Some("grant_revoked"),
                Some("dec-revoked"),
                auth_scheme,
                policy_gen,
            );
            let err_body = serde_json::to_vec(&json!({
                "error": {"code": "grant_revoked", "message": "secret-revocation-detail"}
            }))
            .unwrap();
            send_http(stream, 403, &hdrs, &err_body);
            return;
        }

        // Enforce required DPoP nonce if configured
        let required_nonce = state.required_nonce.lock().unwrap().clone();
        if let Some(expected_nonce) = required_nonce {
            let has_nonce = req
                .header("dpop")
                .is_some_and(|p| p.contains(&format!("nonce={expected_nonce}")));
            if !has_nonce {
                let mut hdrs = build_provenance_headers(
                    origin,
                    attempt_id,
                    request_digest,
                    scope_ref,
                    &request_id,
                    "refused",
                    "refused",
                    "write",
                    Some("use_dpop_nonce"),
                    None,
                    auth_scheme,
                    policy_gen,
                );
                hdrs.push((
                    "WWW-Authenticate".into(),
                    "DPoP error=\"use_dpop_nonce\"".into(),
                ));
                hdrs.push(("DPoP-Nonce".into(), expected_nonce));
                let err_body = serde_json::to_vec(&json!({
                    "error": {"code": "use_dpop_nonce"}
                }))
                .unwrap();
                send_http(stream, 401, &hdrs, &err_body);
                return;
            }
        }

        let fault = {
            let mut q = state.fault_queue.lock().unwrap();
            if q.is_empty() {
                FaultMode::CleanCommit
            } else {
                q.remove(0)
            }
        };

        let aid = attempt_id.unwrap_or("untracked");
        let dig = request_digest.unwrap_or("sha256:none");
        let val = serde_json::from_slice::<Value>(&body)
            .ok()
            .and_then(|v| {
                v.get("request")
                    .and_then(|r| r.get("val"))
                    .or_else(|| v.get("val"))
                    .and_then(Value::as_i64)
            })
            .unwrap_or(0);

        // Check idempotency conflict (same key, different payload) before fault barriers
        if let Some(existing) = state.ledger.lock().unwrap().get(aid).cloned() {
            if existing.request_digest != dig {
                let hdrs = build_provenance_headers(
                    origin,
                    Some(aid),
                    Some(dig),
                    scope_ref,
                    &request_id,
                    "refused",
                    "refused",
                    "write",
                    Some("idempotency_conflict"),
                    Some("dec-idemp-conflict"),
                    auth_scheme,
                    policy_gen,
                );
                let err_body = serde_json::to_vec(&json!({
                    "error": {"code": "idempotency_conflict"}
                }))
                .unwrap();
                send_http(stream, 409, &hdrs, &err_body);
                return;
            }
        }

        state.authorized_dispatches.fetch_add(1, Ordering::SeqCst);

        match fault {
            FaultMode::CleanCommit => {
                let entry =
                    commit_mutation(rw, state, aid, dig, &request_id, policy_gen, scope_ref, val)
                        .unwrap();
                let hdrs = build_provenance_headers(
                    origin,
                    Some(aid),
                    Some(dig),
                    scope_ref,
                    &entry.request_id,
                    "committed_observed",
                    "allowed",
                    "write",
                    None,
                    Some("dec-commit"),
                    auth_scheme,
                    entry.policy_generation,
                );
                send_http(
                    stream,
                    200,
                    &hdrs,
                    &success_body_for_path(&req.path, entry.committed_value.unwrap_or(1)),
                );
            }
            FaultMode::BeforeCommitRollback => {
                state
                    .ledger
                    .lock()
                    .unwrap()
                    .entry(aid.to_owned())
                    .or_insert_with(|| LedgerEntry {
                        attempt_id: aid.to_owned(),
                        request_digest: dig.to_owned(),
                        request_id: request_id.clone(),
                        policy_generation: policy_gen,
                        scope_ref: scope_ref.map(str::to_owned),
                        committed_value: None,
                        effect_status: "rolled_back",
                        decision: "allowed",
                        reason_code: Some("transaction_aborted"),
                    });
                let hdrs = build_provenance_headers(
                    origin,
                    Some(aid),
                    Some(dig),
                    scope_ref,
                    &request_id,
                    "rolled_back",
                    "allowed",
                    "write",
                    Some("transaction_aborted"),
                    Some("dec-rb"),
                    auth_scheme,
                    policy_gen,
                );
                let err_body = serde_json::to_vec(&json!({
                    "error": {"code": "transaction_aborted"}
                }))
                .unwrap();
                send_http(stream, 500, &hdrs, &err_body);
            }
            FaultMode::BeforeCommitNotAttempted => {
                let hdrs = build_provenance_headers(
                    origin,
                    Some(aid),
                    Some(dig),
                    scope_ref,
                    &request_id,
                    "not_attempted",
                    "allowed",
                    "write",
                    Some("storage_unavailable"),
                    None,
                    auth_scheme,
                    policy_gen,
                );
                let err_body = serde_json::to_vec(&json!({
                    "error": {"code": "storage_unavailable"}
                }))
                .unwrap();
                send_http(stream, 503, &hdrs, &err_body);
            }
            FaultMode::BeforeCommitUncertain500 => {
                let err_body = serde_json::to_vec(&json!({
                    "error": {"message": "internal-pg-dsn-leak"}
                }))
                .unwrap();
                send_http(stream, 500, &[], &err_body);
            }
            FaultMode::AfterCommitDropAck => {
                let _ =
                    commit_mutation(rw, state, aid, dig, &request_id, policy_gen, scope_ref, val)
                        .unwrap();
                let _ = stream.shutdown(Shutdown::Both);
            }
            FaultMode::TruncatedAfterCommit => {
                let entry =
                    commit_mutation(rw, state, aid, dig, &request_id, policy_gen, scope_ref, val)
                        .unwrap();
                let hdrs = build_provenance_headers(
                    origin,
                    Some(aid),
                    Some(dig),
                    scope_ref,
                    &entry.request_id,
                    "committed_observed",
                    "allowed",
                    "write",
                    None,
                    Some("dec-trunc"),
                    auth_scheme,
                    entry.policy_generation,
                );
                let mut raw = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 4096\r\nConnection: close\r\n".to_owned();
                for (k, v) in hdrs {
                    raw.push_str(&format!("{k}: {v}\r\n"));
                }
                raw.push_str("\r\n{\"affected_rows\":1");
                let _ = stream.write_all(raw.as_bytes());
                let _ = stream.flush();
                let _ = stream.shutdown(Shutdown::Both);
            }
            FaultMode::TimeoutLateCommit { delay } => {
                let entry =
                    commit_mutation(rw, state, aid, dig, &request_id, policy_gen, scope_ref, val)
                        .unwrap();
                thread::sleep(delay);
                let hdrs = build_provenance_headers(
                    origin,
                    Some(aid),
                    Some(dig),
                    scope_ref,
                    &entry.request_id,
                    "committed_observed",
                    "allowed",
                    "write",
                    None,
                    None,
                    auth_scheme,
                    entry.policy_generation,
                );
                send_http(
                    stream,
                    200,
                    &hdrs,
                    &success_body_for_path(&req.path, entry.committed_value.unwrap_or(1)),
                );
            }
            FaultMode::TimeoutBeforeCommit { delay } => {
                thread::sleep(delay);
                let err_body = serde_json::to_vec(&json!({
                    "error": {"code": "timeout"}
                }))
                .unwrap();
                send_http(stream, 504, &[], &err_body);
            }
            FaultMode::ConnectionResetBeforeEffect => {
                let _ = stream.shutdown(Shutdown::Both);
            }
            FaultMode::AdmissionRefused { status, code } => {
                let hdrs = build_provenance_headers(
                    origin,
                    Some(aid),
                    Some(dig),
                    scope_ref,
                    &request_id,
                    "refused",
                    "refused",
                    "write",
                    Some(code),
                    Some("dec-admit-refuse"),
                    auth_scheme,
                    policy_gen,
                );
                let err_body = serde_json::to_vec(&json!({
                    "error": {"code": code, "message": "raw-admission-secret"}
                }))
                .unwrap();
                send_http(stream, status, &hdrs, &err_body);
            }
            FaultMode::RateLimitedBeforeEffect => {
                let mut hdrs = build_provenance_headers(
                    origin,
                    Some(aid),
                    Some(dig),
                    scope_ref,
                    &request_id,
                    "refused",
                    "refused",
                    "write",
                    Some("rate_limited"),
                    Some("dec-429"),
                    auth_scheme,
                    policy_gen,
                );
                hdrs.push(("Retry-After".into(), "30".into()));
                let err_body = serde_json::to_vec(&json!({
                    "error": {"code": "rate_limited"}
                }))
                .unwrap();
                send_http(stream, 429, &hdrs, &err_body);
            }
            FaultMode::RateLimitedUncertainProvenance => {
                let _ =
                    commit_mutation(rw, state, aid, dig, &request_id, policy_gen, scope_ref, val)
                        .unwrap();
                let mut hdrs = build_provenance_headers(
                    origin,
                    Some(aid),
                    Some(dig),
                    scope_ref,
                    &request_id,
                    "accepted_not_committed",
                    "allowed",
                    "write",
                    Some("rate_limited"),
                    None,
                    auth_scheme,
                    policy_gen,
                );
                hdrs.push(("Retry-After".into(), "5".into()));
                let err_body = serde_json::to_vec(&json!({
                    "error": {"code": "rate_limited"}
                }))
                .unwrap();
                send_http(stream, 429, &hdrs, &err_body);
            }
            FaultMode::DpopNonceChallengeOnce => {
                *state.required_nonce.lock().unwrap() = Some("nonce-next-1".to_owned());
                let mut hdrs = build_provenance_headers(
                    origin,
                    Some(aid),
                    Some(dig),
                    scope_ref,
                    &request_id,
                    "refused",
                    "refused",
                    "write",
                    Some("use_dpop_nonce"),
                    None,
                    auth_scheme,
                    policy_gen,
                );
                hdrs.push((
                    "WWW-Authenticate".into(),
                    "DPoP error=\"use_dpop_nonce\"".into(),
                ));
                hdrs.push(("DPoP-Nonce".into(), "nonce-next-1".into()));
                let err_body = serde_json::to_vec(&json!({
                    "error": {"code": "use_dpop_nonce"}
                }))
                .unwrap();
                send_http(stream, 401, &hdrs, &err_body);
            }
            FaultMode::DpopNonceChallengeTwice => {
                *state.required_nonce.lock().unwrap() = Some("nonce-twice".to_owned());
                let mut hdrs = build_provenance_headers(
                    origin,
                    Some(aid),
                    Some(dig),
                    scope_ref,
                    &request_id,
                    "refused",
                    "refused",
                    "write",
                    Some("use_dpop_nonce"),
                    None,
                    auth_scheme,
                    policy_gen,
                );
                hdrs.push((
                    "WWW-Authenticate".into(),
                    "DPoP error=\"use_dpop_nonce\"".into(),
                ));
                hdrs.push(("DPoP-Nonce".into(), "nonce-twice".into()));
                let err_body = serde_json::to_vec(&json!({
                    "error": {"code": "use_dpop_nonce"}
                }))
                .unwrap();
                state
                    .fault_queue
                    .lock()
                    .unwrap()
                    .insert(0, FaultMode::DpopNonceChallengeOnce);
                send_http(stream, 401, &hdrs, &err_body);
            }
            FaultMode::DpopNonceChallengeAfterEffect => {
                let _ =
                    commit_mutation(rw, state, aid, dig, &request_id, policy_gen, scope_ref, val)
                        .unwrap();
                let mut hdrs = build_provenance_headers(
                    origin,
                    Some(aid),
                    Some(dig),
                    scope_ref,
                    &request_id,
                    "accepted_not_committed",
                    "allowed",
                    "write",
                    Some("use_dpop_nonce"),
                    None,
                    auth_scheme,
                    policy_gen,
                );
                hdrs.push((
                    "WWW-Authenticate".into(),
                    "DPoP error=\"use_dpop_nonce\"".into(),
                ));
                hdrs.push(("DPoP-Nonce".into(), "nonce-after-effect".into()));
                let err_body = serde_json::to_vec(&json!({
                    "error": {"code": "use_dpop_nonce"}
                }))
                .unwrap();
                send_http(stream, 401, &hdrs, &err_body);
            }
            FaultMode::DuplicateConcatenatedJson => {
                let entry =
                    commit_mutation(rw, state, aid, dig, &request_id, policy_gen, scope_ref, val)
                        .unwrap();
                let hdrs = build_provenance_headers(
                    origin,
                    Some(aid),
                    Some(dig),
                    scope_ref,
                    &entry.request_id,
                    "committed_observed",
                    "allowed",
                    "write",
                    None,
                    None,
                    auth_scheme,
                    entry.policy_generation,
                );
                let one = success_body_for_path(&req.path, 1);
                let mut dup = one.clone();
                dup.extend_from_slice(&one);
                send_http(stream, 200, &hdrs, &dup);
            }
            FaultMode::DuplicateProvenanceHeader => {
                let mut hdrs = build_provenance_headers(
                    origin,
                    Some(aid),
                    Some(dig),
                    scope_ref,
                    &request_id,
                    "committed_observed",
                    "allowed",
                    "write",
                    None,
                    None,
                    auth_scheme,
                    policy_gen,
                );
                hdrs.push((
                    "Decapod-Cloud-Effect-Status".into(),
                    "committed_observed".into(),
                ));
                send_http(stream, 200, &hdrs, &success_body_for_path(&req.path, 1));
            }
            FaultMode::ReorderedWrongAttemptId => {
                let hdrs = build_provenance_headers(
                    origin,
                    Some("att-reordered-other-request"),
                    Some(dig),
                    scope_ref,
                    &request_id,
                    "committed_observed",
                    "allowed",
                    "write",
                    None,
                    None,
                    auth_scheme,
                    policy_gen,
                );
                send_http(stream, 200, &hdrs, &success_body_for_path(&req.path, 1));
            }
            FaultMode::ReorderedWrongScopeRef => {
                let hdrs = build_provenance_headers(
                    origin,
                    Some(aid),
                    Some(dig),
                    Some("scope:wrong-tenant"),
                    &request_id,
                    "committed_observed",
                    "allowed",
                    "write",
                    None,
                    None,
                    auth_scheme,
                    policy_gen,
                );
                send_http(stream, 200, &hdrs, &success_body_for_path(&req.path, 1));
            }
            FaultMode::ReorderedWrongOperationKind => {
                let hdrs = build_provenance_headers(
                    origin,
                    Some(aid),
                    Some(dig),
                    scope_ref,
                    &request_id,
                    "committed_observed",
                    "allowed",
                    "read",
                    None,
                    None,
                    auth_scheme,
                    policy_gen,
                );
                send_http(stream, 200, &hdrs, &success_body_for_path(&req.path, 1));
            }
            FaultMode::RedirectDuringWrite { location } => {
                let mut hdrs = build_provenance_headers(
                    origin,
                    Some(aid),
                    Some(dig),
                    scope_ref,
                    &request_id,
                    "committed_observed",
                    "allowed",
                    "write",
                    None,
                    None,
                    auth_scheme,
                    policy_gen,
                );
                hdrs.push(("Location".into(), location));
                send_http(stream, 307, &hdrs, b"{}");
            }
            FaultMode::UnadvertisedMissingGeneratedKeys200 => {
                let _ =
                    commit_mutation(rw, state, aid, dig, &request_id, policy_gen, scope_ref, val)
                        .unwrap();
                let body = serde_json::to_vec(&json!({
                    "columns": [],
                    "rows": [],
                    "affected_rows": 1
                }))
                .unwrap();
                send_http(
                    stream,
                    200,
                    &[("Decapod-Cloud-Request-Id".into(), request_id)],
                    &body,
                );
            }
        }
    }

    struct DynamicDpopSigner {
        forced_htu: Option<String>,
        observed_nonces: Mutex<Vec<(String, String)>>,
        sign_calls: AtomicUsize,
    }

    impl DynamicDpopSigner {
        fn new() -> Self {
            Self {
                forced_htu: None,
                observed_nonces: Mutex::new(Vec::new()),
                sign_calls: AtomicUsize::new(0),
            }
        }

        fn with_forced_htu(htu: impl Into<String>) -> Self {
            Self {
                forced_htu: Some(htu.into()),
                observed_nonces: Mutex::new(Vec::new()),
                sign_calls: AtomicUsize::new(0),
            }
        }
    }

    impl RequestProofSigner for DynamicDpopSigner {
        fn sign_request(&self, input: &RequestProofInput<'_>) -> Result<RequestProof, DactylError> {
            self.sign_calls.fetch_add(1, Ordering::SeqCst);
            let htu = self.forced_htu.as_deref().unwrap_or(input.canonical_url());
            let nonce = input.nonce().unwrap_or("none");
            RequestProof::dpop(format!(
                "proof;htm={};htu={htu};nonce={nonce}",
                input.method()
            ))
        }

        fn observe_nonce(&self, canonical_url: &str, nonce: &str) {
            self.observed_nonces
                .lock()
                .unwrap()
                .push((canonical_url.to_owned(), nonce.to_owned()));
        }
    }

    fn test_context() -> StorageContext {
        StorageContext::new(1, json!({"org_id": "org-143", "repo_id": "dactyl"})).unwrap()
    }

    fn open_rpc_conn(
        origin: &str,
        access_mode: AccessMode,
        budget: ResourceBudget,
        signer: Option<Arc<dyn RequestProofSigner>>,
        device_gateway: bool,
    ) -> Connection {
        let mut cfg = SupabaseRpcConfig::v1(origin, KEY);
        if device_gateway {
            cfg = cfg.with_device_gateway();
        }
        let mut route = DatastoreRoute::supabase_rpc(
            cfg,
            Some(if device_gateway {
                DEVICE_BEARER.to_owned()
            } else {
                HUMAN_BEARER.to_owned()
            }),
        );
        if let Some(s) = signer {
            route = route.with_proof_signer(s);
        }
        Connection::open_with_budget(
            route,
            OpenOptions {
                access_mode,
                ..OpenOptions::default()
            },
            Some(test_context()),
            budget,
        )
        .unwrap()
    }

    fn bounded_reconcile_control(
        connector: &LogicalConnector,
        attempt: &OperationAttempt,
        prior_status: EffectStatus,
        budget: ResourceBudget,
    ) -> Result<ReconciliationObservation, DactylError> {
        let mut last_err = None;
        for _ in 0..MAX_RECONCILE_STEPS {
            let mut rec_meta = CloudResponseMetadata::default();
            match connector.reconcile_control_attempt(
                "dactyl_ctrl_status",
                attempt,
                Some(HUMAN_BEARER),
                None,
                budget,
                Some(prior_status),
                &mut rec_meta,
            ) {
                Ok(obs) => return Ok(obs),
                Err(err) => last_err = Some(err),
            }
        }
        Err(last_err.unwrap())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn fault_barriers_commit_ack_loss_and_dpop_post_effect_guards_hold() {
        let proxy = FaultProxyServer::start();
        let redirect_sink = TcpListener::bind("127.0.0.1:0").unwrap();
        redirect_sink.set_nonblocking(true).unwrap();
        let sink_url = format!("http://{}/steal", redirect_sink.local_addr().unwrap());

        // 1. Pre-send failure barriers: 0 requests reach server.
        let ro_conn = open_rpc_conn(
            &proxy.origin,
            AccessMode::ReadOnly,
            ResourceBudget::v1(),
            None,
            false,
        );
        let pre_att = OperationAttempt::new("att-presend-ro")
            .unwrap()
            .with_scope_ref("scope:143")
            .unwrap();
        let mut pre_meta = CloudResponseMetadata::default();
        let ro_err = ro_conn
            .write_with_attempt("INSERT INTO t VALUES (1)", &[], &pre_att, &mut pre_meta)
            .unwrap_err();
        assert_eq!(ro_err.mutation_outcome(), MutationOutcome::NotAttempted);
        assert_eq!(
            pre_meta
                .provenance()
                .map(|p| (p.status(), p.observation_source())),
            Some((
                EffectStatus::NotAttempted,
                ObservationSource::LocalPreflight
            ))
        );

        let unsigned_gw = LogicalConnector::cloud_with_config(
            SupabaseRpcConfig::v1(&proxy.origin, KEY).with_device_gateway(),
        )
        .unwrap();
        let mut gw_meta = CloudResponseMetadata::default();
        let gw_err = unsigned_gw
            .control_request_with_attempt(
                "dactyl_ctrl_mutate",
                &json!({"val": 10}),
                Some(DEVICE_BEARER),
                None,
                OperationKind::Write,
                ResourceBudget::v1(),
                &pre_att,
                &mut gw_meta,
            )
            .unwrap_err();
        assert_eq!(gw_err.adapter_code(), Some("device_proof_required"));
        assert_eq!(gw_err.mutation_outcome(), MutationOutcome::NotAttempted);
        assert_eq!(
            gw_meta.provenance().map(|p| p.status()),
            Some(EffectStatus::NotAttempted)
        );
        assert_eq!(proxy.state.requests.lock().unwrap().len(), 0);

        // 2. Commit-ack loss (AfterCommitDropAck) across sync write, async atomic, and control RPC:
        //    Must preserve MutationOutcome::Uncertain + EffectStatus::Uncertain, then reconcile.
        proxy.push_fault(FaultMode::AfterCommitDropAck);
        proxy.push_fault(FaultMode::AfterCommitDropAck);
        proxy.push_fault(FaultMode::AfterCommitDropAck);

        let rw_conn = open_rpc_conn(
            &proxy.origin,
            AccessMode::ReadWrite,
            ResourceBudget::v1(),
            None,
            false,
        );
        let connector =
            LogicalConnector::cloud_with_config(SupabaseRpcConfig::v1(&proxy.origin, KEY)).unwrap();

        let att_write = OperationAttempt::new("att-ack-loss-write")
            .unwrap()
            .with_scope_ref("scope:143")
            .unwrap();
        let mut meta_w = CloudResponseMetadata::default();
        let err_w = rw_conn
            .write_with_attempt("INSERT INTO m VALUES (1)", &[], &att_write, &mut meta_w)
            .unwrap_err();
        assert_eq!(err_w.mutation_outcome(), MutationOutcome::Uncertain);
        assert_eq!(
            meta_w.provenance().map(|p| p.status()),
            Some(EffectStatus::Uncertain)
        );

        let att_atomic = OperationAttempt::new("att-ack-loss-atomic")
            .unwrap()
            .with_scope_ref("scope:143")
            .unwrap();
        let mut meta_a = CloudResponseMetadata::default();
        let err_a = rw_conn
            .atomic_with_attempt_async(
                &[Operation::write("INSERT INTO m VALUES (2)", vec![])],
                &att_atomic,
                &mut meta_a,
            )
            .await
            .unwrap_err();
        assert_eq!(err_a.mutation_outcome(), MutationOutcome::Uncertain);
        assert_eq!(
            meta_a.provenance().map(|p| p.status()),
            Some(EffectStatus::Uncertain)
        );

        let att_ctrl = OperationAttempt::new("att-ack-loss-ctrl")
            .unwrap()
            .with_scope_ref("scope:143")
            .unwrap();
        let mut meta_c = CloudResponseMetadata::default();
        let err_c = connector
            .control_request_with_attempt_async(
                "dactyl_ctrl_mutate",
                &json!({"val": 42}),
                Some(HUMAN_BEARER),
                None,
                OperationKind::Write,
                ResourceBudget::v1(),
                &att_ctrl,
                &mut meta_c,
            )
            .await
            .unwrap_err();
        assert_eq!(err_c.adapter_code(), Some("outcome_unknown"));
        assert_eq!(err_c.mutation_outcome(), MutationOutcome::Uncertain);
        let ctrl_digest = meta_c
            .provenance()
            .unwrap()
            .request_digest()
            .unwrap()
            .to_owned();

        // Reconcile control attempt using original attempt + digest -> CommittedObserved
        let rec_att = OperationAttempt::new("att-ack-loss-ctrl")
            .unwrap()
            .with_request_digest(ctrl_digest)
            .unwrap()
            .with_scope_ref("scope:143")
            .unwrap();
        let obs = bounded_reconcile_control(
            &connector,
            &rec_att,
            EffectStatus::Uncertain,
            ResourceBudget::v1(),
        )
        .unwrap();
        assert_eq!(obs.prior_client_status(), Some(EffectStatus::Uncertain));
        assert_eq!(obs.provenance().status(), EffectStatus::CommittedObserved);
        assert_eq!(
            obs.provenance().observation_source(),
            ObservationSource::ServerReadback
        );
        let persisted = proxy.persisted_attempts();
        assert_eq!(persisted.len(), 3);
        assert_eq!(
            persisted["att-ack-loss-ctrl"].1,
            obs.provenance().request_id().unwrap()
        );
        assert_eq!(persisted["att-ack-loss-ctrl"].3, 42);

        // 3. Truncated body, duplicated JSON, duplicate provenance header, reordered attempts/scopes/ops, redirect.
        for (idx, mode) in [
            FaultMode::TruncatedAfterCommit,
            FaultMode::DuplicateConcatenatedJson,
            FaultMode::DuplicateProvenanceHeader,
            FaultMode::ReorderedWrongAttemptId,
            FaultMode::ReorderedWrongScopeRef,
            FaultMode::ReorderedWrongOperationKind,
            FaultMode::RedirectDuringWrite { location: sink_url },
            FaultMode::UnadvertisedMissingGeneratedKeys200,
        ]
        .into_iter()
        .enumerate()
        {
            proxy.push_fault(mode);
            let att = OperationAttempt::new(format!("att-wire-anom-{idx}"))
                .unwrap()
                .with_scope_ref("scope:143")
                .unwrap();
            let mut meta = CloudResponseMetadata::default();
            let err = rw_conn
                .write_with_attempt("INSERT INTO m VALUES (9)", &[], &att, &mut meta)
                .unwrap_err();
            assert_eq!(
                err.mutation_outcome(),
                MutationOutcome::Uncertain,
                "anomaly case {idx} must remain Uncertain"
            );
            assert_eq!(
                meta.provenance().map(|p| p.status()),
                Some(EffectStatus::Uncertain),
                "anomaly case {idx} provenance must remain Uncertain"
            );
        }
        assert!(
            redirect_sink.accept().is_err(),
            "redirect target must never be contacted"
        );

        // 4. 401 DPoP use_dpop_nonce before effect retries once & succeeds;
        //    401 use_dpop_nonce AFTER effect (accepted_not_committed) MUST NOT retry and MUST stay Uncertain!
        let signer = Arc::new(DynamicDpopSigner::new());
        let dpop_gw = LogicalConnector::cloud_with_config(
            SupabaseRpcConfig::v1(&proxy.origin, KEY).with_device_gateway(),
        )
        .unwrap()
        .with_proof_signer(signer.clone());

        proxy.push_fault(FaultMode::DpopNonceChallengeOnce);
        let att_nonce_ok = OperationAttempt::new("att-dpop-before-effect").unwrap();
        let mut meta_nonce_ok = CloudResponseMetadata::default();
        let val = dpop_gw
            .control_request_with_attempt(
                "dactyl_ctrl_mutate",
                &json!({"val": 77}),
                Some(DEVICE_BEARER),
                None,
                OperationKind::Write,
                ResourceBudget::v1(),
                &att_nonce_ok,
                &mut meta_nonce_ok,
            )
            .unwrap();
        assert_eq!(val["committed_val"], 77);
        assert_eq!(signer.sign_calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            meta_nonce_ok.provenance().map(|p| p.status()),
            Some(EffectStatus::CommittedObserved)
        );
        // Clear required_nonce for subsequent test cases
        *proxy.state.required_nonce.lock().unwrap() = None;

        // Post-effect 401 use_dpop_nonce MUST NOT retry!
        let before_sign_calls = signer.sign_calls.load(Ordering::SeqCst);
        let before_commits = proxy.persisted_attempts().len();
        proxy.push_fault(FaultMode::DpopNonceChallengeAfterEffect);
        let att_nonce_after = OperationAttempt::new("att-dpop-after-effect").unwrap();
        let mut meta_nonce_after = CloudResponseMetadata::default();
        let after_err = dpop_gw
            .control_request_with_attempt_async(
                "dactyl_ctrl_mutate",
                &json!({"val": 88}),
                Some(DEVICE_BEARER),
                None,
                OperationKind::Write,
                ResourceBudget::v1(),
                &att_nonce_after,
                &mut meta_nonce_after,
            )
            .await
            .unwrap_err();
        assert_eq!(
            signer.sign_calls.load(Ordering::SeqCst) - before_sign_calls,
            1,
            "post-effect 401 use_dpop_nonce must NEVER trigger retry"
        );
        assert_eq!(after_err.mutation_outcome(), MutationOutcome::Uncertain);
        assert_eq!(after_err.adapter_code(), Some("outcome_unknown"));
        assert_eq!(
            meta_nonce_after.provenance().map(|p| p.status()),
            Some(EffectStatus::Uncertain)
        );
        assert_eq!(proxy.persisted_attempts().len(), before_commits + 1);

        // 5. 429 rate_limited before effect vs 429 with accepted_not_committed provenance.
        proxy.push_fault(FaultMode::RateLimitedBeforeEffect);
        let mut meta_429_refused = CloudResponseMetadata::default();
        let err_429_refused = connector
            .account_request_with_attempt(
                AccountOperation::Manage,
                &json!({"version": 1}),
                HUMAN_BEARER,
                None,
                None,
                &OperationAttempt::new("att-429-refused").unwrap(),
                &mut meta_429_refused,
            )
            .unwrap_err();
        assert_eq!(
            err_429_refused.adapter_kind(),
            Some(AdapterErrorKind::RateLimited)
        );
        assert_eq!(
            err_429_refused.mutation_outcome(),
            MutationOutcome::RolledBack
        );
        assert_eq!(meta_429_refused.retry_after(), Some("30"));
        assert_eq!(
            meta_429_refused.provenance().map(|p| p.status()),
            Some(EffectStatus::Refused)
        );

        proxy.push_fault(FaultMode::RateLimitedUncertainProvenance);
        let mut meta_429_unc = CloudResponseMetadata::default();
        let err_429_unc = connector
            .account_request_with_attempt_async(
                AccountOperation::Manage,
                &json!({"version": 1, "val": 99}),
                HUMAN_BEARER,
                None,
                None,
                &OperationAttempt::new("att-429-uncertain").unwrap(),
                &mut meta_429_unc,
            )
            .await
            .unwrap_err();
        assert_eq!(
            err_429_unc.adapter_kind(),
            Some(AdapterErrorKind::RateLimited)
        );
        assert_eq!(err_429_unc.mutation_outcome(), MutationOutcome::Uncertain);
        assert_eq!(meta_429_unc.retry_after(), Some("5"));
        assert_eq!(
            meta_429_unc.provenance().map(|p| p.status()),
            Some(EffectStatus::Uncertain)
        );

        // 6. Same idempotency key with different payload on direct write and on reconciliation readback.
        proxy.push_fault(FaultMode::CleanCommit);
        let shared_att = OperationAttempt::new("att-idemp-conflict").unwrap();
        let mut ok_meta = CloudResponseMetadata::default();
        connector
            .control_request_with_attempt(
                "dactyl_ctrl_mutate",
                &json!({"val": 100}),
                Some(HUMAN_BEARER),
                None,
                OperationKind::Write,
                ResourceBudget::v1(),
                &shared_att,
                &mut ok_meta,
            )
            .unwrap();
        let conflict_err = connector
            .control_request_with_attempt(
                "dactyl_ctrl_mutate",
                &json!({"val": 200}),
                Some(HUMAN_BEARER),
                None,
                OperationKind::Write,
                ResourceBudget::v1(),
                &shared_att,
                &mut CloudResponseMetadata::default(),
            )
            .unwrap_err();
        assert_eq!(
            conflict_err.adapter_kind(),
            Some(AdapterErrorKind::IdempotencyConflict)
        );
        assert_eq!(conflict_err.adapter_code(), Some("idempotency_conflict"));

        let wrong_digest_att = OperationAttempt::new("att-idemp-conflict")
            .unwrap()
            .with_request_digest(
                "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
            )
            .unwrap();
        let rec_conflict = connector
            .reconcile_control_attempt(
                "dactyl_ctrl_status",
                &wrong_digest_att,
                Some(HUMAN_BEARER),
                None,
                ResourceBudget::v1(),
                Some(EffectStatus::Uncertain),
                &mut CloudResponseMetadata::default(),
            )
            .unwrap_err();
        assert_eq!(
            rec_conflict.adapter_kind(),
            Some(AdapterErrorKind::IdempotencyConflict)
        );
        assert_eq!(rec_conflict.adapter_code(), Some("idempotency_conflict"));

        // 7. Admission refusal (403 grant_revoked & 403 dpop_htu_mismatch on onboarding),
        //    double 401 use_dpop_nonce bounded retry, and truncated account request_id correlation.
        proxy.push_fault(FaultMode::AdmissionRefused {
            status: 403,
            code: "grant_revoked",
        });
        let mut admit_meta = CloudResponseMetadata::default();
        let admit_err = connector
            .account_request_with_attempt(
                AccountOperation::Manage,
                &json!({"version": 1}),
                HUMAN_BEARER,
                None,
                None,
                &OperationAttempt::new("att-admit-refuse").unwrap(),
                &mut admit_meta,
            )
            .unwrap_err();
        assert_eq!(
            admit_err.adapter_kind(),
            Some(AdapterErrorKind::Authorization)
        );
        assert_eq!(admit_err.failure_origin(), FailureOrigin::RemotePolicy);
        assert_eq!(
            admit_meta.provenance().map(|p| (p.status(), p.decision())),
            Some((EffectStatus::Refused, Some(DecisionOutcome::Refused)))
        );

        proxy.push_fault(FaultMode::AdmissionRefused {
            status: 403,
            code: "dpop_htu_mismatch",
        });
        let htu_403_err = connector
            .account_request_with_attempt(
                AccountOperation::Manage,
                &json!({"version": 1}),
                HUMAN_BEARER,
                None,
                None,
                &OperationAttempt::new("att-admit-htu-403").unwrap(),
                &mut CloudResponseMetadata::default(),
            )
            .unwrap_err();
        assert_eq!(
            htu_403_err.adapter_kind(),
            Some(AdapterErrorKind::Authentication)
        );
        assert_eq!(htu_403_err.adapter_code(), Some("dpop_htu_mismatch"));

        // Double 401 use_dpop_nonce stops after 1 retry (2 sign calls, 0 commits)
        let before_double_calls = signer.sign_calls.load(Ordering::SeqCst);
        let before_double_commits = proxy.persisted_attempts().len();
        proxy.push_fault(FaultMode::DpopNonceChallengeTwice);
        let double_err = dpop_gw
            .control_request_with_attempt(
                "dactyl_ctrl_mutate",
                &json!({"val": 401}),
                Some(DEVICE_BEARER),
                None,
                OperationKind::Write,
                ResourceBudget::v1(),
                &OperationAttempt::new("att-dpop-double").unwrap(),
                &mut CloudResponseMetadata::default(),
            )
            .unwrap_err();
        assert_eq!(double_err.adapter_code(), Some("use_dpop_nonce"));
        assert_eq!(
            signer.sign_calls.load(Ordering::SeqCst) - before_double_calls,
            2
        );
        assert_eq!(proxy.persisted_attempts().len(), before_double_commits);
        *proxy.state.required_nonce.lock().unwrap() = None;

        // Truncated account response preserves request_id correlation in TransportFault provenance
        proxy.push_fault(FaultMode::TruncatedAfterCommit);
        let mut trunc_acc_meta = CloudResponseMetadata::default();
        let trunc_acc_err = connector
            .account_request_with_attempt(
                AccountOperation::Manage,
                &json!({"version": 1, "val": 654}),
                HUMAN_BEARER,
                None,
                None,
                &OperationAttempt::new("att-trunc-acc-reqid").unwrap(),
                &mut trunc_acc_meta,
            )
            .unwrap_err();
        assert_eq!(trunc_acc_err.mutation_outcome(), MutationOutcome::Uncertain);
        let trunc_prov = trunc_acc_meta.provenance().unwrap();
        assert_eq!(trunc_prov.status(), EffectStatus::Uncertain);
        assert_eq!(trunc_prov.auth_scheme(), Some(AuthScheme::Bearer));
        assert!(trunc_prov.request_id().is_some());

        // 8. Bounded reconciliation when readback itself faults -> stays explicitly unresolved.
        proxy.push_fault(FaultMode::AfterCommitDropAck);
        let att_unres = OperationAttempt::new("att-unresolvable").unwrap();
        let mut meta_unres = CloudResponseMetadata::default();
        let err_unres = connector
            .control_request_with_attempt(
                "dactyl_ctrl_mutate",
                &json!({"val": 333}),
                Some(HUMAN_BEARER),
                None,
                OperationKind::Write,
                ResourceBudget::v1(),
                &att_unres,
                &mut meta_unres,
            )
            .unwrap_err();
        assert_eq!(err_unres.mutation_outcome(), MutationOutcome::Uncertain);
        for _ in 0..MAX_RECONCILE_STEPS {
            proxy.push_reconcile_fault(FaultMode::ConnectionResetBeforeEffect);
        }
        let rec_fail = bounded_reconcile_control(
            &connector,
            &att_unres,
            EffectStatus::Uncertain,
            ResourceBudget::v1(),
        )
        .unwrap_err();
        assert_eq!(rec_fail.adapter_kind(), Some(AdapterErrorKind::Transport));
        // Now allow one final reconciliation readback -> succeeds cleanly.
        let rec_ok = bounded_reconcile_control(
            &connector,
            &att_unres,
            EffectStatus::Uncertain,
            ResourceBudget::v1(),
        )
        .unwrap();
        assert_eq!(
            rec_ok.provenance().status(),
            EffectStatus::CommittedObserved
        );
        assert_eq!(rec_ok.prior_client_status(), Some(EffectStatus::Uncertain));

        // 9. Pre-send pool slot exhaustion timeout yields NotAttempted (not RolledBack!)
        //    PoolSettings shares slots across equal pool/connect limits even with shorter request_timeout.
        let holder_budget = ResourceBudget::v1()
            .with_http_pool_limits(1, 0, Duration::from_secs(1))
            .with_request_timeout(Duration::from_millis(350));
        let waiter_budget = ResourceBudget::v1()
            .with_http_pool_limits(1, 0, Duration::from_secs(1))
            .with_request_timeout(Duration::from_millis(25));
        let holder_conn = Arc::new(
            LogicalConnector::cloud_with_config(SupabaseRpcConfig::v1(&proxy.origin, KEY))
                .unwrap()
                .with_http_budget(holder_budget)
                .unwrap(),
        );
        let waiter_conn =
            LogicalConnector::cloud_with_config(SupabaseRpcConfig::v1(&proxy.origin, KEY))
                .unwrap()
                .with_http_budget(waiter_budget)
                .unwrap();
        proxy.push_fault(FaultMode::TimeoutBeforeCommit {
            delay: Duration::from_millis(450),
        });
        let reqs_before_holder = proxy.state.requests.lock().unwrap().len();
        let holder_clone = holder_conn.clone();
        let holder = tokio::spawn(async move {
            let mut m = CloudResponseMetadata::default();
            holder_clone
                .account_request_with_attempt_async(
                    AccountOperation::Manage,
                    &json!({"version": 1}),
                    HUMAN_BEARER,
                    None,
                    None,
                    &OperationAttempt::new("att-pool-holder").unwrap(),
                    &mut m,
                )
                .await
        });
        while proxy.state.requests.lock().unwrap().len() == reqs_before_holder {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let mut waiter_meta = CloudResponseMetadata::default();
        let waiter_err = waiter_conn
            .account_request_with_attempt_async(
                AccountOperation::Manage,
                &json!({"version": 1}),
                HUMAN_BEARER,
                None,
                None,
                &OperationAttempt::new("att-pool-waiter").unwrap(),
                &mut waiter_meta,
            )
            .await
            .unwrap_err();
        assert_eq!(waiter_err.adapter_kind(), Some(AdapterErrorKind::Timeout));
        assert_eq!(
            waiter_err.mutation_outcome(),
            MutationOutcome::NotAttempted,
            "pool slot exhaustion before HTTP send must be NotAttempted"
        );
        assert_eq!(
            waiter_meta
                .provenance()
                .map(|p| (p.status(), p.observation_source())),
            Some((
                EffectStatus::NotAttempted,
                ObservationSource::LocalPreflight
            ))
        );
        let holder_err = holder.await.unwrap().unwrap_err();
        assert_eq!(holder_err.mutation_outcome(), MutationOutcome::Uncertain);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn deterministic_seeded_fault_campaign_matches_reference_model() {
        #[derive(Default)]
        struct ReferenceCampaignModel {
            committed: BTreeMap<String, (String, i64, u64)>,
            refused: HashSet<String>,
            rolled_back: HashSet<String>,
            false_rollback_classifications: usize,
            false_commit_classifications: usize,
            duplicate_commits: usize,
            unauthorized_commits: usize,
        }

        let seeds = [
            0x1430_0001_u64,
            0x1430_0002,
            0x1430_0003,
            0x1430_0004,
            0x1430_0005,
            0x1430_0006,
            0x1430_0007,
            0x1430_0008,
            0x1430_0009,
            0x1430_000A,
            0x1430_000B,
            0x1430_000C,
        ];

        for seed in seeds {
            let mut rng = Rng::new(seed);
            let proxy = FaultProxyServer::start();
            let signer = Arc::new(DynamicDpopSigner::new());
            let short_budget = ResourceBudget::v1().with_request_timeout(Duration::from_millis(50));
            let long_budget = ResourceBudget::v1().with_request_timeout(Duration::from_secs(2));

            let connector_rw = LogicalConnector::cloud_with_config(
                SupabaseRpcConfig::v1(&proxy.origin, KEY).with_device_gateway(),
            )
            .unwrap()
            .with_proof_signer(signer.clone());
            let rpc_ro = open_rpc_conn(
                &proxy.origin,
                AccessMode::ReadOnly,
                long_budget,
                Some(signer.clone()),
                true,
            );
            let rpc_rw = open_rpc_conn(
                &proxy.origin,
                AccessMode::ReadWrite,
                short_budget,
                Some(signer.clone()),
                true,
            );

            let mut model = ReferenceCampaignModel::default();

            for step in 0..24_usize {
                let val = ((seed & 0xFFFF) as i64) * 100 + (step as i64) + 1;
                let aid = format!("seed-{seed:x}-step-{step:02}");
                let attempt = OperationAttempt::new(&aid)
                    .unwrap()
                    .with_scope_ref("scope:campaign")
                    .unwrap();
                let mut meta = CloudResponseMetadata::default();

                // Inject mid-flight policy generation bump + revocation on step 18
                if step == 18 {
                    proxy.state.policy_generation.store(2, Ordering::SeqCst);
                    proxy.state.revoked.store(true, Ordering::SeqCst);
                } else if step == 21 {
                    // Restore grant on step 21 under generation 3
                    proxy.state.policy_generation.store(3, Ordering::SeqCst);
                    proxy.state.revoked.store(false, Ordering::SeqCst);
                }

                let choice = rng.gen_range(12);
                let is_revoked = proxy.state.revoked.load(Ordering::SeqCst);

                if choice == 0 {
                    // Pre-send ReadOnly denial
                    let err = rpc_ro
                        .write_with_attempt("INSERT INTO m VALUES (1)", &[], &attempt, &mut meta)
                        .unwrap_err();
                    assert_eq!(err.mutation_outcome(), MutationOutcome::NotAttempted);
                    assert_eq!(
                        meta.provenance().map(|p| p.status()),
                        Some(EffectStatus::NotAttempted)
                    );
                    continue;
                }

                let mode = match choice {
                    1 => FaultMode::BeforeCommitRollback,
                    2 => FaultMode::BeforeCommitNotAttempted,
                    3 => FaultMode::BeforeCommitUncertain500,
                    4 => FaultMode::AfterCommitDropAck,
                    5 => FaultMode::TruncatedAfterCommit,
                    6 => FaultMode::TimeoutLateCommit {
                        delay: Duration::from_millis(120),
                    },
                    7 => FaultMode::TimeoutBeforeCommit {
                        delay: Duration::from_millis(120),
                    },
                    8 => FaultMode::ConnectionResetBeforeEffect,
                    9 => FaultMode::RateLimitedUncertainProvenance,
                    10 => FaultMode::DpopNonceChallengeOnce,
                    _ => FaultMode::CleanCommit,
                };

                proxy.push_fault(mode.clone());
                let is_timeout_case = matches!(
                    mode,
                    FaultMode::TimeoutLateCommit { .. } | FaultMode::TimeoutBeforeCommit { .. }
                );
                let use_async = (step + rng.gen_range(2)) % 2 == 0;

                let res = if is_timeout_case {
                    if use_async {
                        rpc_rw
                            .write_with_attempt_async(
                                "INSERT INTO m VALUES (1)",
                                &[],
                                &attempt,
                                &mut meta,
                            )
                            .await
                            .map(|_| ())
                    } else {
                        rpc_rw
                            .write_with_attempt(
                                "INSERT INTO m VALUES (1)",
                                &[],
                                &attempt,
                                &mut meta,
                            )
                            .map(|_| ())
                    }
                } else if use_async {
                    connector_rw
                        .control_request_with_attempt_async(
                            "dactyl_ctrl_mutate",
                            &json!({"val": val}),
                            Some(DEVICE_BEARER),
                            None,
                            OperationKind::Write,
                            long_budget,
                            &attempt,
                            &mut meta,
                        )
                        .await
                        .map(|_| ())
                } else {
                    connector_rw
                        .control_request_with_attempt(
                            "dactyl_ctrl_mutate",
                            &json!({"val": val}),
                            Some(DEVICE_BEARER),
                            None,
                            OperationKind::Write,
                            long_budget,
                            &attempt,
                            &mut meta,
                        )
                        .map(|_| ())
                };

                // Clear nonce requirement if DpopNonceChallengeOnce ran while revoked or clean
                *proxy.state.required_nonce.lock().unwrap() = None;

                let persisted_now = proxy.persisted_attempts();
                let actually_persisted = persisted_now.contains_key(&aid);

                if is_revoked && actually_persisted {
                    model.unauthorized_commits += 1;
                }

                match res {
                    Ok(()) => {
                        if !actually_persisted {
                            model.false_commit_classifications += 1;
                        }
                        let prov = meta.provenance().unwrap();
                        assert_eq!(prov.status(), EffectStatus::CommittedObserved);
                        assert_eq!(prov.attempt_id(), Some(aid.as_str()));
                        let (dig, rid, gen, pval) = persisted_now.get(&aid).unwrap();
                        assert_eq!(prov.request_id(), Some(rid.as_str()));
                        assert_eq!(prov.request_digest(), Some(dig.as_str()));
                        if model
                            .committed
                            .insert(aid.clone(), (dig.clone(), *pval, *gen as u64))
                            .is_some()
                        {
                            model.duplicate_commits += 1;
                        }
                    }
                    Err(err) => {
                        match err.mutation_outcome() {
                            MutationOutcome::NotAttempted => {
                                if actually_persisted {
                                    model.false_rollback_classifications += 1;
                                }
                            }
                            MutationOutcome::RolledBack => {
                                if actually_persisted {
                                    model.false_rollback_classifications += 1;
                                }
                                if meta
                                    .provenance()
                                    .is_some_and(|p| p.status() == EffectStatus::Refused)
                                {
                                    model.refused.insert(aid.clone());
                                } else {
                                    model.rolled_back.insert(aid.clone());
                                }
                            }
                            MutationOutcome::Uncertain => {
                                // Reconcile via separate readback using original attempt scope
                                let prior_status = meta
                                    .provenance()
                                    .map(|p| p.status())
                                    .unwrap_or(EffectStatus::Uncertain);
                                assert!(matches!(
                                    prior_status,
                                    EffectStatus::Uncertain | EffectStatus::UnsupportedProvenance
                                ));

                                let mut rec_meta = CloudResponseMetadata::default();
                                let obs = connector_rw
                                    .reconcile_control_attempt_async(
                                        "dactyl_ctrl_status",
                                        &attempt,
                                        Some(DEVICE_BEARER),
                                        None,
                                        long_budget,
                                        Some(prior_status),
                                        &mut rec_meta,
                                    )
                                    .await
                                    .unwrap();
                                assert_eq!(obs.prior_client_status(), Some(prior_status));
                                assert_eq!(
                                    obs.provenance().observation_source(),
                                    ObservationSource::ServerReadback
                                );
                                match obs.provenance().status() {
                                    EffectStatus::CommittedObserved => {
                                        assert!(actually_persisted);
                                        let (dig, rid, gen, pval) =
                                            persisted_now.get(&aid).unwrap();
                                        assert_eq!(
                                            obs.provenance().request_id(),
                                            Some(rid.as_str())
                                        );
                                        assert_eq!(
                                            obs.provenance().request_digest(),
                                            Some(dig.as_str())
                                        );
                                        if model
                                            .committed
                                            .insert(aid.clone(), (dig.clone(), *pval, *gen as u64))
                                            .is_some()
                                        {
                                            model.duplicate_commits += 1;
                                        }
                                    }
                                    EffectStatus::NotAttempted | EffectStatus::RolledBack => {
                                        assert!(!actually_persisted);
                                        model.rolled_back.insert(aid.clone());
                                    }
                                    EffectStatus::Refused => {
                                        assert!(!actually_persisted);
                                        model.refused.insert(aid.clone());
                                    }
                                    other => panic!("unexpected reconciliation status {other:?}"),
                                }
                            }
                        }
                    }
                }
            }

            let final_persisted = proxy.persisted_attempts();
            assert_eq!(model.duplicate_commits, 0, "seed {seed:#x}");
            assert_eq!(model.false_rollback_classifications, 0, "seed {seed:#x}");
            assert_eq!(model.false_commit_classifications, 0, "seed {seed:#x}");
            assert_eq!(model.unauthorized_commits, 0, "seed {seed:#x}");
            assert_eq!(
                final_persisted
                    .iter()
                    .filter(|(_, (_, _, gen, _))| *gen == 2)
                    .count(),
                0,
                "zero commits under revoked generation 2 (seed {seed:#x})"
            );
            assert!(
                proxy.state.unauthorized_dispatches.load(Ordering::SeqCst) <= 3,
                "revoked dispatches bounded and never committed (seed {seed:#x})"
            );
            assert_eq!(
                final_persisted.len(),
                model.committed.len(),
                "persisted state must match reference model (seed {seed:#x})"
            );
            for (aid, (dig, _, gen, val)) in &final_persisted {
                let expected = model.committed.get(aid).unwrap();
                assert_eq!(
                    (dig, *val, *gen as u64),
                    (&expected.0, expected.1, expected.2)
                );
            }
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn issue_138_origin_cutover_resilience_discovery_htu_redirects_and_ack_loss() {
        let old_origin_srv = FaultProxyServer::start();
        let new_origin_srv = FaultProxyServer::start();

        // 1. Stale discovery profile during cutover:
        //    Querying new_origin_srv while it still advertises old_origin_srv (or DEFAULT_CLOUD_ORIGIN)
        //    must fail closed with Protocol, both sync and async, with zero local SQLite fallback.
        *new_origin_srv
            .state
            .expected_origin_override
            .lock()
            .unwrap() = Some(DEFAULT_CLOUD_ORIGIN.to_owned());
        let stale_sync =
            LogicalConnector::resolve(LogicalTarget::cloud_with_origin(&new_origin_srv.origin))
                .unwrap_err();
        let stale_async = LogicalConnector::resolve_async(LogicalTarget::cloud_with_origin(
            &new_origin_srv.origin,
        ))
        .await
        .unwrap_err();
        assert_eq!(stale_sync.adapter_kind(), Some(AdapterErrorKind::Protocol));
        assert_eq!(stale_async.adapter_kind(), Some(AdapterErrorKind::Protocol));

        // Reverse: old origin prematurely advertising FIRST_PARTY_CLOUD_ORIGIN must fail closed too.
        *old_origin_srv
            .state
            .expected_origin_override
            .lock()
            .unwrap() = Some(FIRST_PARTY_CLOUD_ORIGIN.to_owned());
        let premature_err =
            LogicalConnector::resolve(LogicalTarget::cloud_with_origin(&old_origin_srv.origin))
                .unwrap_err();
        assert_eq!(
            premature_err.adapter_kind(),
            Some(AdapterErrorKind::Protocol)
        );

        // Restore accurate discovery profiles for cutover coexistence
        *old_origin_srv
            .state
            .expected_origin_override
            .lock()
            .unwrap() = None;
        *new_origin_srv
            .state
            .expected_origin_override
            .lock()
            .unwrap() = None;

        // 2. Coexistence across DEFAULT_CLOUD_ORIGIN <-> FIRST_PARTY_CLOUD_ORIGIN without ambient env mutation
        let default_conn = LogicalConnector::cloud_with_config(
            SupabaseRpcConfig::v1(DEFAULT_CLOUD_ORIGIN, KEY).with_device_gateway(),
        )
        .unwrap();
        let first_party_conn = LogicalConnector::cloud_with_config(
            SupabaseRpcConfig::v1(FIRST_PARTY_CLOUD_ORIGIN, KEY).with_device_gateway(),
        )
        .unwrap();
        assert_eq!(
            default_conn.canonical_auth_url().unwrap(),
            "https://goyxpcmxetztjlrrcwew.supabase.co/auth"
        );
        assert_eq!(
            first_party_conn.canonical_auth_url().unwrap(),
            "https://api.decapodlabs.ai/auth"
        );
        assert_eq!(
            first_party_conn
                .canonical_repository_url("DecapodLabs", "dactyl")
                .unwrap(),
            "https://api.decapodlabs.ai/decapod/DecapodLabs/dactyl"
        );
        // Traversal / query injection on canonical_repository_url rejected pre-send
        assert_eq!(
            first_party_conn
                .canonical_repository_url("..", "dactyl")
                .unwrap_err()
                .adapter_kind(),
            Some(AdapterErrorKind::Protocol)
        );

        // 3. DPoP htu host & path mismatch rejection across cutover origins:
        //    Signer bound to old origin / old path hits new_origin_srv -> 401 dpop_htu_mismatch,
        //    no retry, no commit, full redaction.
        let stale_host_signer = Arc::new(DynamicDpopSigner::with_forced_htu(format!(
            "{}/functions/v1/decapod-onboarding/cloud-control",
            old_origin_srv.origin
        )));
        let stale_path_signer = Arc::new(DynamicDpopSigner::with_forced_htu(format!(
            "{}/functions/v1/propodus-onboarding/cloud-control",
            new_origin_srv.origin
        )));

        for (label, bad_signer) in [
            ("host_mismatch", stale_host_signer),
            ("path_mismatch", stale_path_signer),
        ] {
            let new_gw = LogicalConnector::cloud_with_config(
                SupabaseRpcConfig::v1(&new_origin_srv.origin, KEY).with_device_gateway(),
            )
            .unwrap()
            .with_proof_signer(bad_signer.clone());
            let mut meta = CloudResponseMetadata::default();
            let err = new_gw
                .control_request_with_attempt(
                    "dactyl_ctrl_mutate",
                    &json!({"val": 501}),
                    Some(DEVICE_BEARER),
                    None,
                    OperationKind::Write,
                    ResourceBudget::v1(),
                    &OperationAttempt::new(format!("att-htu-{label}")).unwrap(),
                    &mut meta,
                )
                .unwrap_err();
            assert_eq!(err.adapter_kind(), Some(AdapterErrorKind::Authentication));
            assert_eq!(err.adapter_code(), Some("dpop_htu_mismatch"));
            assert_eq!(err.mutation_outcome(), MutationOutcome::RolledBack);
            assert_eq!(
                meta.provenance().map(|p| p.status()),
                Some(EffectStatus::Refused)
            );
            assert_eq!(bad_signer.sign_calls.load(Ordering::SeqCst), 1);
            assert!(!format!("{err} {err:?} {meta:?}").contains(DEVICE_BEARER));
            assert!(!format!("{err} {err:?} {meta:?}").contains("raw-htu-secret"));
        }
        assert_eq!(new_origin_srv.persisted_attempts().len(), 0);

        // 4. Unsafe HTTP redirect refusal during cutover (old origin -> new origin redirect must NEVER be followed)
        let sink = TcpListener::bind("127.0.0.1:0").unwrap();
        sink.set_nonblocking(true).unwrap();
        let sink_location = format!(
            "http://{}/functions/v1/decapod-onboarding/cloud-control",
            sink.local_addr().unwrap()
        );

        let old_gw = LogicalConnector::cloud_with_config(
            SupabaseRpcConfig::v1(&old_origin_srv.origin, KEY).with_device_gateway(),
        )
        .unwrap()
        .with_proof_signer(Arc::new(DynamicDpopSigner::new()));

        old_origin_srv.push_fault(FaultMode::RedirectDuringWrite {
            location: sink_location,
        });
        let mut redir_meta = CloudResponseMetadata::default();
        let redir_err = old_gw
            .control_request_with_attempt(
                "dactyl_ctrl_mutate",
                &json!({"val": 502}),
                Some(DEVICE_BEARER),
                None,
                OperationKind::Write,
                ResourceBudget::v1(),
                &OperationAttempt::new("att-cutover-redir").unwrap(),
                &mut redir_meta,
            )
            .unwrap_err();
        assert_eq!(redir_err.adapter_kind(), Some(AdapterErrorKind::Protocol));
        assert_eq!(redir_err.adapter_code(), Some("outcome_unknown"));
        assert_eq!(redir_err.mutation_outcome(), MutationOutcome::Uncertain);
        assert_eq!(
            redir_meta
                .provenance()
                .map(|p| (p.status(), p.origin_bound())),
            Some((EffectStatus::Uncertain, false))
        );
        assert!(
            sink.accept().is_err(),
            "redirect target must never be contacted"
        );

        // 5. Lost response during cutover:
        //    Mutation committed on old_origin_srv with dropped ack -> Uncertain;
        //    must NEVER automatically resend to new_origin_srv;
        //    reconciling on old_origin_srv confirms commit, while querying new_origin_srv sees NotAttempted (0 writes!).
        old_origin_srv.push_fault(FaultMode::AfterCommitDropAck);
        let cutover_att = OperationAttempt::new("att-cutover-lost-ack")
            .unwrap()
            .with_scope_ref("org:cutover")
            .unwrap();
        let mut lost_meta = CloudResponseMetadata::default();
        let lost_err = old_gw
            .control_request_with_attempt_async(
                "dactyl_ctrl_mutate",
                &json!({"val": 777}),
                Some(DEVICE_BEARER),
                None,
                OperationKind::Write,
                ResourceBudget::v1(),
                &cutover_att,
                &mut lost_meta,
            )
            .await
            .unwrap_err();
        assert_eq!(lost_err.mutation_outcome(), MutationOutcome::Uncertain);
        assert_eq!(old_origin_srv.persisted_attempts().len(), 1);
        assert_eq!(new_origin_srv.persisted_attempts().len(), 0);

        // Reconcile against original origin -> CommittedObserved
        let obs_old = old_gw
            .reconcile_control_attempt_async(
                "dactyl_ctrl_status",
                &cutover_att,
                Some(DEVICE_BEARER),
                None,
                ResourceBudget::v1(),
                Some(EffectStatus::Uncertain),
                &mut CloudResponseMetadata::default(),
            )
            .await
            .unwrap();
        assert_eq!(
            obs_old.provenance().status(),
            EffectStatus::CommittedObserved
        );
        assert_eq!(obs_old.prior_client_status(), Some(EffectStatus::Uncertain));

        // Reconcile against new origin -> NotAttempted (and crucially 0 mutations executed there!)
        let new_gw = LogicalConnector::cloud_with_config(
            SupabaseRpcConfig::v1(&new_origin_srv.origin, KEY).with_device_gateway(),
        )
        .unwrap()
        .with_proof_signer(Arc::new(DynamicDpopSigner::new()));
        let obs_new = new_gw
            .reconcile_control_attempt_async(
                "dactyl_ctrl_status",
                &cutover_att,
                Some(DEVICE_BEARER),
                None,
                ResourceBudget::v1(),
                Some(EffectStatus::Uncertain),
                &mut CloudResponseMetadata::default(),
            )
            .await
            .unwrap();
        assert_eq!(obs_new.provenance().status(), EffectStatus::NotAttempted);
        assert_eq!(new_origin_srv.persisted_attempts().len(), 0);
        assert_eq!(old_origin_srv.persisted_attempts().len(), 1);
    }
}
