//! Conformance and fault-matrix suite for Dactyl #140: bounded, origin- and
//! auth-bound per-operation effect provenance, idempotency conflict detection,
//! separate server readback reconciliation, and local SQLite compatibility.
//!
//! Evidence classes:
//! - `local_sqlite` (`#[cfg(feature = "sqlite")]`): host SQLite C-ABI engine.
//! - `synthetic_http` (`#[cfg(feature = "supabase")]`): loopback raw TCP/HTTP
//!   protocol and fault-injection fixtures.
//! - `local_postgres`, `hosted_supabase`, `live_propodus`: unavailable here.

#[cfg(feature = "sqlite")]
mod sqlite_compatibility {
    use dactyl_db::{
        AdapterErrorKind, CloudResponseMetadata, Connection, DatastoreRoute, EffectStatus,
        LogicalConnector, MutationOutcome, ObservationSource, Operation, OperationAttempt,
        Parameter, ResourceBudget,
    };

    #[test]
    fn sqlite_local_operations_work_without_remote_policy_and_reject_cloud_provenance_calls() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("local.sqlite");
        let path_str = db_path.to_str().unwrap().to_owned();

        let conn = Connection::open(DatastoreRoute::sqlite(&path_str)).unwrap();
        assert_eq!(
            conn.write_result(
                "CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT NOT NULL)",
                &[]
            )
            .unwrap()
            .affected_rows,
            0
        );
        assert_eq!(
            conn.write(
                "INSERT INTO items (id, label) VALUES (?1, ?2)",
                &[Parameter::Integer(1), Parameter::Text("alpha".into())],
            )
            .unwrap(),
            1
        );
        let batch = conn
            .atomic(&[
                Operation::write(
                    "INSERT INTO items (id, label) VALUES (?1, ?2)",
                    vec![Parameter::Integer(2), Parameter::Text("beta".into())],
                ),
                Operation::read("SELECT label FROM items ORDER BY id", vec![]),
            ])
            .unwrap();
        assert_eq!(batch.results.len(), 2);

        let attempt = OperationAttempt::new("sqlite-local-attempt").unwrap();
        let mut meta = CloudResponseMetadata::default();
        let write_err = conn
            .write_with_attempt(
                "INSERT INTO items (id, label) VALUES (?1, ?2)",
                &[Parameter::Integer(3), Parameter::Text("gamma".into())],
                &attempt,
                &mut meta,
            )
            .unwrap_err();
        assert_eq!(write_err.adapter_kind(), Some(AdapterErrorKind::Capability));
        assert_eq!(write_err.adapter_code(), Some("cloud_connector_required"));
        assert_eq!(write_err.mutation_outcome(), MutationOutcome::NotAttempted);
        let prov = meta.provenance().expect("local preflight provenance");
        assert_eq!(prov.status(), EffectStatus::NotAttempted);
        assert_eq!(prov.observation_source(), ObservationSource::LocalPreflight);

        let mut atomic_meta = CloudResponseMetadata::default();
        let atomic_err = conn
            .atomic_with_attempt(
                &[Operation::read("SELECT 1", vec![])],
                &attempt,
                &mut atomic_meta,
            )
            .unwrap_err();
        assert_eq!(
            atomic_err.adapter_kind(),
            Some(AdapterErrorKind::Capability)
        );
        assert_eq!(
            atomic_meta.provenance().map(|p| p.status()),
            Some(EffectStatus::NotAttempted)
        );

        let local_connector = LogicalConnector::local(&path_str).unwrap();
        let mut rec_meta = CloudResponseMetadata::default();
        let rec_err = local_connector
            .reconcile_operation(&attempt, "unused", None, None, &mut rec_meta)
            .unwrap_err();
        assert_eq!(rec_err.adapter_kind(), Some(AdapterErrorKind::Capability));
        let ctrl_err = local_connector
            .reconcile_control_attempt(
                "fn_status",
                &attempt,
                None,
                None,
                ResourceBudget::v1(),
                None,
                &mut rec_meta,
            )
            .unwrap_err();
        assert_eq!(ctrl_err.adapter_kind(), Some(AdapterErrorKind::Capability));
    }
}

#[cfg(feature = "supabase")]
mod supabase_provenance {
    use std::collections::HashMap;
    use std::io::{Read, Write};
    use std::net::{Shutdown, TcpListener, TcpStream};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::Duration;

    use dactyl_db::{
        AccessMode, AccountOperation, AdapterErrorKind, AuthScheme, CloudConnector,
        CloudResponseMetadata, Connection, DatastoreRoute, DecisionOutcome, EffectStatus,
        FailureOrigin, LogicalConnector, MutationOutcome, ObservationSource, OpenOptions,
        Operation, OperationAttempt, OperationKind, Parameter, RequestProof, RequestProofInput,
        RequestProofSigner, ResourceBudget, StorageContext, SupabaseRpcConfig,
        PROVENANCE_CONTRACT_VERSION,
    };

    const KEY: &str = "sb_publishable_provenance_test";

    fn test_context() -> StorageContext {
        StorageContext::new(1, serde_json::json!({"org_id": "org", "repo_id": "repo"})).unwrap()
    }

    fn open_supabase_conn(
        origin: &str,
        bearer: &str,
        access_mode: AccessMode,
        signer: Option<Arc<dyn RequestProofSigner>>,
    ) -> Connection {
        let mut route = DatastoreRoute::supabase(origin, Some(bearer.to_owned()));
        if let Some(s) = signer {
            route = route.with_proof_signer(s);
        }
        Connection::open_with_options_and_context(
            route,
            OpenOptions {
                access_mode,
                ..OpenOptions::default()
            },
            Some(test_context()),
        )
        .unwrap()
    }

    #[derive(Clone, Debug)]
    struct RecordedRequest {
        path: String,
        headers: HashMap<String, String>,
    }

    impl RecordedRequest {
        fn header(&self, name: &str) -> Option<&str> {
            self.headers
                .get(&name.to_ascii_lowercase())
                .map(String::as_str)
        }
    }

    enum WireResponse {
        Http {
            status: u16,
            headers: Vec<(String, String)>,
            body: Vec<u8>,
        },
        DropBeforeResponse,
        PartialChunked {
            status: u16,
            headers: Vec<(String, String)>,
            prefix: Vec<u8>,
        },
        DelayedHttp {
            delay: Duration,
            status: u16,
            headers: Vec<(String, String)>,
            body: Vec<u8>,
        },
    }

    fn read_http_req(stream: &mut TcpStream) -> RecordedRequest {
        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        let head_end = loop {
            if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                break pos + 4;
            }
            let n = stream.read(&mut chunk).unwrap();
            assert!(n > 0, "unexpected EOF while reading request headers");
            buf.extend_from_slice(&chunk[..n]);
        };
        let head_str = String::from_utf8_lossy(&buf[..head_end]).into_owned();
        let mut lines = head_str.lines();
        let req_line = lines.next().unwrap_or("");
        let mut parts = req_line.split_whitespace();
        let _method = parts.next().unwrap_or("");
        let path = parts.next().unwrap_or("").to_owned();
        let mut headers = HashMap::new();
        for line in lines {
            if let Some((k, v)) = line.split_once(':') {
                headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_owned());
            }
        }
        let content_length = headers
            .get("content-length")
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(0);
        while buf.len() < head_end + content_length {
            let n = stream.read(&mut chunk).unwrap();
            assert!(n > 0, "unexpected EOF while reading request body");
            buf.extend_from_slice(&chunk[..n]);
        }
        RecordedRequest { path, headers }
    }

    fn write_wire_response(mut stream: TcpStream, response: WireResponse) {
        match response {
            WireResponse::DropBeforeResponse => {
                let _ = stream.shutdown(Shutdown::Both);
                drop(stream);
            }
            WireResponse::PartialChunked {
                status,
                headers,
                prefix,
            } => {
                let mut raw = format!(
                    "HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\nContent-Length: 4096\r\nConnection: close\r\n"
                );
                for (k, v) in headers {
                    raw.push_str(&format!("{k}: {v}\r\n"));
                }
                raw.push_str("\r\n");
                let _ = stream.write_all(raw.as_bytes());
                let _ = stream.write_all(&prefix);
                let _ = stream.flush();
                let _ = stream.shutdown(Shutdown::Both);
                drop(stream);
            }
            WireResponse::DelayedHttp {
                delay,
                status,
                headers,
                body,
            } => {
                thread::sleep(delay);
                let mut raw = format!(
                    "HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n",
                    body.len()
                );
                for (k, v) in headers {
                    raw.push_str(&format!("{k}: {v}\r\n"));
                }
                raw.push_str("\r\n");
                let _ = stream.write_all(raw.as_bytes());
                let _ = stream.write_all(&body);
                let _ = stream.flush();
            }
            WireResponse::Http {
                status,
                headers,
                body,
            } => {
                let mut raw = format!(
                    "HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n",
                    body.len()
                );
                for (k, v) in headers {
                    raw.push_str(&format!("{k}: {v}\r\n"));
                }
                raw.push_str("\r\n");
                let _ = stream.write_all(raw.as_bytes());
                let _ = stream.write_all(&body);
                let _ = stream.flush();
            }
        }
    }

    fn spawn_fault_server<F>(
        max_accepts: usize,
        handler: F,
    ) -> (String, Arc<AtomicUsize>, thread::JoinHandle<()>)
    where
        F: Fn(&RecordedRequest, &str) -> WireResponse + Send + Sync + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let origin_clone = origin.clone();
        let seen = Arc::new(AtomicUsize::new(0));
        let seen_clone = seen.clone();
        let handler = Arc::new(handler);
        let handle = thread::spawn(move || {
            for _ in 0..max_accepts {
                let (mut stream, _) = listener.accept().unwrap();
                seen_clone.fetch_add(1, Ordering::SeqCst);
                let req = read_http_req(&mut stream);
                let resp = handler(&req, &origin_clone);
                write_wire_response(stream, resp);
            }
        });
        (origin, seen, handle)
    }

    #[derive(Default)]
    struct TestDpopSigner {
        audience: Option<String>,
        calls: Mutex<Vec<Option<String>>>,
    }

    impl RequestProofSigner for TestDpopSigner {
        fn sign_request(
            &self,
            input: &RequestProofInput<'_>,
        ) -> Result<RequestProof, dactyl_db::DactylError> {
            let nonce = input.nonce().map(str::to_owned);
            self.calls.lock().unwrap().push(nonce.clone());
            let aud = self.audience.as_deref().unwrap_or(input.canonical_url());
            let proof = format!(
                "dpop-proof.htm={}.aud={}.nonce={}",
                input.method(),
                aud,
                nonce.as_deref().unwrap_or("none")
            );
            RequestProof::dpop(proof)
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn provenance_headers(
        origin: &str,
        req: &RecordedRequest,
        request_id: &str,
        effect_status: &str,
        decision: &str,
        operation_kind: &str,
        reason_code: Option<&str>,
        decision_ref: Option<&str>,
        auth_scheme: &str,
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
        ];
        if let Some(attempt_id) = req.header("decapod-cloud-attempt-id") {
            hdrs.push(("Decapod-Cloud-Attempt-Id".into(), attempt_id.to_owned()));
        }
        if let Some(digest) = req.header("decapod-cloud-request-digest") {
            hdrs.push(("Decapod-Cloud-Request-Digest".into(), digest.to_owned()));
        }
        if let Some(scope) = req.header("decapod-cloud-scope-ref") {
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

    #[test]
    fn fault_before_send_local_preflight_refusal() {
        assert!(OperationAttempt::new("").is_err());
        assert!(OperationAttempt::new("bad space").is_err());
        assert!(OperationAttempt::new("ok")
            .unwrap()
            .with_request_digest("bad\nline")
            .is_err());
        assert!(OperationAttempt::new("ok")
            .unwrap()
            .with_scope_ref("bad\rscope")
            .is_err());

        let attempt = OperationAttempt::new("preflight-att-1")
            .unwrap()
            .with_scope_ref("repo:org/app")
            .unwrap();

        // 1. ReadOnly Supabase connection fails before touching wire.
        let ro_conn = open_supabase_conn(
            "http://127.0.0.1:9",
            "secret-bearer-token",
            AccessMode::ReadOnly,
            None,
        );
        let mut meta = CloudResponseMetadata::default();
        let err = ro_conn
            .write_with_attempt("INSERT INTO t VALUES (1)", &[], &attempt, &mut meta)
            .unwrap_err();
        assert_eq!(err.adapter_kind(), Some(AdapterErrorKind::ReadOnly));
        assert_eq!(err.mutation_outcome(), MutationOutcome::NotAttempted);
        let prov = meta.provenance().expect("preflight provenance");
        assert_eq!(prov.status(), EffectStatus::NotAttempted);
        assert_eq!(prov.observation_source(), ObservationSource::LocalPreflight);
        assert_eq!(prov.reason_code(), Some("read_only"));
        assert_eq!(prov.attempt_id(), Some("preflight-att-1"));
        assert_eq!(prov.scope_ref(), Some("repo:org/app"));
        assert!(!prov.origin_bound());

        // 2. Device gateway without DPoP proof signer fails locally pre-send.
        let rpc = SupabaseRpcConfig::v1("http://127.0.0.1:9", KEY).with_device_gateway();
        let connector = CloudConnector::with_config(rpc).unwrap();
        let mut gw_meta = CloudResponseMetadata::default();
        let gw_err = connector
            .account_request_with_attempt(
                AccountOperation::DeviceEnroll,
                &serde_json::json!({"version": 1}),
                "secret-bearer-token",
                None,
                None,
                &attempt,
                &mut gw_meta,
            )
            .unwrap_err();
        assert_eq!(
            gw_err.adapter_kind(),
            Some(AdapterErrorKind::Authentication)
        );
        assert_eq!(gw_err.adapter_code(), Some("device_proof_required"));
        assert_eq!(gw_err.mutation_outcome(), MutationOutcome::NotAttempted);
        let gw_prov = gw_meta.provenance().expect("gateway preflight provenance");
        assert_eq!(gw_prov.status(), EffectStatus::NotAttempted);
        assert_eq!(
            gw_prov.observation_source(),
            ObservationSource::LocalPreflight
        );
        assert_eq!(gw_prov.reason_code(), Some("device_proof_required"));

        // 3. Oversized local payload fails locally with LocalBudget + NotAttempted.
        let rw_conn = open_supabase_conn(
            "http://127.0.0.1:9",
            "secret-bearer-token",
            AccessMode::ReadWrite,
            None,
        );
        let mut size_meta = CloudResponseMetadata::default();
        let size_err = rw_conn
            .write_with_attempt(
                "INSERT INTO t VALUES ($1)",
                &[Parameter::Text("x".repeat(4 * 1024 * 1024))],
                &attempt,
                &mut size_meta,
            )
            .unwrap_err();
        assert_eq!(size_err.failure_origin(), FailureOrigin::LocalBudget);
        assert_eq!(size_err.mutation_outcome(), MutationOutcome::NotAttempted);
        assert_eq!(
            size_meta.provenance().map(|p| p.status()),
            Some(EffectStatus::NotAttempted)
        );
    }

    #[test]
    fn accepted_not_committed_rolled_back_refused_and_committed_are_distinct() {
        let step = Arc::new(AtomicUsize::new(0));
        let step_srv = step.clone();
        let (origin, seen, handle) = spawn_fault_server(5, move |req, origin| {
            match step_srv.fetch_add(1, Ordering::SeqCst) {
                // Case 0: HTTP 202 accepted_not_committed -> EffectStatus::Uncertain + not committed
                0 => WireResponse::Http {
                    status: 202,
                    headers: provenance_headers(
                        origin,
                        req,
                        "req-202",
                        "accepted_not_committed",
                        "allowed",
                        "write",
                        Some("accepted_not_committed"),
                        Some("dec-202"),
                        "bearer",
                    ),
                    body: serde_json::to_vec(&serde_json::json!({
                        "columns": [],
                        "rows": [],
                        "affected_rows": 0,
                        "generated_keys": []
                    }))
                    .unwrap(),
                },
                // Case 1: HTTP 500 with explicit server rollback provenance -> RolledBack, NOT Uncertain!
                1 => WireResponse::Http {
                    status: 500,
                    headers: provenance_headers(
                        origin,
                        req,
                        "req-rb",
                        "rolled_back",
                        "allowed",
                        "write",
                        Some("transaction_aborted"),
                        Some("dec-rb"),
                        "bearer",
                    ),
                    body: serde_json::to_vec(&serde_json::json!({
                        "error": {"code": "transaction_aborted"}
                    }))
                    .unwrap(),
                },
                // Case 2: HTTP 503 with explicit not_attempted provenance -> NotAttempted!
                2 => WireResponse::Http {
                    status: 503,
                    headers: provenance_headers(
                        origin,
                        req,
                        "req-na",
                        "not_attempted",
                        "allowed",
                        "write",
                        Some("storage_unavailable"),
                        None,
                        "bearer",
                    ),
                    body: serde_json::to_vec(&serde_json::json!({
                        "error": {"code": "storage_unavailable"}
                    }))
                    .unwrap(),
                },
                // Case 3: HTTP 403 refusal with decision_ref + reason -> Refused!
                3 => WireResponse::Http {
                    status: 403,
                    headers: provenance_headers(
                        origin,
                        req,
                        "req-ref",
                        "refused",
                        "refused",
                        "write",
                        Some("grant_revoked"),
                        Some("dec-ref-1"),
                        "bearer",
                    ),
                    body: serde_json::to_vec(&serde_json::json!({
                        "error": {"code": "grant_revoked"}
                    }))
                    .unwrap(),
                },
                // Case 4: HTTP 200 committed_observed (both headers + JSON envelope agree)
                _ => {
                    let hdrs = provenance_headers(
                        origin,
                        req,
                        "req-commit",
                        "committed_observed",
                        "allowed",
                        "write",
                        None,
                        Some("dec-commit-1"),
                        "bearer",
                    );
                    WireResponse::Http {
                        status: 200,
                        headers: hdrs,
                        body: serde_json::to_vec(&serde_json::json!({
                            "columns": [],
                            "rows": [],
                            "affected_rows": 1,
                            "generated_keys": [],
                            "provenance": {
                                "version": 1,
                                "request_id": "req-commit",
                                "attempt_id": req.header("decapod-cloud-attempt-id"),
                                "request_digest": req.header("decapod-cloud-request-digest"),
                                "operation_kind": "write",
                                "decision": "allowed",
                                "effect_status": "committed_observed",
                                "decision_ref": "dec-commit-1",
                                "auth_scheme": "bearer",
                                "origin": origin
                            }
                        }))
                        .unwrap(),
                    }
                }
            }
        });

        let conn = open_supabase_conn(&origin, "bearer-token", AccessMode::ReadWrite, None);

        // 0. accepted_not_committed must NOT claim write success
        let mut meta0 = CloudResponseMetadata::default();
        let err0 = conn
            .write_with_attempt(
                "INSERT INTO t VALUES (1)",
                &[],
                &OperationAttempt::new("att-202").unwrap(),
                &mut meta0,
            )
            .unwrap_err();
        assert_eq!(err0.mutation_outcome(), MutationOutcome::Uncertain);
        let p0 = meta0.provenance().unwrap();
        assert_eq!(p0.status(), EffectStatus::Uncertain);
        assert_eq!(p0.reason_code(), Some("accepted_not_committed"));
        assert_eq!(p0.decision(), Some(DecisionOutcome::Allowed));
        assert!(p0.origin_bound());

        // 1. explicit rolled_back preserves RolledBack cleanly
        let mut meta1 = CloudResponseMetadata::default();
        let err1 = conn
            .write_with_attempt(
                "INSERT INTO t VALUES (2)",
                &[],
                &OperationAttempt::new("att-rb").unwrap(),
                &mut meta1,
            )
            .unwrap_err();
        assert_ne!(err1.mutation_outcome(), MutationOutcome::Uncertain);
        assert_eq!(
            err1.adapter_kind(),
            Some(AdapterErrorKind::TransactionAborted)
        );
        let p1 = meta1.provenance().unwrap();
        assert_eq!(p1.status(), EffectStatus::RolledBack);
        assert_eq!(p1.reason_code(), Some("transaction_aborted"));
        assert_eq!(p1.decision_ref(), Some("dec-rb"));

        // 2. explicit not_attempted on 503 preserves NotAttempted cleanly
        let mut meta2 = CloudResponseMetadata::default();
        let err2 = conn
            .write_with_attempt(
                "INSERT INTO t VALUES (3)",
                &[],
                &OperationAttempt::new("att-na").unwrap(),
                &mut meta2,
            )
            .unwrap_err();
        assert_ne!(err2.mutation_outcome(), MutationOutcome::Uncertain);
        let p2 = meta2.provenance().unwrap();
        assert_eq!(p2.status(), EffectStatus::NotAttempted);
        assert_eq!(p2.reason_code(), Some("storage_unavailable"));

        // 3. refused preserves RemotePolicy + DecisionOutcome::Refused
        let mut meta3 = CloudResponseMetadata::default();
        let err3 = conn
            .write_with_attempt(
                "INSERT INTO t VALUES (4)",
                &[],
                &OperationAttempt::new("att-ref").unwrap(),
                &mut meta3,
            )
            .unwrap_err();
        assert_eq!(err3.failure_origin(), FailureOrigin::RemotePolicy);
        assert_eq!(err3.adapter_code(), Some("grant_revoked"));
        let p3 = meta3.provenance().unwrap();
        assert_eq!(p3.status(), EffectStatus::Refused);
        assert_eq!(p3.decision(), Some(DecisionOutcome::Refused));
        assert_eq!(p3.decision_ref(), Some("dec-ref-1"));

        // 4. committed_observed succeeds cleanly
        let mut meta4 = CloudResponseMetadata::default();
        let res4 = conn
            .write_with_attempt(
                "INSERT INTO t VALUES (5)",
                &[],
                &OperationAttempt::new("att-commit").unwrap(),
                &mut meta4,
            )
            .unwrap();
        assert_eq!(res4.affected_rows, 1);
        let p4 = meta4.provenance().unwrap();
        assert_eq!(p4.contract_version(), Some(PROVENANCE_CONTRACT_VERSION));
        assert_eq!(p4.status(), EffectStatus::CommittedObserved);
        assert_eq!(p4.decision(), Some(DecisionOutcome::Allowed));
        assert_eq!(p4.observation_source(), ObservationSource::DirectResponse);
        assert_eq!(p4.decision_ref(), Some("dec-commit-1"));
        assert!(p4.origin_bound());

        assert_eq!(seen.load(Ordering::SeqCst), 5);
        handle.join().unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn committed_before_ack_is_uncertain_never_auto_retried_and_reconciles_separately() {
        let server_commits = Arc::new(AtomicUsize::new(0));
        let commits_srv = server_commits.clone();
        let (origin, seen, handle) = spawn_fault_server(4, move |req, origin| {
            if req.path.contains("operation-status") || req.path.contains("dactyl_ctrl_status") {
                return WireResponse::Http {
                    status: 200,
                    headers: provenance_headers(
                        origin,
                        req,
                        "req-reconcile",
                        "committed_observed",
                        "allowed",
                        "read",
                        None,
                        Some("dec-committed-before-ack"),
                        "bearer",
                    ),
                    body: serde_json::to_vec(&serde_json::json!({
                        "recorded": true,
                        "commit_seq": commits_srv.load(Ordering::SeqCst)
                    }))
                    .unwrap(),
                };
            }
            commits_srv.fetch_add(1, Ordering::SeqCst);
            WireResponse::DropBeforeResponse
        });

        let rpc = SupabaseRpcConfig::v1(&origin, KEY);
        let connector = LogicalConnector::cloud_with_config(rpc).unwrap();

        let att_acc = OperationAttempt::new("att-dropped-account")
            .unwrap()
            .with_scope_ref("org:acme")
            .unwrap();
        let mut prior_meta = CloudResponseMetadata::default();
        let err = connector
            .account_request_with_attempt(
                AccountOperation::Checkout,
                &serde_json::json!({"version": 1, "plan": "pro"}),
                "secret-bearer",
                None,
                None,
                &att_acc,
                &mut prior_meta,
            )
            .unwrap_err();
        assert_eq!(err.mutation_outcome(), MutationOutcome::Uncertain);
        assert_eq!(err.adapter_code(), Some("outcome_unknown"));
        assert_eq!(server_commits.load(Ordering::SeqCst), 1);
        assert_eq!(seen.load(Ordering::SeqCst), 1);

        let prior_prov = prior_meta.provenance().unwrap().clone();
        assert_eq!(prior_prov.status(), EffectStatus::Uncertain);
        assert_eq!(
            prior_prov.observation_source(),
            ObservationSource::TransportFault
        );
        assert_eq!(prior_prov.attempt_id(), Some("att-dropped-account"));
        let sent_digest = prior_prov
            .request_digest()
            .expect("request digest computed")
            .to_owned();

        let att_rec = OperationAttempt::new("att-dropped-account")
            .unwrap()
            .with_request_digest(sent_digest.clone())
            .unwrap()
            .with_scope_ref("org:acme")
            .unwrap();
        let mut rec_meta = CloudResponseMetadata::default();
        let obs = connector
            .reconcile_operation_async(
                &att_rec,
                "secret-bearer",
                None,
                Some(prior_prov.status()),
                &mut rec_meta,
            )
            .await
            .unwrap();
        assert_eq!(server_commits.load(Ordering::SeqCst), 1);
        assert_eq!(obs.prior_client_status(), Some(EffectStatus::Uncertain));
        assert_eq!(obs.provenance().status(), EffectStatus::CommittedObserved);
        assert_eq!(
            obs.provenance().observation_source(),
            ObservationSource::ServerReadback
        );
        assert_eq!(obs.request_digest(), Some(sent_digest.as_str()));
        assert_eq!(
            prior_meta.provenance().unwrap().status(),
            EffectStatus::Uncertain
        );

        let att_ctrl = OperationAttempt::new("att-dropped-ctrl").unwrap();
        let mut ctrl_prior_meta = CloudResponseMetadata::default();
        let ctrl_err = connector
            .control_request_with_attempt_async(
                "dactyl_ctrl_mutate",
                &serde_json::json!({"op": "rotate"}),
                Some("secret-bearer"),
                None,
                OperationKind::Write,
                ResourceBudget::v1(),
                &att_ctrl,
                &mut ctrl_prior_meta,
            )
            .await
            .unwrap_err();
        assert_eq!(ctrl_err.mutation_outcome(), MutationOutcome::Uncertain);
        assert_eq!(server_commits.load(Ordering::SeqCst), 2);
        assert_eq!(
            ctrl_prior_meta.provenance().map(|p| p.status()),
            Some(EffectStatus::Uncertain)
        );

        let mut ctrl_rec_meta = CloudResponseMetadata::default();
        let ctrl_obs = connector
            .reconcile_control_attempt(
                "dactyl_ctrl_status",
                &att_ctrl,
                Some("secret-bearer"),
                None,
                ResourceBudget::v1(),
                Some(EffectStatus::Uncertain),
                &mut ctrl_rec_meta,
            )
            .unwrap();
        assert_eq!(
            ctrl_obs.prior_client_status(),
            Some(EffectStatus::Uncertain)
        );
        assert_eq!(
            ctrl_obs.provenance().observation_source(),
            ObservationSource::ServerReadback
        );
        assert_eq!(
            ctrl_obs.provenance().status(),
            EffectStatus::CommittedObserved
        );
        assert_eq!(server_commits.load(Ordering::SeqCst), 2);

        handle.join().unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn fault_partial_response_with_committed_header_stays_uncertain_sync_and_async() {
        let mode = Arc::new(AtomicUsize::new(0));
        let mode_srv = mode.clone();
        let (origin, seen, handle) = spawn_fault_server(4, move |req, origin| {
            let hdrs = provenance_headers(
                origin,
                req,
                "req-partial",
                "committed_observed",
                "allowed",
                "write",
                None,
                Some("dec-forged-partial"),
                "bearer",
            );
            match mode_srv.fetch_add(1, Ordering::SeqCst) {
                0 | 1 => WireResponse::PartialChunked {
                    status: 200,
                    headers: hdrs,
                    prefix: b"{\"affected_rows\":1".to_vec(),
                },
                _ => WireResponse::Http {
                    status: 200,
                    headers: hdrs,
                    body: b"{\"affected_rows\":1".to_vec(),
                },
            }
        });

        let conn = open_supabase_conn(&origin, "bearer-token", AccessMode::ReadWrite, None);

        for i in 0..4usize {
            let attempt = OperationAttempt::new(format!("att-partial-{i}")).unwrap();
            let mut meta = CloudResponseMetadata::default();
            let err = if i % 2 == 0 {
                conn.write_with_attempt("UPDATE t SET v = 1", &[], &attempt, &mut meta)
                    .unwrap_err()
            } else {
                conn.write_with_attempt_async("UPDATE t SET v = 1", &[], &attempt, &mut meta)
                    .await
                    .unwrap_err()
            };
            assert_eq!(err.mutation_outcome(), MutationOutcome::Uncertain);
            let prov = meta.provenance().expect("transport fault provenance");
            assert_eq!(
                prov.status(),
                EffectStatus::Uncertain,
                "partial response {i} must NEVER trust committed_observed header"
            );
            assert_eq!(prov.observation_source(), ObservationSource::TransportFault);
        }

        assert_eq!(seen.load(Ordering::SeqCst), 4);
        handle.join().unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn fault_timeout_with_late_effect_is_uncertain_and_reconciles() {
        let late_committed = Arc::new(AtomicUsize::new(0));
        let late_srv = late_committed.clone();
        let (origin, seen, handle) = spawn_fault_server(2, move |req, origin| {
            if req.path.contains("operation-status") {
                return WireResponse::Http {
                    status: 200,
                    headers: provenance_headers(
                        origin,
                        req,
                        "req-late-rec",
                        "committed_observed",
                        "allowed",
                        "read",
                        None,
                        Some("dec-late-1"),
                        "bearer",
                    ),
                    body: serde_json::to_vec(&serde_json::json!({
                        "status": "committed",
                        "commits": late_srv.load(Ordering::SeqCst)
                    }))
                    .unwrap(),
                };
            }
            late_srv.fetch_add(1, Ordering::SeqCst);
            WireResponse::DelayedHttp {
                delay: Duration::from_millis(140),
                status: 200,
                headers: provenance_headers(
                    origin,
                    req,
                    "req-late",
                    "committed_observed",
                    "allowed",
                    "write",
                    None,
                    None,
                    "bearer",
                ),
                body: serde_json::to_vec(&serde_json::json!({"ok": true})).unwrap(),
            }
        });

        let short_budget = ResourceBudget::v1().with_request_timeout(Duration::from_millis(45));
        let connector = LogicalConnector::cloud_with_config(SupabaseRpcConfig::v1(&origin, KEY))
            .unwrap()
            .with_http_budget(short_budget)
            .unwrap();

        let attempt = OperationAttempt::new("att-late-timeout").unwrap();
        let mut meta = CloudResponseMetadata::default();
        let err = connector
            .account_request_with_attempt_async(
                AccountOperation::Manage,
                &serde_json::json!({"version": 1}),
                "bearer-late",
                None,
                None,
                &attempt,
                &mut meta,
            )
            .await
            .unwrap_err();
        assert_eq!(err.adapter_kind(), Some(AdapterErrorKind::Timeout));
        assert_eq!(err.mutation_outcome(), MutationOutcome::Uncertain);
        assert_eq!(seen.load(Ordering::SeqCst), 1);
        assert_eq!(
            meta.provenance()
                .map(|p| (p.status(), p.observation_source())),
            Some((EffectStatus::Uncertain, ObservationSource::TransportFault))
        );

        let long_budget = ResourceBudget::v1().with_request_timeout(Duration::from_secs(2));
        let rec_conn = LogicalConnector::cloud_with_config(SupabaseRpcConfig::v1(&origin, KEY))
            .unwrap()
            .with_http_budget(long_budget)
            .unwrap();
        let mut rec_meta = CloudResponseMetadata::default();
        let obs = rec_conn
            .reconcile_operation_async(
                &attempt,
                "bearer-late",
                None,
                Some(EffectStatus::Uncertain),
                &mut rec_meta,
            )
            .await
            .unwrap();
        assert_eq!(obs.provenance().status(), EffectStatus::CommittedObserved);
        assert_eq!(obs.prior_client_status(), Some(EffectStatus::Uncertain));
        assert_eq!(late_committed.load(Ordering::SeqCst), 1);

        handle.join().unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn two_competing_clients_seven_severed_retries_commit_once_and_surface_digest_conflicts()
    {
        let store: Arc<Mutex<HashMap<String, String>>> = Arc::new(Mutex::new(HashMap::new()));
        let commits = Arc::new(AtomicUsize::new(0));
        let attempts_seen = Arc::new(AtomicUsize::new(0));
        let store_srv = store.clone();
        let commits_srv = commits.clone();
        let attempts_srv = attempts_seen.clone();

        let (origin, seen, handle) = spawn_fault_server(10, move |req, origin| {
            let n = attempts_srv.fetch_add(1, Ordering::SeqCst);
            let attempt_id = req.header("decapod-cloud-attempt-id").unwrap().to_owned();
            let digest = req
                .header("decapod-cloud-request-digest")
                .unwrap()
                .to_owned();

            let mut map = store_srv.lock().unwrap();
            match map.get(&attempt_id) {
                None => {
                    map.insert(attempt_id, digest);
                    commits_srv.fetch_add(1, Ordering::SeqCst);
                    WireResponse::DropBeforeResponse
                }
                Some(existing_digest) if existing_digest == &digest => {
                    if n < 7 {
                        WireResponse::DropBeforeResponse
                    } else {
                        WireResponse::Http {
                            status: 200,
                            headers: provenance_headers(
                                origin,
                                req,
                                "req-idemp-ok",
                                "committed_observed",
                                "allowed",
                                "write",
                                None,
                                Some("dec-idemp-1"),
                                "bearer",
                            ),
                            body: serde_json::to_vec(&serde_json::json!({
                                "results": [{
                                    "columns": [],
                                    "rows": [],
                                    "affected_rows": 1,
                                    "generated_keys": []
                                }]
                            }))
                            .unwrap(),
                        }
                    }
                }
                Some(_) => {
                    if n == 8 {
                        WireResponse::Http {
                            status: 409,
                            headers: provenance_headers(
                                origin,
                                req,
                                "req-conflict",
                                "refused",
                                "refused",
                                "write",
                                Some("idempotency_conflict"),
                                None,
                                "bearer",
                            ),
                            body: serde_json::to_vec(&serde_json::json!({
                                "error": {"code": "idempotency_conflict"}
                            }))
                            .unwrap(),
                        }
                    } else {
                        let mut hdrs = provenance_headers(
                            origin,
                            req,
                            "req-spoof-digest",
                            "committed_observed",
                            "allowed",
                            "write",
                            None,
                            None,
                            "bearer",
                        );
                        for (k, v) in &mut hdrs {
                            if k == "Decapod-Cloud-Request-Digest" {
                                *v = "sha256:0000000000000000000000000000000000000000000000000000000000000000".into();
                            }
                        }
                        WireResponse::Http {
                            status: 200,
                            headers: hdrs,
                            body: serde_json::to_vec(&serde_json::json!({
                                "results": [{
                                    "columns": [],
                                    "rows": [],
                                    "affected_rows": 1,
                                    "generated_keys": []
                                }]
                            }))
                            .unwrap(),
                        }
                    }
                }
            }
        });

        let client_a = open_supabase_conn(&origin, "bearer-client-a", AccessMode::ReadWrite, None);
        let client_b = open_supabase_conn(&origin, "bearer-client-b", AccessMode::ReadWrite, None);

        let shared_attempt = OperationAttempt::new("att-competing-7").unwrap();
        let batch = [Operation::write(
            "INSERT INTO ledger (id, amt) VALUES ($1, $2)",
            vec![Parameter::Integer(99), Parameter::Integer(500)],
        )];

        for retry in 0..7usize {
            let mut meta = CloudResponseMetadata::default();
            let err = if retry % 2 == 0 {
                client_a
                    .atomic_with_attempt(&batch, &shared_attempt, &mut meta)
                    .unwrap_err()
            } else {
                client_b
                    .atomic_with_attempt_async(&batch, &shared_attempt, &mut meta)
                    .await
                    .unwrap_err()
            };
            assert_eq!(err.mutation_outcome(), MutationOutcome::Uncertain);
            assert_eq!(
                meta.provenance().map(|p| p.status()),
                Some(EffectStatus::Uncertain)
            );
            assert_eq!(commits.load(Ordering::SeqCst), 1);
        }

        let mut final_meta = CloudResponseMetadata::default();
        let atomic_ok = client_b
            .atomic_with_attempt_async(&batch, &shared_attempt, &mut final_meta)
            .await
            .unwrap();
        assert_eq!(atomic_ok.results.len(), 1);
        assert_eq!(commits.load(Ordering::SeqCst), 1);
        assert_eq!(
            final_meta.provenance().map(|p| p.status()),
            Some(EffectStatus::CommittedObserved)
        );

        let mutated_batch = [Operation::write(
            "INSERT INTO ledger (id, amt) VALUES ($1, $2)",
            vec![Parameter::Integer(99), Parameter::Integer(999)],
        )];
        let mut conflict_meta = CloudResponseMetadata::default();
        let conflict_err = client_b
            .atomic_with_attempt(&mutated_batch, &shared_attempt, &mut conflict_meta)
            .unwrap_err();
        assert_eq!(
            conflict_err.adapter_kind(),
            Some(AdapterErrorKind::IdempotencyConflict)
        );
        assert_eq!(conflict_err.adapter_code(), Some("idempotency_conflict"));
        assert_eq!(commits.load(Ordering::SeqCst), 1);

        let mut digest_err_meta = CloudResponseMetadata::default();
        let digest_err = client_a
            .atomic_with_attempt(&mutated_batch, &shared_attempt, &mut digest_err_meta)
            .unwrap_err();
        assert_eq!(
            digest_err.adapter_code(),
            Some("provenance_digest_mismatch")
        );
        assert_eq!(commits.load(Ordering::SeqCst), 1);
        assert_eq!(seen.load(Ordering::SeqCst), 10);

        handle.join().unwrap();
    }

    #[test]
    fn dpop_nonce_challenge_preserves_attempt_and_wrong_audience_refusal_maps_cleanly() {
        let step = Arc::new(AtomicUsize::new(0));
        let step_srv = step.clone();
        let (origin, seen, handle) = spawn_fault_server(3, move |req, origin| {
            match step_srv.fetch_add(1, Ordering::SeqCst) {
                0 => {
                    let mut hdrs = provenance_headers(
                        origin,
                        req,
                        "req-dpop-1",
                        "refused",
                        "refused",
                        "write",
                        Some("use_dpop_nonce"),
                        None,
                        "dpop",
                    );
                    hdrs.push((
                        "WWW-Authenticate".into(),
                        "DPoP error=\"use_dpop_nonce\"".into(),
                    ));
                    hdrs.push(("DPoP-Nonce".into(), "fresh-nonce-1".into()));
                    WireResponse::Http {
                        status: 401,
                        headers: hdrs,
                        body: serde_json::to_vec(&serde_json::json!({
                            "error": {"code": "use_dpop_nonce"}
                        }))
                        .unwrap(),
                    }
                }
                1 => {
                    assert!(req.header("dpop").unwrap().contains("nonce=fresh-nonce-1"));
                    WireResponse::Http {
                        status: 200,
                        headers: provenance_headers(
                            origin,
                            req,
                            "req-dpop-2",
                            "committed_observed",
                            "allowed",
                            "write",
                            None,
                            Some("dec-dpop-ok"),
                            "dpop",
                        ),
                        body: serde_json::to_vec(&serde_json::json!({
                            "columns": [],
                            "rows": [],
                            "affected_rows": 1,
                            "generated_keys": []
                        }))
                        .unwrap(),
                    }
                }
                _ => WireResponse::Http {
                    status: 401,
                    headers: provenance_headers(
                        origin,
                        req,
                        "req-dpop-aud",
                        "refused",
                        "refused",
                        "write",
                        Some("dpop_aud_mismatch"),
                        Some("dec-dpop-aud"),
                        "dpop",
                    ),
                    body: serde_json::to_vec(&serde_json::json!({
                        "error": {"code": "dpop_aud_mismatch"}
                    }))
                    .unwrap(),
                },
            }
        });

        let good_signer = Arc::new(TestDpopSigner::default());
        let conn = open_supabase_conn(
            &origin,
            "dpop-secret-bearer",
            AccessMode::ReadWrite,
            Some(good_signer.clone()),
        );

        let attempt = OperationAttempt::new("att-dpop-1").unwrap();
        let mut meta = CloudResponseMetadata::default();
        let write = conn
            .write_with_attempt("INSERT INTO t VALUES (1)", &[], &attempt, &mut meta)
            .unwrap();
        assert_eq!(write.affected_rows, 1);
        let prov = meta.provenance().unwrap();
        assert_eq!(prov.status(), EffectStatus::CommittedObserved);
        assert_eq!(prov.auth_scheme(), Some(AuthScheme::DPoP));
        assert_eq!(good_signer.calls.lock().unwrap().len(), 2);

        let wrong_aud_signer = Arc::new(TestDpopSigner {
            audience: Some("https://wrong.example".into()),
            calls: Mutex::new(Vec::new()),
        });
        let bad_conn = open_supabase_conn(
            &origin,
            "dpop-secret-bearer",
            AccessMode::ReadWrite,
            Some(wrong_aud_signer),
        );
        let mut bad_meta = CloudResponseMetadata::default();
        let aud_err = bad_conn
            .write_with_attempt(
                "INSERT INTO t VALUES (2)",
                &[],
                &OperationAttempt::new("att-dpop-aud").unwrap(),
                &mut bad_meta,
            )
            .unwrap_err();
        assert_eq!(
            aud_err.adapter_kind(),
            Some(AdapterErrorKind::Authentication)
        );
        assert_eq!(aud_err.adapter_code(), Some("dpop_aud_mismatch"));
        let bad_prov = bad_meta.provenance().unwrap();
        assert_eq!(bad_prov.status(), EffectStatus::Refused);
        assert_eq!(bad_prov.reason_code(), Some("dpop_aud_mismatch"));
        assert!(!format!("{aud_err:?} {bad_prov:?}").contains("dpop-secret-bearer"));
        assert!(!format!("{aud_err:?} {bad_prov:?}").contains("dpop-proof"));

        assert_eq!(seen.load(Ordering::SeqCst), 3);
        handle.join().unwrap();
    }

    #[test]
    fn redirect_swapped_ref_mismatched_origin_auth_and_forged_headers_fail_closed() {
        let case = Arc::new(AtomicUsize::new(0));
        let case_srv = case.clone();
        let (origin, seen, handle) = spawn_fault_server(8, move |req, origin| {
            let ok_body = serde_json::to_vec(&serde_json::json!({
                "columns": [],
                "rows": [],
                "affected_rows": 1,
                "generated_keys": []
            }))
            .unwrap();
            match case_srv.fetch_add(1, Ordering::SeqCst) {
                0 => {
                    let mut hdrs = provenance_headers(
                        origin,
                        req,
                        "req-redir",
                        "committed_observed",
                        "allowed",
                        "write",
                        None,
                        None,
                        "bearer",
                    );
                    hdrs.push(("Location".into(), "https://attacker.example/steal".into()));
                    WireResponse::Http {
                        status: 307,
                        headers: hdrs,
                        body: b"{}".to_vec(),
                    }
                }
                1 => {
                    let mut hdrs = provenance_headers(
                        origin,
                        req,
                        "req-swap-att",
                        "committed_observed",
                        "allowed",
                        "write",
                        None,
                        None,
                        "bearer",
                    );
                    for (k, v) in &mut hdrs {
                        if k == "Decapod-Cloud-Attempt-Id" {
                            *v = "att-someone-else".into();
                        }
                    }
                    WireResponse::Http {
                        status: 200,
                        headers: hdrs,
                        body: ok_body,
                    }
                }
                2 => {
                    let mut hdrs = provenance_headers(
                        origin,
                        req,
                        "req-swap-scope",
                        "committed_observed",
                        "allowed",
                        "write",
                        None,
                        None,
                        "bearer",
                    );
                    for (k, v) in &mut hdrs {
                        if k == "Decapod-Cloud-Scope-Ref" {
                            *v = "repo:evil/other".into();
                        }
                    }
                    WireResponse::Http {
                        status: 200,
                        headers: hdrs,
                        body: ok_body,
                    }
                }
                3 => {
                    let mut hdrs = provenance_headers(
                        origin,
                        req,
                        "req-swap-origin",
                        "committed_observed",
                        "allowed",
                        "write",
                        None,
                        None,
                        "bearer",
                    );
                    for (k, v) in &mut hdrs {
                        if k == "Decapod-Cloud-Origin" {
                            *v = "https://wrong-origin.example".into();
                        }
                    }
                    WireResponse::Http {
                        status: 200,
                        headers: hdrs,
                        body: ok_body,
                    }
                }
                4 => WireResponse::Http {
                    status: 200,
                    headers: provenance_headers(
                        origin,
                        req,
                        "req-swap-auth",
                        "committed_observed",
                        "allowed",
                        "write",
                        None,
                        None,
                        "dpop",
                    ),
                    body: ok_body,
                },
                5 => WireResponse::Http {
                    status: 200,
                    headers: provenance_headers(
                        origin,
                        req,
                        "req-hdr-json-conflict",
                        "committed_observed",
                        "allowed",
                        "write",
                        None,
                        None,
                        "bearer",
                    ),
                    body: serde_json::to_vec(&serde_json::json!({
                        "columns": [],
                        "rows": [],
                        "affected_rows": 1,
                        "generated_keys": [],
                        "provenance": {
                            "version": 1,
                            "effect_status": "rolled_back"
                        }
                    }))
                    .unwrap(),
                },
                6 => {
                    let mut hdrs = provenance_headers(
                        origin,
                        req,
                        "req-ver-99",
                        "committed_observed",
                        "allowed",
                        "write",
                        None,
                        None,
                        "bearer",
                    );
                    for (k, v) in &mut hdrs {
                        if k == "Decapod-Cloud-Provenance-Version" {
                            *v = "99".into();
                        }
                    }
                    WireResponse::Http {
                        status: 200,
                        headers: hdrs,
                        body: ok_body,
                    }
                }
                _ => WireResponse::Http {
                    status: 200,
                    headers: provenance_headers(
                        origin,
                        req,
                        "req-wrong-op",
                        "committed_observed",
                        "allowed",
                        "read",
                        None,
                        None,
                        "bearer",
                    ),
                    body: ok_body,
                },
            }
        });

        let conn = open_supabase_conn(&origin, "bearer-token", AccessMode::ReadWrite, None);

        let expected = [
            ("provenance_origin_mismatch", false),
            ("provenance_attempt_mismatch", true),
            ("provenance_scope_mismatch", true),
            ("provenance_origin_mismatch", false),
            ("provenance_auth_mismatch", true),
            ("provenance_conflict", true),
            ("unsupported_provenance_version", true),
            ("provenance_operation_mismatch", true),
        ];

        for (idx, (expected_code, expected_origin_bound)) in expected.iter().enumerate() {
            let attempt = OperationAttempt::new(format!("att-bad-{idx}"))
                .unwrap()
                .with_scope_ref("repo:org/expected")
                .unwrap();
            let mut meta = CloudResponseMetadata::default();
            let err = conn
                .write_with_attempt("INSERT INTO t VALUES (1)", &[], &attempt, &mut meta)
                .unwrap_err();
            assert_eq!(err.adapter_code(), Some(*expected_code), "case {idx}");
            assert_eq!(
                err.mutation_outcome(),
                MutationOutcome::Uncertain,
                "case {idx}"
            );
            let prov = meta.provenance().expect("rejected provenance metadata");
            assert_eq!(prov.status(), EffectStatus::Uncertain, "case {idx}");
            assert_eq!(prov.origin_bound(), *expected_origin_bound, "case {idx}");
        }

        assert_eq!(seen.load(Ordering::SeqCst), 8);
        handle.join().unwrap();
    }

    #[test]
    fn unadvertised_server_yields_unsupported_provenance_and_redacts_debug_surfaces() {
        let step = Arc::new(AtomicUsize::new(0));
        let step_srv = step.clone();
        let (origin, seen, handle) = spawn_fault_server(3, move |req, origin| {
            match step_srv.fetch_add(1, Ordering::SeqCst) {
                0 => WireResponse::Http {
                    status: 200,
                    headers: vec![("Decapod-Cloud-Request-Id".into(), "req-legacy-only".into())],
                    body: serde_json::to_vec(&serde_json::json!({ "status": "ok" })).unwrap(),
                },
                1 => WireResponse::Http {
                    status: 403,
                    headers: provenance_headers(
                        origin,
                        req,
                        "req-grant-revoked",
                        "refused",
                        "refused",
                        "write",
                        Some("grant_revoked"),
                        Some("secret-decision-ref"),
                        "bearer",
                    ),
                    body: serde_json::to_vec(&serde_json::json!({
                        "error": {"code": "grant_revoked"}
                    }))
                    .unwrap(),
                },
                _ => WireResponse::Http {
                    status: 200,
                    headers: provenance_headers(
                        origin,
                        req,
                        "req-grant-rec",
                        "not_attempted",
                        "refused",
                        "read",
                        Some("grant_revoked"),
                        Some("secret-decision-ref"),
                        "bearer",
                    ),
                    body: serde_json::to_vec(&serde_json::json!({
                        "recorded": false
                    }))
                    .unwrap(),
                },
            }
        });

        let connector =
            LogicalConnector::cloud_with_config(SupabaseRpcConfig::v1(&origin, KEY)).unwrap();
        let attempt = OperationAttempt::new("secret-attempt-id-xyz")
            .unwrap()
            .with_scope_ref("secret-scope-ref-xyz")
            .unwrap()
            .with_request_digest("sha256:secret-digest-xyz")
            .unwrap();

        let mut legacy_meta = CloudResponseMetadata::default();
        let legacy_val = connector
            .account_request_with_metadata(
                AccountOperation::Status,
                &serde_json::json!({"version": 1}),
                "ultra-secret-bearer-token",
                None,
                None,
                &mut legacy_meta,
            )
            .unwrap();
        assert_eq!(legacy_val["status"], "ok");
        let legacy_prov = legacy_meta.provenance().unwrap();
        assert_eq!(legacy_prov.status(), EffectStatus::UnsupportedProvenance);
        assert_eq!(legacy_prov.contract_version(), None);
        assert_eq!(legacy_prov.request_id(), Some("req-legacy-only"));

        let mut rev_meta = CloudResponseMetadata::default();
        let rev_err = connector
            .account_request_with_attempt(
                AccountOperation::Checkout,
                &serde_json::json!({"version": 1}),
                "ultra-secret-bearer-token",
                None,
                None,
                &attempt,
                &mut rev_meta,
            )
            .unwrap_err();
        assert_eq!(
            rev_err.adapter_kind(),
            Some(AdapterErrorKind::Authorization)
        );
        assert_eq!(rev_err.adapter_code(), Some("grant_revoked"));

        let rev_prov = rev_meta.provenance().unwrap();
        assert_eq!(rev_prov.status(), EffectStatus::Refused);

        let mut rec_meta = CloudResponseMetadata::default();
        let obs = connector
            .reconcile_operation(
                &attempt,
                "ultra-secret-bearer-token",
                None,
                Some(EffectStatus::Refused),
                &mut rec_meta,
            )
            .unwrap();
        assert_eq!(obs.prior_client_status(), Some(EffectStatus::Refused));
        assert_eq!(obs.provenance().status(), EffectStatus::NotAttempted);

        let redacted_dump = format!("{attempt:?} {rev_prov:?} {rev_meta:?} {obs:?} {rev_err:?}");
        for secret in [
            "ultra-secret-bearer-token",
            "secret-attempt-id-xyz",
            "secret-scope-ref-xyz",
            "sha256:secret-digest-xyz",
            "secret-decision-ref",
            "req-grant-revoked",
            "req-grant-rec",
            &origin,
        ] {
            assert!(
                !redacted_dump.contains(secret),
                "sensitive material `{secret}` leaked into debug output: {redacted_dump}"
            );
        }

        assert_eq!(seen.load(Ordering::SeqCst), 3);
        handle.join().unwrap();
    }
}
