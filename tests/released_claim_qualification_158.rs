//! Controlled negative-fixture and released-contract qualification suite for
//! DecapodLabs/dactyl#158.
//!
//! Evidence classes:
//! - `local_sqlite` (`#[cfg(feature = "sqlite")]`): host SQLite C-ABI engine
//!   verified via an independent read-only observer connection.
//! - `synthetic_http` (`#[cfg(feature = "supabase")]`): loopback raw TCP/HTTP
//!   gateway with independent server-side persisted-state observation.
//! - `local_postgres`, `hosted_supabase`, `live_propodus`: unavailable here.

#[cfg(feature = "sqlite")]
mod sqlite_guarantees {
    use dactyl_db::{
        AccessMode, AdapterErrorKind, CloudResponseMetadata, Connection, DatastoreRoute,
        EffectStatus, LogicalConnector, MutationOutcome, ObservationSource, OpenOptions, Operation,
        OperationAttempt, OperationKind, OperationalAction, ResourceBudget,
    };

    #[test]
    fn sqlite_local_observer_verifies_commit_rollback_and_blocks_cloud_elevation() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("guarantee_matrix.sqlite");
        let path_str = db_path.to_str().unwrap().to_owned();

        let rw = Connection::open(DatastoreRoute::sqlite(&path_str)).unwrap();
        rw.write(
            "CREATE TABLE ledger (id INTEGER PRIMARY KEY, label TEXT NOT NULL)",
            &[],
        )
        .unwrap();

        let observer = Connection::open_with_options(
            DatastoreRoute::sqlite(&path_str),
            OpenOptions {
                access_mode: AccessMode::ReadOnly,
                ..OpenOptions::default()
            },
        )
        .unwrap();
        assert_eq!(
            observer.read("SELECT COUNT(*) FROM ledger", &[]).unwrap().0[0].values[0].as_i64(),
            Some(0)
        );

        assert_eq!(
            rw.write(
                "INSERT INTO ledger (id, label) VALUES (1, 'committed')",
                &[]
            )
            .unwrap(),
            1
        );
        assert_eq!(
            observer.read("SELECT COUNT(*) FROM ledger", &[]).unwrap().0[0].values[0].as_i64(),
            Some(1)
        );

        let atomic_err = rw
            .atomic(&[
                Operation::write(
                    "INSERT INTO ledger (id, label) VALUES (2, 'rolled-back')",
                    vec![],
                ),
                Operation::write(
                    "INSERT INTO ledger (id, label) VALUES (1, 'duplicate-pk')",
                    vec![],
                ),
            ])
            .unwrap_err();
        assert_eq!(
            atomic_err.adapter_kind(),
            Some(AdapterErrorKind::Constraint)
        );
        assert_eq!(atomic_err.mutation_outcome(), MutationOutcome::RolledBack);
        assert_eq!(
            atomic_err
                .operational_disposition(OperationKind::Write)
                .action(),
            OperationalAction::Stop
        );
        assert_eq!(
            observer.read("SELECT COUNT(*) FROM ledger", &[]).unwrap().0[0].values[0].as_i64(),
            Some(1)
        );

        let attempt = OperationAttempt::new("sqlite-no-cloud-elevation").unwrap();
        let mut meta = CloudResponseMetadata::default();
        let err = rw
            .write_with_attempt(
                "INSERT INTO ledger VALUES (3, 'x')",
                &[],
                &attempt,
                &mut meta,
            )
            .unwrap_err();
        assert_eq!(err.adapter_kind(), Some(AdapterErrorKind::Capability));
        assert_eq!(err.adapter_code(), Some("cloud_connector_required"));
        assert_eq!(err.mutation_outcome(), MutationOutcome::NotAttempted);
        let disp = err.operational_disposition_with_context(
            OperationKind::Write,
            Some(&attempt),
            Some(&meta),
        );
        assert_eq!(disp.action(), OperationalAction::Stop);
        assert_eq!(disp.effect_status(), Some(EffectStatus::NotAttempted));
        assert_eq!(
            disp.observation_source(),
            Some(ObservationSource::LocalPreflight)
        );

        let local = LogicalConnector::local(&path_str).unwrap();
        let mut ctrl_meta = CloudResponseMetadata::default();
        let ctrl_err = local
            .reconcile_control_attempt(
                "dactyl_ctrl_status",
                &attempt,
                None,
                None,
                ResourceBudget::v1(),
                None,
                &mut ctrl_meta,
            )
            .unwrap_err();
        assert_eq!(ctrl_err.adapter_kind(), Some(AdapterErrorKind::Capability));
        assert_eq!(ctrl_err.adapter_code(), Some("cloud_connector_required"));
    }
}

#[cfg(feature = "supabase")]
mod cloud_negative_fixtures {
    use std::collections::HashMap;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::thread;

    use dactyl_db::{
        AccountOperation, AdapterErrorKind, AuthScheme, CloudConnector, CloudResponseMetadata,
        Connection, DactylError, DatastoreRoute, DecisionOutcome, EffectStatus, MutationOutcome,
        ObservationSource, OperationAttempt, OperationKind, OperationalAction, RequestProof,
        RequestProofInput, RequestProofSigner, ResourceBudget, StorageContext, SupabaseRpcConfig,
    };

    const KEY: &str = "sb_publishable_158_qualification";
    const HUMAN_BEARER: &str = "human-bearer-158";
    const DEVICE_BEARER: &str = "dpc_at_device_158";

    struct CountingDpopSigner {
        nonces_seen: Mutex<Vec<String>>,
    }

    impl CountingDpopSigner {
        fn new() -> Self {
            Self {
                nonces_seen: Mutex::new(Vec::new()),
            }
        }
    }

    impl RequestProofSigner for CountingDpopSigner {
        fn sign_request(&self, input: &RequestProofInput<'_>) -> Result<RequestProof, DactylError> {
            let nonce = input.nonce().unwrap_or("none");
            RequestProof::dpop(format!(
                "dpop-jwt:{}:{}:{}",
                input.method(),
                input.canonical_url(),
                nonce
            ))
        }

        fn observe_nonce(&self, _canonical_url: &str, nonce: &str) {
            self.nonces_seen.lock().unwrap().push(nonce.to_owned());
        }
    }

    struct WireRequest {
        path: String,
        headers: HashMap<String, String>,
    }

    impl WireRequest {
        fn header(&self, name: &str) -> Option<&str> {
            self.headers
                .get(&name.to_ascii_lowercase())
                .map(String::as_str)
        }
    }

    fn read_http(stream: &mut TcpStream) -> WireRequest {
        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        let head_end = loop {
            if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                break pos + 4;
            }
            let n = stream.read(&mut chunk).unwrap();
            assert!(n > 0, "unexpected EOF while reading headers");
            buf.extend_from_slice(&chunk[..n]);
        };
        let head = String::from_utf8_lossy(&buf[..head_end]);
        let mut lines = head.lines();
        let req_line = lines.next().unwrap();
        let path = req_line.split_whitespace().nth(1).unwrap().to_owned();
        let mut headers = HashMap::new();
        for line in lines {
            if let Some((k, v)) = line.split_once(':') {
                headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_owned());
            }
        }
        let len = headers
            .get("content-length")
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(0);
        while buf.len() < head_end + len {
            let n = stream.read(&mut chunk).unwrap();
            assert!(n > 0, "unexpected EOF while reading body");
            buf.extend_from_slice(&chunk[..n]);
        }
        WireRequest { path, headers }
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
        stream.write_all(raw.as_bytes()).unwrap();
        stream.write_all(body).unwrap();
        stream.flush().unwrap();
    }

    #[allow(clippy::too_many_arguments)]
    fn provenance_headers(
        origin: &str,
        req: &WireRequest,
        req_id: &str,
        effect_status: &str,
        decision: &str,
        op_kind: &str,
        reason: Option<&str>,
        auth_scheme: &str,
    ) -> Vec<(String, String)> {
        let mut hdrs = vec![
            ("Decapod-Cloud-Provenance-Version".into(), "1".into()),
            ("Decapod-Cloud-Request-Id".into(), req_id.into()),
            ("Decapod-Cloud-Effect-Status".into(), effect_status.into()),
            ("Decapod-Cloud-Decision".into(), decision.into()),
            ("Decapod-Cloud-Operation-Kind".into(), op_kind.into()),
            ("Decapod-Cloud-Decision-Ref".into(), format!("dec-{req_id}")),
            ("Decapod-Cloud-Origin".into(), origin.into()),
            ("Decapod-Cloud-Auth-Scheme".into(), auth_scheme.into()),
        ];
        if let Some(aid) = req.header("decapod-cloud-attempt-id") {
            hdrs.push(("Decapod-Cloud-Attempt-Id".into(), aid.into()));
        }
        if let Some(dig) = req.header("decapod-cloud-request-digest") {
            hdrs.push(("Decapod-Cloud-Request-Digest".into(), dig.into()));
        }
        if let Some(scope) = req.header("decapod-cloud-scope-ref") {
            hdrs.push(("Decapod-Cloud-Scope-Ref".into(), scope.into()));
        }
        if let Some(r) = reason {
            hdrs.push(("Decapod-Cloud-Reason-Code".into(), r.into()));
        }
        hdrs
    }

    fn spawn_scripted_server<F>(max_accepts: usize, handler: F) -> (String, thread::JoinHandle<()>)
    where
        F: Fn(usize, &WireRequest, &str) -> (u16, Vec<(String, String)>, Vec<u8>)
            + Send
            + Sync
            + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let origin_srv = origin.clone();
        let step = Arc::new(AtomicUsize::new(0));
        let handle = thread::spawn(move || {
            for _ in 0..max_accepts {
                let (mut stream, _) = listener.accept().unwrap();
                let req = read_http(&mut stream);
                let idx = step.fetch_add(1, Ordering::SeqCst);
                let (status, headers, body) = handler(idx, &req, &origin_srv);
                send_http(&mut stream, status, &headers, &body);
            }
        });
        (origin, handle)
    }

    #[test]
    fn inaccurate_authenticated_claim_remains_direct_response_not_observed_persistence() {
        // Independent backend state ledger never receives a persisted write, even though
        // the authenticated DPoP gateway claims HTTP 200 committed_observed with matching correlation.
        let persisted_rows = Arc::new(Mutex::new(Vec::<String>::new()));
        let persisted_srv = persisted_rows.clone();

        let (origin, srv) = spawn_scripted_server(2, move |idx, req, orig| match idx {
            0 => {
                assert!(req.path.contains("/cloud-query"));
                assert!(req
                    .header("authorization")
                    .is_some_and(|v| v.starts_with("DPoP ")));
                assert!(req.header("dpop").is_some());
                let hdrs = provenance_headers(
                    orig,
                    req,
                    "req-lie-direct",
                    "committed_observed",
                    "allowed",
                    "write",
                    None,
                    "dpop",
                );
                let body = serde_json::to_vec(&serde_json::json!({
                    "columns": [],
                    "rows": [],
                    "affected_rows": 1,
                    "generated_keys": [],
                    "provenance": {
                        "version": 1,
                        "request_id": "req-lie-direct",
                        "attempt_id": req.header("decapod-cloud-attempt-id"),
                        "request_digest": req.header("decapod-cloud-request-digest"),
                        "scope_ref": req.header("decapod-cloud-scope-ref"),
                        "operation_kind": "write",
                        "decision": "allowed",
                        "effect_status": "committed_observed",
                        "decision_ref": "dec-req-lie-direct",
                        "auth_scheme": "dpop",
                        "origin": orig
                    }
                }))
                .unwrap();
                (200, hdrs, body)
            }
            _ => {
                assert!(req.path.contains("/cloud-control"));
                let persisted = !persisted_srv.lock().unwrap().is_empty();
                assert!(!persisted);
                let hdrs = provenance_headers(
                    orig,
                    req,
                    "req-truth-readback",
                    "not_attempted",
                    "refused",
                    "read",
                    Some("not_attempted"),
                    "dpop",
                );
                let body = serde_json::to_vec(&serde_json::json!({
                    "found": false,
                    "effect_status": "absent"
                }))
                .unwrap();
                (200, hdrs, body)
            }
        });

        let signer = Arc::new(CountingDpopSigner::new());
        let ctx =
            StorageContext::new(1, serde_json::json!({"org_id": "o1", "repo_id": "r1"})).unwrap();
        let cfg = SupabaseRpcConfig::v1(&origin, KEY).with_device_gateway();
        let route = DatastoreRoute::supabase_rpc(cfg.clone(), Some(DEVICE_BEARER.into()))
            .with_proof_signer(signer.clone());
        let conn = Connection::open_with_context(route, Some(ctx)).unwrap();
        let connector = CloudConnector::with_config(cfg)
            .unwrap()
            .with_proof_signer(signer);

        let attempt = OperationAttempt::new("att-158-inaccurate")
            .unwrap()
            .with_scope_ref("repo:o1/r1")
            .unwrap();
        let mut direct_meta = CloudResponseMetadata::default();
        conn.write_with_attempt("INSERT INTO t VALUES (1)", &[], &attempt, &mut direct_meta)
            .unwrap();

        let direct_prov = direct_meta.provenance().unwrap();
        assert_eq!(
            direct_prov.observation_source(),
            ObservationSource::DirectResponse
        );
        assert_eq!(direct_prov.status(), EffectStatus::CommittedObserved);
        assert_eq!(direct_prov.decision(), Some(DecisionOutcome::Allowed));
        assert_eq!(direct_prov.auth_scheme(), Some(AuthScheme::DPoP));
        assert!(direct_prov.origin_bound());

        // Independent backend state remains empty despite protocol-conformant DirectResponse.
        assert!(persisted_rows.lock().unwrap().is_empty());

        // Separate ServerReadback captures NotAttempted while retaining DirectResponse prior claim.
        let mut readback_meta = CloudResponseMetadata::default();
        let obs = connector
            .reconcile_control_attempt(
                "dactyl_ctrl_status",
                &attempt,
                Some(DEVICE_BEARER),
                None,
                ResourceBudget::v1(),
                Some(direct_prov.status()),
                &mut readback_meta,
            )
            .unwrap();
        assert_eq!(
            obs.prior_client_status(),
            Some(EffectStatus::CommittedObserved)
        );
        assert_eq!(
            obs.provenance().observation_source(),
            ObservationSource::ServerReadback
        );
        assert_eq!(obs.provenance().status(), EffectStatus::NotAttempted);
        assert!(persisted_rows.lock().unwrap().is_empty());

        srv.join().unwrap();
    }

    #[test]
    fn valid_correlation_envelope_without_committed_effect_fails_closed() {
        let persisted_rows = Arc::new(Mutex::new(Vec::<String>::new()));

        let (origin, srv) = spawn_scripted_server(4, move |idx, req, orig| match idx {
            // 0: Write HTTP 200 with valid correlation + accepted_not_committed -> Uncertain + Reconcile
            0 => {
                let hdrs = provenance_headers(
                    orig,
                    req,
                    "req-200-accepted-not-committed",
                    "accepted_not_committed",
                    "allowed",
                    "write",
                    Some("accepted_not_committed"),
                    "dpop",
                );
                let body = serde_json::to_vec(&serde_json::json!({
                    "columns": [],
                    "rows": [],
                    "affected_rows": 0,
                    "generated_keys": []
                }))
                .unwrap();
                (200, hdrs, body)
            }
            // 1: Account write HTTP 200 with valid correlation + accepted_not_committed -> Uncertain + Reconcile
            1 => {
                let hdrs = provenance_headers(
                    orig,
                    req,
                    "req-acct-accepted-not-committed",
                    "accepted_not_committed",
                    "allowed",
                    "write",
                    Some("accepted_not_committed"),
                    "bearer",
                );
                let body = serde_json::to_vec(&serde_json::json!({
                    "queued": true
                }))
                .unwrap();
                (200, hdrs, body)
            }
            // 2: Direct HTTP 200 claiming not_attempted on write -> provenance_status_conflict -> Uncertain + Stop
            2 => {
                let hdrs = provenance_headers(
                    orig,
                    req,
                    "req-direct-not-attempted",
                    "not_attempted",
                    "refused",
                    "write",
                    Some("not_attempted"),
                    "dpop",
                );
                let body = serde_json::to_vec(&serde_json::json!({
                    "columns": [],
                    "rows": [],
                    "affected_rows": 0,
                    "generated_keys": []
                }))
                .unwrap();
                (200, hdrs, body)
            }
            // 3: ServerReadback HTTP 200 reporting absent / not_attempted -> validly exposes non-commit
            _ => {
                let hdrs = provenance_headers(
                    orig,
                    req,
                    "req-readback-absent",
                    "absent",
                    "refused",
                    "read",
                    None,
                    "dpop",
                );
                let body = serde_json::to_vec(&serde_json::json!({
                    "found": false
                }))
                .unwrap();
                (200, hdrs, body)
            }
        });

        let signer = Arc::new(CountingDpopSigner::new());
        let ctx =
            StorageContext::new(1, serde_json::json!({"org_id": "o1", "repo_id": "r1"})).unwrap();
        let cfg = SupabaseRpcConfig::v1(&origin, KEY).with_device_gateway();
        let route = DatastoreRoute::supabase_rpc(cfg.clone(), Some(DEVICE_BEARER.into()))
            .with_proof_signer(signer.clone());
        let conn = Connection::open_with_context(route, Some(ctx)).unwrap();
        let connector = CloudConnector::with_config(cfg)
            .unwrap()
            .with_proof_signer(signer);

        // 0: Write accepted_not_committed on HTTP 200 -> Uncertain + Reconcile
        let att0 = OperationAttempt::new("att-158-anc")
            .unwrap()
            .with_scope_ref("repo:o1/r1")
            .unwrap();
        let mut meta0 = CloudResponseMetadata::default();
        let err0 = conn
            .write_with_attempt("INSERT INTO t VALUES (1)", &[], &att0, &mut meta0)
            .unwrap_err();
        assert_eq!(
            err0.adapter_kind(),
            Some(AdapterErrorKind::IdempotencyInProgress)
        );
        assert_eq!(err0.mutation_outcome(), MutationOutcome::Uncertain);
        assert_eq!(
            meta0.provenance().unwrap().status(),
            EffectStatus::Uncertain
        );
        let disp0 = err0.operational_disposition_with_context(
            OperationKind::Write,
            Some(&att0),
            Some(&meta0),
        );
        assert_eq!(disp0.action(), OperationalAction::Reconcile);
        assert_eq!(disp0.request_id(), Some("req-200-accepted-not-committed"));

        // 1: Account mutating accepted_not_committed on HTTP 200 -> Uncertain + Reconcile
        let att1 = OperationAttempt::new("att-158-acct-anc")
            .unwrap()
            .with_scope_ref("repo:o1/r1")
            .unwrap();
        let mut meta1 = CloudResponseMetadata::default();
        let err1 = connector
            .account_request_with_attempt(
                AccountOperation::RepositoryEnroll,
                &serde_json::json!({"version": 1}),
                HUMAN_BEARER,
                None,
                None,
                &att1,
                &mut meta1,
            )
            .unwrap_err();
        assert_eq!(err1.mutation_outcome(), MutationOutcome::Uncertain);
        assert_eq!(
            err1.operational_disposition_with_context(
                OperationKind::Write,
                Some(&att1),
                Some(&meta1)
            )
            .action(),
            OperationalAction::Reconcile
        );

        // 2: Direct HTTP 200 cannot disguise a write as not_attempted
        let att2 = OperationAttempt::new("att-158-direct-na")
            .unwrap()
            .with_scope_ref("repo:o1/r1")
            .unwrap();
        let mut meta2 = CloudResponseMetadata::default();
        let err2 = conn
            .write_with_attempt("INSERT INTO t VALUES (2)", &[], &att2, &mut meta2)
            .unwrap_err();
        assert_eq!(err2.adapter_kind(), Some(AdapterErrorKind::Protocol));
        assert_eq!(err2.adapter_code(), Some("provenance_status_conflict"));
        assert_eq!(err2.mutation_outcome(), MutationOutcome::Uncertain);
        assert_eq!(
            err2.operational_disposition_with_context(
                OperationKind::Write,
                Some(&att2),
                Some(&meta2)
            )
            .action(),
            OperationalAction::Reconcile
        );

        // 3: ServerReadback cleanly reports NotAttempted when absent
        let mut meta3 = CloudResponseMetadata::default();
        let obs3 = connector
            .reconcile_control_attempt(
                "dactyl_ctrl_status",
                &att0,
                Some(DEVICE_BEARER),
                None,
                ResourceBudget::v1(),
                Some(EffectStatus::Uncertain),
                &mut meta3,
            )
            .unwrap();
        assert_eq!(obs3.provenance().status(), EffectStatus::NotAttempted);
        assert_eq!(
            obs3.provenance().observation_source(),
            ObservationSource::ServerReadback
        );
        assert!(persisted_rows.lock().unwrap().is_empty());

        srv.join().unwrap();
    }

    #[test]
    fn missing_provenance_and_stale_mismatched_evidence_never_silently_escalate() {
        let (origin, srv) = spawn_scripted_server(7, move |idx, req, orig| match idx {
            // 0: Write HTTP 200 without provenance -> UnsupportedProvenance (no fabricated CommittedObserved)
            0 => (
                200,
                vec![("Decapod-Cloud-Request-Id".into(), "req-no-prov".into())],
                serde_json::to_vec(&serde_json::json!({
                    "columns": [], "rows": [], "affected_rows": 1, "generated_keys": []
                }))
                .unwrap(),
            ),
            // 1: Post-send 503 with RecoverableWithinAuthority recovery facts, AfterSend phase, NO provenance ->
            //    conservative Uncertain + Reconcile (never auto-retried, never Retry!).
            1 => (
                503,
                vec![("Retry-After".into(), "15".into())],
                serde_json::to_vec(&serde_json::json!({
                    "error": {
                        "code": "storage_unavailable",
                        "recovery": {
                            "version": 1,
                            "action_category": "recoverable_within_authority",
                            "execution_phase": "after_send",
                            "retry_after_seconds": 15
                        }
                    }
                }))
                .unwrap(),
            ),
            // 2: ServerReadback returning mismatched request_digest -> IdempotencyConflict + Stop
            2 => {
                let mut hdrs = provenance_headers(
                    orig,
                    req,
                    "req-stale-dig",
                    "committed_observed",
                    "allowed",
                    "read",
                    None,
                    "dpop",
                );
                for (k, v) in &mut hdrs {
                    if k == "Decapod-Cloud-Request-Digest" {
                        *v = "sha256:stale-different-payload".into();
                    }
                }
                (
                    200,
                    hdrs,
                    serde_json::to_vec(&serde_json::json!({"found": true})).unwrap(),
                )
            }
            // 3: ServerReadback returning mismatched attempt_id -> Protocol + provenance_attempt_mismatch + Stop
            3 => {
                let mut hdrs = provenance_headers(
                    orig,
                    req,
                    "req-stale-att",
                    "committed_observed",
                    "allowed",
                    "read",
                    None,
                    "dpop",
                );
                for (k, v) in &mut hdrs {
                    if k == "Decapod-Cloud-Attempt-Id" {
                        *v = "att-different-stale".into();
                    }
                }
                (
                    200,
                    hdrs,
                    serde_json::to_vec(&serde_json::json!({"found": true})).unwrap(),
                )
            }
            // 4: ServerReadback returning mismatched origin -> provenance_origin_mismatch + Stop
            4 => {
                let hdrs = provenance_headers(
                    "https://stale.attacker.example",
                    req,
                    "req-stale-orig",
                    "committed_observed",
                    "allowed",
                    "read",
                    None,
                    "dpop",
                );
                (
                    200,
                    hdrs,
                    serde_json::to_vec(&serde_json::json!({"found": true})).unwrap(),
                )
            }
            // 5 & 6: Double 401 use_dpop_nonce with stale nonce replay -> fails closed after single bounded retry
            5 => (
                401,
                vec![("DPoP-Nonce".into(), "nonce-fresh-1".into())],
                serde_json::to_vec(&serde_json::json!({"error": {"code": "use_dpop_nonce"}}))
                    .unwrap(),
            ),
            _ => (
                401,
                vec![("DPoP-Nonce".into(), "nonce-stale-2".into())],
                serde_json::to_vec(&serde_json::json!({"error": {"code": "use_dpop_nonce"}}))
                    .unwrap(),
            ),
        });

        let signer = Arc::new(CountingDpopSigner::new());
        let ctx =
            StorageContext::new(1, serde_json::json!({"org_id": "o1", "repo_id": "r1"})).unwrap();
        let cfg = SupabaseRpcConfig::v1(&origin, KEY).with_device_gateway();
        let route = DatastoreRoute::supabase_rpc(cfg, Some(DEVICE_BEARER.into()))
            .with_proof_signer(signer.clone());
        let conn = Connection::open_with_context(route, Some(ctx)).unwrap();
        let connector =
            CloudConnector::with_config(SupabaseRpcConfig::v1(&origin, KEY).with_device_gateway())
                .unwrap()
                .with_proof_signer(signer.clone());

        // 0: Missing provenance yields UnsupportedProvenance (contract_version = None)
        let att = OperationAttempt::new("att-158-missing")
            .unwrap()
            .with_request_digest("sha256:expected-payload")
            .unwrap()
            .with_scope_ref("repo:o1/r1")
            .unwrap();
        let mut m0 = CloudResponseMetadata::default();
        conn.write_with_attempt("INSERT INTO t VALUES (1)", &[], &att, &mut m0)
            .unwrap();
        let p0 = m0.provenance().unwrap();
        assert_eq!(p0.status(), EffectStatus::UnsupportedProvenance);
        assert_eq!(p0.contract_version(), None);
        assert_eq!(p0.observation_source(), ObservationSource::DirectResponse);

        // 1: AfterSend 503 without provenance never escalates to Retry on write
        let mut m1 = CloudResponseMetadata::default();
        let e1 = conn
            .write_with_attempt("INSERT INTO t VALUES (2)", &[], &att, &mut m1)
            .unwrap_err();
        assert_eq!(e1.mutation_outcome(), MutationOutcome::Uncertain);
        let d1 =
            e1.operational_disposition_with_context(OperationKind::Write, Some(&att), Some(&m1));
        assert_eq!(d1.action(), OperationalAction::Reconcile);

        // 2: Mismatched digest on ServerReadback -> IdempotencyConflict + Stop
        let mut m2 = CloudResponseMetadata::default();
        let e2 = connector
            .reconcile_control_attempt(
                "dactyl_ctrl_status",
                &att,
                Some(DEVICE_BEARER),
                None,
                ResourceBudget::v1(),
                Some(EffectStatus::Uncertain),
                &mut m2,
            )
            .unwrap_err();
        assert_eq!(
            e2.adapter_kind(),
            Some(AdapterErrorKind::IdempotencyConflict)
        );
        assert_eq!(e2.adapter_code(), Some("idempotency_conflict"));
        assert_eq!(
            e2.operational_disposition_with_context(OperationKind::Read, Some(&att), Some(&m2))
                .action(),
            OperationalAction::Stop
        );

        // 3: Mismatched attempt_id on ServerReadback -> Protocol + Stop
        let mut m3 = CloudResponseMetadata::default();
        let e3 = connector
            .reconcile_control_attempt(
                "dactyl_ctrl_status",
                &att,
                Some(DEVICE_BEARER),
                None,
                ResourceBudget::v1(),
                Some(EffectStatus::Uncertain),
                &mut m3,
            )
            .unwrap_err();
        assert_eq!(e3.adapter_kind(), Some(AdapterErrorKind::Protocol));
        assert_eq!(e3.adapter_code(), Some("provenance_attempt_mismatch"));
        assert_eq!(
            e3.operational_disposition_with_context(OperationKind::Read, Some(&att), Some(&m3))
                .action(),
            OperationalAction::Stop
        );

        // 4: Mismatched origin on ServerReadback -> provenance_origin_mismatch + origin_bound = false
        let mut m4 = CloudResponseMetadata::default();
        let e4 = connector
            .reconcile_control_attempt(
                "dactyl_ctrl_status",
                &att,
                Some(DEVICE_BEARER),
                None,
                ResourceBudget::v1(),
                Some(EffectStatus::Uncertain),
                &mut m4,
            )
            .unwrap_err();
        assert_eq!(e4.adapter_code(), Some("provenance_origin_mismatch"));
        assert!(!m4.provenance().unwrap().origin_bound());
        assert_eq!(
            e4.operational_disposition_with_context(OperationKind::Read, Some(&att), Some(&m4))
                .action(),
            OperationalAction::Stop
        );

        // 5 & 6: Double 401 use_dpop_nonce executes at most one bounded retry, observes both nonces,
        // and stops without falling back to Bearer.
        let mut m5 = CloudResponseMetadata::default();
        let e5 = conn
            .write_with_attempt("INSERT INTO t VALUES (3)", &[], &att, &mut m5)
            .unwrap_err();
        assert_eq!(e5.adapter_kind(), Some(AdapterErrorKind::Authentication));
        assert_eq!(e5.adapter_code(), Some("use_dpop_nonce"));
        assert_eq!(
            e5.operational_disposition_with_context(OperationKind::Write, Some(&att), Some(&m5))
                .action(),
            OperationalAction::Stop
        );
        assert_eq!(
            signer.nonces_seen.lock().unwrap().as_slice(),
            ["nonce-fresh-1", "nonce-stale-2"]
        );

        srv.join().unwrap();
    }
}
