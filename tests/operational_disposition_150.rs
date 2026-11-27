//! Conformance tests for Dactyl #150: unambiguous, machine-readable
//! operational disposition (`OperationalAction` / `OperationalDisposition`)
//! across local SQLite and Decapod Cloud paths without parsing English
//! error strings.

use dactyl_db::{
    AdapterErrorKind, DactylError, FailureOrigin, MutationOutcome, OperationAttempt, OperationKind,
    OperationalAction,
};

#[test]
fn non_adapter_errors_stop_with_not_attempted_on_writes_and_preserve_null_metadata() {
    for err in [
        DactylError::Config("bad config".into()),
        DactylError::UnsupportedOperation("nope".into()),
        DactylError::ColumnNotFound("col".into()),
        DactylError::Conversion("bad int".into()),
    ] {
        let write_disp = err.operational_disposition(OperationKind::Write);
        assert_eq!(write_disp.action(), OperationalAction::Stop);
        assert_eq!(write_disp.error_kind(), None);
        assert_eq!(write_disp.code(), None);
        assert_eq!(write_disp.failure_origin(), FailureOrigin::Other);
        assert_eq!(
            write_disp.mutation_outcome(),
            Some(MutationOutcome::NotAttempted)
        );
        assert_eq!(write_disp.effect_status(), None);
        assert_eq!(write_disp.observation_source(), None);
        assert_eq!(write_disp.request_id(), None);
        assert_eq!(write_disp.attempt_id(), None);
        assert_eq!(write_disp.request_digest(), None);
        assert_eq!(write_disp.retry_after(), None);
        assert_eq!(write_disp.retry_after_seconds(), None);

        let read_disp = err.operational_disposition(OperationKind::Read);
        assert_eq!(read_disp.action(), OperationalAction::Stop);
        assert_eq!(read_disp.mutation_outcome(), None);
    }
}

#[test]
fn retry_after_parsing_is_strict_and_redacted_in_debug() {
    let err = DactylError::Adapter {
        kind: AdapterErrorKind::RateLimited,
        code: Some("rate_limited".into()),
        message: "opaque".into(),
    };
    let disp = err.operational_disposition(OperationKind::Read);
    assert_eq!(disp.action(), OperationalAction::Retry);
    assert_eq!(disp.retry_after(), None);
    assert_eq!(disp.retry_after_seconds(), None);

    let attempt = OperationAttempt::new("att-secret-id-150")
        .unwrap()
        .with_request_digest("sha256:digest150")
        .unwrap();
    let disp_with_att =
        err.operational_disposition_with_context(OperationKind::Read, Some(&attempt), None);
    assert_eq!(disp_with_att.attempt_id(), Some("att-secret-id-150"));
    assert_eq!(disp_with_att.request_digest(), Some("sha256:digest150"));
    let dbg = format!("{disp_with_att:?}");
    assert!(!dbg.contains("att-secret-id-150"));
    assert!(!dbg.contains("sha256:digest150"));
}

#[cfg(feature = "sqlite")]
mod sqlite_disposition {
    use std::time::Duration;

    use dactyl_db::{
        AccessMode, AdapterErrorKind, CloudResponseMetadata, Connection, DatastoreRoute,
        EffectStatus, FailureOrigin, LogicalConnector, MutationOutcome, ObservationSource,
        OpenOptions, Operation, OperationAttempt, OperationKind, OperationalAction, Parameter,
        ResourceBudget,
    };
    use tempfile::NamedTempFile;

    #[test]
    fn sqlite_preflight_refusals_and_rollback_match_actual_storage_state() {
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_string_lossy().into_owned();
        let rw = Connection::open(DatastoreRoute::sqlite(&path)).unwrap();
        rw.write(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT UNIQUE NOT NULL)",
            &[],
        )
        .unwrap();
        rw.write("INSERT INTO items (id, label) VALUES (1, 'seed')", &[])
            .unwrap();

        // 1. Read-only connection write refusal -> NotAttempted + Stop.
        let ro = Connection::open_with_options(
            DatastoreRoute::sqlite(&path),
            OpenOptions {
                access_mode: AccessMode::ReadOnly,
                ..OpenOptions::default()
            },
        )
        .unwrap();
        let ro_err = ro
            .write("INSERT INTO items (id, label) VALUES (2, 'ro')", &[])
            .unwrap_err();
        let ro_disp = ro_err.operational_disposition(OperationKind::Write);
        assert_eq!(ro_disp.action(), OperationalAction::Stop);
        assert_eq!(ro_disp.error_kind(), Some(AdapterErrorKind::ReadOnly));
        assert_eq!(ro_disp.code(), Some("read_only"));
        assert_eq!(
            ro_disp.mutation_outcome(),
            Some(MutationOutcome::NotAttempted)
        );

        // 2. Non-finite Parameter::Real preflight refusal -> NotAttempted + Stop.
        let nan_err = rw
            .write(
                "INSERT INTO items (id, label) VALUES (?1, ?2)",
                &[Parameter::Real(f64::NAN), Parameter::Text("nan".into())],
            )
            .unwrap_err();
        let nan_disp = nan_err.operational_disposition(OperationKind::Write);
        assert_eq!(nan_disp.action(), OperationalAction::Stop);
        assert_eq!(nan_disp.error_kind(), Some(AdapterErrorKind::Value));
        assert_eq!(nan_disp.code(), Some("unsupported_value"));
        assert_eq!(
            nan_disp.mutation_outcome(),
            Some(MutationOutcome::NotAttempted)
        );

        // 3. Local ResourceBudget preflight refusal -> LocalBudget + NotAttempted + Stop.
        let tiny_budget = Connection::open_with_budget(
            DatastoreRoute::sqlite(&path),
            OpenOptions::default(),
            None,
            ResourceBudget::v1().with_max_sql_bytes(8),
        )
        .unwrap();
        let budget_err = tiny_budget
            .write("INSERT INTO items (id, label) VALUES (3, 'big')", &[])
            .unwrap_err();
        let budget_disp = budget_err.operational_disposition(OperationKind::Write);
        assert_eq!(budget_disp.action(), OperationalAction::Stop);
        assert_eq!(budget_disp.failure_origin(), FailureOrigin::LocalBudget);
        assert_eq!(budget_disp.code(), Some("sql_budget_exceeded"));
        assert_eq!(
            budget_disp.mutation_outcome(),
            Some(MutationOutcome::NotAttempted)
        );

        // 4. Atomic batch rollback on constraint violation -> RolledBack + Stop, no partial write.
        let batch_err = rw
            .atomic(&[
                Operation::write(
                    "INSERT INTO items (id, label) VALUES (10, 'partial')",
                    vec![],
                ),
                Operation::write("INSERT INTO items (id, label) VALUES (11, 'seed')", vec![]),
            ])
            .unwrap_err();
        let batch_disp = batch_err.operational_disposition(OperationKind::Write);
        assert_eq!(batch_disp.action(), OperationalAction::Stop);
        assert_eq!(batch_disp.error_kind(), Some(AdapterErrorKind::Constraint));
        assert_eq!(batch_disp.code(), Some("unique_violation"));
        assert_eq!(
            batch_disp.mutation_outcome(),
            Some(MutationOutcome::RolledBack)
        );
        let partial_count = rw
            .read("SELECT COUNT(*) AS n FROM items WHERE id = 10", &[])
            .unwrap()
            .as_slice()[0]
            .get_int("n")
            .unwrap();
        assert_eq!(partial_count, 0);

        // 5. SQLite contention when another connection holds an EXCLUSIVE lock:
        //    Read -> Retry; atomic BEGIN IMMEDIATE pre-dispatch Busy -> NotAttempted + Retry.
        rw.write("PRAGMA locking_mode = EXCLUSIVE", &[]).unwrap();
        rw.write("INSERT INTO items (id, label) VALUES (19, 'locked')", &[])
            .unwrap();
        drop(rw);
        let exclusive = Connection::open(DatastoreRoute::sqlite(&path)).unwrap();
        exclusive
            .write("PRAGMA locking_mode = EXCLUSIVE", &[])
            .unwrap();
        exclusive
            .write(
                "INSERT INTO items (id, label) VALUES (20, 'hold-lock')",
                &[],
            )
            .unwrap();
        // Open a second connection via a copy or wait—SqliteAdapter tracks in-process paths?
        // Actually SqliteAdapter permits multiple connections to the same path (only recovery checks has_other_connection).
        let contender = Connection::open_with_options(
            DatastoreRoute::sqlite(&path),
            OpenOptions {
                access_mode: AccessMode::ReadWrite,
                lock_timeout: Duration::from_millis(5),
            },
        )
        .unwrap();
        let busy_read = contender
            .read("SELECT COUNT(*) FROM items", &[])
            .unwrap_err();
        let busy_read_disp = busy_read.operational_disposition(OperationKind::Read);
        assert_eq!(busy_read_disp.action(), OperationalAction::Retry);
        assert_eq!(busy_read_disp.error_kind(), Some(AdapterErrorKind::Busy));
        assert_eq!(busy_read_disp.mutation_outcome(), None);

        let busy_atomic = contender
            .atomic(&[Operation::write(
                "INSERT INTO items (id, label) VALUES (21, 'blocked')",
                vec![],
            )])
            .unwrap_err();
        let busy_atomic_disp = busy_atomic.operational_disposition(OperationKind::Write);
        assert_eq!(busy_atomic_disp.action(), OperationalAction::Retry);
        assert_eq!(busy_atomic_disp.error_kind(), Some(AdapterErrorKind::Busy));
        assert_eq!(
            busy_atomic_disp.mutation_outcome(),
            Some(MutationOutcome::NotAttempted)
        );
        drop(contender);
        exclusive
            .write("PRAGMA locking_mode = NORMAL", &[])
            .unwrap();
        exclusive.read("SELECT COUNT(*) FROM items", &[]).unwrap();
        let rw = exclusive;

        // 6. Explicit unsupported capability on SQLite -> NotAttempted + LocalPreflight + Stop.
        let attempt = OperationAttempt::new("sqlite-att-150").unwrap();
        let mut meta = CloudResponseMetadata::default();
        let cap_err = rw
            .write_with_attempt(
                "INSERT INTO items (id, label) VALUES (12, 'x')",
                &[],
                &attempt,
                &mut meta,
            )
            .unwrap_err();
        let cap_disp = cap_err.operational_disposition_with_context(
            OperationKind::Write,
            Some(&attempt),
            Some(&meta),
        );
        assert_eq!(cap_disp.action(), OperationalAction::Stop);
        assert_eq!(cap_disp.error_kind(), Some(AdapterErrorKind::Capability));
        assert_eq!(cap_disp.code(), Some("cloud_connector_required"));
        assert_eq!(
            cap_disp.mutation_outcome(),
            Some(MutationOutcome::NotAttempted)
        );
        assert_eq!(cap_disp.effect_status(), Some(EffectStatus::NotAttempted));
        assert_eq!(
            cap_disp.observation_source(),
            Some(ObservationSource::LocalPreflight)
        );
        assert_eq!(cap_disp.attempt_id(), Some("sqlite-att-150"));

        let local_conn = LogicalConnector::local(&path).unwrap();
        let rec_err = local_conn
            .reconcile_operation(&attempt, "unused", None, None, &mut meta)
            .unwrap_err();
        assert_eq!(
            rec_err
                .operational_disposition_with_context(
                    OperationKind::Read,
                    Some(&attempt),
                    Some(&meta)
                )
                .action(),
            OperationalAction::Stop
        );
    }
}

#[cfg(feature = "supabase")]
mod cloud_and_consumer_disposition {
    use std::collections::HashMap;
    use std::io::{Read, Write};
    use std::net::{Shutdown, TcpListener, TcpStream};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::Duration;

    #[cfg(feature = "sqlite")]
    use dactyl_db::AccessMode;
    use dactyl_db::{
        AccountOperation, AdapterErrorKind, CloudConnector, CloudResponseMetadata, Connection,
        DatastoreRoute, EffectStatus, ExecutionPhase, FailureOrigin, MutationOutcome,
        ObservationSource, OpenOptions, Operation, OperationAttempt, OperationKind,
        OperationalAction, OperationalDisposition, RecoveryActionCategory, RequestProof,
        RequestProofInput, RequestProofSigner, ResourceBudget, StorageContext, SupabaseRpcConfig,
        PROVENANCE_CONTRACT_VERSION,
    };
    use serde_json::json;

    const KEY: &str = "sb_publishable_disposition_150";
    const BEARER: &str = "user-bearer-150";
    const DEVICE_BEARER: &str = "dpc_at_device_150";

    struct DummySigner;
    impl RequestProofSigner for DummySigner {
        fn sign_request(
            &self,
            input: &RequestProofInput<'_>,
        ) -> Result<RequestProof, dactyl_db::DactylError> {
            let _ = input;
            RequestProof::dpop("dpop.proof.150")
        }
    }

    fn unused_origin() -> (String, TcpListener) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        (origin, listener)
    }

    fn read_http(stream: &mut TcpStream) -> Option<(String, HashMap<String, String>, Vec<u8>)> {
        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        let head_end = loop {
            if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                break pos + 4;
            }
            match stream.read(&mut chunk) {
                Ok(0) | Err(_) => return None,
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
            }
        };
        let head = String::from_utf8_lossy(&buf[..head_end]);
        let mut lines = head.lines();
        let req_line = lines.next().unwrap_or("");
        let path = req_line.split_whitespace().nth(1).unwrap_or("").to_owned();
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
            match stream.read(&mut chunk) {
                Ok(0) | Err(_) => break,
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
            }
        }
        Some((path, headers, buf[head_end..].to_vec()))
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
    fn prov_headers(
        origin: &str,
        attempt_id: Option<&str>,
        request_digest: Option<&str>,
        request_id: &str,
        effect_status: &str,
        decision: &str,
        operation_kind: &str,
        reason_code: Option<&str>,
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
        if let Some(aid) = attempt_id {
            hdrs.push(("Decapod-Cloud-Attempt-Id".into(), aid.to_owned()));
        }
        if let Some(dig) = request_digest {
            hdrs.push(("Decapod-Cloud-Request-Digest".into(), dig.to_owned()));
        }
        if let Some(code) = reason_code {
            hdrs.push(("Decapod-Cloud-Reason-Code".into(), code.to_owned()));
        }
        hdrs
    }

    fn context() -> StorageContext {
        StorageContext::new(1, json!({"org_id": "org-150", "repo_id": "dactyl"})).unwrap()
    }

    fn open_gateway_conn(origin: &str, budget: ResourceBudget) -> Connection {
        let route = DatastoreRoute::supabase_rpc(
            SupabaseRpcConfig::v1(origin, KEY).with_device_gateway(),
            Some(DEVICE_BEARER.to_owned()),
        )
        .with_proof_signer(Arc::new(DummySigner));
        Connection::open_with_budget(route, OpenOptions::default(), Some(context()), budget)
            .unwrap()
    }

    fn connector_with_budget(origin: &str, budget: ResourceBudget) -> CloudConnector {
        CloudConnector::with_config(SupabaseRpcConfig::v1(origin, KEY).with_device_gateway())
            .unwrap()
            .with_http_budget(budget)
            .unwrap()
            .with_proof_signer(Arc::new(DummySigner))
    }

    #[test]
    fn cloud_disposition_matrix_covers_all_required_fault_classes() {
        // Pre-dispatch refusal (dpc_at_ on human-required account op) -> NotAttempted + Stop.
        let (unused_origin, unused_listener) = unused_origin();
        unused_listener.set_nonblocking(true).unwrap();
        let conn_unused = connector_with_budget(&unused_origin, ResourceBudget::v1());
        let att_pre = OperationAttempt::new("att-pre-150").unwrap();
        let mut meta_pre = CloudResponseMetadata::default();
        let pre_err = conn_unused
            .account_request_with_attempt(
                AccountOperation::RepositoryEnroll,
                &json!({"version": 1}),
                DEVICE_BEARER,
                None,
                None,
                &att_pre,
                &mut meta_pre,
            )
            .unwrap_err();
        let pre_disp = pre_err.operational_disposition_with_context(
            OperationKind::Write,
            Some(&att_pre),
            Some(&meta_pre),
        );
        assert_eq!(pre_disp.action(), OperationalAction::Stop);
        assert_eq!(
            pre_disp.error_kind(),
            Some(AdapterErrorKind::Authentication)
        );
        assert_eq!(pre_disp.code(), Some("authentication_required"));
        assert_eq!(
            pre_disp.mutation_outcome(),
            Some(MutationOutcome::NotAttempted)
        );
        assert_eq!(pre_disp.effect_status(), Some(EffectStatus::NotAttempted));
        assert_eq!(
            pre_disp.observation_source(),
            Some(ObservationSource::LocalPreflight)
        );
        assert_eq!(pre_disp.attempt_id(), Some("att-pre-150"));
        assert!(matches!(
            unused_listener.accept(),
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock
        ));

        // Missing DPoP signer on gateway -> NotAttempted + device_proof_required + Stop.
        let no_signer_route = DatastoreRoute::supabase_rpc(
            SupabaseRpcConfig::v1(&unused_origin, KEY).with_device_gateway(),
            Some(DEVICE_BEARER.to_owned()),
        );
        let no_signer_conn =
            Connection::open_with_context(no_signer_route, Some(context())).unwrap();
        let mut meta_nosig = CloudResponseMetadata::default();
        let nosig_err = no_signer_conn
            .write_with_attempt("INSERT INTO t VALUES (1)", &[], &att_pre, &mut meta_nosig)
            .unwrap_err();
        let nosig_disp = nosig_err.operational_disposition_with_context(
            OperationKind::Write,
            Some(&att_pre),
            Some(&meta_nosig),
        );
        assert_eq!(nosig_disp.action(), OperationalAction::Stop);
        assert_eq!(nosig_disp.code(), Some("device_proof_required"));
        assert_eq!(
            nosig_disp.mutation_outcome(),
            Some(MutationOutcome::NotAttempted)
        );

        // Now exercise live loopback steps in order:
        // 0) 403 entitlement_required with EffectStatus::Refused -> NotAttempted + Stop
        // 1) 429 rate_limited (Retry-After: 30) with EffectStatus::Refused -> NotAttempted + Retry
        // 2) 429 quota_exceeded (Retry-After: 45) with EffectStatus::Refused -> NotAttempted + Stop
        // 3) 503 storage_unavailable (Retry-After: 12) with EffectStatus::Refused on Write -> NotAttempted + Retry
        // 4) 503 storage_unavailable (Retry-After: 12) WITHOUT provenance on Write -> Uncertain + Reconcile (even though is_retryable() is true!)
        // 5) 409 version_conflict with EffectStatus::RolledBack -> RolledBack + InspectAndDecide
        // 6) Post-commit lost acknowledgement (CommitThenDrop) -> Uncertain + Reconcile (even though is_retryable() is true!)
        // 7) Malformed 200 JSON on Write -> Uncertain + MalformedResponse + Reconcile
        // 8) Malformed 200 JSON on Read -> MalformedResponse + Stop
        // 9) Request timeout on Write -> Timeout + Uncertain + Reconcile (NEVER Retry!)
        // 10) Request timeout on Read -> Timeout + Retry
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let expected_requests = 11usize;
        let orig_srv = origin.clone();
        let srv_handle = thread::spawn(move || {
            for idx in 0..expected_requests {
                let (mut stream, _) = listener.accept().unwrap();
                let Some((_, headers, _)) = read_http(&mut stream) else {
                    continue;
                };
                let aid = headers.get("decapod-cloud-attempt-id").map(String::as_str);
                let dig = headers
                    .get("decapod-cloud-request-digest")
                    .map(String::as_str);
                match idx {
                    0 => {
                        let mut hdrs = prov_headers(
                            &orig_srv,
                            aid,
                            dig,
                            "req-0",
                            "refused",
                            "refused",
                            "write",
                            Some("entitlement_required"),
                            "bearer",
                        );
                        hdrs.push(("Decapod-Cloud-Stage".into(), "authz".into()));
                        let body =
                            serde_json::to_vec(&json!({"error": {"code": "entitlement_required"}}))
                                .unwrap();
                        send_http(&mut stream, 403, &hdrs, &body);
                    }
                    1 => {
                        let mut hdrs = prov_headers(
                            &orig_srv,
                            aid,
                            dig,
                            "req-1",
                            "refused",
                            "refused",
                            "write",
                            Some("rate_limited"),
                            "bearer",
                        );
                        hdrs.push(("Retry-After".into(), "30".into()));
                        let body = serde_json::to_vec(&json!({"error": {"code": "rate_limited"}}))
                            .unwrap();
                        send_http(&mut stream, 429, &hdrs, &body);
                    }
                    2 => {
                        let mut hdrs = prov_headers(
                            &orig_srv,
                            aid,
                            dig,
                            "req-2",
                            "refused",
                            "refused",
                            "write",
                            Some("quota_exceeded"),
                            "bearer",
                        );
                        hdrs.push(("Retry-After".into(), "45".into()));
                        let body =
                            serde_json::to_vec(&json!({"error": {"code": "quota_exceeded"}}))
                                .unwrap();
                        send_http(&mut stream, 429, &hdrs, &body);
                    }
                    3 => {
                        let mut hdrs = prov_headers(
                            &orig_srv,
                            aid,
                            dig,
                            "req-3",
                            "refused",
                            "refused",
                            "write",
                            Some("storage_unavailable"),
                            "dpop",
                        );
                        hdrs.push(("Retry-After".into(), "12".into()));
                        let body =
                            serde_json::to_vec(&json!({"error": {"code": "storage_unavailable"}}))
                                .unwrap();
                        send_http(&mut stream, 503, &hdrs, &body);
                    }
                    4 => {
                        let hdrs = vec![("Retry-After".into(), "12".into())];
                        let body =
                            serde_json::to_vec(&json!({"error": {"code": "storage_unavailable"}}))
                                .unwrap();
                        send_http(&mut stream, 503, &hdrs, &body);
                    }
                    5 => {
                        let hdrs = prov_headers(
                            &orig_srv,
                            aid,
                            dig,
                            "req-5",
                            "rolled_back",
                            "allowed",
                            "write",
                            Some("version_conflict"),
                            "dpop",
                        );
                        let body =
                            serde_json::to_vec(&json!({"error": {"code": "version_conflict"}}))
                                .unwrap();
                        send_http(&mut stream, 409, &hdrs, &body);
                    }
                    6 => {
                        let _ = stream.shutdown(Shutdown::Both);
                        drop(stream);
                    }
                    7 => {
                        send_http(&mut stream, 200, &[], b"{not-json");
                    }
                    8 => {
                        send_http(&mut stream, 200, &[], b"{not-json");
                    }
                    9 => {
                        thread::sleep(Duration::from_millis(250));
                        send_http(&mut stream, 200, &[], b"{}");
                    }
                    10 => {
                        thread::sleep(Duration::from_millis(250));
                        send_http(&mut stream, 200, &[], b"{}");
                    }
                    _ => unreachable!(),
                }
            }
        });

        let cloud = connector_with_budget(&origin, ResourceBudget::v1());
        let db = open_gateway_conn(&origin, ResourceBudget::v1());

        // 0) 403 entitlement_required (Refused) -> Stop + NotAttempted + correlation preserved.
        let att0 = OperationAttempt::new("att-0").unwrap();
        let mut m0 = CloudResponseMetadata::default();
        let e0 = cloud
            .account_request_with_attempt(
                AccountOperation::RepositoryEnroll,
                &json!({"version": 1}),
                BEARER,
                None,
                None,
                &att0,
                &mut m0,
            )
            .unwrap_err();
        let d0 =
            e0.operational_disposition_with_context(OperationKind::Write, Some(&att0), Some(&m0));
        assert_eq!(d0.action(), OperationalAction::Stop);
        assert_eq!(d0.code(), Some("entitlement_required"));
        assert_eq!(d0.failure_origin(), FailureOrigin::RemotePolicy);
        assert_eq!(d0.mutation_outcome(), Some(MutationOutcome::NotAttempted));
        assert_eq!(d0.effect_status(), Some(EffectStatus::Refused));
        assert_eq!(d0.request_id(), Some("req-0"));
        assert_eq!(d0.attempt_id(), Some("att-0"));
        assert_eq!(d0.retry_after(), None);

        // 1) 429 rate_limited (Refused + Retry-After: 30) -> Retry + NotAttempted.
        let att1 = OperationAttempt::new("att-1").unwrap();
        let mut m1 = CloudResponseMetadata::default();
        let e1 = cloud
            .account_request_with_attempt(
                AccountOperation::Manage,
                &json!({"version": 1}),
                BEARER,
                None,
                None,
                &att1,
                &mut m1,
            )
            .unwrap_err();
        let d1 =
            e1.operational_disposition_with_context(OperationKind::Write, Some(&att1), Some(&m1));
        assert_eq!(d1.action(), OperationalAction::Retry);
        assert_eq!(d1.code(), Some("rate_limited"));
        assert_eq!(d1.mutation_outcome(), Some(MutationOutcome::NotAttempted));
        assert_eq!(d1.retry_after(), Some("30"));
        assert_eq!(d1.retry_after_seconds(), Some(30));

        // 2) 429 quota_exceeded (Refused + Retry-After: 45) -> Stop (never Retry on Quota!).
        let att2 = OperationAttempt::new("att-2").unwrap();
        let mut m2 = CloudResponseMetadata::default();
        let e2 = cloud
            .account_request_with_attempt(
                AccountOperation::BillingOveragePolicy,
                &json!({"version": 1}),
                BEARER,
                None,
                None,
                &att2,
                &mut m2,
            )
            .unwrap_err();
        let d2 =
            e2.operational_disposition_with_context(OperationKind::Write, Some(&att2), Some(&m2));
        assert_eq!(d2.action(), OperationalAction::Stop);
        assert_eq!(d2.error_kind(), Some(AdapterErrorKind::Quota));
        assert_eq!(d2.code(), Some("quota_exceeded"));
        assert_eq!(d2.retry_after(), Some("45"));
        assert_eq!(d2.retry_after_seconds(), Some(45));

        // 3) 503 storage_unavailable (Refused + Retry-After: 12) on Write -> Retry + NotAttempted.
        let att3 = OperationAttempt::new("att-3").unwrap();
        let mut m3 = CloudResponseMetadata::default();
        let e3 = db
            .write_with_attempt("INSERT INTO t VALUES (3)", &[], &att3, &mut m3)
            .unwrap_err();
        let d3 =
            e3.operational_disposition_with_context(OperationKind::Write, Some(&att3), Some(&m3));
        assert_eq!(d3.action(), OperationalAction::Retry);
        assert_eq!(d3.code(), Some("storage_unavailable"));
        assert_eq!(d3.mutation_outcome(), Some(MutationOutcome::NotAttempted));
        assert_eq!(d3.retry_after(), Some("12"));
        assert_eq!(d3.retry_after_seconds(), Some(12));

        // 4) 503 storage_unavailable WITHOUT provenance on Write -> Reconcile (even though is_retryable() == true!).
        let att4 = OperationAttempt::new("att-4").unwrap();
        let mut m4 = CloudResponseMetadata::default();
        let e4 = db
            .write_with_attempt("INSERT INTO t VALUES (4)", &[], &att4, &mut m4)
            .unwrap_err();
        assert!(e4.is_retryable());
        let d4 =
            e4.operational_disposition_with_context(OperationKind::Write, Some(&att4), Some(&m4));
        assert_eq!(d4.action(), OperationalAction::Reconcile);
        assert_eq!(d4.mutation_outcome(), Some(MutationOutcome::Uncertain));
        assert_eq!(d4.retry_after(), Some("12"));

        // 5) 409 version_conflict (RolledBack) -> InspectAndDecide.
        let att5 = OperationAttempt::new("att-5").unwrap();
        let mut m5 = CloudResponseMetadata::default();
        let e5 = db
            .write_with_attempt("UPDATE t SET v = 2 WHERE v = 1", &[], &att5, &mut m5)
            .unwrap_err();
        let d5 =
            e5.operational_disposition_with_context(OperationKind::Write, Some(&att5), Some(&m5));
        assert_eq!(d5.action(), OperationalAction::InspectAndDecide);
        assert_eq!(d5.error_kind(), Some(AdapterErrorKind::VersionConflict));
        assert_eq!(d5.code(), Some("version_conflict"));
        assert_eq!(d5.mutation_outcome(), Some(MutationOutcome::RolledBack));
        assert_eq!(d5.request_id(), Some("req-5"));
        assert_eq!(d5.attempt_id(), Some("att-5"));

        // 6) Post-commit lost ack (socket drop) -> Uncertain + Reconcile (never Retry!).
        let att6 = OperationAttempt::new("att-6")
            .unwrap()
            .with_request_digest("sha256:write6")
            .unwrap();
        let mut m6 = CloudResponseMetadata::default();
        let e6 = db
            .write_with_attempt("INSERT INTO t VALUES (6)", &[], &att6, &mut m6)
            .unwrap_err();
        assert_eq!(e6.adapter_kind(), Some(AdapterErrorKind::Transport));
        let d6 =
            e6.operational_disposition_with_context(OperationKind::Write, Some(&att6), Some(&m6));
        assert_eq!(d6.action(), OperationalAction::Reconcile);
        assert_eq!(d6.mutation_outcome(), Some(MutationOutcome::Uncertain));
        assert_eq!(d6.attempt_id(), Some("att-6"));
        assert_eq!(d6.request_digest(), Some("sha256:write6"));
        assert_eq!(d6.retry_after(), None);

        // 7) Malformed 200 JSON on Write -> Reconcile + MalformedResponse + Uncertain.
        let att7 = OperationAttempt::new("att-7").unwrap();
        let mut m7 = CloudResponseMetadata::default();
        let e7 = db
            .write_with_attempt("INSERT INTO t VALUES (7)", &[], &att7, &mut m7)
            .unwrap_err();
        let d7 =
            e7.operational_disposition_with_context(OperationKind::Write, Some(&att7), Some(&m7));
        assert_eq!(d7.action(), OperationalAction::Reconcile);
        assert_eq!(d7.failure_origin(), FailureOrigin::MalformedResponse);
        assert_eq!(d7.mutation_outcome(), Some(MutationOutcome::Uncertain));

        // 8) Malformed 200 JSON on Read -> Stop + MalformedResponse + mutation_outcome None.
        let e8 = db.read("SELECT 1", &[]).unwrap_err();
        let d8 = e8.operational_disposition(OperationKind::Read);
        assert_eq!(d8.action(), OperationalAction::Stop);
        assert_eq!(d8.failure_origin(), FailureOrigin::MalformedResponse);
        assert_eq!(d8.mutation_outcome(), None);

        // 9) Post-send timeout on Write -> Reconcile (even though is_retryable() is true!).
        let short_budget = ResourceBudget::v1().with_request_timeout(Duration::from_millis(60));
        let db_short = open_gateway_conn(&origin, short_budget);
        let att9 = OperationAttempt::new("att-9").unwrap();
        let mut m9 = CloudResponseMetadata::default();
        let e9 = db_short
            .write_with_attempt("INSERT INTO t VALUES (9)", &[], &att9, &mut m9)
            .unwrap_err();
        assert_eq!(e9.adapter_kind(), Some(AdapterErrorKind::Timeout));
        assert!(e9.is_retryable());
        let d9 =
            e9.operational_disposition_with_context(OperationKind::Write, Some(&att9), Some(&m9));
        assert_eq!(d9.action(), OperationalAction::Reconcile);
        assert_eq!(d9.mutation_outcome(), Some(MutationOutcome::Uncertain));

        // 10) Post-send timeout on Read -> Retry.
        let e10 = db_short.read("SELECT 10", &[]).unwrap_err();
        assert_eq!(e10.adapter_kind(), Some(AdapterErrorKind::Timeout));
        let d10 = e10.operational_disposition(OperationKind::Read);
        assert_eq!(d10.action(), OperationalAction::Retry);
        assert_eq!(d10.mutation_outcome(), None);

        srv_handle.join().unwrap();
    }

    #[derive(Debug, PartialEq, Eq)]
    enum ConsumerDecision {
        RetrySameCall { wait_secs: Option<u64> },
        InspectStateThenDecide,
        ReconcileByAttemptId(String),
        HaltWithErrorCode(Option<String>),
    }

    /// Example Decapod consumer decision function that relies exclusively on
    /// `OperationalDisposition` and NEVER inspects displayed error strings.
    fn decapod_recovery_decision(disposition: &OperationalDisposition) -> ConsumerDecision {
        match disposition.action() {
            OperationalAction::Retry => ConsumerDecision::RetrySameCall {
                wait_secs: disposition.retry_after_seconds(),
            },
            OperationalAction::InspectAndDecide => ConsumerDecision::InspectStateThenDecide,
            OperationalAction::Reconcile => match disposition.attempt_id() {
                Some(id) => ConsumerDecision::ReconcileByAttemptId(id.to_owned()),
                None => ConsumerDecision::InspectStateThenDecide,
            },
            OperationalAction::Stop => {
                ConsumerDecision::HaltWithErrorCode(disposition.code().map(str::to_owned))
            }
        }
    }

    #[test]
    fn decapod_consumer_recovers_from_lost_ack_without_duplicate_write_or_string_parsing() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let orig_srv = origin.clone();
        let committed = Arc::new(Mutex::new(HashMap::<String, (String, i64)>::new()));
        let committed_srv = committed.clone();
        let write_dispatches = Arc::new(AtomicUsize::new(0));
        let wd_srv = write_dispatches.clone();

        let srv = thread::spawn(move || {
            for _ in 0..2 {
                let (mut stream, _) = listener.accept().unwrap();
                let Some((path, headers, req_body)) = read_http(&mut stream) else {
                    continue;
                };
                let body_str = String::from_utf8_lossy(&req_body);
                let aid = headers
                    .get("decapod-cloud-attempt-id")
                    .cloned()
                    .unwrap_or_default();
                let dig = headers
                    .get("decapod-cloud-request-digest")
                    .cloned()
                    .unwrap_or_default();
                if path.contains("dactyl_ctrl_status")
                    || path.contains("operation-status")
                    || body_str.contains("dactyl_ctrl_status")
                {
                    let guard = committed_srv.lock().unwrap();
                    let (stored_dig, val) = guard.get(&aid).cloned().unwrap();
                    let hdrs = prov_headers(
                        &orig_srv,
                        Some(&aid),
                        Some(&stored_dig),
                        "req-rec-1",
                        "committed_observed",
                        "allowed",
                        "read",
                        Some("committed_observed"),
                        "dpop",
                    );
                    let body = serde_json::to_vec(&json!({
                        "found": true,
                        "effect_status": "committed_observed",
                        "task_id": val
                    }))
                    .unwrap();
                    send_http(&mut stream, 200, &hdrs, &body);
                } else {
                    wd_srv.fetch_add(1, Ordering::SeqCst);
                    committed_srv.lock().unwrap().insert(aid, (dig, 42));
                    // Drop socket before ack!
                    let _ = stream.shutdown(Shutdown::Both);
                    drop(stream);
                }
            }
        });

        let connector = connector_with_budget(&origin, ResourceBudget::v1());
        let attempt = OperationAttempt::new("decapod-todo-add-150")
            .unwrap()
            .with_request_digest("sha256:todo150")
            .unwrap();
        let mut meta = CloudResponseMetadata::default();
        let err = connector
            .control_request_with_attempt(
                "dactyl_todo_add",
                &json!({"title": "test"}),
                Some(DEVICE_BEARER),
                None,
                OperationKind::Write,
                ResourceBudget::v1(),
                &attempt,
                &mut meta,
            )
            .unwrap_err();
        let disp = err.operational_disposition_with_context(
            OperationKind::Write,
            Some(&attempt),
            Some(&meta),
        );
        assert_eq!(
            decapod_recovery_decision(&disp),
            ConsumerDecision::ReconcileByAttemptId("decapod-todo-add-150".into())
        );

        let mut rec_meta = CloudResponseMetadata::default();
        let obs = connector
            .reconcile_control_attempt(
                "dactyl_ctrl_status",
                &attempt,
                Some(DEVICE_BEARER),
                None,
                ResourceBudget::v1(),
                Some(EffectStatus::Uncertain),
                &mut rec_meta,
            )
            .unwrap();
        assert_eq!(obs.provenance().status(), EffectStatus::CommittedObserved);
        assert_eq!(obs.payload()["task_id"], 42);
        // Only one write was ever dispatched; zero duplicate writes occurred!
        assert_eq!(write_dispatches.load(Ordering::SeqCst), 1);

        srv.join().unwrap();
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_and_cloud_disposition_semantics_agree_on_comparable_cases() {
        let dir = tempfile::tempdir().unwrap();
        let sqlite_path = dir.path().join("parity.sqlite");
        drop(Connection::open(DatastoreRoute::sqlite(sqlite_path.to_string_lossy())).unwrap());
        let sqlite_ro = Connection::open_with_options(
            DatastoreRoute::sqlite(sqlite_path.to_string_lossy()),
            OpenOptions {
                access_mode: AccessMode::ReadOnly,
                ..OpenOptions::default()
            },
        )
        .unwrap();
        let sqlite_ro_err = sqlite_ro
            .write("INSERT INTO t VALUES (1)", &[])
            .unwrap_err();

        let (cloud_origin, _cloud_listener) = unused_origin();
        let cloud_ro = Connection::open_with_options_and_context(
            DatastoreRoute::supabase_rpc(
                SupabaseRpcConfig::v1(&cloud_origin, KEY).with_device_gateway(),
                Some(DEVICE_BEARER.to_owned()),
            )
            .with_proof_signer(Arc::new(DummySigner)),
            OpenOptions {
                access_mode: AccessMode::ReadOnly,
                ..OpenOptions::default()
            },
            Some(context()),
        )
        .unwrap();
        let cloud_ro_err = cloud_ro.write("INSERT INTO t VALUES (1)", &[]).unwrap_err();

        let sq_disp = sqlite_ro_err.operational_disposition(OperationKind::Write);
        let cl_disp = cloud_ro_err.operational_disposition(OperationKind::Write);
        assert_eq!(sq_disp.action(), cl_disp.action());
        assert_eq!(sq_disp.action(), OperationalAction::Stop);
        assert_eq!(sq_disp.error_kind(), cl_disp.error_kind());
        assert_eq!(sq_disp.mutation_outcome(), cl_disp.mutation_outcome());
        assert_eq!(
            sq_disp.mutation_outcome(),
            Some(MutationOutcome::NotAttempted)
        );

        // Unsupported capabilities also agree across SQLite and Cloud without network I/O.
        let mut sq_meta = CloudResponseMetadata::default();
        let att = OperationAttempt::new("parity-cap").unwrap();
        let sq_cap_err = sqlite_ro
            .write_with_attempt("INSERT INTO t VALUES (1)", &[], &att, &mut sq_meta)
            .unwrap_err();
        let cl_cap_err = cloud_ro.has_table("items").unwrap_err();
        assert_eq!(
            sq_cap_err
                .operational_disposition(OperationKind::Write)
                .action(),
            OperationalAction::Stop
        );
        assert_eq!(
            cl_cap_err
                .operational_disposition(OperationKind::Read)
                .action(),
            OperationalAction::Stop
        );
        assert_eq!(sq_cap_err.adapter_kind(), cl_cap_err.adapter_kind());
    }

    #[test]
    fn sqlite_and_cloud_local_budget_preflight_parity_and_atomic_stop() {
        let (origin, listener) = unused_origin();
        listener.set_nonblocking(true).unwrap();
        let cloud_small = open_gateway_conn(&origin, ResourceBudget::v1().with_max_sql_bytes(8));
        let err = cloud_small
            .atomic(&[Operation::write("INSERT INTO t VALUES (1)", vec![])])
            .unwrap_err();
        let disp = err.operational_disposition(OperationKind::Write);
        assert_eq!(disp.action(), OperationalAction::Stop);
        assert_eq!(disp.failure_origin(), FailureOrigin::LocalBudget);
        assert_eq!(disp.mutation_outcome(), Some(MutationOutcome::NotAttempted));
        assert!(matches!(
            listener.accept(),
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock
        ));
    }

    #[test]
    fn cloud_recovery_facts_138_drive_operational_disposition_without_blind_retry() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());

        let srv = thread::spawn(move || {
            type RecoveryTestResponse = (u16, Vec<(String, String)>, serde_json::Value);
            let responses: Vec<RecoveryTestResponse> = vec![
                // 0) Write: OutcomeUnknown + after_send + attempt_ref + retry_after_seconds (no Retry-After header)
                (
                    503_u16,
                    vec![],
                    json!({
                        "error": {
                            "code": "reconciliation_required",
                            "message": "reconciliation_required",
                            "recovery": {
                                "version": 1,
                                "classification": "outcome_unknown",
                                "dispatch_phase": "after_send",
                                "missing_fact": "uncertain_outcome",
                                "authority_freshness": "unknown",
                                "attempt_ref": "att-rec-unknown-150",
                                "retry_after_seconds": 17
                            }
                        }
                    }),
                ),
                // 1) Write: RecoverableWithinAuthority + after_send -> MUST remain Uncertain + Reconcile (never Retry!)
                (
                    503_u16,
                    vec![],
                    json!({
                        "error": {
                            "code": "service_unavailable",
                            "message": "service_unavailable",
                            "recovery": {
                                "version": 1,
                                "classification": "recoverable_within_authority",
                                "dispatch_phase": "after_send",
                                "retry_after_seconds": 9
                            }
                        }
                    }),
                ),
                // 2) Write: RecoverableWithinAuthority + pre_dispatch (no provenance header) -> NotAttempted + Retry
                (
                    503_u16,
                    vec![],
                    json!({
                        "error": {
                            "code": "tenant_upgrading",
                            "message": "tenant_upgrading",
                            "recovery": {
                                "version": 1,
                                "classification": "recoverable_within_authority",
                                "dispatch_phase": "pre_dispatch",
                                "retry_after_seconds": 12
                            }
                        }
                    }),
                ),
                // 3) Write: RefreshReconcileState + pre_dispatch -> NotAttempted + InspectAndDecide
                (
                    403_u16,
                    vec![],
                    json!({
                        "error": {
                            "code": "grant_stale_generation",
                            "message": "grant_stale_generation",
                            "recovery": {
                                "version": 1,
                                "classification": "refresh_reconcile_state",
                                "dispatch_phase": "pre_dispatch",
                                "missing_fact": "delegated_permission",
                                "authority_freshness": "stale",
                                "grant_generation": 11,
                                "session_generation": 4
                            }
                        }
                    }),
                ),
                // 4) Write: RequiresHumanAction (402 payment_required) + pre_dispatch -> NotAttempted + Stop
                (
                    402_u16,
                    vec![],
                    json!({
                        "error": {
                            "code": "payment_required",
                            "message": "payment_required",
                            "recovery": {
                                "version": 1,
                                "classification": "requires_human_action",
                                "dispatch_phase": "pre_dispatch",
                                "missing_fact": "paid_entitlement"
                            }
                        }
                    }),
                ),
                // 5) Write: DeniedPermanentlyForScope (403 repository_access_denied) + pre_dispatch -> NotAttempted + Stop
                (
                    403_u16,
                    vec![],
                    json!({
                        "error": {
                            "code": "repository_access_denied",
                            "message": "repository_access_denied",
                            "recovery": {
                                "version": 1,
                                "classification": "denied_permanently_for_scope",
                                "dispatch_phase": "pre_dispatch"
                            }
                        }
                    }),
                ),
                // 6) Read: RecoverableWithinAuthority + after_send -> Retry (reads do not mutate)
                (
                    503_u16,
                    vec![],
                    json!({
                        "error": {
                            "code": "service_unavailable",
                            "message": "service_unavailable",
                            "recovery": {
                                "version": 1,
                                "classification": "recoverable_within_authority",
                                "dispatch_phase": "after_send",
                                "retry_after_seconds": 5
                            }
                        }
                    }),
                ),
            ];
            for (status, hdrs, body) in responses {
                let (mut stream, _) = listener.accept().unwrap();
                let _ = read_http(&mut stream);
                let payload = serde_json::to_vec(&body).unwrap();
                send_http(&mut stream, status, &hdrs, &payload);
            }
        });

        let connector = connector_with_budget(&origin, ResourceBudget::v1());
        let db = open_gateway_conn(&origin, ResourceBudget::v1());
        let att = OperationAttempt::new("att-wire-150").unwrap();

        // 0) OutcomeUnknown + after_send on Write (control_request_with_metadata without OperationAttempt proves attempt_ref fallback)
        let mut m0 = CloudResponseMetadata::default();
        let e0 = connector
            .control_request_with_metadata(
                "dactyl_todo_add",
                &json!({"version": 1}),
                Some(DEVICE_BEARER),
                None,
                OperationKind::Write,
                ResourceBudget::v1(),
                &mut m0,
            )
            .unwrap_err();
        let d0 = e0.operational_disposition_with_context(OperationKind::Write, None, Some(&m0));
        assert_eq!(d0.action(), OperationalAction::Reconcile);
        assert_eq!(d0.mutation_outcome(), Some(MutationOutcome::Uncertain));
        assert_eq!(d0.attempt_id(), Some("att-rec-unknown-150"));
        assert_eq!(d0.retry_after(), None);
        assert_eq!(d0.retry_after_seconds(), Some(17));
        let f0 = d0.recovery_facts().expect("recovery facts preserved");
        assert_eq!(f0.action_category(), RecoveryActionCategory::OutcomeUnknown);
        assert_eq!(f0.execution_phase(), ExecutionPhase::AfterSend);
        assert_eq!(f0.missing_fact(), Some("uncertain_outcome"));
        assert_eq!(f0.authority_freshness(), Some("unknown"));
        assert!(!format!("{d0:?}").contains("att-rec-unknown-150"));

        // 1) RecoverableWithinAuthority + after_send on Write -> MUST Reconcile (never blind Retry!)
        let mut m1 = CloudResponseMetadata::default();
        let e1 = db
            .write_with_attempt("INSERT INTO t VALUES (101)", &[], &att, &mut m1)
            .unwrap_err();
        let d1 =
            e1.operational_disposition_with_context(OperationKind::Write, Some(&att), Some(&m1));
        assert_eq!(d1.action(), OperationalAction::Reconcile);
        assert_eq!(d1.mutation_outcome(), Some(MutationOutcome::Uncertain));
        assert_eq!(d1.retry_after_seconds(), Some(9));

        // 2) RecoverableWithinAuthority + pre_dispatch on Write -> NotAttempted + Retry
        let mut m2 = CloudResponseMetadata::default();
        let e2 = db
            .write_with_attempt("INSERT INTO t VALUES (102)", &[], &att, &mut m2)
            .unwrap_err();
        let d2 =
            e2.operational_disposition_with_context(OperationKind::Write, Some(&att), Some(&m2));
        assert_eq!(d2.action(), OperationalAction::Retry);
        assert_eq!(d2.code(), Some("tenant_upgrading"));
        assert_eq!(d2.mutation_outcome(), Some(MutationOutcome::NotAttempted));
        assert_eq!(d2.retry_after_seconds(), Some(12));

        // 3) RefreshReconcileState + pre_dispatch -> NotAttempted + InspectAndDecide
        let mut m3 = CloudResponseMetadata::default();
        let e3 = db
            .write_with_attempt("INSERT INTO t VALUES (103)", &[], &att, &mut m3)
            .unwrap_err();
        let d3 =
            e3.operational_disposition_with_context(OperationKind::Write, Some(&att), Some(&m3));
        assert_eq!(d3.action(), OperationalAction::InspectAndDecide);
        assert_eq!(d3.code(), Some("grant_stale_generation"));
        assert_eq!(d3.mutation_outcome(), Some(MutationOutcome::NotAttempted));
        let f3 = d3.recovery_facts().expect("recovery facts preserved");
        assert_eq!(f3.grant_generation(), Some(11));
        assert_eq!(f3.session_generation(), Some(4));
        assert_eq!(f3.missing_fact(), Some("delegated_permission"));

        // 4) RequiresHumanAction -> NotAttempted + Stop
        let mut m4 = CloudResponseMetadata::default();
        let e4 = db
            .write_with_attempt("INSERT INTO t VALUES (104)", &[], &att, &mut m4)
            .unwrap_err();
        let d4 =
            e4.operational_disposition_with_context(OperationKind::Write, Some(&att), Some(&m4));
        assert_eq!(d4.action(), OperationalAction::Stop);
        assert_eq!(d4.code(), Some("payment_required"));
        assert_eq!(d4.mutation_outcome(), Some(MutationOutcome::NotAttempted));

        // 5) DeniedPermanentlyForScope -> NotAttempted + Stop
        let mut m5 = CloudResponseMetadata::default();
        let e5 = db
            .write_with_attempt("INSERT INTO t VALUES (105)", &[], &att, &mut m5)
            .unwrap_err();
        let d5 =
            e5.operational_disposition_with_context(OperationKind::Write, Some(&att), Some(&m5));
        assert_eq!(d5.action(), OperationalAction::Stop);
        assert_eq!(d5.code(), Some("repository_access_denied"));
        assert_eq!(d5.mutation_outcome(), Some(MutationOutcome::NotAttempted));

        // 6) Read: RecoverableWithinAuthority + after_send -> Retry (safe on Read)
        let mut m6 = CloudResponseMetadata::default();
        let e6 = connector
            .control_request_with_metadata(
                "dactyl_ctrl_status",
                &json!({"version": 1}),
                Some(DEVICE_BEARER),
                None,
                OperationKind::Read,
                ResourceBudget::v1(),
                &mut m6,
            )
            .unwrap_err();
        let d6 = e6.operational_disposition_with_context(OperationKind::Read, None, Some(&m6));
        assert_eq!(d6.action(), OperationalAction::Retry);
        assert_eq!(d6.mutation_outcome(), None);
        assert_eq!(d6.retry_after_seconds(), Some(5));

        srv.join().unwrap();
    }
}
