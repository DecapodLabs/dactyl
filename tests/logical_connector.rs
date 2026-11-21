//! Unified logical local/cloud connector, multi-workspace isolation, session
//! lifecycle, GoTrue/Propodus error normalization, and `outcome_unknown` suite
//! for Dactyl issue #100.
//!
//! Evidence classes in this file:
//! - `local_sqlite`: real host SQLite C-ABI engine (`#[cfg(feature = "sqlite")]`)
//! - `synthetic_http`: loopback HTTP wire fixtures (`#[cfg(feature = "supabase")]`)
//! - `local_postgres`, `hosted_supabase`, `live_propodus`: unavailable here and never claimed as passed.

use dactyl_db::{
    AccountOperation, AdapterErrorKind, AuthenticationSecrets, AuthenticationStart, Backend,
    DactylError, LogicalConnector, LogicalTarget, OpenOptions, ScmCredential, ScmProvider,
    UserSession, DEFAULT_CLOUD_ORIGIN,
};
use serde_json::json;

#[test]
fn portable_connector_types_redact_secrets_and_reject_local_misuse() {
    let local_target = LogicalTarget::local("/tmp/private-workspace/decapod.db");
    let cloud_target = LogicalTarget::cloud();
    let custom_cloud = LogicalTarget::cloud_with_origin("https://private.example");
    assert_eq!(local_target.backend(), Backend::Local);
    assert_eq!(cloud_target.backend(), Backend::Cloud);
    assert_eq!(custom_cloud.backend(), Backend::Cloud);
    assert!(DEFAULT_CLOUD_ORIGIN.starts_with("https://"));
    assert_eq!(
        DEFAULT_CLOUD_ORIGIN,
        "https://goyxpcmxetztjlrrcwew.supabase.co"
    );

    let debug_targets = format!("{local_target:?} {cloud_target:?} {custom_cloud:?}");
    assert!(!debug_targets.contains("private-workspace"));
    assert!(!debug_targets.contains("private.example"));

    assert!(AccountOperation::Register.is_mutating());
    assert!(!AccountOperation::Status.is_mutating());
    assert!(!AccountOperation::Quote.is_mutating());
    assert!(AccountOperation::Checkout.is_mutating());
    assert!(AccountOperation::Manage.is_mutating());
    assert!(AccountOperation::Portal.is_mutating());
    assert!(AccountOperation::BillingOveragePolicy.is_mutating());
    assert!(!AccountOperation::DeviceChallenge.is_mutating());
    assert!(AccountOperation::DeviceEnroll.is_mutating());
    assert!(!AccountOperation::DeviceKeys.is_mutating());
    assert!(!AccountOperation::DeviceSessions.is_mutating());
    assert!(AccountOperation::DeviceSession.is_mutating());
    assert!(AccountOperation::DeviceRefresh.is_mutating());
    assert!(AccountOperation::DeviceStepUp.is_mutating());
    assert!(AccountOperation::DeviceRevoke.is_mutating());

    let scm = ScmCredential::github("gho_secret_linking_token").unwrap();
    assert_eq!(scm.provider(), ScmProvider::GitHub);
    assert_eq!(scm.host(), "github.com");
    assert!(!format!("{scm:?}").contains("gho_secret_linking_token"));
    assert!(ScmCredential::github("bad token").is_err());

    let start = AuthenticationStart {
        flow_id: "00000000-0000-4000-8000-000000000001".into(),
        browser_url: "https://project.example/begin?ticket=secret-ticket".into(),
        expires_at: "2099-01-01T00:00:00Z".into(),
    };
    let session = UserSession {
        access_token: "access-secret".into(),
        refresh_token: "refresh-secret".into(),
        expires_at_unix: 1_900_000_000,
        provider_token: Some("provider-secret".into()),
    };
    let secrets: AuthenticationSecrets = serde_json::from_value(json!({
        "verifier": "a".repeat(43),
        "poll_secret": "b".repeat(43),
    }))
    .unwrap();
    let redacted = format!("{start:?} {session:?} {secrets:?}");
    for sensitive in [
        "secret-ticket",
        "access-secret",
        "refresh-secret",
        "provider-secret",
        &"a".repeat(43),
    ] {
        assert!(!redacted.contains(sensitive));
    }

    assert!(matches!(
        LogicalConnector::local("   "),
        Err(DactylError::Config(_))
    ));

    let local = LogicalConnector::local("/tmp/private-local.db").unwrap();
    assert_eq!(local.backend(), Backend::Local);
    assert!(!format!("{local:?}").contains("private-local.db"));
    assert!(matches!(
        local.route(Some("cloud-bearer".into())),
        Err(DactylError::Config(_))
    ));
    assert!(matches!(
        local.open(OpenOptions::default(), None, Some("cloud-bearer".into())),
        Err(DactylError::Config(_))
    ));

    for err in [
        local
            .start_authentication("org/repo", &secrets)
            .unwrap_err(),
        local.authentication_status("flow", &secrets).unwrap_err(),
        local
            .exchange_authentication_code("flow", &secrets)
            .unwrap_err(),
        local.exchange_session("code", &secrets).unwrap_err(),
        local.refresh_session("refresh").unwrap_err(),
        local
            .complete_authentication("flow", &secrets, "bearer")
            .unwrap_err(),
        local.revoke_session("bearer").unwrap_err(),
        local
            .account_request(AccountOperation::Status, &json!({"version": 1}), "b", None)
            .unwrap_err(),
    ] {
        assert_eq!(err.adapter_kind(), Some(AdapterErrorKind::Capability));
        assert_eq!(err.adapter_code(), Some("cloud_connector_required"));
    }
}

#[cfg(feature = "sqlite")]
mod local_workspaces {
    use dactyl_db::{LogicalConnector, OpenOptions, Parameter};
    use tempfile::TempDir;

    #[test]
    fn independent_local_workspaces_isolate_data_without_process_global_env() {
        let dir = TempDir::new().unwrap();
        let db_a_path = dir.path().join("workspace-a.db");
        let db_b_path = dir.path().join("workspace-b.db");

        let ws_a = LogicalConnector::local(db_a_path.to_string_lossy()).unwrap();
        let ws_b = LogicalConnector::local(db_b_path.to_string_lossy()).unwrap();

        let conn_a = ws_a.open(OpenOptions::default(), None, None).unwrap();
        let conn_b = ws_b.open(OpenOptions::default(), None, None).unwrap();

        conn_a
            .write("CREATE TABLE items (id TEXT PRIMARY KEY)", &[])
            .unwrap();
        conn_a
            .write(
                "INSERT INTO items VALUES ($1)",
                &[Parameter::Text("alpha".into())],
            )
            .unwrap();
        conn_b
            .write("CREATE TABLE items (id TEXT PRIMARY KEY)", &[])
            .unwrap();
        conn_b
            .write(
                "INSERT INTO items VALUES ($1)",
                &[Parameter::Text("beta".into())],
            )
            .unwrap();

        let rows_a = conn_a
            .read(
                "SELECT id FROM items WHERE id = $1",
                &[Parameter::Text("alpha".into())],
            )
            .unwrap();
        let rows_b = conn_b
            .read(
                "SELECT id FROM items WHERE id = $1",
                &[Parameter::Text("alpha".into())],
            )
            .unwrap();
        assert_eq!(rows_a.len(), 1);
        assert_eq!(rows_b.len(), 0);
    }
}

#[cfg(feature = "supabase")]
mod cloud_and_multi_workspace {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    use dactyl_db::{
        AccountOperation, AdapterErrorKind, AuthenticationSecrets, Backend, Datastore,
        LogicalConnector, LogicalTarget, OpenOptions, StorageContext, SupabaseRpcConfig,
    };
    use serde_json::{json, Value};
    use tempfile::TempDir;

    const KEY_B: &str = "sb_publishable_workspace_b";
    const KEY_C: &str = "sb_publishable_workspace_c";
    const FLOW: &str = "00000000-0000-4000-8000-000000000099";

    fn mock_server(
        responses: Vec<(u16, String)>,
    ) -> (String, mpsc::Receiver<String>, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let origin_for_body = origin.clone();
        let (tx, rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            for (status, raw_body) in responses {
                let (mut stream, _) = listener.accept().unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut bytes = Vec::new();
                let (headers, offset, len) = loop {
                    let mut buf = [0_u8; 4096];
                    let n = stream.read(&mut buf).unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&buf[..n]);
                    if let Some(pos) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                        let hdr = String::from_utf8(bytes[..pos].to_vec()).unwrap();
                        let content_len = hdr
                            .lines()
                            .find_map(|line| {
                                let (k, v) = line.split_once(':')?;
                                k.eq_ignore_ascii_case("content-length")
                                    .then(|| v.trim().parse::<usize>().unwrap())
                            })
                            .unwrap_or(0);
                        break (hdr, pos + 4, content_len);
                    }
                };
                while bytes.len() - offset < len {
                    let mut buf = [0_u8; 4096];
                    let n = stream.read(&mut buf).unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&buf[..n]);
                }
                let full_req = format!(
                    "{headers}\r\n\r\n{}",
                    String::from_utf8_lossy(&bytes[offset..offset + len])
                );
                tx.send(full_req).unwrap();
                if status == 0 {
                    // Simulate post-send connection drop without response!
                    continue;
                }
                let body = raw_body.replace("ORIGIN", &origin_for_body);
                let _ = write!(
                    stream,
                    "HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
            }
        });
        (origin, rx, handle)
    }

    #[test]
    fn clean_consumer_reuses_bearer_completes_missing_session_and_isolates_workspaces() {
        let temp = TempDir::new().unwrap();
        let sqlite_nonexistent = temp.path().join("must-never-be-created.db");

        // Workspace B: existing bearer reuse through LogicalConnector::resolve.
        let (origin_b, rx_b, srv_b) = mock_server(vec![
            (
                200,
                json!({
                    "version": 1,
                    "project_origin": "ORIGIN",
                    "publishable_key": KEY_B,
                    "schema": "dactyl_b"
                })
                .to_string(),
            ),
            (
                200,
                json!({
                    "columns": ["workspace"],
                    "rows": [{"workspace": "b"}],
                    "affected_rows": 0,
                    "generated_keys": []
                })
                .to_string(),
            ),
        ]);

        // Workspace C: missing-session bootstrap -> readiness -> query -> logout on a distinct cloud origin.
        let (origin_c, rx_c, srv_c) = mock_server(vec![
            (
                200,
                json!({
                    "version": 1,
                    "project_origin": "ORIGIN",
                    "publishable_key": KEY_C,
                    "schema": "dactyl_c"
                })
                .to_string(),
            ),
            (
                200,
                json!({
                    "flow_id": FLOW,
                    "browser_url": format!("ORIGIN/functions/v1/decapod-onboarding/begin?flow_id={FLOW}&ticket=t1"),
                    "expires_at": "2099-01-01T00:00:00Z"
                })
                .to_string(),
            ),
            (200, json!({"status": "ready"}).to_string()),
            (200, json!({"auth_code": "code-c"}).to_string()),
            (
                200,
                json!({
                    "access_token": "bearer-c",
                    "refresh_token": "refresh-c",
                    "expires_in": 900
                })
                .to_string(),
            ),
            (200, json!({"status": "complete"}).to_string()),
            (
                200,
                json!({
                    "backend_authorized": true,
                    "project_id": "proj-c",
                    "entitlement": "active"
                })
                .to_string(),
            ),
            (
                200,
                json!({
                    "columns": ["workspace"],
                    "rows": [{"workspace": "c"}],
                    "affected_rows": 0,
                    "generated_keys": []
                })
                .to_string(),
            ),
            (204, String::new()),
        ]);

        let ws_b = LogicalConnector::resolve(LogicalTarget::cloud_with_origin(&origin_b)).unwrap();
        let ws_c = LogicalConnector::resolve(LogicalTarget::cloud_with_origin(&origin_c)).unwrap();
        assert_eq!(ws_b.backend(), Backend::Cloud);
        assert_eq!(ws_c.backend(), Backend::Cloud);

        // Verify discovery on both origins sent zero user credentials or API keys.
        let disc_b = rx_b.recv().unwrap();
        let disc_c = rx_c.recv().unwrap();
        assert!(disc_b.starts_with("GET /functions/v1/decapod-onboarding/config "));
        assert!(disc_c.starts_with("GET /functions/v1/decapod-onboarding/config "));
        assert!(!disc_b.to_lowercase().contains("authorization:"));
        assert!(!disc_b.to_lowercase().contains("apikey:"));
        assert!(!disc_c.to_lowercase().contains("authorization:"));
        assert!(!disc_c.to_lowercase().contains("apikey:"));

        // Workspace B reuses an existing bearer directly.
        let ctx_b = StorageContext::new(1, json!({"workspace": "b"})).unwrap();
        let conn_b = ws_b
            .open(OpenOptions::default(), Some(ctx_b), Some("bearer-b".into()))
            .unwrap();
        assert_eq!(conn_b.datastore(), Datastore::Supabase);
        let rows_b = conn_b.read("SELECT workspace", &[]).unwrap();
        assert_eq!(rows_b.as_slice()[0].get_str("workspace").unwrap(), "b");

        let query_b = rx_b.recv().unwrap();
        assert!(query_b.contains(&format!("apikey: {KEY_B}")));
        assert!(query_b.contains("authorization: Bearer bearer-b"));
        assert!(query_b.contains("content-profile: dactyl_b"));
        assert!(!query_b.contains(KEY_C));
        assert!(!query_b.contains("bearer-c"));

        // Workspace C starts without a bearer: open fails closed before network I/O!
        let ctx_c = StorageContext::new(1, json!({"workspace": "c"})).unwrap();
        let missing_auth = ws_c
            .open(OpenOptions::default(), Some(ctx_c.clone()), None)
            .err()
            .unwrap();
        assert_eq!(
            missing_auth.adapter_kind(),
            Some(AdapterErrorKind::Authentication)
        );
        assert_eq!(missing_auth.adapter_code(), Some("authentication_required"));

        // Malformed bearer also fails closed before network I/O without echoing secret.
        let bad_bearer = ws_c
            .open(
                OpenOptions::default(),
                Some(ctx_c.clone()),
                Some("bad secret token".into()),
            )
            .err()
            .unwrap();
        assert_eq!(
            bad_bearer.adapter_kind(),
            Some(AdapterErrorKind::Authentication)
        );
        assert_eq!(bad_bearer.adapter_code(), Some("invalid_token"));
        assert!(!format!("{bad_bearer} {bad_bearer:?}").contains("bad secret token"));

        // Complete missing-session transport + readiness check through the same ws_c handle!
        let secrets = AuthenticationSecrets::generate().unwrap();
        let start = ws_c.start_authentication("org/repo-c", &secrets).unwrap();
        assert_eq!(
            ws_c.authentication_status(&start.flow_id, &secrets)
                .unwrap(),
            "ready"
        );
        let code = ws_c
            .exchange_authentication_code(&start.flow_id, &secrets)
            .unwrap();
        let session = ws_c.exchange_session(&code, &secrets).unwrap();
        ws_c.complete_authentication(&start.flow_id, &secrets, &session.access_token)
            .unwrap();
        let readiness = ws_c
            .account_request(
                AccountOperation::Status,
                &json!({"version": 1, "project_id": "proj-c"}),
                &session.access_token,
                None,
            )
            .unwrap();
        assert_eq!(readiness["backend_authorized"], Value::Bool(true));

        let conn_c = ws_c
            .open(
                OpenOptions::default(),
                Some(ctx_c),
                Some(session.access_token.clone()),
            )
            .unwrap();
        let rows_c = conn_c.read("SELECT workspace", &[]).unwrap();
        assert_eq!(rows_c.as_slice()[0].get_str("workspace").unwrap(), "c");

        ws_c.revoke_session(&session.access_token).unwrap();

        let c_reqs: Vec<String> = (0..8).map(|_| rx_c.recv().unwrap()).collect();
        assert!(c_reqs[6].contains(&format!("apikey: {KEY_C}")));
        assert!(c_reqs[6].contains("authorization: Bearer bearer-c"));
        assert!(c_reqs[6].contains("content-profile: dactyl_c"));
        assert!(!c_reqs[6].contains("bearer-b"));
        assert!(c_reqs[7].starts_with("POST /auth/v1/logout?scope=local "));
        assert!(c_reqs[7].contains("authorization: Bearer bearer-c"));

        // Failed cloud discovery never falls back to SQLite.
        assert!(LogicalConnector::cloud_with_origin("http://127.0.0.1:9").is_err());
        assert!(!sqlite_nonexistent.exists());

        srv_b.join().unwrap();
        srv_c.join().unwrap();
    }

    #[test]
    fn discovery_rejects_malformed_schema_oversized_and_mismatched_profiles_without_fallback() {
        let invalid_profiles = vec![
            // Invalid SQL identifier schema rejected by validate_rpc as Config error.
            (
                json!({
                    "version": 1,
                    "project_origin": "ORIGIN",
                    "publishable_key": KEY_B,
                    "schema": "public; DROP TABLE users"
                })
                .to_string(),
                None,
            ),
            // Unknown extra field (deny_unknown_fields) rejected as Protocol error.
            (
                json!({
                    "version": 1,
                    "project_origin": "ORIGIN",
                    "publishable_key": KEY_B,
                    "schema": "dactyl_api",
                    "unexpected": true
                })
                .to_string(),
                Some(AdapterErrorKind::Protocol),
            ),
            // Oversized body (> 16 KiB) rejected as Protocol error.
            (
                format!(
                    "{{\"version\":1,\"project_origin\":\"ORIGIN\",\"publishable_key\":\"{KEY_B}\",\"schema\":\"{}\"}}",
                    "a".repeat(20_000)
                ),
                Some(AdapterErrorKind::Protocol),
            ),
        ];

        for (body, expected_kind) in invalid_profiles {
            let (origin, rx, srv) = mock_server(vec![(200, body)]);
            let err = LogicalConnector::cloud_with_origin(&origin).unwrap_err();
            assert_eq!(err.adapter_kind(), expected_kind);
            if expected_kind.is_none() {
                assert!(matches!(err, dactyl_db::DactylError::Config(_)));
            }
            rx.recv().unwrap();
            srv.join().unwrap();
        }
    }

    #[test]
    fn gotrue_oauth_and_propodus_error_envelopes_preserve_allowlisted_codes_and_redact_messages() {
        let cases = [
            // Supabase GoTrue v2 integer `code` + string `error_code` on expired/reused refresh token.
            (
                400,
                json!({
                    "code": 400,
                    "error_code": "refresh_token_already_used",
                    "msg": "private-refresh-secret"
                }),
                AdapterErrorKind::Authentication,
                Some("refresh_token_already_used"),
            ),
            // GoTrue revoked session.
            (
                401,
                json!({
                    "code": 401,
                    "error_code": "session_not_found",
                    "msg": "private-jwt-secret"
                }),
                AdapterErrorKind::Authentication,
                Some("session_not_found"),
            ),
            // OAuth2 string `error` envelope.
            (
                400,
                json!({
                    "error": "invalid_grant",
                    "error_description": "private-verifier-secret"
                }),
                AdapterErrorKind::Authentication,
                Some("invalid_grant"),
            ),
            // Propodus repository authority / seat / custody conflict codes.
            (
                403,
                json!({
                    "error": {
                        "code": "repository_not_authorized",
                        "message": "private-org-detail"
                    }
                }),
                AdapterErrorKind::Authorization,
                Some("repository_not_authorized"),
            ),
            (
                403,
                json!({
                    "error": {
                        "code": "organization_seat_required",
                        "message": "private-seat-detail"
                    }
                }),
                AdapterErrorKind::Authorization,
                Some("organization_seat_required"),
            ),
            (
                409,
                json!({
                    "error": {
                        "code": "custody_conflict",
                        "message": "private-custody-detail"
                    }
                }),
                AdapterErrorKind::Conflict,
                Some("custody_conflict"),
            ),
            // Unrecognized provider code is stripped so arbitrary strings never leak.
            (
                403,
                json!({
                    "error": {
                        "code": "untrusted_secret_token_leak",
                        "message": "private-detail"
                    }
                }),
                AdapterErrorKind::Authorization,
                None,
            ),
        ];

        for (status, payload, expected_kind, expected_code) in cases {
            let (origin, rx, srv) = mock_server(vec![(status, payload.to_string())]);
            let conn =
                LogicalConnector::cloud_with_config(SupabaseRpcConfig::v1(&origin, KEY_B)).unwrap();
            let err = conn
                .account_request(
                    AccountOperation::Manage,
                    &json!({"version": 1}),
                    "user-bearer",
                    None,
                )
                .unwrap_err();
            assert_eq!(err.adapter_kind(), Some(expected_kind));
            assert_eq!(err.adapter_code(), expected_code);
            let rendered = format!("{err} {err:?}");
            for secret in [
                "private-refresh-secret",
                "private-jwt-secret",
                "private-verifier-secret",
                "private-org-detail",
                "private-seat-detail",
                "private-custody-detail",
                "untrusted_secret_token_leak",
            ] {
                assert!(!rendered.contains(secret));
            }
            rx.recv().unwrap();
            srv.join().unwrap();
        }
    }

    #[test]
    fn post_send_mutating_losses_report_outcome_unknown_while_reads_and_preflight_do_not() {
        let secrets = AuthenticationSecrets::generate().unwrap();

        // 1. Post-send drop on one-time refresh_session -> outcome_unknown.
        let (origin, rx, srv) = mock_server(vec![(0, String::new())]);
        let conn =
            LogicalConnector::cloud_with_config(SupabaseRpcConfig::v1(&origin, KEY_B)).unwrap();
        let err = conn.refresh_session("refresh-tok").unwrap_err();
        assert_eq!(err.adapter_kind(), Some(AdapterErrorKind::Unavailable));
        assert_eq!(err.adapter_code(), Some("outcome_unknown"));
        rx.recv().unwrap();
        srv.join().unwrap();

        // 2. Post-send malformed 200 JSON on mutating Checkout / Portal -> outcome_unknown.
        let (origin, rx, srv) = mock_server(vec![(200, "{truncated".into())]);
        let conn =
            LogicalConnector::cloud_with_config(SupabaseRpcConfig::v1(&origin, KEY_B)).unwrap();
        let err = conn
            .account_request(
                AccountOperation::Portal,
                &json!({"version": 1}),
                "user-bearer",
                None,
            )
            .unwrap_err();
        assert_eq!(err.adapter_kind(), Some(AdapterErrorKind::Protocol));
        assert_eq!(err.adapter_code(), Some("outcome_unknown"));
        rx.recv().unwrap();
        srv.join().unwrap();

        // 3. Post-send drop on read-only AccountOperation::Status -> NOT outcome_unknown.
        let (origin, rx, srv) = mock_server(vec![(0, String::new())]);
        let conn =
            LogicalConnector::cloud_with_config(SupabaseRpcConfig::v1(&origin, KEY_B)).unwrap();
        let err = conn
            .account_request(
                AccountOperation::Status,
                &json!({"version": 1}),
                "user-bearer",
                None,
            )
            .unwrap_err();
        assert_eq!(err.adapter_kind(), Some(AdapterErrorKind::Unavailable));
        assert_eq!(err.adapter_code(), None);
        rx.recv().unwrap();
        srv.join().unwrap();

        // 4. Deeply nested (> 64) JSON response on mutating Register -> Protocol + outcome_unknown.
        let mut deep = json!(1);
        for _ in 0..65 {
            deep = json!([deep]);
        }
        let (origin, rx, srv) = mock_server(vec![(200, deep.to_string())]);
        let conn =
            LogicalConnector::cloud_with_config(SupabaseRpcConfig::v1(&origin, KEY_B)).unwrap();
        let err = conn
            .account_request(
                AccountOperation::Register,
                &json!({"version": 1}),
                "user-bearer",
                None,
            )
            .unwrap_err();
        assert_eq!(err.adapter_kind(), Some(AdapterErrorKind::Protocol));
        assert_eq!(err.adapter_code(), Some("outcome_unknown"));
        rx.recv().unwrap();
        srv.join().unwrap();

        // 5. Pre-dispatch invalid flow_id on exchange_authentication_code -> no network, no outcome_unknown.
        let err = conn
            .exchange_authentication_code("not-a-uuid", &secrets)
            .unwrap_err();
        assert_eq!(err.adapter_kind(), Some(AdapterErrorKind::Protocol));
        assert_eq!(err.adapter_code(), None);

        // 6. DeviceEnroll / DeviceStepUp / DeviceRevoke post-send loss -> outcome_unknown + Uncertain,
        //    while DeviceChallenge post-send loss is NOT outcome_unknown.
        for op in [
            AccountOperation::DeviceEnroll,
            AccountOperation::DeviceStepUp,
            AccountOperation::DeviceRevoke,
        ] {
            let (origin, rx, srv) = mock_server(vec![(0, String::new())]);
            let conn =
                LogicalConnector::cloud_with_config(SupabaseRpcConfig::v1(&origin, KEY_B)).unwrap();
            let err = conn
                .account_request(op, &json!({"version": 1}), "user-bearer", None)
                .unwrap_err();
            assert_eq!(err.adapter_code(), Some("outcome_unknown"));
            assert_eq!(
                err.mutation_outcome(),
                dactyl_db::MutationOutcome::Uncertain
            );
            rx.recv().unwrap();
            srv.join().unwrap();
        }
        let (origin, rx, srv) = mock_server(vec![(0, String::new())]);
        let conn =
            LogicalConnector::cloud_with_config(SupabaseRpcConfig::v1(&origin, KEY_B)).unwrap();
        let err = conn
            .account_request(
                AccountOperation::DeviceChallenge,
                &json!({"version": 1}),
                "user-bearer",
                None,
            )
            .unwrap_err();
        assert_eq!(err.adapter_code(), None);
        assert_ne!(
            err.mutation_outcome(),
            dactyl_db::MutationOutcome::Uncertain
        );
        rx.recv().unwrap();
        srv.join().unwrap();
    }

    #[test]
    fn logical_connector_proof_signer_and_metadata_propagate_and_redact() {
        use dactyl_db::{
            CloudResponseMetadata, OperationKind, RequestProof, RequestProofInput,
            RequestProofSigner, ResourceBudget,
        };
        use std::sync::Arc;

        struct StaticDpopSigner;
        impl RequestProofSigner for StaticDpopSigner {
            fn sign_request(
                &self,
                input: &RequestProofInput<'_>,
            ) -> Result<RequestProof, dactyl_db::DactylError> {
                assert_eq!(input.method(), "POST");
                assert!(input.access_token_sha256().is_some());
                RequestProof::dpop(format!("logical-dpop-{}", input.nonce().unwrap_or("none")))
            }
        }

        let (origin, rx, srv) = mock_server(vec![
            (
                200,
                json!({
                    "version": 1,
                    "project_origin": "ORIGIN",
                    "publishable_key": KEY_B,
                    "schema": "dactyl_b"
                })
                .to_string(),
            ),
            (200, json!({"decision": "allow"}).to_string()),
        ]);
        let (connector, disc_meta) =
            LogicalConnector::resolve_with_metadata(LogicalTarget::cloud_with_origin(&origin))
                .unwrap();
        assert!(disc_meta.cloud_headers().is_empty());

        let connector = connector
            .with_proof_signer(Arc::new(StaticDpopSigner))
            .with_dpop_nonce("init-nonce");
        let mut ctrl_meta = CloudResponseMetadata::default();
        let out = connector
            .control_request_with_metadata(
                "propodus_repository_access_v1",
                &json!({"version": 1}),
                Some("bearer-token"),
                None,
                OperationKind::Read,
                ResourceBudget::v1(),
                &mut ctrl_meta,
            )
            .unwrap();
        assert_eq!(out["decision"], "allow");
        let _ = rx.recv().unwrap();
        let ctrl_req = rx.recv().unwrap();
        assert!(ctrl_req.contains("authorization: DPoP bearer-token"));
        assert!(ctrl_req.contains("dpop: logical-dpop-init-nonce"));
        srv.join().unwrap();
    }

    #[test]
    fn single_origin_serves_multiple_orgs_and_repos_without_cross_contamination_or_redirects() {
        use dactyl_db::{
            CloudResponseMetadata, OperationKind, RequestProof, RequestProofInput,
            RequestProofSigner, ResourceBudget,
        };
        use std::sync::{Arc, Barrier};

        struct MultiOrgSigner(&'static str);
        impl RequestProofSigner for MultiOrgSigner {
            fn sign_request(
                &self,
                input: &RequestProofInput<'_>,
            ) -> Result<RequestProof, dactyl_db::DactylError> {
                assert!(input.access_token_sha256().is_some());
                RequestProof::dpop(format!("sig-{}-{}", self.0, input.nonce().unwrap_or("0")))
            }
        }

        // Single-origin mock that inspects each request's bearer + opaque payload
        // and enforces multi-org routing isolation without any fallback or redirects.
        let sink = TcpListener::bind("127.0.0.1:0").unwrap();
        let sink_location = format!("http://{}/pricing-redirect", sink.local_addr().unwrap());

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let origin_srv = origin.clone();
        let srv = thread::spawn(move || {
            // Expect: 2x discovery, 8x concurrent org/repo ops, 1x legacy rpc,
            // 4x denial/stale/not_found cases, 1x redirect refusal = 16 requests.
            for _ in 0..16 {
                let (mut stream, _) = listener.accept().unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut bytes = Vec::new();
                let (headers, offset, len) = loop {
                    let mut buf = [0_u8; 4096];
                    let n = stream.read(&mut buf).unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&buf[..n]);
                    if let Some(pos) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                        let hdr = String::from_utf8(bytes[..pos].to_vec()).unwrap();
                        let content_len = hdr
                            .lines()
                            .find_map(|line| {
                                let (k, v) = line.split_once(':')?;
                                k.eq_ignore_ascii_case("content-length")
                                    .then(|| v.trim().parse::<usize>().unwrap())
                            })
                            .unwrap_or(0);
                        break (hdr, pos + 4, content_len);
                    }
                };
                while bytes.len() - offset < len {
                    let mut buf = [0_u8; 4096];
                    let n = stream.read(&mut buf).unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&buf[..n]);
                }
                let body: Value = if len == 0 {
                    Value::Null
                } else {
                    serde_json::from_slice(&bytes[offset..offset + len]).unwrap()
                };
                let first_line = headers.lines().next().unwrap();
                let lower_hdr = headers.to_ascii_lowercase();
                let (status, extra, resp_body) = if first_line
                    .contains("/functions/v1/decapod-onboarding/config")
                {
                    (
                        200,
                        String::new(),
                        json!({
                            "version": 1,
                            "project_origin": origin_srv,
                            "publishable_key": KEY_B,
                            "schema": "dactyl_api"
                        })
                        .to_string(),
                    )
                } else if first_line.contains("/functions/v1/decapod-onboarding/cloud-control") {
                    if lower_hdr.contains("authorization: dpop tok-cross-org") {
                        (
                            403,
                            String::new(),
                            json!({"error":{"code":"repo_access_denied","message":"dsn-secret-alpha"}}).to_string(),
                        )
                    } else if lower_hdr.contains("authorization: dpop tok-revoked-seat") {
                        (
                            403,
                            String::new(),
                            json!({"error":{"code":"entitlement_required","message":"seat-secret"}}).to_string(),
                        )
                    } else if lower_hdr.contains("authorization: dpop tok-stale-device") {
                        (
                            401,
                            String::new(),
                            json!({"error":{"code":"device_key_expired","message":"key-secret"}})
                                .to_string(),
                        )
                    } else if lower_hdr.contains("authorization: dpop tok-unknown-project") {
                        (
                            404,
                            String::new(),
                            json!({"error":{"code":"project_not_found","message":"proj-secret"}})
                                .to_string(),
                        )
                    } else if lower_hdr.contains("authorization: dpop tok-redirect") {
                        (
                            302,
                            format!("Location: {sink_location}\r\n"),
                            json!({}).to_string(),
                        )
                    } else if lower_hdr.contains("authorization: dpop tok-alpha") {
                        assert_eq!(body["request"]["project_id"], "org-alpha/repo-one");
                        assert!(lower_hdr.contains("dpop: sig-alpha-"));
                        (
                            200,
                            String::new(),
                            json!({"org": "org-alpha", "allowed": true}).to_string(),
                        )
                    } else if lower_hdr.contains("authorization: dpop tok-beta") {
                        assert_eq!(body["request"]["project_id"], "org-beta/repo-two");
                        assert!(lower_hdr.contains("dpop: sig-beta-"));
                        (
                            200,
                            String::new(),
                            json!({"org": "org-beta", "allowed": true}).to_string(),
                        )
                    } else {
                        panic!("unexpected control auth header: {headers}");
                    }
                } else if first_line.contains("/functions/v1/decapod-onboarding/cloud-query") {
                    if lower_hdr.contains("authorization: dpop tok-alpha") {
                        assert_eq!(body["context"]["payload"]["repo"], "org-alpha/repo-one");
                        assert!(!body.to_string().contains("org-beta"));
                        (
                            200,
                            String::new(),
                            json!({"columns":["tenant"],"rows":[{"tenant":"alpha"}],"affected_rows":0,"generated_keys":[]}).to_string(),
                        )
                    } else if lower_hdr.contains("authorization: dpop tok-beta") {
                        assert_eq!(body["context"]["payload"]["repo"], "org-beta/repo-two");
                        assert!(!body.to_string().contains("org-alpha"));
                        (
                            200,
                            String::new(),
                            json!({"columns":["tenant"],"rows":[{"tenant":"beta"}],"affected_rows":0,"generated_keys":[]}).to_string(),
                        )
                    } else {
                        panic!("unexpected query auth header: {headers}");
                    }
                } else if first_line.contains("/rest/v1/rpc/dactyl_query_v1") {
                    assert!(lower_hdr.contains("authorization: bearer tok-legacy"));
                    (
                        200,
                        String::new(),
                        json!({"columns":["tenant"],"rows":[{"tenant":"legacy"}],"affected_rows":0,"generated_keys":[]}).to_string(),
                    )
                } else {
                    panic!("unexpected path: {first_line}");
                };
                write!(
                    stream,
                    "HTTP/1.1 {status} Fixture\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n{extra}\r\n{resp_body}",
                    resp_body.len()
                )
                .ok();
            }
            listener.set_nonblocking(true).unwrap();
            thread::sleep(Duration::from_millis(80));
            assert!(listener.accept().is_err());
        });

        let conn_alpha = Arc::new(
            LogicalConnector::resolve(LogicalTarget::cloud_with_origin(&origin))
                .unwrap()
                .with_device_gateway()
                .with_proof_signer(Arc::new(MultiOrgSigner("alpha"))),
        );
        let conn_beta = Arc::new(
            LogicalConnector::resolve(LogicalTarget::cloud_with_origin(&origin))
                .unwrap()
                .with_device_gateway()
                .with_proof_signer(Arc::new(MultiOrgSigner("beta"))),
        );

        // Concurrent pooled requests for two orgs/repos over ONE shared origin stay isolated.
        let barrier = Arc::new(Barrier::new(2));
        let a_handle = {
            let conn = conn_alpha.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                for _ in 0..2 {
                    let out = conn
                        .control_request(
                            "propodus_repository_access_v1",
                            &json!({"version": 1, "project_id": "org-alpha/repo-one"}),
                            Some("tok-alpha"),
                            OperationKind::Read,
                            ResourceBudget::v1(),
                        )
                        .unwrap();
                    assert_eq!(out["org"], "org-alpha");
                    let ctx =
                        StorageContext::new(1, json!({"repo": "org-alpha/repo-one"})).unwrap();
                    let db = conn
                        .open(OpenOptions::default(), Some(ctx), Some("tok-alpha".into()))
                        .unwrap();
                    assert_eq!(db.datastore(), Datastore::Supabase);
                    let rows = db.read("SELECT 'alpha' AS tenant", &[]).unwrap();
                    assert_eq!(rows.as_slice()[0].get_str("tenant").unwrap(), "alpha");
                }
            })
        };
        let b_handle = {
            let conn = conn_beta.clone();
            thread::spawn(move || {
                barrier.wait();
                for _ in 0..2 {
                    let out = conn
                        .control_request(
                            "propodus_repository_access_v1",
                            &json!({"version": 1, "project_id": "org-beta/repo-two"}),
                            Some("tok-beta"),
                            OperationKind::Read,
                            ResourceBudget::v1(),
                        )
                        .unwrap();
                    assert_eq!(out["org"], "org-beta");
                    let ctx = StorageContext::new(1, json!({"repo": "org-beta/repo-two"})).unwrap();
                    let db = conn
                        .open(OpenOptions::default(), Some(ctx), Some("tok-beta".into()))
                        .unwrap();
                    let rows = db.read("SELECT 'beta' AS tenant", &[]).unwrap();
                    assert_eq!(rows.as_slice()[0].get_str("tenant").unwrap(), "beta");
                }
            })
        };
        a_handle.join().unwrap();
        b_handle.join().unwrap();

        // Backwards compatibility: legacy connector without device_gateway keeps /rest/v1/rpc/*.
        let legacy_conn =
            LogicalConnector::cloud_with_config(SupabaseRpcConfig::v1(&origin, KEY_B)).unwrap();
        let legacy_db = legacy_conn
            .open(
                OpenOptions::default(),
                Some(StorageContext::new(1, json!({"legacy": true})).unwrap()),
                Some("tok-legacy".into()),
            )
            .unwrap();
        assert_eq!(
            legacy_db
                .read("SELECT 'legacy' AS tenant", &[])
                .unwrap()
                .as_slice()[0]
                .get_str("tenant")
                .unwrap(),
            "legacy"
        );

        // Multi-org refusal matrix through the single origin fails closed without redirects/leaks.
        for (token, expected_kind, expected_code, hidden) in [
            (
                "tok-cross-org",
                AdapterErrorKind::Authorization,
                Some("repo_access_denied"),
                "dsn-secret-alpha",
            ),
            (
                "tok-revoked-seat",
                AdapterErrorKind::Authorization,
                Some("entitlement_required"),
                "seat-secret",
            ),
            (
                "tok-stale-device",
                AdapterErrorKind::Authentication,
                Some("device_key_expired"),
                "key-secret",
            ),
            (
                "tok-unknown-project",
                AdapterErrorKind::NotFound,
                Some("project_not_found"),
                "proj-secret",
            ),
            (
                "tok-redirect",
                AdapterErrorKind::Protocol,
                Some("outcome_unknown"),
                "pricing-redirect",
            ),
        ] {
            let mut meta = CloudResponseMetadata::default();
            let err = conn_alpha
                .control_request_with_metadata(
                    "propodus_repository_access_v1",
                    &json!({"version": 1, "project_id": "org-beta/repo-two"}),
                    Some(token),
                    None,
                    OperationKind::Write,
                    ResourceBudget::v1(),
                    &mut meta,
                )
                .unwrap_err();
            assert_eq!(err.adapter_kind(), Some(expected_kind));
            assert_eq!(err.adapter_code(), expected_code);
            let rendered = format!("{err} {err:?} {meta:?}");
            assert!(!rendered.contains(hidden));
            assert!(!rendered.contains(token));
        }

        sink.set_nonblocking(true).unwrap();
        assert!(
            sink.accept().is_err(),
            "redirect target must never be contacted"
        );
        srv.join().unwrap();
    }
}
