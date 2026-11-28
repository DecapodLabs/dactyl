//! Deterministic versioned wire, provenance, context, and resource-budget
//! compatibility tests across release skew for Dactyl issue #146.
//!
//! Uses recorded JSON fixtures under `tests/fixtures/wire_compat/` and loopback
//! HTTP servers to distinguish supported additive extensions from breaking,
//! incompatible, downgraded, malformed, and stale-discovery envelopes across
//! both synchronous and asynchronous connectors.

use std::time::Duration;

use dactyl_db::{
    AccessMode, AdapterErrorKind, OpenOptions, Parameter, ResourceBudget, StorageContext,
    FIRST_PARTY_AUTH_PATH, FIRST_PARTY_CLOUD_ORIGIN, FIRST_PARTY_REPO_PATH_PREFIX,
    PROVENANCE_CONTRACT_VERSION, RESOURCE_BUDGET_VERSION, STORAGE_CONTEXT_VERSION,
    SUPABASE_RPC_CONTRACT_VERSION,
};
use serde_json::json;

#[test]
fn contract_version_floors_context_budget_and_parameter_skew_are_enforced() {
    assert_eq!(PROVENANCE_CONTRACT_VERSION, 1);
    assert_eq!(STORAGE_CONTEXT_VERSION, 1);
    assert_eq!(RESOURCE_BUDGET_VERSION, 1);
    assert_eq!(SUPABASE_RPC_CONTRACT_VERSION, 1);
    assert_eq!(FIRST_PARTY_CLOUD_ORIGIN, "https://api.decapodlabs.ai");
    assert_eq!(FIRST_PARTY_AUTH_PATH, "/auth");
    assert_eq!(FIRST_PARTY_REPO_PATH_PREFIX, "/decapod");

    // OpenOptions struct-literal compatibility with ResourceBudget::v1().
    let opts = OpenOptions {
        access_mode: AccessMode::ReadWrite,
        lock_timeout: Duration::from_millis(125),
    };
    assert_eq!(opts.access_mode, AccessMode::ReadWrite);
    assert_eq!(ResourceBudget::default(), ResourceBudget::v1());
    assert_eq!(ResourceBudget::v1().version(), 1);
    assert!(ResourceBudget::v1().validate().is_ok());

    // StorageContext: version 0 downgrade and non-object payloads fail closed.
    for (ver, payload) in [
        (0_u16, json!({"org": "acme"})),
        (1_u16, json!(null)),
        (1_u16, json!([1, 2, 3])),
        (1_u16, json!("string-not-object")),
    ] {
        let err = StorageContext::new(ver, payload).unwrap_err();
        assert_eq!(err.adapter_kind(), Some(AdapterErrorKind::Protocol));
        assert_eq!(err.adapter_code(), Some("invalid_context"));
    }

    // StorageContext: v1 and additive/newer caller-owned versions with additive keys succeed.
    for ver in [1_u16, 2_u16, 17_u16] {
        let ctx = StorageContext::new(
            ver,
            json!({
                "org_id": "org-1",
                "repo_id": "repo-1",
                "additive_future_field": {"nested": [true, 42]}
            }),
        )
        .unwrap();
        assert_eq!(ctx.version(), ver);
        assert_eq!(ctx.payload()["org_id"], "org-1");
    }

    // Parameter deserialization: signed i64 boundaries succeed; u64 > i64::MAX must NOT wrap.
    assert_eq!(
        serde_json::from_value::<Parameter>(json!(i64::MAX)).unwrap(),
        Parameter::Integer(i64::MAX)
    );
    assert_eq!(
        serde_json::from_value::<Parameter>(json!(i64::MIN)).unwrap(),
        Parameter::Integer(i64::MIN)
    );
    assert!(serde_json::from_value::<Parameter>(json!(9_223_372_036_854_775_808_u64)).is_err());
    assert!(serde_json::from_value::<Parameter>(json!(u64::MAX)).is_err());
}

#[cfg(feature = "supabase")]
mod cloud_wire_compatibility {
    use std::collections::HashMap;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::Duration;

    use dactyl_db::{
        AccessMode, AccountOperation, AdapterErrorKind, AuthScheme, AuthenticationSecrets,
        CloudConnector, CloudResponseMetadata, Connection, DactylError, DatastoreRoute,
        DecisionOutcome, EffectStatus, FailureOrigin, GeneratedKey, LogicalConnector,
        LogicalTarget, MutationOutcome, ObservationSource, OpenOptions, Operation,
        OperationAttempt, OperationKind, OperationResult, RequestProof, RequestProofInput,
        RequestProofSigner, ResourceBudget, StorageContext, SupabaseRpcConfig,
        FIRST_PARTY_CLOUD_ORIGIN, PROVENANCE_CONTRACT_VERSION,
    };
    use serde_json::{json, Value};

    const DISCOVERY_FIXTURES: &str = include_str!("fixtures/wire_compat/discovery_fixtures.json");
    const RPC_FIXTURES: &str = include_str!("fixtures/wire_compat/rpc_and_service_fixtures.json");
    const PROVENANCE_FIXTURES: &str = include_str!("fixtures/wire_compat/provenance_fixtures.json");
    const AUTH_FIXTURES: &str = include_str!("fixtures/wire_compat/auth_and_context_fixtures.json");

    const KEY: &str = "sb_publishable_compat_v1";
    const BEARER: &str = "compat-bearer-token";

    #[derive(Clone, Debug)]
    struct RecordedHttpRequest {
        request_line: String,
        headers: HashMap<String, String>,
        body: Vec<u8>,
    }

    impl RecordedHttpRequest {
        fn header(&self, name: &str) -> Option<&str> {
            self.headers
                .get(&name.to_ascii_lowercase())
                .map(String::as_str)
        }
    }

    #[derive(Clone)]
    struct CannedResponse {
        status: u16,
        headers: Vec<(String, String)>,
        body: String,
    }

    impl CannedResponse {
        fn json(status: u16, body: &Value) -> Self {
            Self {
                status,
                headers: Vec::new(),
                body: body.to_string(),
            }
        }

        fn with_headers(mut self, headers: Vec<(String, String)>) -> Self {
            self.headers = headers;
            self
        }
    }

    fn spawn_canned_server(
        responses: Vec<CannedResponse>,
    ) -> (
        String,
        Arc<Mutex<Vec<RecordedHttpRequest>>>,
        thread::JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let origin_copy = origin.clone();
        let recorded = Arc::new(Mutex::new(Vec::new()));
        let recorded_srv = recorded.clone();
        let handle = thread::spawn(move || {
            for resp in responses {
                let (mut stream, _) = listener.accept().unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut buf = Vec::new();
                let mut chunk = [0_u8; 4096];
                let (head_str, head_end, content_length) = loop {
                    let n = stream.read(&mut chunk).unwrap();
                    assert!(n > 0, "unexpected EOF reading request");
                    buf.extend_from_slice(&chunk[..n]);
                    if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                        let head = String::from_utf8(buf[..pos].to_vec()).unwrap();
                        let len = head
                            .lines()
                            .find_map(|line| {
                                let (k, v) = line.split_once(':')?;
                                k.eq_ignore_ascii_case("content-length")
                                    .then(|| v.trim().parse::<usize>().unwrap())
                            })
                            .unwrap_or(0);
                        break (head, pos + 4, len);
                    }
                };
                while buf.len() < head_end + content_length {
                    let n = stream.read(&mut chunk).unwrap();
                    assert!(n > 0, "unexpected EOF reading body");
                    buf.extend_from_slice(&chunk[..n]);
                }
                let mut lines = head_str.lines();
                let request_line = lines.next().unwrap_or("").to_owned();
                let mut headers = HashMap::new();
                for line in lines {
                    if let Some((k, v)) = line.split_once(':') {
                        headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_owned());
                    }
                }
                let req = RecordedHttpRequest {
                    request_line,
                    headers: headers.clone(),
                    body: buf[head_end..head_end + content_length].to_vec(),
                };
                recorded_srv.lock().unwrap().push(req.clone());

                let mut body = resp.body.replace("ORIGIN", &origin_copy);
                if let Some(att) = req.header("decapod-cloud-attempt-id") {
                    body = body.replace("ATTEMPT_ID", att);
                }
                if let Some(dig) = req.header("decapod-cloud-request-digest") {
                    body = body.replace("DIGEST", dig);
                }
                let mut wire = format!(
                    "HTTP/1.1 {} Compat\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n",
                    resp.status,
                    body.len()
                );
                for (k, v) in resp.headers {
                    let v = v.replace("ORIGIN", &origin_copy);
                    wire.push_str(&format!("{k}: {v}\r\n"));
                }
                wire.push_str("\r\n");
                wire.push_str(&body);
                let _ = stream.write_all(wire.as_bytes());
                let _ = stream.flush();
            }
        });
        (origin, recorded, handle)
    }

    fn context_v1() -> StorageContext {
        StorageContext::new(1, json!({"org_id": "org-compat", "repo_id": "dactyl"})).unwrap()
    }

    fn rpc_conn(origin: &str, access_mode: AccessMode) -> Connection {
        Connection::open_with_options_and_context(
            DatastoreRoute::supabase_rpc(SupabaseRpcConfig::v1(origin, KEY), Some(BEARER.into())),
            OpenOptions {
                access_mode,
                ..OpenOptions::default()
            },
            Some(context_v1()),
        )
        .unwrap()
    }

    fn legacy_service_conn(origin: &str, access_mode: AccessMode) -> Connection {
        Connection::open_with_options_and_context(
            DatastoreRoute::supabase(origin, Some(BEARER.into())),
            OpenOptions {
                access_mode,
                ..OpenOptions::default()
            },
            Some(context_v1()),
        )
        .unwrap()
    }

    #[tokio::test(flavor = "current_thread")]
    async fn discovery_fixtures_enforce_version_floor_strict_schema_and_origin_sync_async() {
        let doc: Value = serde_json::from_str(DISCOVERY_FIXTURES).unwrap();

        // 1. Supported v1 discovery succeeds both sync and async (with 404 fallback to legacy onboarding).
        let canonical = &doc["supported"]["v1_canonical"];
        let (origin, recorded, handle) = spawn_canned_server(vec![
            CannedResponse::json(200, canonical).with_headers(vec![
                ("Decapod-Cloud-Release".into(), "propodus-2026.11".into()),
                ("Sunset".into(), "Wed, 01 Jul 2027 00:00:00 GMT".into()),
            ]),
            CannedResponse::json(404, &json!({"code": "not_found"})),
            CannedResponse::json(200, canonical),
        ]);
        let (sync_conn, sync_meta) =
            LogicalConnector::resolve_with_metadata(LogicalTarget::cloud_with_origin(&origin))
                .unwrap();
        assert_eq!(
            sync_meta.cloud_header("Decapod-Cloud-Release"),
            Some("propodus-2026.11")
        );
        assert_eq!(sync_meta.sunset(), Some("Wed, 01 Jul 2027 00:00:00 GMT"));
        assert_eq!(
            sync_conn.cloud_connector().unwrap().rpc_config().schema(),
            "dactyl_api"
        );

        let async_conn = LogicalConnector::resolve_async(LogicalTarget::cloud_with_origin(&origin))
            .await
            .unwrap();
        assert_eq!(
            async_conn.cloud_connector().unwrap().rpc_config().schema(),
            "dactyl_api"
        );
        handle.join().unwrap();
        {
            let reqs = recorded.lock().unwrap();
            assert!(reqs[0]
                .request_line
                .starts_with("GET /functions/v1/decapod-onboarding/config "));
            assert!(reqs[1]
                .request_line
                .starts_with("GET /functions/v1/decapod-onboarding/config "));
            assert!(reqs[2]
                .request_line
                .starts_with("GET /functions/v1/propodus-onboarding/config "));
        }

        // 2. Rejected discovery fixtures (downgrade v0, unsupported v2, unknown extra field,
        //    stale origin skew, secret key, legacy JWT key, bad SQL schema) fail closed sync & async.
        let rejected_cases = [
            ("downgrade_v0", Some(AdapterErrorKind::Protocol)),
            ("unsupported_v2", Some(AdapterErrorKind::Protocol)),
            ("unknown_extra_field", Some(AdapterErrorKind::Protocol)),
            (
                "stale_default_origin_on_first_party",
                Some(AdapterErrorKind::Protocol),
            ),
            (
                "stale_first_party_origin_on_legacy",
                Some(AdapterErrorKind::Protocol),
            ),
            ("secret_key_downgrade", None),
            ("legacy_jwt_anon_key", None),
            ("invalid_schema_identifier", None),
        ];

        for (name, expected_kind) in rejected_cases {
            let fixture_body = &doc["rejected"][name];
            let (origin, _rec, handle) = spawn_canned_server(vec![
                CannedResponse::json(200, fixture_body),
                CannedResponse::json(200, fixture_body),
            ]);
            let sync_err = CloudConnector::discover(&origin).unwrap_err();
            let async_err = CloudConnector::discover_async(&origin).await.unwrap_err();
            for err in [sync_err, async_err] {
                assert_eq!(
                    err.adapter_kind(),
                    expected_kind,
                    "unexpected error kind for discovery fixture {name}: {err:?}"
                );
                if expected_kind.is_none() {
                    assert!(matches!(err, DactylError::Config(_)));
                }
            }
            handle.join().unwrap();
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn rpc_and_legacy_service_envelopes_distinguish_additive_from_breaking_skew() {
        let doc: Value = serde_json::from_str(RPC_FIXTURES).unwrap();

        // 1. Supported additive fields on native RPC query & batch + legacy 0.15 response omitting generated_keys.
        let (origin, recorded, handle) = spawn_canned_server(vec![
            CannedResponse::json(200, &doc["supported"]["rpc_v1_query_with_additive_fields"]),
            CannedResponse::json(200, &doc["supported"]["rpc_v1_batch_with_additive_fields"]),
            CannedResponse::json(
                200,
                &doc["supported"]["legacy_service_0_15_without_generated_keys"],
            ),
        ]);

        let rpc = rpc_conn(&origin, AccessMode::ReadWrite);
        let rows = rpc.read_async("SELECT 1", &[]).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows.as_slice()[0].get_int("id").unwrap(), 42);
        assert_eq!(rows.as_slice()[0].get_str("label").unwrap(), "compat-v1");
        assert_eq!(
            rows.as_slice()[0].get_blob("payload").unwrap(),
            vec![1, 2, 255]
        );

        let batch = rpc
            .atomic_async(&[
                Operation::write("INSERT", vec![]),
                Operation::read("SELECT", vec![]),
            ])
            .await
            .unwrap();
        assert!(matches!(
            &batch.results[0],
            OperationResult::Write(w)
                if w.affected_rows == 1
                    && w.generated_keys == vec![GeneratedKey::Integer(7), GeneratedKey::Text("key_7".into())]
        ));
        assert!(matches!(
            &batch.results[1],
            OperationResult::Rows(r) if r.as_slice()[0].get_int("id").unwrap() == 7
        ));

        let legacy = legacy_service_conn(&origin, AccessMode::ReadOnly);
        let legacy_rows = legacy.read("SELECT 1", &[]).unwrap();
        assert_eq!(legacy_rows.as_slice()[0].get_int("id").unwrap(), 9);
        assert_eq!(
            legacy_rows.as_slice()[0].get_str("label").unwrap(),
            "legacy-0.15"
        );
        handle.join().unwrap();

        let reqs = recorded.lock().unwrap();
        assert_eq!(
            reqs[0].header("dactyl-contract-version"),
            Some("1"),
            "native RPC must advertise contract version 1"
        );
        let rpc_body: Value = serde_json::from_slice(&reqs[0].body).unwrap();
        assert_eq!(rpc_body["request"]["version"], 1);
        assert!(reqs[2].header("dactyl-contract-version").is_none());

        // 2. Rejected incompatible/skewed responses.
        let rejected_query = [
            ("rpc_missing_generated_keys", AdapterErrorKind::Protocol),
            ("rpc_null_generated_keys", AdapterErrorKind::Protocol),
            ("rpc_invalid_generated_key_bool", AdapterErrorKind::Protocol),
            ("rpc_unsupported_object_cell", AdapterErrorKind::Capability),
            ("rpc_u64_overflow_cell", AdapterErrorKind::Capability),
            ("rpc_duplicate_columns", AdapterErrorKind::Protocol),
        ];
        for (name, expected_kind) in rejected_query {
            let (origin, _rec, handle) =
                spawn_canned_server(vec![CannedResponse::json(200, &doc["rejected"][name])]);
            let err = rpc_conn(&origin, AccessMode::ReadOnly)
                .read("SELECT 1", &[])
                .unwrap_err();
            assert_eq!(
                err.adapter_kind(),
                Some(expected_kind),
                "unexpected error kind for RPC fixture {name}: {err:?}"
            );
            handle.join().unwrap();
        }

        // Batch count mismatch fails as Protocol and marks write batch Uncertain.
        let (origin, _rec, handle) = spawn_canned_server(vec![CannedResponse::json(
            200,
            &doc["rejected"]["rpc_batch_count_mismatch"],
        )]);
        let batch_err = rpc_conn(&origin, AccessMode::ReadWrite)
            .atomic(&[
                Operation::write("INSERT", vec![]),
                Operation::read("SELECT", vec![]),
            ])
            .unwrap_err();
        assert_eq!(batch_err.adapter_kind(), Some(AdapterErrorKind::Protocol));
        assert_eq!(batch_err.mutation_outcome(), MutationOutcome::Uncertain);
        handle.join().unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn provenance_fixtures_enforce_version_floor_enums_and_legacy_unadvertised() {
        let doc: Value = serde_json::from_str(PROVENANCE_FIXTURES).unwrap();

        #[derive(Default)]
        struct CompatDpopSigner;
        impl RequestProofSigner for CompatDpopSigner {
            fn sign_request(
                &self,
                input: &RequestProofInput<'_>,
            ) -> Result<RequestProof, DactylError> {
                RequestProof::dpop(format!(
                    "dpop.htm={}.htu={}",
                    input.method(),
                    input.canonical_url()
                ))
            }
        }

        // 1. Supported v1 provenance with additive JSON fields & headers + unadvertised legacy response.
        let (origin, _rec, handle) = spawn_canned_server(vec![
            CannedResponse::json(200, &doc["supported"]["v1_committed_with_additive_fields"])
                .with_headers(vec![(
                    "Decapod-Cloud-Extra-Diag".into(),
                    "compat-ok".into(),
                )]),
            CannedResponse::json(200, &doc["supported"]["unadvertised_legacy_response"]),
        ]);
        let conn = Connection::open_with_options_and_context(
            DatastoreRoute::supabase_rpc(SupabaseRpcConfig::v1(&origin, KEY), Some(BEARER.into()))
                .with_proof_signer(Arc::new(CompatDpopSigner)),
            OpenOptions::default(),
            Some(context_v1()),
        )
        .unwrap();
        let attempt = OperationAttempt::new("att-compat-1").unwrap();
        let mut meta = CloudResponseMetadata::default();
        let wr = conn
            .write_with_attempt_async("INSERT INTO t VALUES (1)", &[], &attempt, &mut meta)
            .await
            .unwrap();
        assert_eq!(wr.affected_rows, 1);
        let prov = meta.provenance().unwrap();
        assert_eq!(prov.contract_version(), Some(PROVENANCE_CONTRACT_VERSION));
        assert_eq!(prov.status(), EffectStatus::CommittedObserved);
        assert_eq!(prov.decision(), Some(DecisionOutcome::Allowed));
        assert_eq!(prov.auth_scheme(), Some(AuthScheme::DPoP));
        assert_eq!(
            meta.cloud_header("decapod-cloud-extra-diag"),
            Some("compat-ok")
        );

        let mut legacy_meta = CloudResponseMetadata::default();
        let legacy_attempt = OperationAttempt::new("att-compat-legacy").unwrap();
        let legacy_wr = conn
            .write_with_attempt(
                "INSERT INTO t VALUES (2)",
                &[],
                &legacy_attempt,
                &mut legacy_meta,
            )
            .unwrap();
        assert_eq!(legacy_wr.affected_rows, 0);
        let legacy_prov = legacy_meta.provenance().unwrap();
        assert_eq!(legacy_prov.contract_version(), None);
        assert_eq!(legacy_prov.status(), EffectStatus::UnsupportedProvenance);
        assert_eq!(
            legacy_prov.observation_source(),
            ObservationSource::DirectResponse
        );
        handle.join().unwrap();

        // 2. Rejected provenance fixtures (downgrade v0, unsupported v2, missing version,
        //    unknown enums, unknown reason_code, origin mismatch).
        let rejected = [
            ("downgrade_v0", "unsupported_provenance_version"),
            ("unsupported_v2", "unsupported_provenance_version"),
            ("missing_version_with_fields", "invalid_provenance"),
            ("unknown_effect_status", "invalid_provenance"),
            ("unknown_decision", "invalid_provenance"),
            ("unknown_operation_kind", "invalid_provenance"),
            ("unknown_auth_scheme", "invalid_provenance"),
            ("unknown_reason_code", "invalid_provenance"),
            ("stale_origin_mismatch", "provenance_origin_mismatch"),
        ];
        for (name, expected_code) in rejected {
            let (origin, _rec, handle) =
                spawn_canned_server(vec![CannedResponse::json(200, &doc["rejected"][name])]);
            let conn = rpc_conn(&origin, AccessMode::ReadWrite);
            let att = OperationAttempt::new("att-reject-1").unwrap();
            let mut meta = CloudResponseMetadata::default();
            let err = conn
                .write_with_attempt("INSERT INTO t VALUES (1)", &[], &att, &mut meta)
                .unwrap_err();
            assert_eq!(
                err.adapter_kind(),
                Some(AdapterErrorKind::Protocol),
                "fixture {name} expected Protocol, got {err:?}"
            );
            assert_eq!(
                err.adapter_code(),
                Some(expected_code),
                "fixture {name} expected code {expected_code}, got {:?}",
                err.adapter_code()
            );
            assert_eq!(err.failure_origin(), FailureOrigin::MalformedResponse);
            assert_eq!(err.mutation_outcome(), MutationOutcome::Uncertain);
            handle.join().unwrap();
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn auth_token_and_first_party_api_decapodlabs_ai_dpop_htu_compat() {
        let doc: Value = serde_json::from_str(AUTH_FIXTURES).unwrap();
        let secrets = AuthenticationSecrets::generate().unwrap();

        // 1. Supported additive auth_start & GoTrue v2 token + rejected unknown status / bad expiry.
        let (origin, requests, handle) = spawn_canned_server(vec![
            CannedResponse::json(200, &doc["supported"]["auth_start_additive"]),
            CannedResponse::json(200, &doc["supported"]["gotrue_token_v2_additive"]),
            CannedResponse::json(200, &doc["rejected"]["auth_status_unknown_enum"]),
            CannedResponse::json(200, &doc["rejected"]["token_zero_expiry"]),
            CannedResponse::json(200, &doc["rejected"]["token_excessive_expiry"]),
            CannedResponse::json(200, &doc["rejected"]["auth_start_cross_origin_redirect"]),
        ]);
        let connector = CloudConnector::with_config(SupabaseRpcConfig::v1(&origin, KEY)).unwrap();
        assert_eq!(
            connector
                .start_authentication_with_purpose(
                    "DecapodLabs/dactyl",
                    &secrets,
                    Some("bad purpose")
                )
                .unwrap_err()
                .adapter_kind(),
            Some(AdapterErrorKind::Protocol)
        );
        let start = connector
            .start_authentication_with_purpose_async(
                "DecapodLabs/dactyl",
                &secrets,
                Some("contributor"),
            )
            .await
            .unwrap();
        assert_eq!(start.flow_id, "00000000-0000-4000-8000-000000000146");
        let session = connector
            .exchange_session_async("auth-code-compat", &secrets)
            .await
            .unwrap();
        assert_eq!(session.access_token, "compat-access-token");
        assert!(session.provider_token.is_none());

        assert_eq!(
            connector
                .authentication_status("00000000-0000-4000-8000-000000000146", &secrets)
                .unwrap_err()
                .adapter_kind(),
            Some(AdapterErrorKind::Protocol)
        );
        assert_eq!(
            connector
                .refresh_session("compat-refresh-token")
                .unwrap_err()
                .adapter_kind(),
            Some(AdapterErrorKind::Protocol)
        );
        assert_eq!(
            connector
                .refresh_session("compat-refresh-token")
                .unwrap_err()
                .adapter_kind(),
            Some(AdapterErrorKind::Protocol)
        );
        assert_eq!(
            connector
                .start_authentication("DecapodLabs/dactyl", &secrets)
                .unwrap_err()
                .adapter_kind(),
            Some(AdapterErrorKind::Protocol)
        );
        handle.join().unwrap();
        {
            let captured = requests.lock().unwrap();
            assert_eq!(captured.len(), 6);
            let contributor: Value = serde_json::from_slice(&captured[0].body).unwrap();
            let legacy: Value = serde_json::from_slice(&captured[5].body).unwrap();
            assert_eq!(contributor["purpose"], "contributor");
            assert!(legacy.get("purpose").is_none());
            for key in ["version", "repository", "pkce_challenge", "poll_challenge"] {
                assert_eq!(contributor[key], legacy[key], "authentication field {key}");
            }
            assert!(captured[0].header("authorization").is_none());
        }

        // 2. #138 first-party custom origin (`https://api.decapodlabs.ai`) URLs, DPoP htu
        //    validation, and human-bearer / device-gateway fail-closed gates (no network I/O).
        struct RecordingSigner(Arc<Mutex<Vec<(String, String)>>>);
        impl RequestProofSigner for RecordingSigner {
            fn sign_request(
                &self,
                input: &RequestProofInput<'_>,
            ) -> Result<RequestProof, DactylError> {
                self.0
                    .lock()
                    .unwrap()
                    .push((input.method().to_owned(), input.canonical_url().to_owned()));
                RequestProof::dpop("dpop.api.decapodlabs.ai.proof")
            }
        }
        let recorded_htus = Arc::new(Mutex::new(Vec::new()));
        let signer: Arc<dyn RequestProofSigner> = Arc::new(RecordingSigner(recorded_htus.clone()));

        let first_party = LogicalConnector::cloud_with_config(
            SupabaseRpcConfig::v1(FIRST_PARTY_CLOUD_ORIGIN, KEY).with_device_gateway(),
        )
        .unwrap()
        .with_proof_signer(signer);
        assert_eq!(
            first_party.canonical_auth_url().unwrap(),
            "https://api.decapodlabs.ai/auth"
        );
        assert_eq!(
            first_party
                .canonical_repository_url("DecapodLabs", "dactyl")
                .unwrap(),
            "https://api.decapodlabs.ai/decapod/DecapodLabs/dactyl"
        );

        // Human-bearer-required operation rejects dpc_at_ device token locally before dispatch.
        let device_token_err = first_party
            .account_request(
                AccountOperation::RepositoryEnroll,
                &json!({"version": 1}),
                "dpc_at_device_token",
                None,
            )
            .unwrap_err();
        assert_eq!(
            device_token_err.adapter_kind(),
            Some(AdapterErrorKind::Authentication)
        );
        assert_eq!(
            device_token_err.adapter_code(),
            Some("authentication_required")
        );
        assert_eq!(
            device_token_err.mutation_outcome(),
            MutationOutcome::NotAttempted
        );

        // Device gateway without DPoP proof signer refuses locally without Bearer fallback.
        let unsigned_first_party = LogicalConnector::cloud_with_config(
            SupabaseRpcConfig::v1(FIRST_PARTY_CLOUD_ORIGIN, KEY).with_device_gateway(),
        )
        .unwrap();
        let no_dpop_err = unsigned_first_party
            .control_request(
                "propodus_repository_access_v1",
                &json!({"version": 1}),
                Some("dpc_at_device_token"),
                OperationKind::Read,
                ResourceBudget::v1(),
            )
            .unwrap_err();
        assert_eq!(
            no_dpop_err.adapter_kind(),
            Some(AdapterErrorKind::Authentication)
        );
        assert_eq!(no_dpop_err.adapter_code(), Some("device_proof_required"));
        assert_eq!(
            no_dpop_err.mutation_outcome(),
            MutationOutcome::NotAttempted
        );

        // Verify loopback DPoP htu parity for sync + async device gateway & direct RPC paths.
        let (origin, reqs, handle) = spawn_canned_server(vec![
            CannedResponse::json(200, &json!({"authorized": true})),
            CannedResponse::json(200, &json!({"authorized": true})),
            CannedResponse::json(
                200,
                &json!({"columns": ["v"], "rows": [{"v": 1}], "affected_rows": 0, "generated_keys": []}),
            ),
        ]);
        let gw_conn = LogicalConnector::cloud_with_config(
            SupabaseRpcConfig::v1(&origin, KEY).with_device_gateway(),
        )
        .unwrap()
        .with_proof_signer(Arc::new(RecordingSigner(recorded_htus.clone())));

        assert_eq!(
            gw_conn
                .control_request(
                    "propodus_repository_access_v1",
                    &json!({"version": 1}),
                    Some("dpc_at_dev"),
                    OperationKind::Read,
                    ResourceBudget::v1(),
                )
                .unwrap()["authorized"],
            json!(true)
        );
        assert_eq!(
            gw_conn
                .control_request_async(
                    "propodus_repository_access_v1",
                    &json!({"version": 1}),
                    Some("dpc_at_dev"),
                    OperationKind::Read,
                    ResourceBudget::v1(),
                )
                .await
                .unwrap()["authorized"],
            json!(true)
        );
        let db = gw_conn
            .open(
                OpenOptions::default(),
                Some(context_v1()),
                Some("dpc_at_dev".into()),
            )
            .unwrap();
        assert_eq!(
            db.read_async("SELECT 1 AS v", &[])
                .await
                .unwrap()
                .as_slice()[0]
                .get_int("v")
                .unwrap(),
            1
        );
        handle.join().unwrap();

        let htus = recorded_htus.lock().unwrap();
        assert_eq!(
            htus.as_slice(),
            &[
                (
                    "POST".to_owned(),
                    format!("{origin}/functions/v1/decapod-onboarding/cloud-control")
                ),
                (
                    "POST".to_owned(),
                    format!("{origin}/functions/v1/decapod-onboarding/cloud-control")
                ),
                (
                    "POST".to_owned(),
                    format!("{origin}/functions/v1/decapod-onboarding/cloud-query")
                ),
            ]
        );
        let wire_reqs = reqs.lock().unwrap();
        assert_eq!(
            wire_reqs[0].header("authorization"),
            Some("DPoP dpc_at_dev")
        );
        assert_eq!(
            wire_reqs[0].header("dpop"),
            Some("dpop.api.decapodlabs.ai.proof")
        );
    }
}
