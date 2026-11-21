//! Offline connector and managed-auth transport contract, without hosted claims.
#![cfg(feature = "supabase")]

use dactyl_db::{
    AccountOperation, AdapterErrorKind, AuthenticationSecrets, AuthenticationStart, Backend,
    CloudConnector, CloudResponseMetadata, Connection, Datastore, DatastoreRoute, MutationOutcome,
    OperationKind, RequestProof, RequestProofInput, RequestProofSigner, ResourceBudget,
    ScmCredential, ScmProvider, StorageContext, SupabaseRpcConfig, UserSession,
    DEFAULT_CLOUD_ORIGIN,
};
use serde_json::{json, Value};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::Duration;

const KEY: &str = "sb_publishable_fixture";
const FLOW: &str = "00000000-0000-4000-8000-000000000001";

fn server(
    responses: Vec<(u16, Value)>,
) -> (
    String,
    mpsc::Receiver<(String, Value)>,
    thread::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let (tx, rx) = mpsc::channel();
    let address = origin.clone();
    let handle = thread::spawn(move || {
        for (status, response) in responses {
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
            let body = if length == 0 {
                Value::Null
            } else {
                serde_json::from_slice(&bytes[offset..offset + length]).unwrap()
            };
            tx.send((headers, body)).unwrap();
            if status == 0 {
                continue;
            }
            let body = response.to_string().replace("ORIGIN", &address);
            let extra = if status == 302 {
                "Location: http://127.0.0.1:9/stolen\r\n"
            } else {
                ""
            };
            write!(stream,"HTTP/1.1 {status} Fixture\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n{extra}\r\n{body}",body.len()).ok();
        }
        listener.set_nonblocking(true).unwrap();
        thread::sleep(Duration::from_millis(100));
        assert!(
            listener.accept().is_err(),
            "unexpected redirect or automatic replay"
        );
    });
    (origin, rx, handle)
}

fn profile() -> Value {
    json!({"version":1,"project_origin":"ORIGIN","publishable_key":KEY,"schema":"dactyl_api"})
}

#[test]
fn account_denial_preserves_only_the_public_code() {
    for (status, code, kind) in [
        (403, "seat_required", AdapterErrorKind::Authorization),
        (503, "billing_not_configured", AdapterErrorKind::Unavailable),
    ] {
        let (origin, received, handle) = server(vec![(
            status,
            json!({"error":{"code":code,"message":"private-provider-secret"}}),
        )]);
        let error = connector(&origin)
            .account_request(
                dactyl_db::AccountOperation::Status,
                &json!({"version":1}),
                "user-bearer",
                None,
            )
            .unwrap_err();
        assert_eq!(error.adapter_kind(), Some(kind));
        assert_eq!(error.adapter_code(), Some(code));
        assert!(!format!("{error:?}{error}").contains("private-provider-secret"));
        received.recv().unwrap();
        handle.join().unwrap();
    }
}
fn connector(origin: &str) -> CloudConnector {
    CloudConnector::with_config(SupabaseRpcConfig::v1(origin, KEY)).unwrap()
}
fn session() -> Value {
    json!({"access_token":"user-access","refresh_token":"user-refresh","expires_in":900})
}

fn header<'a>(headers: &'a str, name: &str) -> Option<&'a str> {
    headers.lines().find_map(|line| {
        let (key, value) = line.split_once(':')?;
        key.eq_ignore_ascii_case(name).then(|| value.trim())
    })
}

#[test]
fn account_routes_forward_opaque_policy_and_keep_scm_out_of_auth_and_data() {
    let operations = [
        (AccountOperation::Register, "account-register"),
        (AccountOperation::Status, "account-status"),
        (AccountOperation::Quote, "billing-quote"),
        (AccountOperation::Checkout, "billing-checkout"),
        (AccountOperation::Manage, "billing-manage"),
        (AccountOperation::Portal, "billing-portal"),
        (
            AccountOperation::BillingOveragePolicy,
            "billing-overage-policy",
        ),
        (AccountOperation::DeviceChallenge, "device-challenge"),
        (AccountOperation::DeviceEnroll, "device-enroll"),
        (AccountOperation::DeviceKeys, "device-keys"),
        (AccountOperation::DeviceSessions, "device-sessions"),
        (AccountOperation::DeviceSession, "device-session"),
        (AccountOperation::DeviceRefresh, "device-refresh"),
        (AccountOperation::DeviceStepUp, "device-step-up"),
        (AccountOperation::DeviceRevoke, "device-revoke"),
    ];
    let result =
        json!({"backend_authorized":false,"future":{"opaque":[1,false,"provider-policy"]}});
    let mut responses = vec![(200, result.clone()); operations.len() + 1];
    responses.push((200, session()));
    responses.push((
        200,
        json!({"columns":[],"rows":[],"affected_rows":0,"generated_keys":[]}),
    ));
    let (origin, rx, handle) = server(responses);
    let connector = connector(&origin);
    let github = ScmCredential::github("scm-credential").unwrap();
    let gitlab = ScmCredential::gitlab("scm-credential").unwrap();
    assert_eq!(github.provider(), ScmProvider::GitHub);
    assert_eq!(github.host(), "github.com");
    assert_eq!(gitlab.provider(), ScmProvider::GitLab);
    assert_eq!(gitlab.host(), "gitlab.com");
    assert!(!format!("{github:?}{gitlab:?}").contains("scm-credential"));
    let payload =
        json!({"version":71,"future":{"scm":{"provider":"gitlab","opaque":[1,null,false]}}});
    for (index, (operation, _)) in operations.iter().enumerate() {
        let scm = if index % 2 == 0 { &github } else { &gitlab };
        assert_eq!(
            connector
                .account_request(*operation, &payload, "user-access", Some(scm))
                .unwrap(),
            result
        );
    }
    connector
        .account_request(AccountOperation::Status, &payload, "user-access", None)
        .unwrap();
    connector.refresh_session("user-refresh").unwrap();
    Connection::open_with_context(
        connector.route(Some("user-access".into())),
        Some(StorageContext::new(1, json!({"opaque":true})).unwrap()),
    )
    .unwrap()
    .read("select fixture", &[])
    .unwrap();
    let requests: Vec<_> = (0..operations.len() + 3)
        .map(|_| rx.recv().unwrap())
        .collect();
    for (index, ((_, action), (headers, body))) in operations.iter().zip(&requests).enumerate() {
        assert!(headers.starts_with(&format!("POST /functions/v1/decapod-onboarding/{action} ")));
        assert_eq!(header(headers, "authorization"), Some("Bearer user-access"));
        assert_eq!(header(headers, "x-scm-token"), Some("scm-credential"));
        let (provider, host) = if index % 2 == 0 {
            ("github", "github.com")
        } else {
            ("gitlab", "gitlab.com")
        };
        assert_eq!(header(headers, "x-scm-provider"), Some(provider));
        assert_eq!(header(headers, "x-scm-host"), Some(host));
        assert_eq!(header(headers, "apikey"), Some(KEY));
        assert_eq!(*body, payload);
        assert!(!body.to_string().contains("scm-credential"));
    }
    for (headers, _) in &requests[operations.len()..] {
        for name in ["x-scm-token", "x-scm-provider", "x-scm-host"] {
            assert_eq!(header(headers, name), None);
        }
    }
    assert_eq!(
        header(&requests[operations.len() + 1].0, "authorization"),
        None
    );
    assert!(requests[operations.len() + 2]
        .0
        .starts_with("POST /rest/v1/rpc/dactyl_query_v1 "));
    assert_eq!(
        header(&requests[operations.len() + 2].0, "authorization"),
        Some("Bearer user-access")
    );
    assert!(!requests[operations.len() + 2]
        .1
        .to_string()
        .contains("user-refresh"));
    handle.join().unwrap();
}

#[test]
fn account_failures_redact_credentials_and_never_redirect_or_replay() {
    for (status, kind) in [
        (302, AdapterErrorKind::Protocol),
        (0, AdapterErrorKind::Unavailable),
        (403, AdapterErrorKind::Authorization),
        (500, AdapterErrorKind::Unavailable),
    ] {
        let (origin, rx, handle) = server(vec![(
            status,
            json!({"message":"user-access scm-credential sb_publishable_fixture private-payload"}),
        )]);
        let scm = ScmCredential::github("scm-credential").unwrap();
        let error = connector(&origin)
            .account_request(
                AccountOperation::Checkout,
                &json!({"opaque":"private-payload"}),
                "user-access",
                Some(&scm),
            )
            .unwrap_err();
        assert_eq!(error.adapter_kind(), Some(kind));
        let diagnostic = format!("{error} {error:?}");
        for secret in ["user-access", "scm-credential", KEY, "private-payload"] {
            assert!(!diagnostic.contains(secret));
        }
        rx.recv().unwrap();
        handle.join().unwrap();
    }
}

#[test]
fn account_credentials_and_body_are_validated_before_network_io() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let connector = connector(&format!("http://{}", listener.local_addr().unwrap()));
    for invalid in [
        String::new(),
        "has space".into(),
        "injected\r\nheader: value".into(),
        "x".repeat(16385),
    ] {
        let error = connector
            .account_request(AccountOperation::Register, &json!({}), &invalid, None)
            .unwrap_err();
        assert_eq!(error.adapter_kind(), Some(AdapterErrorKind::Protocol));
        for invalid_scm in [
            ScmCredential::github(invalid.clone()),
            ScmCredential::gitlab(invalid.clone()),
        ] {
            let error = invalid_scm.unwrap_err();
            assert_eq!(error.adapter_kind(), Some(AdapterErrorKind::Protocol));
        }
    }
    let error = connector
        .account_request(
            AccountOperation::Register,
            &json!({"opaque":"x".repeat(16384)}),
            "user-access",
            None,
        )
        .unwrap_err();
    assert_eq!(error.adapter_kind(), Some(AdapterErrorKind::Protocol));
    assert!(listener.accept().is_err());
}

#[test]
fn sign_in_provider_token_stays_out_of_normal_account_requests_and_refresh_may_omit_it() {
    let mut issued = session();
    issued["provider_token"] = json!("google-sign-in-provider-credential");
    let (origin, rx, handle) = server(vec![
        (200, issued),
        (200, json!({"opaque":"registration"})),
        (200, json!({"opaque":"account-status"})),
        (200, session()),
    ]);
    let connector = connector(&origin);
    let secrets = AuthenticationSecrets::generate().unwrap();
    let user = connector
        .exchange_session("one-time-code", &secrets)
        .unwrap();
    assert_eq!(
        user.provider_token.as_deref(),
        Some("google-sign-in-provider-credential")
    );
    assert!(!format!("{user:?}").contains("google-sign-in-provider-credential"));
    let stored = serde_json::to_value(&user).unwrap();
    let restored: UserSession = serde_json::from_value(stored).unwrap();
    assert_eq!(restored.provider_token, user.provider_token);
    for operation in [AccountOperation::Register, AccountOperation::Status] {
        connector
            .account_request(operation, &json!({"version":1}), &user.access_token, None)
            .unwrap();
    }
    let refreshed = connector.refresh_session(&user.refresh_token).unwrap();
    assert!(refreshed.provider_token.is_none());
    assert!(serde_json::to_value(&refreshed)
        .unwrap()
        .get("provider_token")
        .is_none());
    let legacy: UserSession = serde_json::from_value(
        json!({"access_token":"old-access","refresh_token":"old-refresh","expires_at_unix":900}),
    )
    .unwrap();
    assert!(legacy.provider_token.is_none());
    for index in 0..4 {
        let (headers, body) = rx.recv().unwrap();
        for name in ["x-scm-token", "x-scm-provider", "x-scm-host"] {
            assert_eq!(header(&headers, name), None);
        }
        if index == 1 || index == 2 {
            assert_eq!(
                header(&headers, "authorization"),
                Some("Bearer user-access")
            );
        }
        assert!(!body
            .to_string()
            .contains("google-sign-in-provider-credential"));
    }
    handle.join().unwrap();
}

#[test]
fn malformed_provider_token_is_rejected_without_echoing_it() {
    let mut issued = session();
    issued["provider_token"] = json!("private malformed credential");
    let (origin, rx, handle) = server(vec![(200, issued)]);
    let error = connector(&origin)
        .refresh_session("user-refresh")
        .unwrap_err();
    assert_eq!(error.adapter_kind(), Some(AdapterErrorKind::Protocol));
    assert!(!format!("{error} {error:?}").contains("private malformed credential"));
    rx.recv().unwrap();
    handle.join().unwrap();
}

#[test]
fn handoff_rejects_ambiguous_tickets_and_malformed_origin_metadata() {
    let connector = connector("https://project.example");
    let valid = format!(
        "https://project.example/functions/v1/decapod-onboarding/begin?flow_id={FLOW}&ticket=one"
    );
    let legacy_valid = format!(
        "https://project.example/functions/v1/propodus-onboarding/begin?flow_id={FLOW}&ticket=one"
    );
    connector
        .validate_start(&AuthenticationStart {
            flow_id: FLOW.into(),
            browser_url: valid.clone(),
            expires_at: "2099-01-01T00:00:00Z".into(),
        })
        .unwrap();
    connector
        .validate_start(&AuthenticationStart {
            flow_id: FLOW.into(),
            browser_url: legacy_valid,
            expires_at: "2099-01-01T00:00:00Z".into(),
        })
        .unwrap();
    for browser_url in [
        format!("{valid}&ticket="),
        format!("{valid}&ticket=two"),
        format!("{valid}&flow_id={FLOW}"),
        format!("{valid}#fragment"),
        valid.replace("https://project.example", "https://other.example"),
        valid.replace(
            "https://project.example",
            "https://user:password@project.example",
        ),
        valid.replace("/begin?", "/exchange?"),
    ] {
        let error = connector
            .validate_start(&AuthenticationStart {
                flow_id: FLOW.into(),
                browser_url,
                expires_at: "2099-01-01T00:00:00Z".into(),
            })
            .unwrap_err();
        assert_eq!(error.adapter_kind(), Some(AdapterErrorKind::Protocol));
        assert!(!error.to_string().contains("ticket=one"));
    }
}

#[test]
fn cloud_discovers_its_connector_without_sending_user_credentials() {
    let (origin, rx, handle) = server(vec![(200, profile())]);
    let route =
        DatastoreRoute::for_backend(Backend::Cloud, &origin, Some("user-bearer".into())).unwrap();
    assert_eq!(route.datastore(), Datastore::Supabase);
    assert!(route.supabase_rpc_config().is_some());
    assert_eq!(route.token(), Some("user-bearer"));
    let (headers, body) = rx.recv().unwrap();
    assert!(headers.starts_with("GET /functions/v1/decapod-onboarding/config "));
    assert!(!headers.to_lowercase().contains("authorization:"));
    assert!(!headers.to_lowercase().contains("apikey:"));
    assert_eq!(body, Value::Null);
    handle.join().unwrap();
    let local = DatastoreRoute::for_backend(Backend::Local, ":memory:", None).unwrap();
    assert_eq!(local.datastore(), Datastore::Sqlite);
    assert!(DatastoreRoute::for_backend(Backend::Local, ":memory:", Some("token".into())).is_err());
}

#[test]
fn managed_auth_uses_one_origin_and_keeps_refresh_out_of_data_requests() {
    let (origin, rx, handle) = server(vec![
        (
            200,
            json!({"flow_id":FLOW,"browser_url":format!("ORIGIN/functions/v1/decapod-onboarding/begin?flow_id={FLOW}&ticket=ticket"),"expires_at":"2099-01-01T00:00:00Z"}),
        ),
        (200, json!({"status":"ready"})),
        (200, json!({"auth_code":"one-time-code"})),
        (200, session()),
        (
            200,
            json!({"status":"complete","backend_authorized":false,"account_status":"seat_required"}),
        ),
        (200, session()),
    ]);
    let connector = connector(&origin);
    let secrets = AuthenticationSecrets::generate().unwrap();
    let flow = connector
        .start_authentication("owner/repository", &secrets)
        .unwrap();
    assert_eq!(
        connector
            .authentication_status(&flow.flow_id, &secrets)
            .unwrap(),
        "ready"
    );
    let code = connector
        .exchange_authentication_code(&flow.flow_id, &secrets)
        .unwrap();
    let user = connector.exchange_session(&code, &secrets).unwrap();
    connector
        .complete_authentication(&flow.flow_id, &secrets, &user.access_token)
        .unwrap();
    let refreshed = connector.refresh_session(&user.refresh_token).unwrap();
    assert!(refreshed.expires_at_unix > 900);
    let requests: Vec<_> = (0..6).map(|_| rx.recv().unwrap()).collect();
    for (headers, _) in &requests {
        assert!(headers.contains(&format!("apikey: {KEY}")));
    }
    assert!(requests[0].1["pkce_challenge"].as_str().unwrap().len() == 43);
    assert!(requests[0].1.get("code_verifier").is_none());
    assert!(requests[3]
        .0
        .starts_with("POST /auth/v1/token?grant_type=pkce "));
    assert_eq!(requests[3].1["auth_code"], "one-time-code");
    assert_ne!(
        requests[3].1["code_verifier"],
        requests[0].1["pkce_challenge"]
    );
    assert!(requests[4].0.contains("authorization: Bearer user-access"));
    assert!(requests[5]
        .0
        .starts_with("POST /auth/v1/token?grant_type=refresh_token "));
    assert_eq!(requests[5].1, json!({"refresh_token":"user-refresh"}));
    assert!(!requests[5].0.to_lowercase().contains("authorization:"));
    assert!(!format!("{secrets:?}{user:?}{flow:?}{connector:?}").contains("user-refresh"));
    assert!(!format!("{flow:?}").contains("ticket"));
    handle.join().unwrap();
}

#[test]
fn profile_rejects_wrong_origin_version_and_secret_keys() {
    for body in [
        json!({"version":2,"project_origin":"ORIGIN","publishable_key":KEY,"schema":"dactyl_api"}),
        json!({"version":1,"project_origin":"https://other.example","publishable_key":KEY,"schema":"dactyl_api"}),
        json!({"version":1,"project_origin":"ORIGIN","publishable_key":"sb_secret_do-not-use","schema":"dactyl_api"}),
    ] {
        let (origin, rx, handle) = server(vec![(200, body)]);
        let error = CloudConnector::discover(&origin).unwrap_err();
        assert!(!error.to_string().contains("sb_secret_do-not-use"));
        rx.recv().unwrap();
        handle.join().unwrap();
    }
}

#[test]
fn redirects_and_lost_refresh_responses_are_not_replayed() {
    for status in [302, 0, 401] {
        let (origin, rx, handle) = server(vec![(status, json!({"message":"user-refresh"}))]);
        let error = connector(&origin)
            .refresh_session("user-refresh")
            .unwrap_err();
        assert!(!error.to_string().contains("user-refresh"));
        if status == 401 {
            assert_eq!(error.adapter_kind(), Some(AdapterErrorKind::Authentication));
        }
        rx.recv().unwrap();
        handle.join().unwrap();
    }
}

#[tokio::test]
async fn auth_worker_is_safe_inside_an_async_runtime() {
    let (origin, rx, handle) = server(vec![(200, session())]);
    connector(&origin).refresh_session("user-refresh").unwrap();
    rx.recv().unwrap();
    handle.join().unwrap();
}

fn server_with_headers(
    responses: Vec<(u16, String, Value)>,
) -> (
    String,
    mpsc::Receiver<(String, Value)>,
    thread::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let (tx, rx) = mpsc::channel();
    let address = origin.clone();
    let handle = thread::spawn(move || {
        for (status, extra_headers, response) in responses {
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
            let body = if length == 0 {
                Value::Null
            } else {
                serde_json::from_slice(&bytes[offset..offset + length]).unwrap()
            };
            tx.send((headers, body)).unwrap();
            if status == 0 {
                continue;
            }
            let body = response.to_string().replace("ORIGIN", &address);
            write!(
                stream,
                "HTTP/1.1 {status} Fixture\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n{extra_headers}\r\n{body}",
                body.len()
            )
            .ok();
        }
        listener.set_nonblocking(true).unwrap();
        thread::sleep(Duration::from_millis(100));
        assert!(
            listener.accept().is_err(),
            "unexpected redirect or automatic replay"
        );
    });
    (origin, rx, handle)
}

type RecordedProofCall = (String, String, Option<String>, Option<String>);

#[derive(Default)]
struct RecordingProofSigner {
    calls: Mutex<Vec<RecordedProofCall>>,
    observed_nonces: Mutex<Vec<(String, String)>>,
    bearer_mode: bool,
}

impl RequestProofSigner for RecordingProofSigner {
    fn sign_request(
        &self,
        input: &RequestProofInput<'_>,
    ) -> Result<RequestProof, dactyl_db::DactylError> {
        let debug_input = format!("{input:?}");
        assert!(!debug_input.contains("http://"));
        assert!(!debug_input.contains("https://"));
        if let Some(ath) = input.access_token_sha256() {
            assert!(!debug_input.contains(ath));
        }
        if let Some(nonce) = input.nonce() {
            assert!(!debug_input.contains(nonce));
        }
        self.calls.lock().unwrap().push((
            input.method().to_owned(),
            input.canonical_url().to_owned(),
            input.access_token_sha256().map(str::to_owned),
            input.nonce().map(str::to_owned),
        ));
        if self.bearer_mode {
            return Ok(RequestProof::bearer());
        }
        let observed = self
            .observed_nonces
            .lock()
            .unwrap()
            .last()
            .map(|(_, n)| n.clone());
        let nonce_part = input.nonce().or(observed.as_deref()).unwrap_or("none");
        let proof = format!(
            "dpop-proof.{}.{}.{}",
            input.method(),
            input.calls_len_tag(),
            nonce_part
        );
        let built = RequestProof::dpop(proof)?;
        assert!(!format!("{built:?}").contains("dpop-proof."));
        Ok(built)
    }

    fn observe_nonce(&self, canonical_url: &str, nonce: &str) {
        self.observed_nonces
            .lock()
            .unwrap()
            .push((canonical_url.to_owned(), nonce.to_owned()));
    }
}

trait InputTag {
    fn calls_len_tag(&self) -> usize;
}

impl InputTag for RequestProofInput<'_> {
    fn calls_len_tag(&self) -> usize {
        self.canonical_url().len()
    }
}

#[test]
fn discovery_and_account_fall_back_on_404_and_capture_redacted_cloud_headers() {
    let extra = "Decapod-Cloud-Region: us-west\r\nDecapod-Cloud-Release: 2026.11\r\nSunset: Wed, 31 Dec 2026 23:59:59 GMT\r\nDeprecation: true\r\n";
    let (origin, rx, handle) = server_with_headers(vec![
        (404, String::new(), json!({"error": "not_found"})),
        (200, extra.into(), profile()),
        (200, extra.into(), json!({"backend_authorized": true})),
    ]);
    let (connector, discovery_meta) = CloudConnector::discover_with_metadata(&origin).unwrap();
    assert_eq!(
        discovery_meta.sunset(),
        Some("Wed, 31 Dec 2026 23:59:59 GMT")
    );
    assert_eq!(discovery_meta.deprecation(), Some("true"));
    assert_eq!(
        discovery_meta.cloud_header("Decapod-Cloud-Region"),
        Some("us-west")
    );
    assert_eq!(
        discovery_meta.cloud_header("decapod-cloud-release"),
        Some("2026.11")
    );
    let meta_debug = format!("{discovery_meta:?}");
    assert!(!meta_debug.contains("us-west"));
    assert!(!meta_debug.contains("2026.11"));

    let mut account_meta = CloudResponseMetadata::default();
    let res = connector
        .account_request_with_metadata(
            AccountOperation::Status,
            &json!({"version": 1}),
            "user-bearer",
            None,
            None,
            &mut account_meta,
        )
        .unwrap();
    assert_eq!(res["backend_authorized"], true);
    assert_eq!(
        account_meta.cloud_header("decapod-cloud-region"),
        Some("us-west")
    );

    let (first_disc, _) = rx.recv().unwrap();
    let (second_disc, _) = rx.recv().unwrap();
    let (account_req, _) = rx.recv().unwrap();
    assert!(first_disc.starts_with("GET /functions/v1/decapod-onboarding/config "));
    assert!(second_disc.starts_with("GET /functions/v1/propodus-onboarding/config "));
    // Discovered legacy prefix is remembered for subsequent onboarding requests.
    assert!(account_req.starts_with("POST /functions/v1/propodus-onboarding/account-status "));
    handle.join().unwrap();

    // Direct with_config falls back on generic 404 (and re-signs canonical_url),
    // but never falls back on domain-coded 404 (e.g. project_not_found) or 403.
    let signer = Arc::new(RecordingProofSigner::default());
    let (origin, rx, handle) = server_with_headers(vec![
        (
            404,
            String::new(),
            json!({"message": "Requested function was not found"}),
        ),
        (200, String::new(), json!({"ok": true})),
        (
            404,
            String::new(),
            json!({"error": {"code": "project_not_found"}}),
        ),
    ]);
    let direct = connector_with_signer(&origin, signer.clone());
    assert_eq!(
        direct
            .account_request(
                AccountOperation::Status,
                &json!({"version": 1}),
                "user-bearer",
                None
            )
            .unwrap()["ok"],
        true
    );
    let domain_err = direct
        .account_request(
            AccountOperation::Status,
            &json!({"version": 1}),
            "user-bearer",
            None,
        )
        .unwrap_err();
    assert_eq!(domain_err.adapter_kind(), Some(AdapterErrorKind::NotFound));
    assert_eq!(domain_err.adapter_code(), Some("project_not_found"));

    let (r1, _) = rx.recv().unwrap();
    let (r2, _) = rx.recv().unwrap();
    let (r3, _) = rx.recv().unwrap();
    assert!(r1.starts_with("POST /functions/v1/decapod-onboarding/account-status "));
    assert!(r2.starts_with("POST /functions/v1/propodus-onboarding/account-status "));
    assert!(r3.starts_with("POST /functions/v1/decapod-onboarding/account-status "));
    let calls = signer.calls.lock().unwrap();
    assert!(calls[0]
        .1
        .ends_with("/functions/v1/decapod-onboarding/account-status"));
    assert!(calls[1]
        .1
        .ends_with("/functions/v1/propodus-onboarding/account-status"));
    handle.join().unwrap();
}

fn connector_with_signer(origin: &str, signer: Arc<dyn RequestProofSigner>) -> CloudConnector {
    CloudConnector::with_config(SupabaseRpcConfig::v1(origin, KEY))
        .unwrap()
        .with_proof_signer(signer)
}

#[test]
fn dpop_proof_signer_binds_all_surfaces_handles_nonces_and_redacts_challenges() {
    let signer = Arc::new(RecordingProofSigner::default());
    let sink = TcpListener::bind("127.0.0.1:0").unwrap();
    let location = format!("Location: http://{}/stolen\r\n", sink.local_addr().unwrap());

    let (origin, rx, handle) = server_with_headers(vec![
        (
            401,
            "DPoP-Nonce: server-nonce-1\r\n".into(),
            json!({"error": "use_dpop_nonce", "message": "secret-nonce-hint"}),
        ),
        (200, String::new(), json!({"enrolled": true})),
        (200, String::new(), json!({"decision": "allow"})),
        (
            200,
            String::new(),
            json!({"columns": ["ok"], "rows": [{"ok": true}], "affected_rows": 0, "generated_keys": []}),
        ),
        (307, location, json!({})),
    ]);
    let client = connector_with_signer(&origin, signer.clone());

    // 1. Account request hits 401 use_dpop_nonce, surfaces nonce to signer, retries once automatically,
    //    and updates metadata from the final 200 response.
    let mut challenge_meta = CloudResponseMetadata::default();
    let enrolled = client
        .account_request_with_metadata(
            AccountOperation::DeviceEnroll,
            &json!({"version": 1, "jwk_thumbprint": "thumb-1"}),
            "dpop-access-token",
            None,
            None,
            &mut challenge_meta,
        )
        .unwrap();
    assert_eq!(enrolled["enrolled"], true);
    assert_eq!(challenge_meta.dpop_nonce(), None);
    let observed = signer.observed_nonces.lock().unwrap();
    assert_eq!(observed.len(), 1);
    assert_eq!(observed[0].1, "server-nonce-1");
    drop(observed);

    // 2. control_request carries DPoP headers.
    let ctrl = client
        .control_request(
            "propodus_repository_access_v1",
            &json!({"version": 1}),
            Some("dpop-access-token"),
            dactyl_db::OperationKind::Read,
            dactyl_db::ResourceBudget::v1(),
        )
        .unwrap();
    assert_eq!(ctrl["decision"], "allow");

    // 3. DatastoreRoute::supabase_rpc via CloudConnector::route inherits the proof signer!
    let db = Connection::open_with_context(
        client.route(Some("dpop-access-token".into())),
        Some(StorageContext::new(1, json!({"opaque": true})).unwrap()),
    )
    .unwrap();
    let rows = db.read("SELECT 1 AS ok", &[]).unwrap();
    assert!(rows.as_slice()[0].get_bool("ok").unwrap());

    // 4. Redirect never forwards DPoP proof or Authorization: DPoP!
    let redirect_err = client
        .account_request(
            AccountOperation::DeviceRevoke,
            &json!({"version": 1}),
            "dpop-access-token",
            None,
        )
        .unwrap_err();
    assert_eq!(
        redirect_err.adapter_kind(),
        Some(AdapterErrorKind::Protocol)
    );
    assert_eq!(redirect_err.adapter_code(), Some("outcome_unknown"));
    assert_eq!(redirect_err.mutation_outcome(), MutationOutcome::Uncertain);

    for index in 0..5 {
        let (headers, _) = rx.recv().unwrap();
        assert_eq!(
            header(&headers, "authorization"),
            Some("DPoP dpop-access-token")
        );
        let dpop_hdr = header(&headers, "dpop").expect("DPoP header missing");
        if index == 1 {
            assert!(dpop_hdr.ends_with(".server-nonce-1"));
        }
    }
    let calls = signer.calls.lock().unwrap();
    assert_eq!(calls.len(), 5);
    assert_eq!(calls[0].0, "POST");
    assert!(calls[0]
        .1
        .ends_with("/functions/v1/decapod-onboarding/device-enroll"));
    assert_eq!(calls[1].3.as_deref(), Some("server-nonce-1"));
    assert!(calls[2]
        .1
        .ends_with("/rest/v1/rpc/propodus_repository_access_v1"));
    assert!(calls[3].1.ends_with("/rest/v1/rpc/dactyl_query_v1"));
    // All calls computed RFC 9449 ath SHA-256 digest of "dpop-access-token".
    assert!(calls.iter().all(|c| c.2.is_some()));
    sink.set_nonblocking(true).unwrap();
    assert!(sink.accept().is_err());
    handle.join().unwrap();

    // 5. Explicit Bearer compatibility mode via RequestProof::bearer() sends Authorization: Bearer.
    let bearer_signer = Arc::new(RecordingProofSigner {
        bearer_mode: true,
        ..Default::default()
    });
    let (origin, rx, handle) = server(vec![(200, json!({"ok": true}))]);
    let bearer_client = connector_with_signer(&origin, bearer_signer);
    bearer_client
        .account_request(
            AccountOperation::DeviceChallenge,
            &json!({"version": 1}),
            "dpop-access-token",
            None,
        )
        .unwrap();
    let (headers, _) = rx.recv().unwrap();
    assert_eq!(
        header(&headers, "authorization"),
        Some("Bearer dpop-access-token")
    );
    assert_eq!(header(&headers, "dpop"), None);
    handle.join().unwrap();
}

#[test]
fn dpop_rejections_and_http_error_matrix_map_deterministically_without_leaks() {
    for code in [
        "stolen_bearer",
        "dpop_proof_required",
        "missing_dpop_proof",
        "dpop_replay",
        "dpop_clock_skew",
        "invalid_dpop_proof",
        "invalid_binding",
    ] {
        let (origin, rx, handle) = server(vec![(
            401,
            json!({"error": {"code": code, "message": "raw-dpop-secret"}}),
        )]);
        let err = connector(&origin)
            .account_request(
                AccountOperation::DeviceStepUp,
                &json!({"version": 1}),
                "user-bearer-secret",
                None,
            )
            .unwrap_err();
        assert_eq!(err.adapter_kind(), Some(AdapterErrorKind::Authentication));
        assert_eq!(err.adapter_code(), Some(code));
        assert_ne!(err.mutation_outcome(), MutationOutcome::Uncertain);
        let rendered = format!("{err} {err:?}");
        assert!(!rendered.contains("raw-dpop-secret"));
        assert!(!rendered.contains("user-bearer-secret"));
        rx.recv().unwrap();
        handle.join().unwrap();
    }

    // Account HTTP 401/403/409/413/429/5xx outcome matrix:
    for (op, status, code, expected_kind, expected_uncertain) in [
        (
            AccountOperation::DeviceEnroll,
            401,
            "invalid_token",
            AdapterErrorKind::Authentication,
            false,
        ),
        (
            AccountOperation::DeviceEnroll,
            403,
            "authorization_denied",
            AdapterErrorKind::Authorization,
            false,
        ),
        (
            AccountOperation::DeviceEnroll,
            409,
            "custody_conflict",
            AdapterErrorKind::Conflict,
            false,
        ),
        (
            AccountOperation::DeviceEnroll,
            413,
            "payload_too_large",
            AdapterErrorKind::Quota,
            false,
        ),
        (
            AccountOperation::DeviceEnroll,
            429,
            "rate_limited",
            AdapterErrorKind::RateLimited,
            false,
        ),
        (
            AccountOperation::DeviceEnroll,
            503,
            "provider_unavailable",
            AdapterErrorKind::Unavailable,
            true,
        ),
        (
            AccountOperation::DeviceChallenge,
            503,
            "provider_unavailable",
            AdapterErrorKind::Unavailable,
            false,
        ),
    ] {
        let (origin, rx, handle) = server(vec![(
            status,
            json!({"error": {"code": code, "message": "secret-backend-info"}}),
        )]);
        let err = connector(&origin)
            .account_request(op, &json!({"version": 1}), "user-bearer", None)
            .unwrap_err();
        assert_eq!(err.adapter_kind(), Some(expected_kind));
        assert_eq!(
            err.mutation_outcome() == MutationOutcome::Uncertain,
            expected_uncertain
        );
        if expected_uncertain {
            assert_eq!(err.adapter_code(), Some("outcome_unknown"));
        } else {
            assert_eq!(err.adapter_code(), Some(code));
        }
        assert!(!format!("{err} {err:?}").contains("secret-backend-info"));
        rx.recv().unwrap();
        handle.join().unwrap();
    }

    // Invalid proof string fails locally before dispatch (NotAttempted).
    assert_eq!(
        RequestProof::dpop("bad proof\nheader")
            .unwrap_err()
            .mutation_outcome(),
        MutationOutcome::NotAttempted
    );
    assert_eq!(
        RequestProof::dpop("").unwrap_err().mutation_outcome(),
        MutationOutcome::NotAttempted
    );
}

#[test]
fn billing_overage_policy_quota_codes_and_retry_after_are_preserved_with_default_origin() {
    assert_eq!(
        DEFAULT_CLOUD_ORIGIN,
        "https://goyxpcmxetztjlrrcwew.supabase.co"
    );
    assert!(AccountOperation::BillingOveragePolicy.is_mutating());
    assert!(!AccountOperation::DeviceKeys.is_mutating());
    assert!(!AccountOperation::DeviceSessions.is_mutating());
    assert!(AccountOperation::DeviceSession.is_mutating());
    assert!(AccountOperation::DeviceRefresh.is_mutating());

    for (status, code, retry_after, expected_kind) in [
        (429, "quota_exceeded", Some("45"), AdapterErrorKind::Quota),
        (
            429,
            "overage_cap_exhausted",
            Some("120"),
            AdapterErrorKind::Quota,
        ),
        (
            429,
            "rate_limited",
            Some("15"),
            AdapterErrorKind::RateLimited,
        ),
        (
            403,
            "provider_disabled",
            None,
            AdapterErrorKind::Authorization,
        ),
        (
            403,
            "mfa_step_up_required",
            None,
            AdapterErrorKind::Authorization,
        ),
        (
            403,
            "repo_access_denied",
            None,
            AdapterErrorKind::Authorization,
        ),
        (
            401,
            "device_key_expired",
            None,
            AdapterErrorKind::Authentication,
        ),
        (
            401,
            "refresh_reuse_detected",
            None,
            AdapterErrorKind::Authentication,
        ),
    ] {
        let extra = retry_after
            .map(|value| format!("Retry-After: {value}\r\n"))
            .unwrap_or_default();
        let (origin, rx, handle) = server_with_headers(vec![(
            status,
            extra,
            json!({"error": {"code": code, "message": "raw-internal-billing-msg"}}),
        )]);
        let mut meta = CloudResponseMetadata::default();
        let err = connector(&origin)
            .account_request_with_metadata(
                AccountOperation::BillingOveragePolicy,
                &json!({"version": 1, "organization_id": "org-1", "enabled": true}),
                "user-access",
                None,
                None,
                &mut meta,
            )
            .unwrap_err();
        assert_eq!(err.adapter_kind(), Some(expected_kind));
        assert_eq!(err.adapter_code(), Some(code));
        assert_ne!(err.mutation_outcome(), MutationOutcome::Uncertain);
        assert_eq!(meta.retry_after(), retry_after);
        assert!(!format!("{err} {err:?}").contains("raw-internal-billing-msg"));
        assert!(!format!("{meta:?}").contains("raw-internal-billing-msg"));
        let (headers, body) = rx.recv().unwrap();
        assert!(
            headers.starts_with("POST /functions/v1/decapod-onboarding/billing-overage-policy ")
        );
        assert_eq!(body["enabled"], true);
        handle.join().unwrap();
    }

    // Mutating BillingOveragePolicy preserves outcome_unknown on post-send connection loss.
    let (origin, rx, handle) = server_with_headers(vec![(0, String::new(), json!({}))]);
    let err = connector(&origin)
        .account_request(
            AccountOperation::BillingOveragePolicy,
            &json!({"version": 1, "enabled": false}),
            "user-access",
            None,
        )
        .unwrap_err();
    assert_eq!(err.adapter_code(), Some("outcome_unknown"));
    assert_eq!(err.mutation_outcome(), MutationOutcome::Uncertain);
    rx.recv().unwrap();
    handle.join().unwrap();
}

#[test]
fn use_dpop_nonce_retries_once_stops_on_double_challenge_and_never_retries_bearer_or_unknown() {
    // 1. Double challenge fails closed after exactly two attempts, keeps final nonce in metadata,
    //    and redacts both nonces + server message.
    let signer = Arc::new(RecordingProofSigner::default());
    let (origin, rx, handle) = server_with_headers(vec![
        (
            401,
            "DPoP-Nonce: challenge-one\r\n".into(),
            json!({"error": {"code": "use_dpop_nonce", "message": "secret-nonce-hint"}}),
        ),
        (
            401,
            "DPoP-Nonce: challenge-two\r\nDecapod-Cloud-Stage: dpop-check\r\n".into(),
            json!({"error": {"code": "use_dpop_nonce", "message": "secret-nonce-hint"}}),
        ),
    ]);
    let client = connector_with_signer(&origin, signer.clone());
    let mut meta = CloudResponseMetadata::default();
    let err = client
        .account_request_with_metadata(
            AccountOperation::DeviceEnroll,
            &json!({"version": 1}),
            "dpop-access-token",
            None,
            None,
            &mut meta,
        )
        .unwrap_err();
    assert_eq!(err.adapter_kind(), Some(AdapterErrorKind::Authentication));
    assert_eq!(err.adapter_code(), Some("use_dpop_nonce"));
    assert_ne!(err.mutation_outcome(), MutationOutcome::Uncertain);
    assert_eq!(meta.dpop_nonce(), Some("challenge-two"));
    assert_eq!(
        meta.cloud_headers(),
        &[("decapod-cloud-stage".into(), "dpop-check".into())]
    );
    let rendered = format!("{err} {err:?} {meta:?}");
    for secret in ["challenge-one", "challenge-two", "secret-nonce-hint"] {
        assert!(!rendered.contains(secret));
    }
    let calls = signer.calls.lock().unwrap();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0].3, None);
    assert_eq!(calls[1].3.as_deref(), Some("challenge-one"));
    drop(calls);
    rx.recv().unwrap();
    rx.recv().unwrap();
    handle.join().unwrap();

    // 2. RFC 9449 WWW-Authenticate header challenge without body code triggers single retry,
    //    exposing final response metadata (updated DPoP-Nonce + Retry-After).
    let header_signer = Arc::new(RecordingProofSigner::default());
    let (origin, rx, handle) = server_with_headers(vec![
        (
            401,
            "WWW-Authenticate: DPoP error=\"use_dpop_nonce\"\r\nDPoP-Nonce: hdr-nonce-1\r\n".into(),
            json!({}),
        ),
        (
            200,
            "DPoP-Nonce: hdr-nonce-2\r\nRetry-After: 5\r\n".into(),
            json!({"ok": true}),
        ),
    ]);
    let mut final_meta = CloudResponseMetadata::default();
    let ok = connector_with_signer(&origin, header_signer.clone())
        .account_request_with_metadata(
            AccountOperation::DeviceEnroll,
            &json!({"version": 1}),
            "dpop-access-token",
            None,
            None,
            &mut final_meta,
        )
        .unwrap();
    assert_eq!(ok["ok"], true);
    assert_eq!(final_meta.dpop_nonce(), Some("hdr-nonce-2"));
    assert_eq!(final_meta.retry_after(), Some("5"));
    assert_eq!(header_signer.calls.lock().unwrap().len(), 2);
    rx.recv().unwrap();
    rx.recv().unwrap();
    handle.join().unwrap();

    // 3. Switching to AuthScheme::Bearer on challenge prevents automatic retry.
    struct SwitchToBearerOnRetry(Mutex<usize>);
    impl RequestProofSigner for SwitchToBearerOnRetry {
        fn sign_request(
            &self,
            input: &RequestProofInput<'_>,
        ) -> Result<RequestProof, dactyl_db::DactylError> {
            let mut count = self.0.lock().unwrap();
            *count += 1;
            if *count == 1 && input.nonce().is_none() {
                RequestProof::dpop("first-dpop-proof")
            } else {
                Ok(RequestProof::bearer())
            }
        }
    }
    let switch_signer = Arc::new(SwitchToBearerOnRetry(Mutex::new(0)));
    let (origin, rx, handle) = server_with_headers(vec![(
        401,
        "DPoP-Nonce: switch-nonce\r\n".into(),
        json!({"error": {"code": "use_dpop_nonce"}}),
    )]);
    let err = connector_with_signer(&origin, switch_signer.clone())
        .account_request(
            AccountOperation::DeviceEnroll,
            &json!({"version": 1}),
            "dpop-access-token",
            None,
        )
        .unwrap_err();
    assert_eq!(err.adapter_code(), Some("use_dpop_nonce"));
    assert_eq!(*switch_signer.0.lock().unwrap(), 2);
    rx.recv().unwrap();
    handle.join().unwrap();

    // 4. Bearer signer never retries on 401 use_dpop_nonce, and outcome_unknown never retries.
    let bearer_signer = Arc::new(RecordingProofSigner {
        bearer_mode: true,
        ..Default::default()
    });
    let (origin, rx, handle) = server_with_headers(vec![(
        401,
        "DPoP-Nonce: bearer-ignored\r\n".into(),
        json!({"error": {"code": "use_dpop_nonce"}}),
    )]);
    let err = connector_with_signer(&origin, bearer_signer.clone())
        .account_request(
            AccountOperation::DeviceEnroll,
            &json!({"version": 1}),
            "dpop-access-token",
            None,
        )
        .unwrap_err();
    assert_eq!(err.adapter_code(), Some("use_dpop_nonce"));
    assert_eq!(bearer_signer.calls.lock().unwrap().len(), 1);
    rx.recv().unwrap();
    handle.join().unwrap();

    let dpop_signer = Arc::new(RecordingProofSigner::default());
    let (origin, rx, handle) = server_with_headers(vec![(
        503,
        "DPoP-Nonce: unused-on-5xx\r\n".into(),
        json!({"error": {"code": "provider_unavailable"}}),
    )]);
    let err = connector_with_signer(&origin, dpop_signer.clone())
        .account_request(
            AccountOperation::DeviceEnroll,
            &json!({"version": 1}),
            "dpop-access-token",
            None,
        )
        .unwrap_err();
    assert_eq!(err.adapter_code(), Some("outcome_unknown"));
    assert_eq!(err.mutation_outcome(), MutationOutcome::Uncertain);
    assert_eq!(dpop_signer.calls.lock().unwrap().len(), 1);
    rx.recv().unwrap();
    handle.join().unwrap();
}

#[test]
fn device_gateway_enforces_endpoint_auth_matrix_and_control_fallback() {
    let signer = Arc::new(RecordingProofSigner::default());
    let (origin, rx, handle) = server_with_headers(vec![
        // 1. DeviceChallenge / DeviceKeys / DeviceSessions / BillingOveragePolicy send Bearer only.
        (200, String::new(), json!({"challenge_id": "ch-1"})),
        (200, String::new(), json!({"keys": []})),
        (200, String::new(), json!({"sessions": []})),
        (200, String::new(), json!({"updated": true})),
        // 2. DeviceSession hits 401 use_dpop_nonce -> single retry keeping Authorization: Bearer!
        (
            401,
            "DPoP-Nonce: gw-nonce-1\r\n".into(),
            json!({"error": {"code": "use_dpop_nonce"}}),
        ),
        (
            200,
            "DPoP-Nonce: gw-nonce-2\r\n".into(),
            json!({"access_token": "dpc_at_ok"}),
        ),
        // 3. DeviceRefresh requires DPoP proof and succeeds with DPoP.
        (200, String::new(), json!({"access_token": "dpc_at_next"})),
        // 4. control_request routes canonical 404 -> falls back once to propodus-onboarding/cloud-control.
        (404, String::new(), json!({"error": "not_found"})),
        (
            200,
            "Decapod-Cloud-Stage: edge\r\n".into(),
            json!({"decision": "allow"}),
        ),
    ]);
    let client = CloudConnector::with_config(SupabaseRpcConfig::v1(&origin, KEY))
        .unwrap()
        .with_device_gateway()
        .with_proof_signer(signer.clone());
    assert!(client.uses_device_gateway());

    for (op, expected_path) in [
        (AccountOperation::DeviceChallenge, "device-challenge"),
        (AccountOperation::DeviceKeys, "device-keys"),
        (AccountOperation::DeviceSessions, "device-sessions"),
        (
            AccountOperation::BillingOveragePolicy,
            "billing-overage-policy",
        ),
    ] {
        client
            .account_request(op, &json!({"version": 1}), "user-jwt-secret", None)
            .unwrap();
        let (headers, _) = rx.recv().unwrap();
        assert!(headers.starts_with(&format!(
            "POST /functions/v1/decapod-onboarding/{expected_path} "
        )));
        assert_eq!(
            header(&headers, "authorization"),
            Some("Bearer user-jwt-secret")
        );
        assert_eq!(header(&headers, "dpop"), None);
    }

    let mut session_meta = CloudResponseMetadata::default();
    let minted = client
        .account_request_with_metadata(
            AccountOperation::DeviceSession,
            &json!({"version": 1, "project_id": "org/repo"}),
            "user-jwt-secret",
            None,
            None,
            &mut session_meta,
        )
        .unwrap();
    assert_eq!(minted["access_token"], "dpc_at_ok");
    assert_eq!(session_meta.dpop_nonce(), Some("gw-nonce-2"));
    for idx in 0..2 {
        let (headers, _) = rx.recv().unwrap();
        assert!(headers.starts_with("POST /functions/v1/decapod-onboarding/device-session "));
        assert_eq!(
            header(&headers, "authorization"),
            Some("Bearer user-jwt-secret")
        );
        let dpop_hdr = header(&headers, "dpop").unwrap();
        if idx == 1 {
            assert!(dpop_hdr.ends_with(".gw-nonce-1"));
        }
    }

    let refreshed = client
        .account_request(
            AccountOperation::DeviceRefresh,
            &json!({"version": 1, "refresh_token": "dpc_rt_1"}),
            "dpc_at_ok",
            None,
        )
        .unwrap();
    assert_eq!(refreshed["access_token"], "dpc_at_next");
    let (headers, _) = rx.recv().unwrap();
    assert!(headers.starts_with("POST /functions/v1/decapod-onboarding/device-refresh "));
    assert_eq!(header(&headers, "authorization"), Some("DPoP dpc_at_ok"));
    assert!(header(&headers, "dpop").is_some());

    let mut ctrl_meta = CloudResponseMetadata::default();
    let decision = client
        .control_request_with_metadata(
            "propodus_repository_access_v1",
            &json!({"version": 1, "project_id": "org/repo"}),
            Some("dpc_at_next"),
            None,
            OperationKind::Read,
            ResourceBudget::v1(),
            &mut ctrl_meta,
        )
        .unwrap();
    assert_eq!(decision["decision"], "allow");
    assert_eq!(
        ctrl_meta.cloud_headers(),
        &[("decapod-cloud-stage".into(), "edge".into())]
    );
    let (h1, b1) = rx.recv().unwrap();
    assert!(h1.starts_with("POST /functions/v1/decapod-onboarding/cloud-control "));
    assert_eq!(b1["version"], 1);
    assert_eq!(b1["function"], "propodus_repository_access_v1");
    assert_eq!(b1["request"]["project_id"], "org/repo");
    let (h2, b2) = rx.recv().unwrap();
    assert!(h2.starts_with("POST /functions/v1/propodus-onboarding/cloud-control "));
    assert_eq!(header(&h2, "authorization"), Some("DPoP dpc_at_next"));
    assert!(header(&h2, "dpop").is_some());
    assert_eq!(b2["function"], "propodus_repository_access_v1");
    handle.join().unwrap();

    // Missing DPoP signer in gateway mode fails local preflight before network dispatch.
    let (unused_origin, _, unused_handle) = server(vec![]);
    let unsigned_gw = CloudConnector::with_config(SupabaseRpcConfig::v1(&unused_origin, KEY))
        .unwrap()
        .with_device_gateway();
    for op in [
        AccountOperation::DeviceEnroll,
        AccountOperation::DeviceSession,
        AccountOperation::DeviceRefresh,
    ] {
        let err = unsigned_gw
            .account_request(op, &json!({"version": 1}), "user-jwt-secret", None)
            .unwrap_err();
        assert_eq!(err.adapter_code(), Some("device_proof_required"));
        assert_eq!(err.mutation_outcome(), MutationOutcome::NotAttempted);
    }
    let ctrl_err = unsigned_gw
        .control_request(
            "propodus_repository_access_v1",
            &json!({"version": 1}),
            Some("user-jwt-secret"),
            OperationKind::Read,
            ResourceBudget::v1(),
        )
        .unwrap_err();
    assert_eq!(ctrl_err.adapter_code(), Some("device_proof_required"));
    assert_eq!(ctrl_err.mutation_outcome(), MutationOutcome::NotAttempted);
    unused_handle.join().unwrap();
}
