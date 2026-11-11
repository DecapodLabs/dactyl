//! Offline connector and managed-auth transport contract, without hosted claims.
#![cfg(feature = "supabase")]

use dactyl_db::{
    AdapterErrorKind, AuthenticationSecrets, AuthenticationStart, AuthenticationState, Backend,
    CloudConnector, Connection, Datastore, DatastoreRoute, StorageContext, SupabaseRpcConfig,
    DEFAULT_CLOUD_ORIGIN,
};
use serde_json::{json, Value};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

const KEY: &str = "sb_publishable_fixture";
const FLOW: &str = "00000000-0000-4000-8000-000000000001";
const TICKET: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
const REPO: &str = "owner/repository";
const EXPIRY: &str = "2099-01-01T00:00:00Z";

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
fn connector(origin: &str) -> CloudConnector {
    CloudConnector::with_config(SupabaseRpcConfig::v1(origin, KEY)).unwrap()
}
fn session() -> Value {
    json!({"access_token":"user-access","refresh_token":"user-refresh","token_type":"bearer","expires_in":900})
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
    assert!(headers.starts_with("GET /functions/v1/propodus-onboarding/config "));
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
            json!({"flow_id":FLOW,"browser_url":format!("ORIGIN/functions/v1/propodus-onboarding/begin?flow_id={FLOW}&ticket={TICKET}"),"expires_at":EXPIRY}),
        ),
        (
            200,
            json!({"status":"ready","repository":REPO,"expires_at":EXPIRY}),
        ),
        (
            200,
            json!({"auth_code":"one-time-auth-code","repository":REPO}),
        ),
        (200, session()),
        (200, json!({"status":"complete","repository":REPO})),
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
            .unwrap()
            .status,
        AuthenticationState::Ready
    );
    let code = connector
        .exchange_authentication_code(&flow.flow_id, &secrets)
        .unwrap();
    assert_eq!(code.repository, REPO);
    let user = connector
        .exchange_session(&code.auth_code, &secrets)
        .unwrap();
    let completed = connector
        .complete_authentication(&flow.flow_id, &secrets, &user.access_token)
        .unwrap();
    assert_eq!(completed.repository, REPO);
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
    assert_eq!(requests[3].1["auth_code"], "one-time-auth-code");
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
        json!({"version":1,"project_origin":"ORIGIN","publishable_key":KEY,"schema":"other_api"}),
        json!({"version":1,"project_origin":"ORIGIN","publishable_key":KEY,"schema":"dactyl_api","token":"do-not-use"}),
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
            assert_eq!(error.adapter_code(), None);
        } else if status == 0 {
            assert_eq!(error.adapter_kind(), Some(AdapterErrorKind::Transport));
            assert_eq!(error.adapter_code(), Some("outcome_unknown"));
        }
        rx.recv().unwrap();
        handle.join().unwrap();
    }
}

#[test]
fn network_failure_is_distinct_from_provider_unavailability() {
    for (status, expected) in [
        (0, AdapterErrorKind::Transport),
        (503, AdapterErrorKind::Unavailable),
    ] {
        let (origin, rx, handle) = server(vec![(
            status,
            json!({"error":{"code":"service_unavailable"}}),
        )]);
        let error = CloudConnector::discover(&origin).unwrap_err();
        assert_eq!(error.adapter_kind(), Some(expected));
        assert_ne!(error.adapter_code(), Some("outcome_unknown"));
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

#[test]
fn typed_flow_results_preserve_repository_and_expiry() {
    for state in [
        AuthenticationState::Pending,
        AuthenticationState::Ready,
        AuthenticationState::Consumed,
        AuthenticationState::Complete,
    ] {
        let (origin, rx, handle) = server(vec![(
            200,
            json!({"status":state,"repository":"different/repository","expires_at":EXPIRY}),
        )]);
        let result = connector(&origin)
            .authentication_status(FLOW, &AuthenticationSecrets::generate().unwrap())
            .unwrap();
        assert_eq!(result.status, state);
        assert_eq!(result.repository, "different/repository");
        assert_eq!(result.expires_at, EXPIRY);
        rx.recv().unwrap();
        handle.join().unwrap();
    }
    for body in [
        json!({"status":"ready"}),
        json!({"status":"ready","repository":REPO,"expires_at":"2099-02-30T00:00:00Z"}),
        json!({"status":"unknown","repository":REPO,"expires_at":EXPIRY}),
    ] {
        let (origin, rx, handle) = server(vec![(200, body)]);
        let error = connector(&origin)
            .authentication_status(FLOW, &AuthenticationSecrets::generate().unwrap())
            .unwrap_err();
        assert_eq!(error.adapter_kind(), Some(AdapterErrorKind::Protocol));
        assert_ne!(error.adapter_code(), Some("outcome_unknown"));
        rx.recv().unwrap();
        handle.join().unwrap();
    }
}

#[test]
fn handoff_rejects_extra_duplicate_foreign_or_invalid_ticket_parameters() {
    let connector = connector("https://project.example");
    for query in [
        format!("flow_id={FLOW}&ticket={TICKET}&extra=secret"),
        format!("flow_id={FLOW}&ticket={TICKET}&ticket={TICKET}"),
        format!("flow_id={FLOW}&ticket=short"),
        format!("flow_id={FLOW}&ticket={}", "!".repeat(43)),
        format!("flow_id={FLOW}&flow_id={FLOW}"),
    ] {
        let flow = AuthenticationStart {
            flow_id: FLOW.into(),
            browser_url: format!(
                "https://project.example/functions/v1/propodus-onboarding/begin?{query}"
            ),
            expires_at: EXPIRY.into(),
        };
        assert!(connector.validate_start(&flow).is_err());
    }
    let flow=AuthenticationStart{flow_id:FLOW.into(),browser_url:format!("https://other.example/functions/v1/propodus-onboarding/begin?flow_id={FLOW}&ticket={TICKET}"),expires_at:EXPIRY.into()};
    assert!(connector.validate_start(&flow).is_err());
}

#[test]
fn known_denial_codes_survive_without_remote_messages() {
    for code in [
        "entitlement_required",
        "authorization_denied",
        "invalid_binding",
    ] {
        let (origin, rx, handle) = server(vec![(
            403,
            json!({"error":{"code":code,"message":"secret-access-refresh"}}),
        )]);
        let error = connector(&origin)
            .complete_authentication(
                FLOW,
                &AuthenticationSecrets::generate().unwrap(),
                "access-private",
            )
            .unwrap_err();
        assert_eq!(error.adapter_kind(), Some(AdapterErrorKind::Authorization));
        assert_eq!(error.adapter_code(), Some(code));
        assert!(!format!("{error:?} {error}").contains("secret-access-refresh"));
        assert!(!format!("{error:?} {error}").contains("access-private"));
        rx.recv().unwrap();
        handle.join().unwrap();
    }
    for code in [
        "session_expired",
        "session_revoked",
        "refresh_token_already_used",
    ] {
        let (origin, rx, handle) = server(vec![(
            401,
            json!({"error_code":code,"msg":"private refresh token"}),
        )]);
        let error = connector(&origin)
            .refresh_session("private-refresh")
            .unwrap_err();
        assert_eq!(error.adapter_kind(), Some(AdapterErrorKind::Authentication));
        assert_eq!(error.adapter_code(), Some(code));
        assert!(!format!("{error:?}").contains("private"));
        rx.recv().unwrap();
        handle.join().unwrap();
    }
}

#[test]
fn malformed_mutation_success_is_ambiguous_and_never_replayed() {
    for body in [
        json!({}),
        json!({"status":"pending","repository":REPO}),
        json!({"status":"complete"}),
    ] {
        let (origin, rx, handle) = server(vec![(200, body)]);
        let error = connector(&origin)
            .complete_authentication(
                FLOW,
                &AuthenticationSecrets::generate().unwrap(),
                "access-private",
            )
            .unwrap_err();
        assert_eq!(error.adapter_code(), Some("outcome_unknown"));
        rx.recv().unwrap();
        handle.join().unwrap();
    }
    let (origin, rx, handle) = server(vec![(200, json!({"auth_code":"one-time-auth-code"}))]);
    let error = connector(&origin)
        .exchange_authentication_code(FLOW, &AuthenticationSecrets::generate().unwrap())
        .unwrap_err();
    assert_eq!(error.adapter_code(), Some("outcome_unknown"));
    rx.recv().unwrap();
    handle.join().unwrap();
}

#[test]
fn invalid_sessions_never_become_a_reusable_bearer() {
    for body in [
        json!({"access_token":"access","refresh_token":"refresh","token_type":"other","expires_in":900}),
        json!({"access_token":"access","refresh_token":"refresh","token_type":"bearer","expires_in":0}),
        json!({"access_token":"access","refresh_token":"refresh","token_type":"bearer","expires_in":900,"expires_at":1}),
        json!({"access_token":"access\nprivate","refresh_token":"refresh","token_type":"bearer","expires_in":900}),
    ] {
        let (origin, rx, handle) = server(vec![(200, body)]);
        let error = connector(&origin)
            .refresh_session("old-refresh")
            .unwrap_err();
        assert_eq!(error.adapter_code(), Some("outcome_unknown"));
        assert!(!error.to_string().contains("access\nprivate"));
        assert!(!error.to_string().contains("old-refresh"));
        rx.recv().unwrap();
        handle.join().unwrap();
    }
}

#[test]
fn secrets_restore_exactly_and_reject_invalid_private_records() {
    let original = AuthenticationSecrets::generate().unwrap();
    let record = serde_json::to_value(&original).unwrap();
    let restored = AuthenticationSecrets::from_parts(
        record["verifier"].as_str().unwrap().into(),
        record["poll_secret"].as_str().unwrap().into(),
    )
    .unwrap();
    assert_eq!(serde_json::to_value(restored).unwrap(), record);
    for bad in ["".to_string(), "!".repeat(43), "A".repeat(42)] {
        assert!(AuthenticationSecrets::from_parts(bad, TICKET.into()).is_err());
    }
    assert!(!format!("{original:?}").contains(record["verifier"].as_str().unwrap()));
}

#[test]
fn independent_connectors_cannot_switch_each_others_data_origin() {
    let (first, first_rx, first_handle) = server(vec![(200, profile())]);
    let (second, second_rx, second_handle) = server(vec![(200, profile())]);
    let first_connector = CloudConnector::discover(&first).unwrap();
    let second_connector = CloudConnector::discover(&second).unwrap();
    assert_eq!(first_connector.origin(), first);
    assert_eq!(second_connector.origin(), second);
    assert_eq!(
        first_connector.route(Some("first-bearer".into())).route(),
        first
    );
    assert_eq!(
        second_connector.route(Some("second-bearer".into())).route(),
        second
    );
    for rx in [first_rx, second_rx] {
        let (headers, _) = rx.recv().unwrap();
        assert!(!headers.to_ascii_lowercase().contains("authorization:"));
    }
    first_handle.join().unwrap();
    second_handle.join().unwrap();
}

#[test]
fn missing_cloud_bearer_fails_before_any_data_connection() {
    let route = connector("http://127.0.0.1:1").route(None);
    let error = Connection::open_with_context(
        route,
        Some(StorageContext::new(1, json!({"scope":"fixture"})).unwrap()),
    )
    .err()
    .unwrap();
    assert_eq!(error.adapter_code(), Some("authentication_required"));
}

#[test]
fn default_origin_ignores_environment() {
    if std::env::var_os("DACTYL_ORIGIN_CHILD").is_some() {
        assert_eq!(CloudConnector::default_origin(), DEFAULT_CLOUD_ORIGIN);
        return;
    }
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "default_origin_ignores_environment"])
        .env("DACTYL_ORIGIN_CHILD", "1")
        .env("DACTYL_CLOUD_ORIGIN", "https://untrusted.example")
        .env("DACTYL_CLOUD_PUBLISHABLE_KEY", "sb_publishable_untrusted")
        .status()
        .unwrap();
    assert!(status.success());
}

#[test]
fn response_size_is_bounded_without_echoing_remote_content() {
    let (origin, rx, handle) = server(vec![(200, json!({"secret": "s".repeat(17*1024)}))]);
    let error = connector(&origin)
        .refresh_session("refresh-private")
        .unwrap_err();
    assert_eq!(error.adapter_code(), Some("outcome_unknown"));
    assert!(error.to_string().len() < 200);
    rx.recv().unwrap();
    handle.join().unwrap();
}
