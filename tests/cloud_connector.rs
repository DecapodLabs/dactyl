//! Offline connector and managed-auth transport contract, without hosted claims.
#![cfg(feature = "supabase")]

use dactyl_db::{
    AccountOperation, AdapterErrorKind, AuthenticationSecrets, AuthenticationStart, Backend,
    CloudConnector, Connection, Datastore, DatastoreRoute, ScmCredential, ScmProvider,
    StorageContext, SupabaseRpcConfig, UserSession,
};
use serde_json::{json, Value};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::mpsc;
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
        assert!(headers.starts_with(&format!("POST /functions/v1/propodus-onboarding/{action} ")));
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
        "https://project.example/functions/v1/propodus-onboarding/begin?flow_id={FLOW}&ticket=one"
    );
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
            json!({"flow_id":FLOW,"browser_url":format!("ORIGIN/functions/v1/propodus-onboarding/begin?flow_id={FLOW}&ticket=ticket"),"expires_at":"2099-01-01T00:00:00Z"}),
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
