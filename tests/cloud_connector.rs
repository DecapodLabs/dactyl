//! Offline connector and managed-auth transport contract, without hosted claims.
#![cfg(feature = "supabase")]

use dactyl_db::{
    AdapterErrorKind, AuthenticationSecrets, Backend, CloudConnector, Datastore, DatastoreRoute,
    SupabaseRpcConfig,
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
fn connector(origin: &str) -> CloudConnector {
    CloudConnector::with_config(SupabaseRpcConfig::v1(origin, KEY)).unwrap()
}
fn session() -> Value {
    json!({"access_token":"user-access","refresh_token":"user-refresh","expires_in":900})
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
        (200, json!({"status":"complete"})),
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
