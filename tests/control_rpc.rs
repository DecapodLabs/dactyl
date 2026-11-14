//! Public transport evidence only. Fixture permission labels do not prove authority.
use dactyl_db::{
    AdapterErrorKind, LogicalConnector, MutationOutcome, OperationKind, ResourceBudget,
};
use serde_json::json;

#[test]
fn local_control_never_substitutes_authority() {
    let local = LogicalConnector::local("must-not-be-created.sqlite").unwrap();
    let error = local
        .control_request(
            "propodus_repository_access_v1",
            &json!({}),
            None,
            OperationKind::Write,
            ResourceBudget::v1(),
        )
        .unwrap_err();
    assert_eq!(error.adapter_kind(), Some(AdapterErrorKind::Capability));
    assert_eq!(error.mutation_outcome(), MutationOutcome::NotAttempted);
}

#[cfg(feature = "supabase")]
mod wire {
    use super::*;
    use dactyl_db::{CloudConnector, FailureOrigin, LogicalTarget, SupabaseRpcConfig};
    use serde_json::Value;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::mpsc::{self, Receiver};
    use std::thread::{self, JoinHandle};
    use std::time::Duration;

    const FUNCTION: &str = "propodus_repository_access_v1";
    const KEY: &str = "sb_publishable_control_fixture";
    const BEARER: &str = "private-user-bearer";
    const PRIVATE: &str = "private-policy-payload";

    enum Reply {
        Http(u16, String, Vec<u8>),
        Lost,
        Truncated,
        SlowHeaders,
        SlowBody,
    }
    fn reply(status: u16, body: Value) -> Reply {
        Reply::Http(status, String::new(), body.to_string().into_bytes())
    }
    type Recorded = (String, Value);
    fn read_request(stream: &mut TcpStream) -> Recorded {
        stream
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        let mut bytes = Vec::new();
        let (headers, offset, length) = loop {
            let mut chunk = [0; 4096];
            let n = stream.read(&mut chunk).unwrap();
            assert!(n > 0);
            bytes.extend_from_slice(&chunk[..n]);
            assert!(bytes.len() <= 1024 * 1024);
            if let Some(end) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
                let headers = String::from_utf8(bytes[..end].to_vec()).unwrap();
                let length = header(&headers, "content-length")
                    .unwrap_or("0")
                    .parse::<usize>()
                    .unwrap();
                break (headers, end + 4, length);
            }
        };
        while bytes.len() - offset < length {
            let mut chunk = [0; 4096];
            let n = stream.read(&mut chunk).unwrap();
            assert!(n > 0);
            bytes.extend_from_slice(&chunk[..n]);
        }
        let body = if length == 0 {
            Value::Null
        } else {
            serde_json::from_slice(&bytes[offset..offset + length]).unwrap()
        };
        (headers, body)
    }
    fn fixture(replies: Vec<Reply>) -> (String, Receiver<Recorded>, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let address = origin.clone();
        let (tx, rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            for response in replies {
                let (mut stream, _) = listener.accept().unwrap();
                tx.send(read_request(&mut stream)).unwrap();
                match response {
                    Reply::Lost => (),
                    Reply::SlowHeaders => thread::sleep(Duration::from_millis(250)),
                    Reply::Truncated => {
                        write!(
                            stream,
                            "HTTP/1.1 200 OK\r\nContent-Length: 200\r\nConnection: close\r\n\r\n{{"
                        )
                        .ok();
                    }
                    Reply::SlowBody => {
                        write!(
                            stream,
                            "HTTP/1.1 200 OK\r\nContent-Length: 200\r\nConnection: close\r\n\r\n{{"
                        )
                        .ok();
                        stream.flush().ok();
                        thread::sleep(Duration::from_millis(250));
                    }
                    Reply::Http(status, extra, body) => {
                        let body = String::from_utf8(body).unwrap().replace("ORIGIN", &address);
                        write!(stream, "HTTP/1.1 {status} Fixture\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n{extra}\r\n{body}", body.len()).ok();
                    }
                }
            }
            listener.set_nonblocking(true).unwrap();
            thread::sleep(Duration::from_millis(70));
            assert!(listener.accept().is_err(), "automatic replay or redirect");
        });
        (origin, rx, handle)
    }
    fn header<'a>(headers: &'a str, name: &str) -> Option<&'a str> {
        headers.lines().find_map(|line| {
            let (k, v) = line.split_once(':')?;
            k.eq_ignore_ascii_case(name).then(|| v.trim())
        })
    }
    fn connector(origin: &str) -> LogicalConnector {
        LogicalConnector::cloud_with_config(SupabaseRpcConfig::v1(origin, KEY)).unwrap()
    }
    fn call(
        client: &LogicalConnector,
        kind: OperationKind,
        budget: ResourceBudget,
    ) -> Result<Value, dactyl_db::DactylError> {
        client.control_request(
            FUNCTION,
            &json!({"version":1,"action":"evaluate_repair_authority","opaque":PRIVATE}),
            Some(BEARER),
            kind,
            budget,
        )
    }
    fn redacted(error: &dactyl_db::DactylError, origin: &str) {
        let rendered = format!("{error:?}{error}");
        for secret in [BEARER, KEY, PRIVATE, origin, "provider-secret", "Location:"] {
            assert!(
                !rendered.contains(secret),
                "diagnostic disclosed protected input"
            );
        }
    }

    #[test]
    fn discovery_and_opaque_control_use_the_public_managed_session_boundary() {
        let opaque = json!({"decision":"denied","future":[null,false,123],"results":{"not":"datastore rows"}});
        let (origin, rx, server) = fixture(vec![
            reply(
                200,
                json!({"version":1,"project_origin":"ORIGIN","publishable_key":KEY,"schema":"tenant_api"}),
            ),
            reply(200, opaque.clone()),
        ]);
        let client = LogicalConnector::resolve(LogicalTarget::cloud_with_origin(&origin)).unwrap();
        assert_eq!(
            call(&client, OperationKind::Write, ResourceBudget::v1()).unwrap(),
            opaque
        );
        let (discovery, body) = rx.recv().unwrap();
        assert!(discovery.starts_with("GET /functions/v1/propodus-onboarding/config HTTP/1.1"));
        assert_eq!(body, Value::Null);
        assert_eq!(header(&discovery, "authorization"), None);
        let (headers, body) = rx.recv().unwrap();
        assert!(headers.starts_with(&format!("POST /rest/v1/rpc/{FUNCTION} HTTP/1.1")));
        for (name, value) in [
            ("apikey", KEY),
            ("authorization", "Bearer private-user-bearer"),
            ("content-profile", "tenant_api"),
            ("accept-profile", "tenant_api"),
            ("content-type", "application/json"),
            ("accept", "application/json"),
            ("dactyl-contract-version", "1"),
        ] {
            assert_eq!(header(&headers, name), Some(value));
        }
        assert_eq!(header(&headers, "x-scm-token"), None);
        assert_eq!(
            body,
            json!({"request":{"version":1,"action":"evaluate_repair_authority","opaque":PRIVATE}})
        );
        assert!(!format!("{client:?}").contains(&origin));
        server.join().unwrap();
    }

    #[test]
    fn preflight_rejects_missing_unsafe_bearers_identifiers_and_budgets_without_dispatch() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let client = connector(&origin);
        for bearer in [None, Some(""), Some("line\nbreak"), Some("space token")] {
            let error = client
                .control_request(
                    FUNCTION,
                    &json!({}),
                    bearer,
                    OperationKind::Write,
                    ResourceBudget::v1(),
                )
                .unwrap_err();
            assert_eq!(error.adapter_kind(), Some(AdapterErrorKind::Authentication));
            assert_eq!(error.mutation_outcome(), MutationOutcome::NotAttempted);
            redacted(&error, &origin);
        }
        for function in [
            "",
            "../stolen",
            "https://example.com",
            "public.fn",
            "fn?query=1",
            "fn#fragment",
            &"a".repeat(64),
        ] {
            assert!(client
                .control_request(
                    function,
                    &json!({}),
                    Some(BEARER),
                    OperationKind::Write,
                    ResourceBudget::v1()
                )
                .is_err());
        }
        let error = call(
            &client,
            OperationKind::Write,
            ResourceBudget::v1().with_max_wire_bytes(8),
        )
        .unwrap_err();
        assert_eq!(error.adapter_code(), Some("payload_too_large"));
        assert_eq!(error.failure_origin(), FailureOrigin::LocalBudget);
        assert_eq!(error.mutation_outcome(), MutationOutcome::NotAttempted);
        let mut deep = json!(0);
        for _ in 0..70 {
            deep = json!([deep]);
        }
        let error = client
            .control_request(
                FUNCTION,
                &deep,
                Some(BEARER),
                OperationKind::Write,
                ResourceBudget::v1(),
            )
            .unwrap_err();
        assert_eq!(error.adapter_code(), Some("payload_too_deep"));
        assert_eq!(error.mutation_outcome(), MutationOutcome::NotAttempted);
        assert!(call(
            &client,
            OperationKind::Write,
            ResourceBudget::v1().with_request_timeout(Duration::ZERO)
        )
        .is_err());
        listener.set_nonblocking(true).unwrap();
        assert!(listener.accept().is_err(), "preflight dispatched a request");
    }

    #[test]
    fn profile_origin_schema_and_project_keys_fail_closed() {
        for config in [
            SupabaseRpcConfig::v1("https://example.com/path", KEY),
            SupabaseRpcConfig::v1("https://example.com", KEY).with_schema("public,private"),
            SupabaseRpcConfig::v1("https://example.com", "sb_secret_never_use"),
            SupabaseRpcConfig::v1("https://example.com", "service-role-jwt"),
        ] {
            assert!(LogicalConnector::cloud_with_config(config).is_err());
        }
        let (origin, rx, server) = fixture(vec![reply(
            200,
            json!({"version":1,"project_origin":"https://different.example","publishable_key":KEY,"schema":"dactyl_api"}),
        )]);
        assert!(LogicalConnector::cloud_with_origin(origin).is_err());
        rx.recv().unwrap();
        server.join().unwrap();
    }

    #[test]
    fn remote_auth_denials_schema_errors_and_rate_limits_keep_their_semantics() {
        for (status, code, expected, kind) in [
            (
                401,
                "session_expired",
                "session_expired",
                AdapterErrorKind::Authentication,
            ),
            (
                401,
                "session_revoked",
                "session_revoked",
                AdapterErrorKind::Authentication,
            ),
            (
                401,
                "PGRST301",
                "invalid_token",
                AdapterErrorKind::Authentication,
            ),
            (
                403,
                "42501",
                "authorization_denied",
                AdapterErrorKind::Authorization,
            ),
            (
                403,
                "repository_not_authorized",
                "repository_not_authorized",
                AdapterErrorKind::Authorization,
            ),
            (
                406,
                "PGRST106",
                "unsupported_schema",
                AdapterErrorKind::Capability,
            ),
            (
                404,
                "PGRST202",
                "unsupported_rpc",
                AdapterErrorKind::Capability,
            ),
            (
                429,
                "rate_limited",
                "rate_limited",
                AdapterErrorKind::RateLimited,
            ),
        ] {
            let (origin, rx, server) = fixture(vec![reply(
                status,
                json!({"code":code,"message":"provider-secret","details":PRIVATE}),
            )]);
            let error = call(
                &connector(&origin),
                OperationKind::Write,
                ResourceBudget::v1(),
            )
            .unwrap_err();
            assert_eq!(error.adapter_kind(), Some(kind));
            assert_eq!(error.adapter_code(), Some(expected));
            assert_ne!(error.mutation_outcome(), MutationOutcome::Uncertain);
            redacted(&error, &origin);
            rx.recv().unwrap();
            server.join().unwrap();
        }
    }

    #[test]
    fn unknown_remote_codes_are_redacted() {
        let (origin, rx, server) =
            fixture(vec![reply(403, json!({"code":PRIVATE,"message":BEARER}))]);
        let error = call(
            &connector(&origin),
            OperationKind::Read,
            ResourceBudget::v1(),
        )
        .unwrap_err();
        assert_eq!(error.adapter_kind(), Some(AdapterErrorKind::Authorization));
        assert_eq!(error.adapter_code(), None);
        redacted(&error, &origin);
        rx.recv().unwrap();
        server.join().unwrap();
    }

    #[test]
    fn redirects_never_follow_a_new_origin() {
        let sink = TcpListener::bind("127.0.0.1:0").unwrap();
        let location = format!("Location: http://{}/stolen\r\n", sink.local_addr().unwrap());
        let (origin, rx, server) = fixture(vec![Reply::Http(307, location, b"{}".to_vec())]);
        let error = call(
            &connector(&origin),
            OperationKind::Write,
            ResourceBudget::v1(),
        )
        .unwrap_err();
        assert_eq!(error.adapter_kind(), Some(AdapterErrorKind::Protocol));
        redacted(&error, &origin);
        rx.recv().unwrap();
        server.join().unwrap();
        sink.set_nonblocking(true).unwrap();
        assert!(sink.accept().is_err());
    }

    #[test]
    fn lost_or_unusable_acknowledgements_preserve_read_vs_mutation_uncertainty() {
        for kind in [OperationKind::Read, OperationKind::Write] {
            let too_deep = format!("{}0{}", "[".repeat(70), "]".repeat(70));
            for response in [
                Reply::Lost,
                Reply::Truncated,
                Reply::SlowHeaders,
                Reply::SlowBody,
                Reply::Http(200, String::new(), b"not-json provider-secret".to_vec()),
                Reply::Http(200, String::new(), vec![b' '; 1025]),
                Reply::Http(200, String::new(), too_deep.into_bytes()),
                reply(502, json!({"message":"provider-secret"})),
                reply(503, json!({"code":"storage_unavailable"})),
            ] {
                let timeout = matches!(response, Reply::SlowHeaders | Reply::SlowBody);
                let (origin, rx, server) = fixture(vec![response]);
                let budget = ResourceBudget::v1()
                    .with_max_wire_bytes(1024)
                    .with_request_timeout(Duration::from_millis(100));
                let error = call(&connector(&origin), kind, budget).unwrap_err();
                if timeout {
                    assert_eq!(error.adapter_kind(), Some(AdapterErrorKind::Timeout));
                }
                if kind == OperationKind::Write {
                    assert_eq!(error.adapter_code(), Some("outcome_unknown"));
                    assert_eq!(error.mutation_outcome(), MutationOutcome::Uncertain);
                } else {
                    assert_ne!(error.adapter_code(), Some("outcome_unknown"));
                    assert_ne!(error.mutation_outcome(), MutationOutcome::Uncertain);
                }
                redacted(&error, &origin);
                rx.recv().unwrap();
                server.join().unwrap();
            }
        }
    }

    #[tokio::test]
    async fn control_is_safe_inside_an_async_consumer_runtime() {
        let (origin, rx, server) = fixture(vec![reply(200, json!({"opaque":true}))]);
        let client = CloudConnector::with_config(SupabaseRpcConfig::v1(&origin, KEY)).unwrap();
        assert_eq!(
            client
                .control_request(
                    FUNCTION,
                    &json!({}),
                    Some(BEARER),
                    OperationKind::Read,
                    ResourceBudget::v1()
                )
                .unwrap(),
            json!({"opaque":true})
        );
        rx.recv().unwrap();
        server.join().unwrap();
    }
}
