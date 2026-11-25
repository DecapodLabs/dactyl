//! Permanent Trust & Zero-Egress Gate (#1944).
//!
//! Verifies:
//! 1. Local `sqlite` mode performs zero network egress even when `SUPABASE_*`,
//!    `DATABASE_URL`, `DATASTORE=supabase`, or home auth files are present.
//! 2. Negative-control tests prove HTTPS-only enforcement, allowed-origin
//!    pinning, and redirect blocking fail closed on untrusted destinations.
//! 3. Static/coverage checks verify every transport and env callsite in `src/`
//!    is accounted for in `docs/trust-and-egress-inventory.md` and that `sqlite`
//!    remains completely isolated from HTTP dependencies.

use std::fs;
use std::path::{Path, PathBuf};

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn collect_rs_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let mut entries = fs::read_dir(dir)
        .expect("read source dir")
        .map(|entry| entry.expect("dir entry").path())
        .collect::<Vec<_>>();
    entries.sort();
    for path in entries {
        if path.is_dir() {
            collect_rs_files(&path, out);
        } else if path.extension().and_then(|ext| ext.to_str()) == Some("rs") {
            out.push(path);
        }
    }
}

#[test]
fn all_transport_and_env_callsites_are_inventoried_and_feature_gated() {
    let root = repo_root();
    let inventory = fs::read_to_string(root.join("docs/trust-and-egress-inventory.md"))
        .expect("docs/trust-and-egress-inventory.md must exist");
    let cargo_toml = fs::read_to_string(root.join("Cargo.toml")).expect("read Cargo.toml");

    // 1. Network dependencies must remain strictly optional and excluded from default/sqlite.
    let mut section = String::new();
    for line in cargo_toml.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('[') && !trimmed.starts_with("[[") {
            section = trimmed.to_string();
            continue;
        }
        if section == "[features]" {
            if trimmed.starts_with("default =") {
                assert_eq!(
                    trimmed, "default = []",
                    "default features must remain empty"
                );
            }
            if trimmed.starts_with("sqlite =") {
                for forbidden in [
                    "reqwest",
                    "hyper",
                    "tokio",
                    "url",
                    "quinn",
                    "base64",
                    "getrandom",
                    "sha2",
                ] {
                    assert!(
                        !trimmed.contains(forbidden),
                        "sqlite feature must not pull network/HTTP dependency `{forbidden}`: {trimmed}"
                    );
                }
            }
        }
        if section == "[dependencies]"
            || (section.starts_with("[target.") && !section.contains("dev-dependencies"))
        {
            for dep in [
                "reqwest =",
                "hyper-rustls =",
                "hyper-util =",
                "tokio =",
                "url =",
                "quinn-proto =",
                "quinn-udp =",
            ] {
                if trimmed.starts_with(dep) {
                    assert!(
                        trimmed.contains("optional = true"),
                        "network dependency `{dep}` must stay optional in Cargo.toml"
                    );
                }
            }
        }
    }

    // 2. Scan every Rust source file under src/ for transport, socket, or env callsites.
    let mut src_files = Vec::new();
    collect_rs_files(&root.join("src"), &mut src_files);
    assert!(!src_files.is_empty());

    let transport_markers = [
        "reqwest::",
        "SharedHttpClient",
        "TcpStream",
        "UdpSocket",
        "std::net::",
        "std::env::var",
        "libloading::",
    ];

    for path in src_files {
        let rel = path
            .strip_prefix(&root)
            .expect("relative path")
            .to_string_lossy()
            .replace('\\', "/");
        let content = fs::read_to_string(&path).expect("read src file");

        if rel.starts_with("src/adapter/sqlite/") {
            for forbidden in [
                "reqwest",
                "http_pool",
                "std::net",
                "TcpStream",
                "UdpSocket",
                "crate::cloud",
                "supabase_rpc",
            ] {
                assert!(
                    !content.contains(forbidden),
                    "SQLite adapter `{rel}` must not reference `{forbidden}`"
                );
            }
        }

        // Redirects and automatic retries must never be re-enabled anywhere in src/.
        assert!(
            !content.contains("Policy::default") && !content.contains("Policy::limited"),
            "redirect-following policy is forbidden in `{rel}`"
        );
        if content.contains("Client::builder()") {
            assert!(
                content.contains(".redirect(reqwest::redirect::Policy::none())")
                    && content.contains(".retry(reqwest::retry::never())"),
                "reqwest client builder in `{rel}` must disable redirects and retries"
            );
        }

        let has_marker = transport_markers
            .iter()
            .any(|marker| content.contains(marker));
        if has_marker {
            assert!(
                inventory.contains(&rel),
                "source file `{rel}` contains transport/env callsite(s) missing from docs/trust-and-egress-inventory.md"
            );
        }
    }
}

#[cfg(feature = "sqlite")]
mod sqlite_zero_egress {
    use std::fs;
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex, OnceLock};
    use std::time::Duration;

    use dactyl_db::{
        AccessMode, AdapterErrorKind, Backend, Connection, Datastore, DatastoreRoute,
        DatastoreTransport, LogicalConnector, LogicalTarget, OpenOptions, Operation, Parameter,
        StorageContext,
    };
    use serde_json::json;
    use tempfile::TempDir;

    static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    struct EnvGuard {
        saved: Vec<(&'static str, Option<std::ffi::OsString>)>,
    }

    impl EnvGuard {
        fn set(vars: &[(&'static str, String)]) -> Self {
            let mut saved = Vec::with_capacity(vars.len());
            for (key, value) in vars {
                saved.push((*key, std::env::var_os(key)));
                std::env::set_var(key, value);
            }
            Self { saved }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for (key, prior) in self.saved.drain(..).rev() {
                match prior {
                    Some(value) => std::env::set_var(key, value),
                    None => std::env::remove_var(key),
                }
            }
        }
    }

    #[test]
    fn sqlite_performs_zero_network_egress_even_with_canary_env_and_auth_files() {
        let _lock = ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(|p| p.into_inner());

        // Bind a canary TCP listener that counts any attempted inbound connection.
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind canary listener");
        let canary_addr = listener.local_addr().unwrap();
        let canary_origin = format!("http://{canary_addr}");
        listener.set_nonblocking(true).unwrap();

        let hits = Arc::new(AtomicUsize::new(0));
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let hits_bg = hits.clone();
        let stop_bg = stop.clone();
        let accept_thread = std::thread::spawn(move || {
            while !stop_bg.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((_stream, _)) => {
                        hits_bg.fetch_add(1, Ordering::SeqCst);
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    Err(_) => break,
                }
            }
        });

        // Populate synthetic local auth/credential files and canary env vars.
        let tmp = TempDir::new().unwrap();
        let decapod_dir = tmp.path().join(".decapod");
        let config_dir = tmp.path().join(".config/decapod");
        fs::create_dir_all(&decapod_dir).unwrap();
        fs::create_dir_all(&config_dir).unwrap();
        std::fs::write(
            decapod_dir.join("auth.json"),
            format!(r#"{{"origin":"{canary_origin}","access_token":"canary-jwt"}}"#),
        )
        .unwrap();
        std::fs::write(
            config_dir.join("credentials.json"),
            format!(r#"{{"supabase_url":"{canary_origin}","token":"canary-secret"}}"#),
        )
        .unwrap();

        let db_path = tmp.path().join("local-zero-egress.db");
        let backup_path = tmp.path().join("local-zero-egress-backup.db");
        let db_path_str = db_path.to_string_lossy().into_owned();

        let _env = EnvGuard::set(&[
            ("HOME", tmp.path().to_string_lossy().into_owned()),
            ("SUPABASE_URL", canary_origin.clone()),
            ("SUPABASE_ANON_KEY", "sb_publishable_canary".into()),
            ("SUPABASE_SERVICE_ROLE_KEY", "canary-service-role".into()),
            (
                "DATABASE_URL",
                format!("postgres://canary:canary@{canary_addr}/db"),
            ),
            ("DACTYL_CLOUD_ORIGIN", canary_origin.clone()),
            (
                "DACTYL_CLOUD_PUBLISHABLE_KEY",
                "sb_publishable_canary".into(),
            ),
            ("DATASTORE", "supabase".into()),
            ("DATASTORE_ROUTE", canary_origin.clone()),
            ("DATASTORE_TOKEN", "canary-bearer".into()),
        ]);

        // 1. Explicit DatastoreRoute::sqlite + Connection lifecycle.
        let route = DatastoreRoute::sqlite(&db_path_str);
        assert_eq!(route.datastore(), Datastore::Sqlite);
        assert_eq!(route.transport(), DatastoreTransport::Local);
        assert!(route.validate().is_ok());

        // Local backend rejects cloud credentials before any DB/network work.
        assert!(DatastoreRoute::for_backend(
            Backend::Local,
            &db_path_str,
            Some("canary-token".into())
        )
        .is_err());

        let ctx = StorageContext::new(
            1,
            json!({
                "org_id": "canary-org",
                "cloud_origin": canary_origin,
            }),
        )
        .unwrap();

        let conn = Connection::open_with_options_and_context(
            route,
            OpenOptions {
                access_mode: AccessMode::ReadWrite,
                ..OpenOptions::default()
            },
            Some(ctx.clone()),
        )
        .unwrap();

        conn.write(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
            &[],
        )
        .unwrap();
        conn.atomic(&[
            Operation::write(
                "INSERT INTO items (name) VALUES ($1)",
                vec![Parameter::Text("alpha".into())],
            ),
            Operation::read("SELECT id, name FROM items", vec![]),
        ])
        .unwrap();
        assert!(conn.has_table("items").unwrap());
        assert!(conn.has_column("items", "name").unwrap());
        assert_eq!(conn.inspect_schema().unwrap().tables.len(), 1);
        assert!(conn.verify_integrity().is_ok());
        conn.backup(&backup_path).unwrap();
        drop(conn);

        // 2. LogicalTarget::local / LogicalConnector::local lifecycle.
        let connector = LogicalConnector::resolve(LogicalTarget::local(&db_path_str)).unwrap();
        assert_eq!(connector.backend(), Backend::Local);
        assert!(connector.route(Some("canary-token".into())).is_err());
        assert!(connector
            .open(
                OpenOptions::default(),
                Some(ctx.clone()),
                Some("canary".into())
            )
            .is_err());
        let local_conn = connector
            .open(OpenOptions::default(), Some(ctx), None)
            .unwrap();
        let rows = local_conn
            .read(
                "SELECT name FROM items WHERE id = $1",
                &[Parameter::Integer(1)],
            )
            .unwrap();
        assert_eq!(rows.as_slice()[0].get_str("name").unwrap(), "alpha");

        // Cloud-only connector methods fail locally with Capability and zero egress.
        assert_eq!(
            connector.canonical_auth_url().unwrap_err().adapter_kind(),
            Some(AdapterErrorKind::Capability)
        );
        assert_eq!(
            connector
                .canonical_repository_url("org", "repo")
                .unwrap_err()
                .adapter_kind(),
            Some(AdapterErrorKind::Capability)
        );

        stop.store(true, Ordering::Relaxed);
        accept_thread.join().unwrap();
        assert_eq!(
            hits.load(Ordering::SeqCst),
            0,
            "local SQLite mode must make zero outbound network connections"
        );
    }
}

#[cfg(any(feature = "neon", feature = "supabase"))]
mod remote_negative_controls {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    use dactyl_db::{
        AdapterErrorKind, Connection, DatastoreRoute, MutationOutcome, StorageContext,
    };
    use serde_json::json;

    fn ctx() -> StorageContext {
        StorageContext::new(1, json!({"scope": "trust-gate"})).unwrap()
    }

    fn spawn_redirect_and_canary(
        status: u16,
    ) -> (
        String,
        Arc<AtomicUsize>,
        thread::JoinHandle<()>,
        Arc<std::sync::atomic::AtomicBool>,
        thread::JoinHandle<()>,
    ) {
        let target_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let target_addr = target_listener.local_addr().unwrap();
        target_listener.set_nonblocking(true).unwrap();
        let target_hits = Arc::new(AtomicUsize::new(0));
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let hits_bg = target_hits.clone();
        let stop_bg = stop.clone();
        let target_thread = thread::spawn(move || {
            while !stop_bg.load(Ordering::Relaxed) {
                match target_listener.accept() {
                    Ok((_s, _)) => {
                        hits_bg.fetch_add(1, Ordering::SeqCst);
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(_) => break,
                }
            }
        });

        let origin_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let origin_addr = origin_listener.local_addr().unwrap();
        let origin_thread = thread::spawn(move || {
            let (mut stream, _) = origin_listener.accept().unwrap();
            stream.set_read_timeout(Some(Duration::from_secs(3))).ok();
            let mut buf = [0_u8; 4096];
            let _ = stream.read(&mut buf);
            let resp = format!(
                "HTTP/1.1 {status} Redirect\r\nLocation: http://{target_addr}/exfiltrate\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            );
            let _ = stream.write_all(resp.as_bytes());
        });

        (
            format!("http://{origin_addr}"),
            target_hits,
            origin_thread,
            stop,
            target_thread,
        )
    }

    #[test]
    fn non_https_and_untrusted_endpoints_fail_closed_before_socket() {
        let bad_endpoints = [
            "http://untrusted.example",
            "http://localhost:8080",
            "http://127.0.0.1.evil.test",
            "ftp://example.com",
            "postgres://user:secret@db.example/db",
            "https://user:secret@trusted.example",
            "https://trusted.example?token=secret",
            "https://trusted.example#secret",
        ];

        for endpoint in bad_endpoints {
            #[cfg(feature = "neon")]
            {
                let route = DatastoreRoute::neon(endpoint, Some("neon-secret".into()));
                assert!(
                    route.validate().is_err(),
                    "neon validate must reject `{endpoint}`"
                );
                let err = Connection::open_with_context(route, Some(ctx()))
                    .err()
                    .expect("neon open must fail closed");
                assert!(!format!("{err:?}").contains("secret"));
            }

            #[cfg(feature = "supabase")]
            {
                let route = DatastoreRoute::supabase(endpoint, Some("supabase-secret".into()));
                assert!(
                    route.validate().is_err(),
                    "supabase validate must reject `{endpoint}`"
                );
                let err = Connection::open_with_context(route, Some(ctx()))
                    .err()
                    .expect("supabase open must fail closed");
                assert!(!format!("{err:?}").contains("secret"));
            }
        }
    }

    #[cfg(feature = "neon")]
    #[test]
    fn neon_blocks_redirects_without_contacting_target_or_leaking_token() {
        use dactyl_db::OpenOptions;
        for status in [302_u16, 307, 308] {
            let (origin, target_hits, origin_thread, stop, target_thread) =
                spawn_redirect_and_canary(status);
            let db = Connection::open_with_options_and_context(
                DatastoreRoute::neon(&origin, Some("neon-secret-token".into())),
                OpenOptions::default(),
                Some(ctx()),
            )
            .unwrap();
            let read_err = db.read("SELECT 1", &[]).unwrap_err();
            assert_eq!(read_err.adapter_kind(), Some(AdapterErrorKind::Protocol));
            assert!(!format!("{read_err:?}").contains("neon-secret-token"));
            assert!(!format!("{read_err:?}").contains("exfiltrate"));

            origin_thread.join().unwrap();
            stop.store(true, Ordering::Relaxed);
            target_thread.join().unwrap();
            assert_eq!(target_hits.load(Ordering::SeqCst), 0);
        }

        let (origin, target_hits, origin_thread, stop, target_thread) =
            spawn_redirect_and_canary(307);
        let db = Connection::open_with_options_and_context(
            DatastoreRoute::neon(&origin, Some("neon-secret-token".into())),
            OpenOptions::default(),
            Some(ctx()),
        )
        .unwrap();
        let write_err = db.write("INSERT INTO t VALUES (1)", &[]).unwrap_err();
        assert_eq!(write_err.adapter_kind(), Some(AdapterErrorKind::Protocol));
        assert_eq!(write_err.mutation_outcome(), MutationOutcome::Uncertain);

        origin_thread.join().unwrap();
        stop.store(true, Ordering::Relaxed);
        target_thread.join().unwrap();
        assert_eq!(target_hits.load(Ordering::SeqCst), 0);
    }

    #[cfg(feature = "supabase")]
    #[test]
    fn supabase_and_cloud_connector_enforce_origin_pinning_and_block_redirects() {
        use dactyl_db::{
            AuthenticationStart, CloudConnector, LogicalConnector, OperationKind, ResourceBudget,
            SupabaseRpcConfig,
        };

        // 1. Origin validation rejects non-HTTPS, pathful, or secret-key configs.
        for bad_origin in [
            "http://untrusted.example",
            "http://localhost:8080",
            "https://trusted.example/subpath",
            "https://user:secret@trusted.example",
            "https://trusted.example?leak=1",
            "https://trusted.example#frag",
        ] {
            assert!(CloudConnector::validate_origin(bad_origin).is_err());
            assert!(LogicalConnector::cloud_with_origin(bad_origin).is_err());
            assert!(LogicalConnector::cloud_with_config(SupabaseRpcConfig::v1(
                bad_origin,
                "sb_publishable_valid_key"
            ))
            .is_err());
        }
        assert!(LogicalConnector::cloud_with_config(SupabaseRpcConfig::v1(
            "https://trusted.example",
            "sb_secret_forbidden"
        ))
        .is_err());

        // 2. Discovery fails closed if profile project_origin disagrees with selected origin.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buf = [0_u8; 4096];
            let _ = stream.read(&mut buf);
            let body = r#"{"version":1,"project_origin":"https://evil.example","publishable_key":"sb_publishable_test","schema":"dactyl_api"}"#;
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(resp.as_bytes());
        });
        let err = CloudConnector::discover(&format!("http://{addr}")).unwrap_err();
        assert_eq!(err.adapter_kind(), Some(AdapterErrorKind::Protocol));
        server.join().unwrap();

        // 3. Browser handoff validation rejects cross-origin, traversal, or unexpected paths.
        let connector = CloudConnector::with_config(SupabaseRpcConfig::v1(
            "https://trusted.example",
            "sb_publishable_valid_key",
        ))
        .unwrap();
        let flow_id = "11111111-1111-4111-8111-111111111111";
        for bad_url in [
            format!("https://evil.example/functions/v1/decapod-onboarding/begin?flow_id={flow_id}&ticket=abc"),
            format!("http://trusted.example/functions/v1/decapod-onboarding/begin?flow_id={flow_id}&ticket=abc"),
            format!("https://trusted.example/evil/begin?flow_id={flow_id}&ticket=abc"),
            format!("https://trusted.example/functions/v1/decapod-onboarding/begin?flow_id={flow_id}&ticket=abc#leak"),
        ] {
            let start = AuthenticationStart {
                flow_id: flow_id.into(),
                browser_url: bad_url,
                expires_at: "2099-01-01T00:00:00Z".into(),
            };
            assert!(connector.validate_start(&start).is_err());
        }

        // 4. Redirect target is never contacted by CloudConnector discovery, control RPC, or SupabaseAdapter.
        let (origin, target_hits, origin_thread, stop, target_thread) =
            spawn_redirect_and_canary(308);
        let err = LogicalConnector::cloud_with_origin(&origin).unwrap_err();
        assert_eq!(err.adapter_kind(), Some(AdapterErrorKind::Protocol));
        origin_thread.join().unwrap();
        stop.store(true, Ordering::Relaxed);
        target_thread.join().unwrap();
        assert_eq!(target_hits.load(Ordering::SeqCst), 0);

        let (origin, target_hits, origin_thread, stop, target_thread) =
            spawn_redirect_and_canary(307);
        let conn =
            CloudConnector::with_config(SupabaseRpcConfig::v1(&origin, "sb_publishable_valid_key"))
                .unwrap();
        let err = conn
            .control_request(
                "trust_probe",
                &json!({}),
                Some("secret-bearer-token"),
                OperationKind::Write,
                ResourceBudget::v1(),
            )
            .unwrap_err();
        assert_eq!(err.adapter_kind(), Some(AdapterErrorKind::Protocol));
        assert_eq!(err.mutation_outcome(), MutationOutcome::Uncertain);
        assert!(!format!("{err:?}").contains("secret-bearer-token"));
        assert!(!format!("{err:?}").contains("exfiltrate"));
        origin_thread.join().unwrap();
        stop.store(true, Ordering::Relaxed);
        target_thread.join().unwrap();
        assert_eq!(target_hits.load(Ordering::SeqCst), 0);

        let (origin, target_hits, origin_thread, stop, target_thread) =
            spawn_redirect_and_canary(302);
        let db = Connection::open_with_context(
            DatastoreRoute::supabase(&origin, Some("secret-bearer-token".into())),
            Some(ctx()),
        )
        .unwrap();
        let err = db.read("SELECT 1", &[]).unwrap_err();
        assert_eq!(err.adapter_kind(), Some(AdapterErrorKind::Protocol));
        assert!(!format!("{err:?}").contains("secret-bearer-token"));
        origin_thread.join().unwrap();
        stop.store(true, Ordering::Relaxed);
        target_thread.join().unwrap();
        assert_eq!(target_hits.load(Ordering::SeqCst), 0);
    }
}
