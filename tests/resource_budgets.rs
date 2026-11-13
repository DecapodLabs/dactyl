//! Cross-adapter resource budget, streaming `visit_rows`, failure-origin, and
//! mutation-outcome regression suite for Dactyl issue #101.
//!
//! Evidence classes in this file:
//! - `local_sqlite`: real host SQLite C-ABI engine (`#[cfg(feature = "sqlite")]`)
//! - `neon_mock` / `synthetic_http`: loopback HTTP wire/envelope fixtures (`#[cfg(feature = "neon")]`, `#[cfg(feature = "supabase")]`)
//! - `local_postgres`, `hosted_supabase`, `live_propodus`: unavailable here and never claimed as passed.

use std::time::Duration;

use dactyl_db::{AccessMode, DactylError, OpenOptions, ResourceBudget, RESOURCE_BUDGET_VERSION};

#[test]
fn open_options_struct_literal_and_budget_version_remain_compatible() {
    let options = OpenOptions {
        access_mode: AccessMode::ReadOnly,
        lock_timeout: Duration::from_millis(250),
    };
    assert_eq!(options.access_mode, AccessMode::ReadOnly);
    assert_eq!(options.lock_timeout, Duration::from_millis(250));

    let budget = ResourceBudget::v1();
    assert_eq!(budget.version(), RESOURCE_BUDGET_VERSION);
    assert_eq!(budget, ResourceBudget::default());
    assert!(budget.validate().is_ok());

    let invalid = budget.with_connect_timeout(Duration::ZERO);
    assert!(matches!(invalid.validate(), Err(DactylError::Config(_))));
}

#[cfg(feature = "sqlite")]
mod sqlite_budgets {
    use std::time::Duration;

    use dactyl_db::{
        AccessMode, AdapterErrorKind, Connection, DatastoreRoute, FailureOrigin, MutationOutcome,
        OpenOptions, Operation, Parameter, RecoveryJournalMode, RecoveryOptions, ResourceBudget,
        ScanOutcome, StorageContext,
    };
    use serde_json::json;
    use tempfile::TempDir;

    #[test]
    fn context_sql_param_cell_column_row_and_batch_boundaries_are_exact() {
        let context_ok = StorageContext::new(1, json!({"k": "abcd"})).unwrap();
        let serialized_len = serde_json::to_vec(&context_ok).unwrap().len();

        let exact_ctx_budget = ResourceBudget::v1().with_max_context_bytes(serialized_len);
        assert!(Connection::open_with_budget(
            DatastoreRoute::sqlite(":memory:"),
            OpenOptions::default(),
            Some(context_ok.clone()),
            exact_ctx_budget,
        )
        .is_ok());

        let over_ctx_error = Connection::open_with_budget(
            DatastoreRoute::sqlite(":memory:"),
            OpenOptions::default(),
            Some(context_ok),
            ResourceBudget::v1().with_max_context_bytes(serialized_len - 1),
        )
        .err()
        .expect("context exceeding budget should fail");
        assert_eq!(over_ctx_error.adapter_kind(), Some(AdapterErrorKind::Quota));
        assert_eq!(
            over_ctx_error.adapter_code(),
            Some("context_budget_exceeded")
        );
        assert_eq!(over_ctx_error.failure_origin(), FailureOrigin::LocalBudget);
        assert_eq!(
            over_ctx_error.mutation_outcome(),
            MutationOutcome::NotAttempted
        );

        let sql_ok = "SELECT 'a' AS a, 'b' AS b";
        let db = Connection::open_with_budget(
            DatastoreRoute::sqlite(":memory:"),
            OpenOptions::default(),
            None,
            ResourceBudget::v1()
                .with_max_sql_bytes(sql_ok.len())
                .with_max_columns(2)
                .with_max_rows(2)
                .with_max_cell_bytes(4)
                .with_max_param_bytes(8)
                .with_max_batch_operations(2),
        )
        .unwrap();
        assert_eq!(db.read(sql_ok, &[]).unwrap().len(), 1);

        // 8-byte INTEGER result cell exceeds 4-byte max_cell_bytes.
        let int_cell_err = db.read("SELECT 1", &[]).unwrap_err();
        assert_eq!(int_cell_err.adapter_code(), Some("cell_budget_exceeded"));

        // N + 1 SQL bytes rejected before statement preparation.
        let sql_over = format!("{sql_ok} ");
        let err = db.read(&sql_over, &[]).unwrap_err();
        assert_eq!(err.adapter_code(), Some("sql_budget_exceeded"));
        assert_eq!(err.failure_origin(), FailureOrigin::LocalBudget);
        assert_eq!(err.mutation_outcome(), MutationOutcome::NotAttempted);

        // N + 1 columns rejected.
        let err = db.read("SELECT 'a', 'b', 'c'", &[]).unwrap_err();
        assert_eq!(err.adapter_code(), Some("column_budget_exceeded"));
        assert_eq!(err.failure_origin(), FailureOrigin::LocalBudget);

        // N vs N + 1 parameter cell and aggregate parameter bytes.
        assert!(db
            .read(
                "SELECT $1, $2",
                &[
                    Parameter::Text("abcd".into()),
                    Parameter::Text("efgh".into())
                ],
            )
            .is_ok());
        let cell_err = db
            .read("SELECT $1", &[Parameter::Text("abcde".into())])
            .unwrap_err();
        assert_eq!(cell_err.adapter_code(), Some("cell_budget_exceeded"));
        assert_eq!(cell_err.mutation_outcome(), MutationOutcome::NotAttempted);

        let param_err = db
            .read(
                "SELECT $1, $2, $3",
                &[
                    Parameter::Text("abcd".into()),
                    Parameter::Text("efgh".into()),
                    Parameter::Bool(true),
                ],
            )
            .unwrap_err();
        assert_eq!(param_err.adapter_code(), Some("parameter_budget_exceeded"));
        assert_eq!(param_err.mutation_outcome(), MutationOutcome::NotAttempted);

        // N vs N + 1 batch operations.
        assert!(db
            .atomic(&[
                Operation::read("SELECT 'a'", []),
                Operation::read("SELECT 'b'", []),
            ])
            .is_ok());
        let batch_err = db
            .atomic(&[
                Operation::read("SELECT 'a'", []),
                Operation::read("SELECT 'b'", []),
                Operation::read("SELECT 'c'", []),
            ])
            .unwrap_err();
        assert_eq!(batch_err.adapter_code(), Some("batch_budget_exceeded"));
        assert_eq!(batch_err.mutation_outcome(), MutationOutcome::NotAttempted);
    }

    #[test]
    fn blob_result_budget_charges_before_32x_json_array_materialization() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("blobs.db");
        let writer = Connection::open(DatastoreRoute::sqlite(path.to_string_lossy())).unwrap();
        writer
            .write("CREATE TABLE blobs (id INTEGER PRIMARY KEY, raw BLOB)", &[])
            .unwrap();
        writer
            .write(
                "INSERT INTO blobs (id, raw) VALUES (1, $1)",
                &[Parameter::Blob(vec![0xAB; 4096])],
            )
            .unwrap();
        drop(writer);

        // Raw cell is 4 KiB (within max_cell_bytes = 8 KiB), but 32x JSON Value::Array
        // expansion is >128 KiB, exceeding max_result_bytes = 32 KiB before materializing.
        let reader = Connection::open_with_budget(
            DatastoreRoute::sqlite(path.to_string_lossy()),
            OpenOptions {
                access_mode: AccessMode::ReadOnly,
                lock_timeout: Duration::from_secs(1),
            },
            None,
            ResourceBudget::v1()
                .with_max_cell_bytes(8 * 1024)
                .with_max_result_bytes(32 * 1024),
        )
        .unwrap();

        let err = reader
            .read("SELECT raw FROM blobs WHERE id = 1", &[])
            .unwrap_err();
        assert_eq!(err.adapter_kind(), Some(AdapterErrorKind::Quota));
        assert_eq!(err.adapter_code(), Some("result_budget_exceeded"));
        assert_eq!(err.failure_origin(), FailureOrigin::LocalBudget);
    }

    #[test]
    fn visit_rows_distinguishes_early_stop_from_budget_exhaustion_and_releases_locks() {
        let db = Connection::open_with_budget(
            DatastoreRoute::sqlite(":memory:"),
            OpenOptions::default(),
            None,
            ResourceBudget::v1().with_max_rows(3),
        )
        .unwrap();
        db.write(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT)",
            &[],
        )
        .unwrap();
        for idx in 1..=4 {
            db.write(
                "INSERT INTO items (id, label) VALUES ($1, $2)",
                &[
                    Parameter::Integer(idx),
                    Parameter::Text(format!("item-{idx}")),
                ],
            )
            .unwrap();
        }

        // Deliberate early stop after 2 rows returns ScanOutcome { rows: 2, complete: false }
        // and finalizes the SQLite statement immediately so DROP TABLE does not hit SQLITE_LOCKED.
        let mut seen = Vec::new();
        let outcome = db
            .visit_rows("SELECT id, label FROM items ORDER BY id", &[], |row| {
                seen.push(row.get_int("id")?);
                Ok(seen.len() < 2)
            })
            .unwrap();
        assert_eq!(
            outcome,
            ScanOutcome {
                rows: 2,
                complete: false
            }
        );
        assert_eq!(seen, vec![1, 2]);

        // Unterminated scan over 4 rows with max_rows = 3 returns Err, never silent truncation.
        let err = db
            .visit_rows("SELECT id FROM items ORDER BY id", &[], |_row| Ok(true))
            .unwrap_err();
        assert_eq!(err.adapter_kind(), Some(AdapterErrorKind::Quota));
        assert_eq!(err.adapter_code(), Some("row_budget_exceeded"));
        assert_eq!(err.failure_origin(), FailureOrigin::LocalBudget);

        // Statement lock was deterministically released on both early stop and budget error.
        db.write("DROP TABLE items", &[]).unwrap();
        assert!(!db.has_table("items").unwrap());
    }

    #[test]
    fn local_atomic_read_budget_failure_rolls_back_prior_writes_cleanly() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("atomic_rollback.db");
        let setup = Connection::open(DatastoreRoute::sqlite(path.to_string_lossy())).unwrap();
        setup
            .write(
                "CREATE TABLE ledger (id INTEGER PRIMARY KEY, entry TEXT)",
                &[],
            )
            .unwrap();
        for idx in 1..=3 {
            setup
                .write(
                    "INSERT INTO ledger (id, entry) VALUES ($1, $2)",
                    &[Parameter::Integer(idx), Parameter::Text("seed".into())],
                )
                .unwrap();
        }
        drop(setup);

        let bounded = Connection::open_with_budget(
            DatastoreRoute::sqlite(path.to_string_lossy()),
            OpenOptions::default(),
            None,
            ResourceBudget::v1().with_max_rows(2),
        )
        .unwrap();

        let err = bounded
            .atomic(&[
                Operation::write(
                    "INSERT INTO ledger (id, entry) VALUES (99, $1)",
                    [Parameter::Text("must-rollback".into())],
                ),
                Operation::read("SELECT id, entry FROM ledger ORDER BY id", []),
            ])
            .unwrap_err();
        assert_eq!(err.adapter_kind(), Some(AdapterErrorKind::Quota));
        assert_eq!(err.adapter_code(), Some("row_budget_exceeded"));
        assert_eq!(err.failure_origin(), FailureOrigin::LocalBudget);
        assert_eq!(err.mutation_outcome(), MutationOutcome::RolledBack);

        let verify = Connection::open(DatastoreRoute::sqlite(path.to_string_lossy())).unwrap();
        let count = verify
            .read("SELECT COUNT(*) AS n FROM ledger WHERE id = 99", &[])
            .unwrap()
            .as_slice()[0]
            .get_int("n")
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    #[cfg(any(target_os = "linux", target_os = "android"))]
    fn synthetic_component_reference_growth_and_recovery_stay_bounded() {
        let dir = TempDir::new().unwrap();
        let source = dir.path().join("growth.db");
        let backup = dir.path().join("growth.backup.db");
        let archive = dir.path().join("growth.archive.db");

        // Caller-owned schema storing compact opaque component references alongside
        // deduplicated content blobs; Dactyl treats all columns as opaque SQL values.
        let writer = Connection::open(DatastoreRoute::sqlite(source.to_string_lossy())).unwrap();
        writer
            .atomic(&[
                Operation::schema(
                    "CREATE TABLE blobs (content_sha256 TEXT PRIMARY KEY, codec_version INTEGER NOT NULL, byte_len INTEGER NOT NULL, payload BLOB NOT NULL)",
                    [],
                ),
                Operation::schema(
                    "CREATE TABLE event_components (event_id INTEGER PRIMARY KEY, component_kind TEXT NOT NULL, codec_version INTEGER NOT NULL, byte_len INTEGER NOT NULL, content_sha256 TEXT NOT NULL)",
                    [],
                ),
            ])
            .unwrap();

        let shared_blob = vec![0x5A_u8; 32 * 1024];
        let digest = "sha256:5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a";
        writer
            .write(
                "INSERT INTO blobs (content_sha256, codec_version, byte_len, payload) VALUES ($1, $2, $3, $4)",
                &[
                    Parameter::Text(digest.into()),
                    Parameter::Integer(1),
                    Parameter::Integer(shared_blob.len() as i64),
                    Parameter::Blob(shared_blob.clone()),
                ],
            )
            .unwrap();

        let mut batch = Vec::with_capacity(128);
        for event_id in 1..=128_i64 {
            batch.push(Operation::write(
                "INSERT INTO event_components (event_id, component_kind, codec_version, byte_len, content_sha256) VALUES ($1, $2, $3, $4, $5)",
                vec![
                    Parameter::Integer(event_id),
                    Parameter::Text("trajectory_capsule".into()),
                    Parameter::Integer(1),
                    Parameter::Integer(shared_blob.len() as i64),
                    Parameter::Text(digest.into()),
                ],
            ));
        }
        writer.atomic(&batch).unwrap();
        let backup_report = writer.backup(&backup).unwrap();
        assert!(backup_report.bytes > 0);
        drop(writer);

        // Open with a tight result budget (64 KiB) that would reject a 32x JSON-array
        // decode of the 32 KiB blob (~1 MiB), proving logical recovery streams raw C-ABI
        // columns without 32x JSON BLOB amplification.
        let mut maint = Connection::open_with_budget(
            DatastoreRoute::sqlite(source.to_string_lossy()),
            OpenOptions::default(),
            None,
            ResourceBudget::v1()
                .with_max_cell_bytes(64 * 1024)
                .with_max_result_bytes(64 * 1024),
        )
        .unwrap();
        let recovered = maint
            .recover_from_dump_reload(RecoveryOptions::new(
                archive.to_string_lossy(),
                RecoveryJournalMode::Delete,
            ))
            .unwrap();
        assert_eq!(recovered.journal_mode, RecoveryJournalMode::Delete);

        let mut ref_count = 0_u64;
        let scan = maint
            .visit_rows(
                "SELECT component_kind, codec_version, byte_len, content_sha256 FROM event_components ORDER BY event_id",
                &[],
                |row| {
                    assert_eq!(row.get_str("component_kind")?, "trajectory_capsule");
                    assert_eq!(row.get_int("codec_version")?, 1);
                    assert_eq!(row.get_int("byte_len")?, 32 * 1024);
                    assert_eq!(row.get_str("content_sha256")?, digest);
                    ref_count += 1;
                    Ok(true)
                },
            )
            .unwrap();
        assert_eq!(
            scan,
            ScanOutcome {
                rows: 128,
                complete: true
            }
        );
        assert_eq!(ref_count, 128);
    }
}

#[cfg(any(feature = "neon", feature = "supabase"))]
mod remote_budgets {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;
    use std::time::Duration;

    use dactyl_db::{
        AdapterErrorKind, Connection, DatastoreRoute, FailureOrigin, MutationOutcome, OpenOptions,
        ResourceBudget, StorageContext,
    };
    use serde_json::json;

    fn context() -> StorageContext {
        StorageContext::new(1, json!({"scope": "budget-fixture"})).unwrap()
    }

    fn one_shot_http(
        status: u16,
        declared_len: Option<usize>,
        body: String,
        drop_without_response: bool,
    ) -> (String, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut buf = [0_u8; 4096];
            let _ = stream.read(&mut buf);
            if drop_without_response {
                return;
            }
            let content_len = declared_len.unwrap_or(body.len());
            let _ = write!(
                stream,
                "HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\nContent-Length: {content_len}\r\nConnection: close\r\n\r\n{body}"
            );
        });
        (endpoint, handle)
    }

    #[cfg(feature = "neon")]
    #[test]
    fn neon_enforces_wire_depth_visit_and_uncertain_mutation_contracts() {
        use dactyl_db::{Parameter, ScanOutcome};
        use serde_json::Value;
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc, Mutex,
        };

        // 1a. Unframed (no Content-Length) oversized body stopped mid-stream by read_bounded -> response_too_large.
        let oversized_body = format!(
            "{{\"columns\":[],\"rows\":[],\"pad\":\"{}\"}}",
            "x".repeat(4096)
        );
        let unframed_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let unframed_endpoint = format!("http://{}", unframed_listener.local_addr().unwrap());
        let unframed_body = oversized_body.clone();
        let unframed_server = thread::spawn(move || {
            let (mut stream, _) = unframed_listener.accept().unwrap();
            let mut buf = [0_u8; 4096];
            let _ = stream.read(&mut buf);
            let _ = write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{unframed_body}"
            );
        });
        let db = Connection::open_with_budget(
            DatastoreRoute::neon(&unframed_endpoint, Some("tok".into())),
            OpenOptions::default(),
            Some(context()),
            ResourceBudget::v1().with_max_wire_bytes(512),
        )
        .unwrap();
        let err = db.read("SELECT 1", &[]).unwrap_err();
        assert_eq!(err.adapter_kind(), Some(AdapterErrorKind::Protocol));
        assert_eq!(err.adapter_code(), Some("response_too_large"));
        assert_eq!(err.failure_origin(), FailureOrigin::MalformedResponse);
        unframed_server.join().unwrap();

        // 1b. Lying small Content-Length truncates JSON frame -> malformed_response.
        let (endpoint, server) = one_shot_http(200, Some(32), oversized_body, false);
        let db = Connection::open_with_budget(
            DatastoreRoute::neon(&endpoint, Some("tok".into())),
            OpenOptions::default(),
            Some(context()),
            ResourceBudget::v1().with_max_wire_bytes(512),
        )
        .unwrap();
        let err = db.read("SELECT 1", &[]).unwrap_err();
        assert_eq!(err.adapter_kind(), Some(AdapterErrorKind::Protocol));
        assert_eq!(err.adapter_code(), Some("malformed_response"));
        assert_eq!(err.failure_origin(), FailureOrigin::MalformedResponse);
        server.join().unwrap();

        // 2. Deeply nested JSON (>64) rejected as MalformedResponse.
        let mut deep = json!(1);
        for _ in 0..65 {
            deep = json!([deep]);
        }
        let body = json!({"columns": ["x"], "rows": [{"x": deep}]}).to_string();
        let (endpoint, server) = one_shot_http(200, None, body, false);
        let db = Connection::open_with_context(
            DatastoreRoute::neon(&endpoint, Some("tok".into())),
            Some(context()),
        )
        .unwrap();
        let err = db.read("SELECT x", &[]).unwrap_err();
        assert_eq!(err.adapter_kind(), Some(AdapterErrorKind::Protocol));
        assert_eq!(err.adapter_code(), Some("malformed_response"));
        assert_eq!(err.failure_origin(), FailureOrigin::MalformedResponse);
        server.join().unwrap();

        // 3a. visit_rows early stop within envelope row budget.
        let body = json!({
            "columns": ["id"],
            "rows": [{"id": 1}, {"id": 2}, {"id": 3}]
        })
        .to_string();
        let (endpoint, server) = one_shot_http(200, None, body.clone(), false);
        let db = Connection::open_with_budget(
            DatastoreRoute::neon(&endpoint, Some("tok".into())),
            OpenOptions::default(),
            Some(context()),
            ResourceBudget::v1().with_max_rows(3),
        )
        .unwrap();
        let scan = db
            .visit_rows("SELECT id FROM t", &[], |row| Ok(row.get_int("id")? < 2))
            .unwrap();
        assert_eq!(
            scan,
            ScanOutcome {
                rows: 2,
                complete: false
            }
        );
        server.join().unwrap();

        // 3b. Envelope exceeding max_rows fails closed before materializing unbounded rows.
        let (endpoint, server) = one_shot_http(200, None, body, false);
        let db = Connection::open_with_budget(
            DatastoreRoute::neon(&endpoint, Some("tok".into())),
            OpenOptions::default(),
            Some(context()),
            ResourceBudget::v1().with_max_rows(2),
        )
        .unwrap();
        let row_err = db
            .visit_rows("SELECT id FROM t", &[], |_| Ok(true))
            .unwrap_err();
        assert_eq!(row_err.adapter_kind(), Some(AdapterErrorKind::Quota));
        assert_eq!(row_err.adapter_code(), Some("row_budget_exceeded"));
        assert_eq!(row_err.failure_origin(), FailureOrigin::LocalBudget);
        server.join().unwrap();

        // 4. Post-send disconnect on mutating write -> MutationOutcome::Uncertain,
        //    followed by idempotent caller reconciliation instead of blind duplicate insert.
        let (endpoint, server) = one_shot_http(200, None, String::new(), true);
        let db = Connection::open_with_context(
            DatastoreRoute::neon(&endpoint, Some("tok".into())),
            Some(context()),
        )
        .unwrap();
        let err = db
            .write(
                "INSERT INTO tasks (id, title) VALUES ($1, $2)",
                &[
                    Parameter::Text("req-1".into()),
                    Parameter::Text("demo".into()),
                ],
            )
            .unwrap_err();
        assert_eq!(err.adapter_kind(), Some(AdapterErrorKind::Transport));
        assert_eq!(err.mutation_outcome(), MutationOutcome::Uncertain);
        server.join().unwrap();

        // Simulate idempotent server state where first write actually committed before disconnect:
        // caller inspects/upserts by idempotency key rather than blindly replaying non-idempotent INSERT.
        let persisted = Arc::new(Mutex::new(vec!["req-1".to_owned()]));
        let attempts = Arc::new(AtomicUsize::new(0));
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let recon_endpoint = format!("http://{}", listener.local_addr().unwrap());
        let persisted_srv = persisted.clone();
        let attempts_srv = attempts.clone();
        let recon_srv = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buf = [0_u8; 4096];
            let n = stream.read(&mut buf).unwrap();
            let req = String::from_utf8_lossy(&buf[..n]);
            attempts_srv.fetch_add(1, Ordering::SeqCst);
            assert!(
                req.contains("SELECT id FROM tasks WHERE id = $1"),
                "uncertain outcome must reconcile state before replaying: {req}"
            );
            let rows: Vec<Value> = persisted_srv
                .lock()
                .unwrap()
                .iter()
                .map(|id| json!({"id": id}))
                .collect();
            let resp = json!({"columns": ["id"], "rows": rows}).to_string();
            let _ = write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{resp}",
                resp.len()
            );
        });
        let recon_db = Connection::open_with_context(
            DatastoreRoute::neon(&recon_endpoint, Some("tok".into())),
            Some(context()),
        )
        .unwrap();
        let existing = recon_db
            .read(
                "SELECT id FROM tasks WHERE id = $1",
                &[Parameter::Text("req-1".into())],
            )
            .unwrap();
        assert_eq!(existing.len(), 1);
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        assert_eq!(persisted.lock().unwrap().len(), 1);
        recon_srv.join().unwrap();
    }

    #[cfg(feature = "supabase")]
    #[test]
    fn supabase_distinguishes_not_attempted_remote_policy_and_uncertain_mutations() {
        use dactyl_db::{AccessMode, Operation};

        // Preflight batch limit fails before network -> NotAttempted + LocalBudget.
        let offline = Connection::open_with_budget(
            DatastoreRoute::supabase("http://127.0.0.1:9", Some("bearer".into())),
            OpenOptions {
                access_mode: AccessMode::ReadWrite,
                lock_timeout: Duration::from_secs(1),
            },
            Some(context()),
            ResourceBudget::v1().with_max_batch_operations(1),
        )
        .unwrap();
        let preflight_err = offline
            .atomic(&[
                Operation::write("INSERT INTO t VALUES (1)", []),
                Operation::write("INSERT INTO t VALUES (2)", []),
            ])
            .unwrap_err();
        assert_eq!(preflight_err.adapter_code(), Some("batch_budget_exceeded"));
        assert_eq!(preflight_err.failure_origin(), FailureOrigin::LocalBudget);
        assert_eq!(
            preflight_err.mutation_outcome(),
            MutationOutcome::NotAttempted
        );

        // Remote 429 rate-limit denial -> RemotePolicy + RolledBack.
        let (endpoint, server) = one_shot_http(
            429,
            None,
            json!({"error": {"code": "rate_limited", "message": "slow down"}}).to_string(),
            false,
        );
        let db = Connection::open_with_context(
            DatastoreRoute::supabase(&endpoint, Some("bearer".into())),
            Some(context()),
        )
        .unwrap();
        let policy_err = db.write("DELETE FROM t", &[]).unwrap_err();
        assert_eq!(
            policy_err.adapter_kind(),
            Some(AdapterErrorKind::RateLimited)
        );
        assert_eq!(policy_err.failure_origin(), FailureOrigin::RemotePolicy);
        assert_eq!(policy_err.mutation_outcome(), MutationOutcome::RolledBack);
        server.join().unwrap();

        // Post-send mutating batch whose returned read exceeds local row budget ->
        // server may already have committed the batch, so outcome MUST be Uncertain!
        let batch_resp = json!({
            "results": [
                {"columns": [], "rows": [], "affected_rows": 1, "generated_keys": []},
                {"columns": ["id"], "rows": [{"id": 1}, {"id": 2}], "affected_rows": 2, "generated_keys": []}
            ]
        })
        .to_string();
        let (endpoint, server) = one_shot_http(200, None, batch_resp, false);
        let db = Connection::open_with_budget(
            DatastoreRoute::supabase(&endpoint, Some("bearer".into())),
            OpenOptions::default(),
            Some(context()),
            ResourceBudget::v1().with_max_rows(1),
        )
        .unwrap();
        let uncertain_err = db
            .atomic(&[
                Operation::write("INSERT INTO t VALUES (1)", []),
                Operation::read("SELECT id FROM t", []),
            ])
            .unwrap_err();
        assert_eq!(uncertain_err.adapter_kind(), Some(AdapterErrorKind::Quota));
        assert_eq!(uncertain_err.failure_origin(), FailureOrigin::LocalBudget);
        assert_eq!(uncertain_err.mutation_outcome(), MutationOutcome::Uncertain);
        server.join().unwrap();
    }
}
