//! Opt-in client assertions against an externally provisioned real service.
//!
//! No service, database, principal, policy, or cleanup is provisioned here. The
//! ignored tests require a protected fixture and leave fixed synthetic records
//! for an independent database observer. A client receipt is not PostgreSQL,
//! authorization-policy, cleanup, or hosted-pooler proof. See the operator
//! contract in docs/supabase-conformance.md before explicitly running either test.
#![cfg(feature = "supabase")]

use std::fs::{File, OpenOptions as FileOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use dactyl_db::{
    AdapterErrorKind, Connection, DactylError, Datastore, DatastoreRoute, DatastoreTransport,
    GeneratedKey, Operation, OperationResult, Parameter, RecoveryJournalMode, RecoveryOptions, Row,
    Rows, StorageContext, WriteResult,
};
use serde::Deserialize;
use serde_json::{json, Value};

const FIXTURE_ENV: &str = "DACTYL_SUPABASE_FIXTURE";
const HOSTED_ENV: &str = "DACTYL_SUPABASE_HOSTED_PROOF";
const SQL_PROFILE: &str = "dactyl_service_v1";
const TABLE: &str = "dactyl_service_conformance";
const MAX_FIXTURE_BYTES: u64 = 64 * 1024;
const COLUMNS: [&str; 8] = [
    "id",
    "flag",
    "signed_value",
    "real_value",
    "text_value",
    "blob_value",
    "nullable_text",
    "version",
];
const INSERT: &str = "INSERT INTO dactyl_service_conformance (id, flag, signed_value, real_value, text_value, blob_value, nullable_text, version) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING id";
const SELECT_ALL: &str = "SELECT id, flag, signed_value, real_value, text_value, blob_value, nullable_text, version FROM dactyl_service_conformance ORDER BY id";
const SELECT_ONE: &str = "SELECT id, flag, signed_value, real_value, text_value, blob_value, nullable_text, version FROM dactyl_service_conformance WHERE id = $1";
const SELECT_COUNT: &str = "SELECT COUNT(*) AS row_count FROM dactyl_service_conformance";
const CAS: &str = "UPDATE dactyl_service_conformance SET text_value = $1, version = version + 1 WHERE id = $2 AND version = $3";
const WRITE_AS_READ: &str = "UPDATE dactyl_service_conformance SET text_value = $1, version = version + 1 WHERE id = $2 RETURNING id";
const CTE_WRITE_AS_READ: &str = "WITH changed AS (UPDATE dactyl_service_conformance SET text_value = $1, version = version + 1 WHERE id = $2 RETURNING id) SELECT id FROM changed";
const UNSUPPORTED_SCHEMA: &str =
    "ALTER TABLE dactyl_service_conformance ADD COLUMN forbidden_column TEXT";
const INJECTION_TEXT: &str = "bound text '); DROP TABLE dactyl_service_conformance; --";

#[derive(Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum Profile {
    LocalPostgres,
    HostedSupabase,
}

impl Profile {
    fn label(self) -> &'static str {
        match self {
            Self::LocalPostgres => "local_postgres",
            Self::HostedSupabase => "hosted_supabase",
        }
    }
}

// Deliberately no Debug/Serialize implementations for credential-bearing data.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Principal {
    token: String,
    context: StorageContext,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    version: u16,
    profile: Profile,
    disposable: bool,
    independent_principals: bool,
    sql_profile: String,
    endpoint: String,
    allowed: [Principal; 2],
    authentication_denied: Principal,
    authorization_denied: Principal,
    receipt_path: PathBuf,
}

#[derive(Debug)]
struct FixtureError(&'static str);

impl std::fmt::Display for FixtureError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "unavailable is not a pass: {}", self.0)
    }
}

fn parse_fixture(
    bytes: &[u8],
    profile: Profile,
    hosted_opt_in: bool,
) -> Result<Fixture, FixtureError> {
    if bytes.len() as u64 > MAX_FIXTURE_BYTES {
        return Err(FixtureError("fixture exceeds size limit"));
    }
    // serde errors may echo unknown fields or enum values, including secrets.
    let fixture: Fixture =
        serde_json::from_slice(bytes).map_err(|_| FixtureError("invalid fixture JSON or shape"))?;
    if fixture.version != 1 || fixture.profile != profile || fixture.sql_profile != SQL_PROFILE {
        return Err(FixtureError("fixture version or profile mismatch"));
    }
    if !fixture.disposable {
        return Err(FixtureError(
            "fresh disposable database acknowledgement required",
        ));
    }
    if !fixture.independent_principals || fixture.allowed[0].token == fixture.allowed[1].token {
        return Err(FixtureError(
            "two independently provisioned allowed principals required",
        ));
    }
    if fixture
        .allowed
        .iter()
        .any(|principal| principal.token == fixture.authentication_denied.token)
    {
        return Err(FixtureError(
            "authentication-denied token must differ from allowed tokens",
        ));
    }
    if fixture.allowed.iter().any(|principal| {
        principal.token == fixture.authorization_denied.token
            && principal.context == fixture.authorization_denied.context
    }) {
        return Err(FixtureError(
            "authorization-denied pair must differ from allowed pairs",
        ));
    }
    DatastoreRoute::supabase(&fixture.endpoint, None)
        .validate()
        .map_err(|_| FixtureError("invalid service endpoint"))?;
    let url = reqwest::Url::parse(&fixture.endpoint)
        .map_err(|_| FixtureError("invalid service endpoint"))?;
    match profile {
        Profile::LocalPostgres => {
            let loopback = url.host_str().is_some_and(|host| {
                host.trim_matches(['[', ']'])
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|address| address.is_loopback())
            });
            if !loopback {
                return Err(FixtureError(
                    "local profile requires a literal loopback service",
                ));
            }
        }
        Profile::HostedSupabase if !hosted_opt_in || url.scheme() != "https" => {
            return Err(FixtureError(
                "hosted profile requires explicit opt-in and HTTPS",
            ));
        }
        Profile::HostedSupabase => {}
    }
    for principal in fixture.allowed.iter().chain([
        &fixture.authentication_denied,
        &fixture.authorization_denied,
    ]) {
        if principal.token.is_empty()
            || !principal
                .token
                .bytes()
                .all(|byte| (0x21..=0x7e).contains(&byte))
        {
            return Err(FixtureError("invalid user-session bearer shape"));
        }
        StorageContext::new(
            principal.context.version(),
            principal.context.payload().clone(),
        )
        .map_err(|_| FixtureError("invalid opaque context envelope"))?;
    }
    if !fixture.receipt_path.is_absolute() || fixture.receipt_path.file_name().is_none() {
        return Err(FixtureError("receipt path must name a new absolute file"));
    }
    Ok(fixture)
}

fn load_fixture(
    path: &Path,
    profile: Profile,
    hosted_opt_in: bool,
) -> Result<Fixture, FixtureError> {
    let original_metadata =
        std::fs::symlink_metadata(path).map_err(|_| FixtureError("fixture file unavailable"))?;
    if !original_metadata.file_type().is_file() {
        return Err(FixtureError("fixture must be a protected regular file"));
    }
    let file = File::open(path).map_err(|_| FixtureError("fixture file unavailable"))?;
    let metadata = file
        .metadata()
        .map_err(|_| FixtureError("fixture metadata unavailable"))?;
    if !metadata.is_file() {
        return Err(FixtureError("fixture must be a protected regular file"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        if metadata.dev() != original_metadata.dev() || metadata.ino() != original_metadata.ino() {
            return Err(FixtureError("fixture changed while opening"));
        }
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(FixtureError("fixture must not allow group or other access"));
        }
    }
    #[cfg(not(unix))]
    return Err(FixtureError(
        "protected fixture permission checks require Unix",
    ));

    #[cfg(unix)]
    {
        let mut bytes = Vec::new();
        file.take(MAX_FIXTURE_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| FixtureError("fixture could not be read"))?;
        parse_fixture(&bytes, profile, hosted_opt_in)
    }
}

#[derive(Debug)]
struct CheckError {
    stage: &'static str,
    kind: Option<AdapterErrorKind>,
}

type Check<T> = Result<T, CheckError>;

fn require(condition: bool, stage: &'static str) -> Check<()> {
    if condition {
        Ok(())
    } else {
        Err(CheckError { stage, kind: None })
    }
}

fn checked<T>(result: Result<T, DactylError>, stage: &'static str) -> Check<T> {
    result.map_err(|error| CheckError {
        stage,
        kind: error.adapter_kind(),
    })
}

fn denied<T>(
    result: Result<T, DactylError>,
    kind: AdapterErrorKind,
    stage: &'static str,
) -> Check<()> {
    match result {
        Err(error) if error.adapter_kind() == Some(kind) => Ok(()),
        Err(error) => Err(CheckError {
            stage,
            kind: error.adapter_kind(),
        }),
        Ok(_) => Err(CheckError { stage, kind: None }),
    }
}

fn open(fixture: &Fixture, principal: &Principal) -> Check<Connection> {
    let db = checked(
        Connection::open_with_context(
            DatastoreRoute::supabase(&fixture.endpoint, Some(principal.token.clone())),
            Some(principal.context.clone()),
        ),
        "open explicit Supabase route",
    )?;
    require(
        db.datastore() == Datastore::Supabase && db.route().transport() == DatastoreTransport::Http,
        "provider must remain Supabase over HTTP; no fallback",
    )?;
    Ok(db)
}

fn sample(id: i64, committed: bool) -> Vec<Parameter> {
    match id {
        101 => vec![
            id.into(),
            true.into(),
            (-9_007_199_254_740_993_i64).into(),
            1.25.into(),
            if committed {
                "committed update"
            } else {
                INJECTION_TEXT
            }
            .into(),
            Parameter::Blob(vec![0, 1, 127, 255]),
            Parameter::Null,
            if committed { 2_i64 } else { 1_i64 }.into(),
        ],
        202 => vec![
            id.into(),
            false.into(),
            42_i64.into(),
            (-2.5).into(),
            "second row".into(),
            Parameter::Blob(vec![]),
            "present".into(),
            1_i64.into(),
        ],
        _ => unreachable!("only fixed public fixture IDs exist"),
    }
}

fn check_row(row: &Row, id: i64, committed: bool) -> Check<()> {
    require(
        row.columns.iter().map(String::as_str).eq(COLUMNS),
        "ordered named columns",
    )?;
    // Exact JSON equality checks normalization too: bool is not integer 1, a
    // blob is not base64 text, and the large signed integer must not be rounded.
    let expected: Vec<Value> = sample(id, committed)
        .iter()
        .map(|parameter| serde_json::to_value(parameter).expect("fixed synthetic value serializes"))
        .collect();
    require(row.values == expected, "exact scalar/blob/null roundtrip")?;
    require(
        checked(row.get_int("id"), "typed ID")? == id,
        "typed ID value",
    )?;
    require(
        checked(row.get_bool("flag"), "typed bool")? == (id == 101),
        "typed bool value",
    )?;
    require(
        checked(row.get_int("signed_value"), "typed signed integer")?
            == if id == 101 {
                -9_007_199_254_740_993
            } else {
                42
            },
        "typed signed integer value",
    )?;
    require(
        checked(row.get_real("real_value"), "typed real")? == if id == 101 { 1.25 } else { -2.5 },
        "typed finite real value",
    )?;
    require(
        checked(row.get_str("text_value"), "typed text")?
            == if id == 202 {
                "second row"
            } else if committed {
                "committed update"
            } else {
                INJECTION_TEXT
            },
        "typed bound text value",
    )?;
    require(
        checked(row.get_blob("blob_value"), "typed blob")?
            == if id == 101 {
                vec![0, 1, 127, 255]
            } else {
                vec![]
            },
        "typed blob value",
    )?;
    require(
        checked(
            row.get::<_, Option<String>>("nullable_text"),
            "typed nullable text",
        )? == if id == 101 {
            None
        } else {
            Some("present".into())
        },
        "typed nullable text value",
    )?;
    require(
        checked(row.get_int("version"), "typed version")?
            == if id == 101 && committed { 2 } else { 1 },
        "typed version value",
    )
}

fn check_single(rows: &Rows, id: i64, committed: bool) -> Check<()> {
    require(rows.len() == 1, "single-row projection")?;
    check_row(&rows.as_slice()[0], id, committed)
}

fn check_write(result: &WriteResult, count: u64, keys: &[GeneratedKey]) -> Check<()> {
    require(result.affected_rows == count, "explicit affected-row count")?;
    require(
        result.generated_keys == keys,
        "only explicitly returned keys",
    )
}

fn check_state(db: &Connection, committed: bool) -> Check<()> {
    let rows = checked(db.read(SELECT_ALL, &[]), "read persisted fixture rows")?;
    require(
        rows.len() == if committed { 2 } else { 1 },
        "persisted row count",
    )?;
    check_row(&rows.as_slice()[0], 101, committed)?;
    if committed {
        check_row(&rows.as_slice()[1], 202, true)?;
    }
    let count = checked(db.read(SELECT_COUNT, &[]), "read explicit table count")?;
    require(count.len() == 1, "single count result")?;
    require(
        checked(
            count.as_slice()[0].get_int("row_count"),
            "typed table count",
        )? == if committed { 2 } else { 1 },
        "explicit table count value",
    )
}

fn commit_operations() -> Vec<Operation> {
    vec![
        Operation::write(INSERT, sample(202, false)),
        Operation::read(SELECT_ONE, vec![202_i64.into()]),
        Operation::write(
            CAS,
            vec!["committed update".into(), 101_i64.into(), 1_i64.into()],
        ),
        Operation::read(SELECT_ONE, vec![101_i64.into()]),
    ]
}

fn exercise(fixture: &Fixture) -> Check<Vec<&'static str>> {
    let primary = open(fixture, &fixture.allowed[0])?;
    let secondary = open(fixture, &fixture.allowed[1])?;
    require(
        checked(
            primary.read(SELECT_ALL, &[]),
            "verify fresh empty fixture table",
        )?
        .is_empty(),
        "fixture table must initially be empty; never clean up an existing table",
    )?;

    check_write(
        &checked(
            primary.write_result(INSERT, &sample(101, false)),
            "insert typed fixture",
        )?,
        1,
        &[GeneratedKey::Integer(101)],
    )?;
    check_state(&primary, false)?;
    check_state(&secondary, false)?;

    let batch = checked(primary.atomic(&commit_operations()), "ordered commit batch")?;
    require(batch.results.len() == 4, "ordered commit result count")?;
    match batch.results.as_slice() {
        [OperationResult::Write(insert), OperationResult::Rows(second), OperationResult::Write(update), OperationResult::Rows(first)] =>
        {
            check_write(insert, 1, &[GeneratedKey::Integer(202)])?;
            check_single(second, 202, false)?;
            check_write(update, 1, &[])?;
            check_single(first, 101, true)?;
        }
        _ => {
            return Err(CheckError {
                stage: "ordered write/read result kinds",
                kind: None,
            })
        }
    }
    check_state(&primary, true)?;
    check_state(&secondary, true)?;

    check_write(
        &checked(
            primary.write_result(
                CAS,
                &[
                    "stale CAS must not persist".into(),
                    101_i64.into(),
                    1_i64.into(),
                ],
            ),
            "stale CAS request",
        )?,
        0,
        &[],
    )?;
    check_state(&secondary, true)?;

    denied(
        primary.atomic(&[
            Operation::write(
                CAS,
                vec![
                    "rollback must not persist".into(),
                    101_i64.into(),
                    2_i64.into(),
                ],
            ),
            Operation::write(INSERT, sample(202, false)),
        ]),
        AdapterErrorKind::Constraint,
        "duplicate key must roll back preceding mutation with typed constraint error",
    )?;
    check_state(&secondary, true)?;

    for sql in [WRITE_AS_READ, CTE_WRITE_AS_READ] {
        denied(
            primary.read(
                sql,
                &["read mutation must not persist".into(), 101_i64.into()],
            ),
            AdapterErrorKind::ReadOnly,
            "write disguised as read must fail read-only",
        )?;
        check_state(&secondary, true)?;
        denied(
            primary.atomic(&[
                Operation::write(
                    CAS,
                    vec![
                        "batch rollback must not persist".into(),
                        101_i64.into(),
                        2_i64.into(),
                    ],
                ),
                Operation::read(
                    sql,
                    vec![
                        "batch read mutation must not persist".into(),
                        202_i64.into(),
                    ],
                ),
            ]),
            AdapterErrorKind::ReadOnly,
            "read operation in read-write batch must fail and roll back all mutations",
        )?;
        check_state(&secondary, true)?;
    }

    for (principal, kind) in [
        (
            &fixture.authentication_denied,
            AdapterErrorKind::Authentication,
        ),
        (
            &fixture.authorization_denied,
            AdapterErrorKind::Authorization,
        ),
    ] {
        let rejected = open(fixture, principal)?;
        denied(rejected.read(SELECT_ALL, &[]), kind, "typed denied read")?;
        check_state(&secondary, true)?;
        denied(
            rejected.write_result(
                CAS,
                &[
                    "denied write must not persist".into(),
                    101_i64.into(),
                    2_i64.into(),
                ],
            ),
            kind,
            "typed denied write",
        )?;
        check_state(&secondary, true)?;
        denied(
            rejected.atomic(&[
                Operation::write(
                    CAS,
                    vec![
                        "denied batch must not persist".into(),
                        101_i64.into(),
                        2_i64.into(),
                    ],
                ),
                Operation::read(SELECT_ALL, vec![]),
            ]),
            kind,
            "typed denied atomic batch",
        )?;
        check_state(&secondary, true)?;
    }

    denied(
        primary.atomic(&[Operation::schema(UNSUPPORTED_SCHEMA, vec![])]),
        AdapterErrorKind::Capability,
        "service must explicitly reject unsupported schema operation",
    )?;
    denied(
        primary.inspect_schema(),
        AdapterErrorKind::Capability,
        "unsupported schema inspection",
    )?;
    denied(
        primary.verify_integrity(),
        AdapterErrorKind::Capability,
        "unsupported integrity check",
    )?;
    // These local paths cannot be used by a Supabase adapter. They live in a
    // temporary directory so a regression cannot touch a caller's real files.
    let scratch = tempfile::tempdir().map_err(|_| CheckError {
        stage: "maintenance probe directory",
        kind: None,
    })?;
    denied(
        primary.backup(scratch.path().join("unsupported-backup")),
        AdapterErrorKind::Capability,
        "unsupported local backup",
    )?;
    let mut maintenance = open(fixture, &fixture.allowed[0])?;
    denied(
        maintenance.recover_from_dump_reload(RecoveryOptions::new(
            scratch
                .path()
                .join("unsupported-recovery")
                .to_string_lossy(),
            RecoveryJournalMode::Delete,
        )),
        AdapterErrorKind::Capability,
        "unsupported local recovery",
    )?;
    check_state(&primary, true)?;
    check_state(&secondary, true)?;

    Ok(vec![
        "explicit_supabase_http_route_no_fallback",
        "initially_empty_fixture",
        "typed_scalar_blob_null_and_bound_text_roundtrip",
        "explicit_returned_keys_and_affected_counts",
        "second_independent_principal_shared_read",
        "ordered_atomic_write_read_commit",
        "zero_row_stale_cas",
        "duplicate_constraint_rollback",
        "direct_and_cte_writes_disguised_as_reads_rejected",
        "read_only_operations_inside_read_write_batch_rollback",
        "typed_authentication_and_authorization_denials_with_unchanged_state",
        "explicit_unsupported_schema_and_local_maintenance",
        "final_state_visible_to_both_principals",
    ])
}

fn receipt(profile: Profile, checks: &[&str], complete: bool) -> Value {
    let expected_rows: Vec<Value> = [101, 202]
        .into_iter()
        .map(|id| {
            let values = sample(id, true);
            let row: serde_json::Map<String, Value> = COLUMNS
                .into_iter()
                .zip(values)
                .map(|(column, value)| {
                    (
                        column.into(),
                        serde_json::to_value(value).expect("fixed synthetic value serializes"),
                    )
                })
                .collect();
            Value::Object(row)
        })
        .collect();
    json!({
        "receipt_version": 1,
        "profile": profile.label(),
        "sql_profile": SQL_PROFILE,
        "evidence_class": "client_assertions_only",
        "status": if complete { "client_assertions_passed" } else { "incomplete" },
        "provider": "supabase",
        "transport": "http",
        "client_checks": checks,
        "expected_persisted_state": {"table": TABLE, "rows": expected_rows},
        "expected_counts": {"table_rows": 2, "first_insert": 1, "batch_insert": 1, "committed_cas": 1, "stale_cas": 0},
        "expected_returned_keys": {"first_insert": [101], "batch_insert": [202], "committed_cas": [], "stale_cas": []},
        "independent_evidence": {
            "database_state_and_physical_transactions": "pending",
            "principal_provisioning_and_authorization_policy": "pending",
            "cleanup": "pending",
            "hosted_connection_mode_and_prepared_statements": if profile == Profile::HostedSupabase { "pending" } else { "not_exercised" }
        }
    })
}

fn write_receipt(file: &mut File, value: &Value) -> Result<(), FixtureError> {
    let bytes = serde_json::to_vec_pretty(value)
        .map_err(|_| FixtureError("client receipt serialization failed"))?;
    file.seek(SeekFrom::Start(0))
        .and_then(|_| file.set_len(0))
        .and_then(|_| file.write_all(&bytes))
        .and_then(|_| file.write_all(b"\n"))
        .and_then(|_| file.sync_all())
        .map_err(|_| FixtureError("client receipt could not be written"))
}

fn reserve_receipt(path: &Path, profile: Profile) -> Result<File, FixtureError> {
    let mut options = FileOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path).map_err(|_| {
        FixtureError("receipt must be a new writable file; existing receipts are never overwritten")
    })?;
    write_receipt(&mut file, &receipt(profile, &[], false))?;
    Ok(file)
}

fn run(profile: Profile) {
    let path = std::env::var_os(FIXTURE_ENV)
        .filter(|path| !path.is_empty())
        .unwrap_or_else(|| {
            panic!("unavailable is not a pass: DACTYL_SUPABASE_FIXTURE is required")
        });
    let hosted_opt_in = std::env::var(HOSTED_ENV).is_ok_and(|value| value == "1");
    let fixture = load_fixture(Path::new(&path), profile, hosted_opt_in)
        .unwrap_or_else(|error| panic!("{error}"));
    // Reserve before the first request, so a stale receipt cannot hide a run.
    let mut output =
        reserve_receipt(&fixture.receipt_path, profile).unwrap_or_else(|error| panic!("{error}"));
    let checks = exercise(&fixture).unwrap_or_else(|error| {
        // Never Debug-print a result, fixture, context, row, or remote error.
        panic!("service client assertion failed: {}; category={:?}; independent database outcome requires external verification", error.stage, error.kind)
    });
    write_receipt(&mut output, &receipt(profile, &checks, true))
        .unwrap_or_else(|error| panic!("{error}"));
}

#[test]
#[ignore = "requires external disposable local PostgreSQL service; unavailable is not a pass"]
fn local_postgres_service_contract() {
    run(Profile::LocalPostgres);
}

#[test]
#[ignore = "requires protected hosted Supabase service and explicit opt-in; unavailable is not a pass"]
fn protected_hosted_supabase_service_contract() {
    run(Profile::HostedSupabase);
}

#[cfg(test)]
mod offline {
    use super::*;

    fn fixture_value() -> Value {
        json!({
            "version": 1,
            "profile": "local_postgres",
            "disposable": true,
            "independent_principals": true,
            "sql_profile": SQL_PROFILE,
            "endpoint": "http://127.0.0.1:1/private-endpoint-sentinel",
            "allowed": [
                {"token":"allowed-token-one-sentinel", "context":{"version":1,"payload":{"opaque":"context-one-sentinel"}}},
                {"token":"allowed-token-two-sentinel", "context":{"version":1,"payload":{"opaque":"context-two-sentinel"}}}
            ],
            "authentication_denied": {"token":"invalid-token-sentinel", "context":{"version":1,"payload":{"opaque":"invalid-context-sentinel"}}},
            "authorization_denied": {"token":"allowed-token-one-sentinel", "context":{"version":1,"payload":{"opaque":"denied-context-sentinel"}}},
            "receipt_path": std::env::temp_dir().join("unused-receipt-sentinel.json")
        })
    }

    fn parse(value: &Value, profile: Profile, opt_in: bool) -> Result<Fixture, FixtureError> {
        parse_fixture(&serde_json::to_vec(value).unwrap(), profile, opt_in)
    }

    fn failure(value: &Value, profile: Profile, opt_in: bool) -> String {
        match parse(value, profile, opt_in) {
            Ok(_) => panic!("invalid fixture unexpectedly accepted"),
            Err(error) => format!("{error}; {error:?}"),
        }
    }

    #[test]
    fn validates_fixture_and_preserves_opaque_contexts_without_network() {
        let value = fixture_value();
        let fixture = parse(&value, Profile::LocalPostgres, false).ok().unwrap();
        assert_eq!(
            fixture.allowed[0].context.payload(),
            &value["allowed"][0]["context"]["payload"]
        );
        // Shared target context is valid; independent principals are determined
        // externally, not by inventing tenant semantics for the opaque payload.
        let mut shared = value;
        shared["allowed"][1]["context"] = shared["allowed"][0]["context"].clone();
        assert!(parse(&shared, Profile::LocalPostgres, false).is_ok());
    }

    #[test]
    fn requires_disposable_reviewed_profile_and_independent_allowed_tokens() {
        for (field, replacement) in [
            ("version", json!(2)),
            ("profile", json!("hosted_supabase")),
            ("sql_profile", json!("narrow_task_only_profile")),
            ("disposable", json!(false)),
            ("independent_principals", json!(false)),
            ("receipt_path", json!("relative.json")),
        ] {
            let mut value = fixture_value();
            value[field] = replacement;
            assert!(failure(&value, Profile::LocalPostgres, false)
                .contains("unavailable is not a pass"));
        }
        let mut value = fixture_value();
        value["allowed"][1]["token"] = value["allowed"][0]["token"].clone();
        assert!(parse(&value, Profile::LocalPostgres, false).is_err());
        let mut value = fixture_value();
        value["authentication_denied"]["token"] = value["allowed"][0]["token"].clone();
        assert!(parse(&value, Profile::LocalPostgres, false).is_err());
        let mut value = fixture_value();
        value["authorization_denied"] = value["allowed"][0].clone();
        assert!(parse(&value, Profile::LocalPostgres, false).is_err());
    }

    #[test]
    fn enforces_endpoint_and_hosted_opt_in_without_network() {
        for endpoint in [
            "postgres://user:secret@db.example/db",
            "https://user:secret@service.example",
            "https://service.example?secret=value",
            "https://service.example#secret",
            "http://service.example",
            "http://localhost:1234",
            "http://127.0.0.1.example",
            "https://service.example",
        ] {
            let mut value = fixture_value();
            value["endpoint"] = json!(endpoint);
            assert!(parse(&value, Profile::LocalPostgres, false).is_err());
        }
        let mut value = fixture_value();
        value["profile"] = json!("hosted_supabase");
        assert!(parse(&value, Profile::HostedSupabase, true).is_err());
        value["endpoint"] = json!("https://service.example");
        assert!(parse(&value, Profile::HostedSupabase, false).is_err());
        assert!(parse(&value, Profile::HostedSupabase, true).is_ok());
        let mut value = fixture_value();
        value["endpoint"] = json!("http://[::1]:1");
        assert!(parse(&value, Profile::LocalPostgres, false).is_ok());
    }

    #[test]
    fn rejects_missing_fields_extra_secrets_hooks_and_bad_context_or_bearer() {
        let mut value = fixture_value();
        value
            .as_object_mut()
            .unwrap()
            .remove("authorization_denied");
        assert!(parse(&value, Profile::LocalPostgres, false).is_err());
        for field in [
            "database_password",
            "service_role_key",
            "setup_command",
            "cleanup_hook",
        ] {
            let mut value = fixture_value();
            value[field] = json!("forbidden-secret-sentinel");
            let error = failure(&value, Profile::LocalPostgres, false);
            assert!(!error.contains("sentinel"));
            assert!(!error.contains(field));
        }
        for token in ["", " ", "bearer\r\nsecret-sentinel", "token with spaces"] {
            let mut value = fixture_value();
            value["allowed"][0]["token"] = json!(token);
            assert!(parse(&value, Profile::LocalPostgres, false).is_err());
        }
        for context in [
            json!({"version":0,"payload":{}}),
            json!({"version":1,"payload":"secret-sentinel"}),
        ] {
            let mut value = fixture_value();
            value["allowed"][0]["context"] = context;
            assert!(!failure(&value, Profile::LocalPostgres, false).contains("sentinel"));
        }
    }

    #[test]
    fn parse_errors_and_assertion_failures_never_echo_untrusted_values() {
        let bytes = br#"{"profile":"secret-sentinel","token":"private-sentinel"}"#;
        let error = match parse_fixture(bytes, Profile::LocalPostgres, false) {
            Ok(_) => panic!("invalid fixture accepted"),
            Err(error) => format!("{error:?} {error}"),
        };
        assert!(!error.contains("sentinel"));
        assert!(parse_fixture(
            &vec![b' '; MAX_FIXTURE_BYTES as usize + 1],
            Profile::LocalPostgres,
            false
        )
        .is_err());
        let row = Row {
            columns: vec!["private-column-sentinel".into()],
            values: vec![json!("private-row-sentinel")],
        };
        let error = check_row(&row, 101, false).unwrap_err();
        assert!(!format!("{error:?}").contains("sentinel"));
        let error = checked::<()>(
            Err(DactylError::Config("private-error-sentinel".into())),
            "fixed stage",
        )
        .unwrap_err();
        assert!(!format!("{error:?}").contains("sentinel"));
    }

    #[test]
    fn receipt_has_fixed_expectations_only_and_refuses_collisions() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("receipt.json");
        let mut file = reserve_receipt(&path, Profile::LocalPostgres).unwrap();
        let initial = std::fs::read(&path).unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&initial).unwrap()["status"],
            "incomplete"
        );
        assert!(reserve_receipt(&path, Profile::LocalPostgres).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), initial);
        write_receipt(
            &mut file,
            &receipt(Profile::LocalPostgres, &["fixed_check"], true),
        )
        .unwrap();
        let text = std::fs::read_to_string(&path).unwrap();
        let output: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(output["status"], "client_assertions_passed");
        assert_eq!(output["evidence_class"], "client_assertions_only");
        assert_eq!(
            output["expected_persisted_state"]["rows"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            output["expected_persisted_state"]["rows"][0]["signed_value"],
            json!(-9_007_199_254_740_993_i64)
        );
        assert_eq!(output["independent_evidence"]["cleanup"], "pending");
        for forbidden in [
            "sentinel",
            "token",
            "context",
            "endpoint",
            "receipt_path",
            "password",
        ] {
            assert!(!text.contains(forbidden));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn protected_fixture_rejects_public_files_and_symlinks() {
        use std::os::unix::fs::{symlink, PermissionsExt};
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("fixture.json");
        std::fs::write(&path, serde_json::to_vec(&fixture_value()).unwrap()).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(load_fixture(&path, Profile::LocalPostgres, false).is_err());
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert!(load_fixture(&path, Profile::LocalPostgres, false).is_ok());
        let link = directory.path().join("fixture-link.json");
        symlink(&path, &link).unwrap();
        assert!(load_fixture(&link, Profile::LocalPostgres, false).is_err());
        let receipt_link = directory.path().join("receipt-link.json");
        symlink(&path, &receipt_link).unwrap();
        assert!(reserve_receipt(&receipt_link, Profile::LocalPostgres).is_err());
    }

    #[test]
    fn schema_and_ordered_sql_are_fixed_and_all_values_are_bound() {
        let schema = include_str!("fixtures/supabase-service-schema.sql");
        assert_eq!(schema.matches("CREATE TABLE ").count(), 1);
        assert!(schema.contains("id BIGINT PRIMARY KEY"));
        assert!(schema.contains("blob_value BYTEA NOT NULL"));
        assert!(!schema.contains("AUTOINCREMENT"));
        let operations = commit_operations();
        assert_eq!(
            operations.iter().map(Operation::sql).collect::<Vec<_>>(),
            vec![INSERT, SELECT_ONE, CAS, SELECT_ONE]
        );
        assert_eq!(
            operations.iter().map(Operation::kind).collect::<Vec<_>>(),
            vec![
                dactyl_db::OperationKind::Write,
                dactyl_db::OperationKind::Read,
                dactyl_db::OperationKind::Write,
                dactyl_db::OperationKind::Read
            ]
        );
        assert!(!INSERT.contains(INJECTION_TEXT));
        assert!(INSERT.ends_with("RETURNING id"));
        assert_eq!(
            sample(101, false)[4],
            Parameter::Text(INJECTION_TEXT.into())
        );
        assert_eq!(operations[0].params(), sample(202, false));
    }

    #[test]
    fn local_schema_and_maintenance_are_capability_errors_without_network() {
        let fixture = parse(&fixture_value(), Profile::LocalPostgres, false)
            .ok()
            .unwrap();
        let mut db = open(&fixture, &fixture.allowed[0]).unwrap();
        assert_eq!(
            db.inspect_schema().unwrap_err().adapter_kind(),
            Some(AdapterErrorKind::Capability)
        );
        assert_eq!(
            db.verify_integrity().unwrap_err().adapter_kind(),
            Some(AdapterErrorKind::Capability)
        );
        let directory = tempfile::tempdir().unwrap();
        assert_eq!(
            db.backup(directory.path().join("backup"))
                .unwrap_err()
                .adapter_kind(),
            Some(AdapterErrorKind::Capability)
        );
        assert_eq!(
            db.recover_from_dump_reload(RecoveryOptions::new(
                directory.path().join("recovery").to_string_lossy(),
                RecoveryJournalMode::Delete,
            ))
            .unwrap_err()
            .adapter_kind(),
            Some(AdapterErrorKind::Capability)
        );
        assert_eq!(db.datastore(), Datastore::Supabase);
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    }
}
