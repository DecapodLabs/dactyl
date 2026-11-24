//! Deterministic seeded differential contract suite comparing a pure-Rust
//! reference model, real local `SqliteAdapter`, and a real HTTP `SupabaseAdapter`
//! wire test server speaking canonical `dactyl_api` RPC envelopes (plus device
//! gateway and legacy `/query` `/batch` envelopes) for Dactyl issue #145.
//!
//! Evidence classes:
//! - `local_sqlite`: host SQLite C-ABI engine (`SqliteAdapter`)
//! - `synthetic_http`: loopback HTTP wire server backed by an independent SQLite
//!   store enforcing `dactyl_api` RPC envelopes, `AccessMode::ReadOnly`, atomic
//!   transactions, and PostgREST error mapping
//! - `local_postgres`, `hosted_supabase`, `live_propodus`: unavailable here and
//!   never implied.

#![cfg(all(feature = "sqlite", feature = "supabase"))]

use std::collections::{BTreeMap, HashMap};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use dactyl_db::{
    AccessMode, AdapterErrorKind, Connection, DactylError, DatastoreRoute, FailureOrigin,
    GeneratedKey, MutationOutcome, OpenOptions, Operation, OperationResult, Parameter,
    RequestProof, RequestProofInput, RequestProofSigner, ResourceBudget, Row, Rows, ScanOutcome,
    StorageContext, SupabaseRpcConfig,
};
use serde_json::{json, Value};
use tempfile::TempDir;

const KEY: &str = "sb_publishable_differential_145";
const BEARER: &str = "diff-user-bearer";

const SCHEMA_SQL: &str = "\
CREATE TABLE items (\
    id INTEGER PRIMARY KEY AUTOINCREMENT, \
    tag TEXT UNIQUE NOT NULL, \
    kind TEXT NOT NULL, \
    score INTEGER NOT NULL, \
    ratio REAL, \
    active INTEGER NOT NULL, \
    note TEXT, \
    payload BLOB NOT NULL, \
    version INTEGER NOT NULL\
); \
CREATE TABLE audits (\
    id INTEGER PRIMARY KEY AUTOINCREMENT, \
    item_id INTEGER NOT NULL REFERENCES items(id) ON DELETE RESTRICT, \
    event TEXT NOT NULL\
);";

// ---------------------------------------------------------------------------
// Deterministic PRNG (SplitMix64)
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn gen_range(&mut self, upper: usize) -> usize {
        (self.next_u64() as usize) % upper
    }

    fn gen_bool(&mut self) -> bool {
        (self.next_u64() & 1) == 1
    }
}

// ---------------------------------------------------------------------------
// Pure-Rust Reference Model
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
struct RefItem {
    id: i64,
    tag: String,
    kind: String,
    score: i64,
    ratio: Option<f64>,
    active: bool,
    note: Option<String>,
    payload: Vec<u8>,
    version: i64,
}

#[derive(Clone, Debug, PartialEq)]
struct RefAudit {
    id: i64,
    item_id: i64,
    event: String,
}

#[derive(Clone, Default, Debug, PartialEq)]
struct ReferenceModel {
    items: BTreeMap<i64, RefItem>,
    audits: BTreeMap<i64, RefAudit>,
    next_item_id: i64,
    next_audit_id: i64,
}

#[derive(Clone, Debug, PartialEq)]
enum StepOutcome {
    Rows(Vec<CanonicalRow>),
    Write {
        affected: u64,
        keys: Vec<GeneratedKey>,
    },
    Error(AdapterErrorKind),
}

#[derive(Clone, Debug, PartialEq)]
struct CanonicalRow {
    id: i64,
    tag: String,
    kind: String,
    score: i64,
    ratio: Option<f64>,
    active: bool,
    note: Option<String>,
    payload: Vec<u8>,
    version: i64,
}

#[derive(Clone, Debug)]
enum HistoryStep {
    InsertItem {
        tag: String,
        kind: String,
        score: i64,
        ratio: Option<f64>,
        active: bool,
        note: Option<String>,
        payload: Vec<u8>,
    },
    InsertIgnoreItem {
        tag: String,
        kind: String,
        score: i64,
    },
    UpdateScoreCas {
        id: i64,
        expected_version: i64,
        new_score: i64,
        new_note: Option<String>,
    },
    InsertAudit {
        item_id: i64,
        event: String,
    },
    DeleteItem {
        id: i64,
    },
    ReadOrderedByScore {
        min_score: i64,
    },
    ReadByNullNote,
    ReadKeysetPage {
        after_id: i64,
        limit: i64,
    },
    AtomicSuccessMixed {
        tag: String,
        score: i64,
        event: String,
    },
    AtomicRollbackUniqueConflict {
        new_tag: String,
        conflicting_tag: String,
    },
    AtomicRollbackForeignKey {
        new_tag: String,
        missing_item_id: i64,
    },
    AtomicRollbackNotNull {
        new_tag: String,
    },
    AtomicReadOnlyDisguisedWrite {
        tag: String,
    },
}

impl ReferenceModel {
    fn new() -> Self {
        Self {
            items: BTreeMap::new(),
            audits: BTreeMap::new(),
            next_item_id: 1,
            next_audit_id: 1,
        }
    }

    fn apply(&mut self, step: &HistoryStep) -> StepOutcome {
        match step {
            HistoryStep::InsertItem {
                tag,
                kind,
                score,
                ratio,
                active,
                note,
                payload,
            } => {
                if self.items.values().any(|item| item.tag == *tag) {
                    return StepOutcome::Error(AdapterErrorKind::Constraint);
                }
                let id = self.next_item_id;
                self.next_item_id += 1;
                self.items.insert(
                    id,
                    RefItem {
                        id,
                        tag: tag.clone(),
                        kind: kind.clone(),
                        score: *score,
                        ratio: *ratio,
                        active: *active,
                        note: note.clone(),
                        payload: payload.clone(),
                        version: 1,
                    },
                );
                StepOutcome::Write {
                    affected: 1,
                    keys: vec![GeneratedKey::Integer(id)],
                }
            }
            HistoryStep::InsertIgnoreItem { tag, kind, score } => {
                // Both SQLite AUTOINCREMENT and PostgreSQL SERIAL/IDENTITY consume
                // a sequence value before conflict-skipping on OR IGNORE / DO NOTHING.
                let id = self.next_item_id;
                self.next_item_id += 1;
                if self.items.values().any(|item| item.tag == *tag) {
                    return StepOutcome::Write {
                        affected: 0,
                        keys: Vec::new(),
                    };
                }
                self.items.insert(
                    id,
                    RefItem {
                        id,
                        tag: tag.clone(),
                        kind: kind.clone(),
                        score: *score,
                        ratio: None,
                        active: true,
                        note: None,
                        payload: vec![0x00, 0xFF],
                        version: 1,
                    },
                );
                StepOutcome::Write {
                    affected: 1,
                    keys: vec![GeneratedKey::Integer(id)],
                }
            }
            HistoryStep::UpdateScoreCas {
                id,
                expected_version,
                new_score,
                new_note,
            } => {
                if let Some(item) = self.items.get_mut(id) {
                    if item.version == *expected_version {
                        item.score = *new_score;
                        item.note = new_note.clone();
                        item.version += 1;
                        return StepOutcome::Write {
                            affected: 1,
                            keys: Vec::new(),
                        };
                    }
                }
                StepOutcome::Write {
                    affected: 0,
                    keys: Vec::new(),
                }
            }
            HistoryStep::InsertAudit { item_id, event } => {
                if !self.items.contains_key(item_id) {
                    return StepOutcome::Error(AdapterErrorKind::Constraint);
                }
                let id = self.next_audit_id;
                self.next_audit_id += 1;
                self.audits.insert(
                    id,
                    RefAudit {
                        id,
                        item_id: *item_id,
                        event: event.clone(),
                    },
                );
                StepOutcome::Write {
                    affected: 1,
                    keys: vec![GeneratedKey::Integer(id)],
                }
            }
            HistoryStep::DeleteItem { id } => {
                if self.audits.values().any(|a| a.item_id == *id) {
                    return StepOutcome::Error(AdapterErrorKind::Constraint);
                }
                let removed = self.items.remove(id).is_some();
                StepOutcome::Write {
                    affected: u64::from(removed),
                    keys: Vec::new(),
                }
            }
            HistoryStep::ReadOrderedByScore { min_score } => {
                let mut rows: Vec<&RefItem> = self
                    .items
                    .values()
                    .filter(|item| item.score >= *min_score)
                    .collect();
                rows.sort_by(|a, b| b.score.cmp(&a.score).then_with(|| a.id.cmp(&b.id)));
                StepOutcome::Rows(rows.into_iter().map(canonical_from_ref).collect())
            }
            HistoryStep::ReadByNullNote => {
                let mut rows: Vec<&RefItem> = self
                    .items
                    .values()
                    .filter(|item| item.note.is_none())
                    .collect();
                rows.sort_by_key(|item| item.id);
                StepOutcome::Rows(rows.into_iter().map(canonical_from_ref).collect())
            }
            HistoryStep::ReadKeysetPage { after_id, limit } => {
                let rows: Vec<CanonicalRow> = self
                    .items
                    .values()
                    .filter(|item| item.id > *after_id)
                    .take((*limit).max(0) as usize)
                    .map(canonical_from_ref)
                    .collect();
                StepOutcome::Rows(rows)
            }
            HistoryStep::AtomicSuccessMixed { tag, score, event } => {
                let snapshot = self.clone();
                if self.items.values().any(|item| item.tag == *tag) {
                    *self = snapshot;
                    return StepOutcome::Error(AdapterErrorKind::Constraint);
                }
                let id = self.next_item_id;
                self.next_item_id += 1;
                self.items.insert(
                    id,
                    RefItem {
                        id,
                        tag: tag.clone(),
                        kind: "batch".into(),
                        score: *score,
                        ratio: Some(1.5),
                        active: true,
                        note: Some("mixed".into()),
                        payload: vec![0xDE, 0xAD, 0xBE, 0xEF],
                        version: 1,
                    },
                );
                let audit_id = self.next_audit_id;
                self.next_audit_id += 1;
                self.audits.insert(
                    audit_id,
                    RefAudit {
                        id: audit_id,
                        item_id: id,
                        event: event.clone(),
                    },
                );
                let mut rows: Vec<&RefItem> = self.items.values().filter(|i| i.id == id).collect();
                rows.sort_by_key(|i| i.id);
                StepOutcome::Rows(rows.into_iter().map(canonical_from_ref).collect())
            }
            HistoryStep::AtomicRollbackUniqueConflict {
                new_tag,
                conflicting_tag,
            } => {
                let conflict = self.items.values().any(|item| item.tag == *new_tag)
                    || new_tag == conflicting_tag
                    || self.items.values().any(|item| item.tag == *conflicting_tag);
                if conflict {
                    return StepOutcome::Error(AdapterErrorKind::Constraint);
                }
                let id1 = self.next_item_id;
                self.next_item_id += 1;
                self.items.insert(
                    id1,
                    RefItem {
                        id: id1,
                        tag: new_tag.clone(),
                        kind: "rb".into(),
                        score: 1,
                        ratio: None,
                        active: true,
                        note: None,
                        payload: vec![1],
                        version: 1,
                    },
                );
                let id2 = self.next_item_id;
                self.next_item_id += 1;
                self.items.insert(
                    id2,
                    RefItem {
                        id: id2,
                        tag: conflicting_tag.clone(),
                        kind: "rb".into(),
                        score: 2,
                        ratio: None,
                        active: true,
                        note: None,
                        payload: vec![2],
                        version: 1,
                    },
                );
                StepOutcome::Write {
                    affected: 0,
                    keys: Vec::new(),
                }
            }
            HistoryStep::AtomicRollbackForeignKey { .. }
            | HistoryStep::AtomicRollbackNotNull { .. } => {
                // Always fails atomically: reference state must remain unchanged.
                StepOutcome::Error(AdapterErrorKind::Constraint)
            }
            HistoryStep::AtomicReadOnlyDisguisedWrite { .. } => {
                StepOutcome::Error(AdapterErrorKind::ReadOnly)
            }
        }
    }

    fn all_items_ordered(&self) -> Vec<CanonicalRow> {
        self.items.values().map(canonical_from_ref).collect()
    }
}

fn canonical_from_ref(item: &RefItem) -> CanonicalRow {
    CanonicalRow {
        id: item.id,
        tag: item.tag.clone(),
        kind: item.kind.clone(),
        score: item.score,
        ratio: item.ratio,
        active: item.active,
        note: item.note.clone(),
        payload: item.payload.clone(),
        version: item.version,
    }
}

fn canonical_from_row(row: &Row) -> CanonicalRow {
    let ratio = if row.is_null("ratio").unwrap() {
        None
    } else {
        Some(row.get_real("ratio").unwrap())
    };
    let note = if row.is_null("note").unwrap() {
        None
    } else {
        Some(row.get_str("note").unwrap())
    };
    CanonicalRow {
        id: row.get_int("id").unwrap(),
        tag: row.get_str("tag").unwrap(),
        kind: row.get_str("kind").unwrap(),
        score: row.get_int("score").unwrap(),
        ratio,
        active: row.get_bool("active").unwrap(),
        note,
        payload: row.get_blob("payload").unwrap(),
        version: row.get_int("version").unwrap(),
    }
}

fn canonical_rows(rows: &Rows) -> Vec<CanonicalRow> {
    rows.iter().map(canonical_from_row).collect()
}

// ---------------------------------------------------------------------------
// Real HTTP dactyl_api RPC Server backed by an independent SQLite store
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq)]
enum ServerMode {
    NativeRpc,
    DeviceGateway,
    LegacyService,
}

struct CloudRpcServer {
    origin: String,
    db_path: String,
    stop: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
    requests: Arc<AtomicUsize>,
}

impl CloudRpcServer {
    fn start(mode: ServerMode) -> (Self, TempDir) {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("cloud_backing.sqlite");
        let db_path_str = db_path.to_string_lossy().into_owned();
        {
            let init = Connection::open(DatastoreRoute::sqlite(&db_path_str)).unwrap();
            init.atomic(&[Operation::schema(SCHEMA_SQL, vec![])])
                .unwrap();
        }
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let stop = Arc::new(AtomicBool::new(false));
        let stop_srv = stop.clone();
        let requests = Arc::new(AtomicUsize::new(0));
        let requests_srv = requests.clone();
        let path_srv = db_path_str.clone();

        let handle = thread::spawn(move || {
            let rw = Connection::open(DatastoreRoute::sqlite(&path_srv)).unwrap();
            let ro = Connection::open_with_options(
                DatastoreRoute::sqlite(&path_srv),
                OpenOptions {
                    access_mode: AccessMode::ReadOnly,
                    ..OpenOptions::default()
                },
            )
            .unwrap();
            while !stop_srv.load(Ordering::Relaxed) {
                let (mut stream, _) = match listener.accept() {
                    Ok(pair) => pair,
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                        continue;
                    }
                    Err(_) => break,
                };
                requests_srv.fetch_add(1, Ordering::SeqCst);
                stream.set_nonblocking(false).unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut buf = Vec::new();
                let mut chunk = [0_u8; 4096];
                let (head_str, head_end, content_length) = loop {
                    match stream.read(&mut chunk) {
                        Ok(0) => break (String::new(), 0, 0),
                        Ok(n) => buf.extend_from_slice(&chunk[..n]),
                        Err(_) => break (String::new(), 0, 0),
                    }
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
                if head_str.is_empty() {
                    continue;
                }
                while buf.len() < head_end + content_length {
                    let n = stream.read(&mut chunk).unwrap();
                    if n == 0 {
                        break;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                }
                let mut lines = head_str.lines();
                let req_line = lines.next().unwrap_or("");
                let path = req_line.split_whitespace().nth(1).unwrap_or("");
                let mut headers = HashMap::new();
                for line in lines {
                    if let Some((k, v)) = line.split_once(':') {
                        headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_owned());
                    }
                }
                let payload: Value =
                    serde_json::from_slice(&buf[head_end..head_end + content_length]).unwrap();
                let (status, resp_body) =
                    handle_rpc_request(mode, path, &headers, &payload, &rw, &ro);
                let body_str = resp_body.to_string();
                let _ = write!(
                    stream,
                    "HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body_str}",
                    body_str.len()
                );
                let _ = stream.flush();
            }
        });

        (
            Self {
                origin,
                db_path: db_path_str,
                stop,
                handle: Some(handle),
                requests,
            },
            dir,
        )
    }

    fn observer(&self) -> Connection {
        Connection::open_with_options(
            DatastoreRoute::sqlite(&self.db_path),
            OpenOptions {
                access_mode: AccessMode::ReadOnly,
                ..OpenOptions::default()
            },
        )
        .unwrap()
    }
}

impl Drop for CloudRpcServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn handle_rpc_request(
    mode: ServerMode,
    path: &str,
    headers: &HashMap<String, String>,
    payload: &Value,
    rw: &Connection,
    ro: &Connection,
) -> (u16, Value) {
    if mode != ServerMode::LegacyService
        && (headers.get("dactyl-contract-version").map(String::as_str) != Some("1")
            || headers.get("content-profile").map(String::as_str) != Some("dactyl_api")
            || headers.get("accept-profile").map(String::as_str) != Some("dactyl_api")
            || headers.get("apikey").map(String::as_str) != Some(KEY))
    {
        return (
            400,
            json!({"code": "invalid_request", "message": "missing native RPC headers"}),
        );
    }
    if mode == ServerMode::DeviceGateway
        && (headers.get("dpop").is_none()
            || !headers
                .get("authorization")
                .is_some_and(|v| v.starts_with("DPoP ")))
    {
        return (
            401,
            json!({"code": "dpop_proof_required", "message": "missing DPoP proof"}),
        );
    }

    let inner = match mode {
        ServerMode::NativeRpc => {
            let Some(req) = payload.get("request") else {
                return (
                    400,
                    json!({"code": "invalid_request", "message": "missing request wrapper"}),
                );
            };
            if req.get("version").and_then(Value::as_u64) != Some(1) {
                return (
                    400,
                    json!({"code": "invalid_request", "message": "unsupported request version"}),
                );
            }
            req
        }
        ServerMode::DeviceGateway => {
            if payload.get("version").and_then(Value::as_u64) != Some(1) {
                return (
                    400,
                    json!({"code": "invalid_request", "message": "unsupported gateway version"}),
                );
            }
            payload
        }
        ServerMode::LegacyService => payload,
    };

    if inner["context"]["version"].as_u64() == Some(0) {
        return (
            400,
            json!({"code": "invalid_context", "message": "invalid context"}),
        );
    }

    let is_batch = matches!(
        path,
        "/rest/v1/rpc/dactyl_batch_v1"
            | "/rest/v1/rpc/dactyl_read_batch_v1"
            | "/functions/v1/decapod-onboarding/cloud-batch"
            | "/batch"
    );
    let is_read_only_route = matches!(path, "/rest/v1/rpc/dactyl_read_batch_v1")
        || inner["access_mode"].as_str() == Some("read_only");

    if !is_batch {
        let sql = inner["sql"].as_str().unwrap_or("");
        let kind = inner["kind"].as_str().unwrap_or("");
        let params: Vec<Parameter> =
            serde_json::from_value(inner["params"].clone()).unwrap_or_default();
        if kind == "schema" {
            return (
                422,
                json!({"code": "unsupported_query", "message": "DDL disallowed on native RPC"}),
            );
        }
        if kind == "read" || is_read_only_route {
            match ro.read(sql, &params) {
                Ok(rows) => (200, rows_to_rpc_json(&rows, mode)),
                Err(err) => map_server_error(&err, mode),
            }
        } else {
            match rw.write_result(sql, &params) {
                Ok(wr) => (
                    200,
                    json!({
                        "columns": [],
                        "rows": [],
                        "affected_rows": wr.affected_rows,
                        "generated_keys": wr.generated_keys,
                    }),
                ),
                Err(err) => map_server_error(&err, mode),
            }
        }
    } else {
        let raw_ops = inner["operations"].as_array().unwrap();
        let mut ops = Vec::with_capacity(raw_ops.len());
        for raw in raw_ops {
            let kind = raw["kind"].as_str().unwrap_or("");
            let sql = raw["sql"].as_str().unwrap_or("");
            let params: Vec<Parameter> =
                serde_json::from_value(raw["params"].clone()).unwrap_or_default();
            match kind {
                "read" => ops.push(Operation::read(sql, params)),
                "write" => {
                    if is_read_only_route {
                        return (
                            400,
                            json!({"code": "25006", "message": "read-only transaction"}),
                        );
                    }
                    ops.push(Operation::write(sql, params));
                }
                "schema" => {
                    return (
                        422,
                        json!({"code": "unsupported_query", "message": "DDL disallowed on native RPC"}),
                    );
                }
                _ => {
                    return (
                        400,
                        json!({"code": "invalid_request", "message": "unknown op kind"}),
                    );
                }
            }
        }
        let target_conn = if is_read_only_route { ro } else { rw };
        match target_conn.atomic(&ops) {
            Ok(batch) => {
                let results: Vec<Value> = batch
                    .results
                    .into_iter()
                    .map(|res| match res {
                        OperationResult::Rows(rows) => rows_to_rpc_json(&rows, mode),
                        OperationResult::Write(wr) => {
                            let mut obj = json!({
                                "columns": [],
                                "rows": [],
                                "affected_rows": wr.affected_rows,
                            });
                            if mode != ServerMode::LegacyService {
                                obj["generated_keys"] = json!(wr.generated_keys);
                            }
                            obj
                        }
                    })
                    .collect();
                (200, json!({"results": results}))
            }
            Err(err) => map_server_error(&err, mode),
        }
    }
}

fn rows_to_rpc_json(rows: &Rows, mode: ServerMode) -> Value {
    let columns = rows
        .as_slice()
        .first()
        .map(|r| r.columns.clone())
        .unwrap_or_default();
    let json_rows: Vec<Value> = rows
        .iter()
        .map(|row| {
            let mut map = serde_json::Map::new();
            for (col, val) in row.columns.iter().zip(row.values.iter()) {
                // Cloud / PostgreSQL emits real JSON booleans for boolean columns (`active`),
                // whereas SQLite stores INTEGER 0/1. Keep this real representation difference
                // intact so `row.get_bool()` unifies while raw JSON remains distinguishable.
                if col == "active" {
                    if let Some(n) = val.as_i64() {
                        map.insert(col.clone(), Value::Bool(n != 0));
                        continue;
                    }
                }
                map.insert(col.clone(), val.clone());
            }
            Value::Object(map)
        })
        .collect();
    let mut obj = json!({
        "columns": columns,
        "rows": json_rows,
        "affected_rows": 0,
    });
    if mode != ServerMode::LegacyService {
        obj["generated_keys"] = json!([]);
    }
    obj
}

fn map_server_error(err: &DactylError, mode: ServerMode) -> (u16, Value) {
    let (status, pg_or_service_code) = match (err.adapter_kind(), err.adapter_code()) {
        (Some(AdapterErrorKind::Constraint), Some("unique_violation")) => (409, "23505"),
        (Some(AdapterErrorKind::Constraint), Some("foreign_key_violation")) => (409, "23503"),
        (Some(AdapterErrorKind::Constraint), Some("not_null_violation")) => (400, "23502"),
        (Some(AdapterErrorKind::Constraint), _) => (400, "23514"),
        (Some(AdapterErrorKind::ReadOnly), _) => (400, "25006"),
        (Some(AdapterErrorKind::InvalidOperation), _) => (400, "invalid_request"),
        (Some(AdapterErrorKind::Capability), _) => (422, "unsupported_query"),
        (Some(AdapterErrorKind::Conflict), _) => (409, "40001"),
        _ => (400, "invalid_request"),
    };
    if mode == ServerMode::LegacyService {
        let code = match pg_or_service_code {
            "23505" => "unique_violation",
            "23503" => "foreign_key_violation",
            "23502" => "not_null_violation",
            "25006" => "read_only",
            _ => pg_or_service_code,
        };
        (
            status,
            json!({"error": {"code": code, "message": "server rejected"}}),
        )
    } else {
        (
            status,
            json!({"code": pg_or_service_code, "message": "server rejected", "details": null, "hint": null}),
        )
    }
}

// ---------------------------------------------------------------------------
// Connection Execution Driver (runs the same HistoryStep on SQLite & Cloud)
// ---------------------------------------------------------------------------

fn execute_step(conn: &Connection, step: &HistoryStep) -> StepOutcome {
    let outcome = match step {
        HistoryStep::InsertItem {
            tag,
            kind,
            score,
            ratio,
            active,
            note,
            payload,
        } => conn
            .write_result(
                "INSERT INTO items (tag, kind, score, ratio, active, note, payload, version) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, 1)",
                &[
                    Parameter::Text(tag.clone()),
                    Parameter::Text(kind.clone()),
                    Parameter::Integer(*score),
                    match ratio {
                        Some(v) => Parameter::Real(*v),
                        None => Parameter::Null,
                    },
                    Parameter::Bool(*active),
                    Parameter::from(note.clone()),
                    Parameter::Blob(payload.clone()),
                ],
            )
            .map(|wr| StepOutcome::Write {
                affected: wr.affected_rows,
                keys: wr.generated_keys,
            }),
        HistoryStep::InsertIgnoreItem { tag, kind, score } => conn
            .write_result(
                "INSERT OR IGNORE INTO items (tag, kind, score, ratio, active, note, payload, version) \
                 VALUES ($1, $2, $3, NULL, 1, NULL, $4, 1)",
                &[
                    Parameter::Text(tag.clone()),
                    Parameter::Text(kind.clone()),
                    Parameter::Integer(*score),
                    Parameter::Blob(vec![0x00, 0xFF]),
                ],
            )
            .map(|wr| StepOutcome::Write {
                affected: wr.affected_rows,
                keys: wr.generated_keys,
            }),
        HistoryStep::UpdateScoreCas {
            id,
            expected_version,
            new_score,
            new_note,
        } => conn
            .write_result(
                "UPDATE items SET score = $1, note = $2, version = version + 1 \
                 WHERE id = $3 AND version = $4",
                &[
                    Parameter::Integer(*new_score),
                    Parameter::from(new_note.clone()),
                    Parameter::Integer(*id),
                    Parameter::Integer(*expected_version),
                ],
            )
            .map(|wr| StepOutcome::Write {
                affected: wr.affected_rows,
                keys: wr.generated_keys,
            }),
        HistoryStep::InsertAudit { item_id, event } => conn
            .write_result(
                "INSERT INTO audits (item_id, event) VALUES ($1, $2)",
                &[
                    Parameter::Integer(*item_id),
                    Parameter::Text(event.clone()),
                ],
            )
            .map(|wr| StepOutcome::Write {
                affected: wr.affected_rows,
                keys: wr.generated_keys,
            }),
        HistoryStep::DeleteItem { id } => conn
            .write_result(
                "DELETE FROM items WHERE id = $1",
                &[Parameter::Integer(*id)],
            )
            .map(|wr| StepOutcome::Write {
                affected: wr.affected_rows,
                keys: wr.generated_keys,
            }),
        HistoryStep::ReadOrderedByScore { min_score } => conn
            .read(
                "SELECT id, tag, kind, score, ratio, active, note, payload, version \
                 FROM items WHERE score >= $1 ORDER BY score DESC, id ASC",
                &[Parameter::Integer(*min_score)],
            )
            .map(|rows| StepOutcome::Rows(canonical_rows(&rows))),
        HistoryStep::ReadByNullNote => conn
            .read(
                "SELECT id, tag, kind, score, ratio, active, note, payload, version \
                 FROM items WHERE note IS NULL ORDER BY id ASC",
                &[],
            )
            .map(|rows| StepOutcome::Rows(canonical_rows(&rows))),
        HistoryStep::ReadKeysetPage { after_id, limit } => conn
            .read(
                "SELECT id, tag, kind, score, ratio, active, note, payload, version \
                 FROM items WHERE id > $1 ORDER BY id ASC LIMIT $2",
                &[Parameter::Integer(*after_id), Parameter::Integer(*limit)],
            )
            .map(|rows| StepOutcome::Rows(canonical_rows(&rows))),
        HistoryStep::AtomicSuccessMixed { tag, score, event } => {
            let ops = vec![
                Operation::write(
                    "INSERT INTO items (tag, kind, score, ratio, active, note, payload, version) \
                     VALUES ($1, 'batch', $2, $3, $4, 'mixed', $5, 1)",
                    vec![
                        Parameter::Text(tag.clone()),
                        Parameter::Integer(*score),
                        Parameter::Real(1.5),
                        Parameter::Bool(true),
                        Parameter::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]),
                    ],
                ),
                Operation::write(
                    "INSERT INTO audits (item_id, event) VALUES ((SELECT id FROM items WHERE tag = $1), $2)",
                    vec![Parameter::Text(tag.clone()), Parameter::Text(event.clone())],
                ),
                Operation::read(
                    "SELECT id, tag, kind, score, ratio, active, note, payload, version \
                     FROM items WHERE tag = $1 ORDER BY id ASC",
                    vec![Parameter::Text(tag.clone())],
                ),
            ];
            conn.atomic(&ops).map(|batch| match &batch.results[2] {
                OperationResult::Rows(rows) => StepOutcome::Rows(canonical_rows(rows)),
                other => panic!("expected Rows in mixed batch, got {other:?}"),
            })
        }
        HistoryStep::AtomicRollbackUniqueConflict {
            new_tag,
            conflicting_tag,
        } => {
            let ops = vec![
                Operation::write(
                    "INSERT INTO items (tag, kind, score, ratio, active, note, payload, version) \
                     VALUES ($1, 'rb', 1, NULL, 1, NULL, $2, 1)",
                    vec![Parameter::Text(new_tag.clone()), Parameter::Blob(vec![1])],
                ),
                Operation::write(
                    "INSERT INTO items (tag, kind, score, ratio, active, note, payload, version) \
                     VALUES ($1, 'rb', 2, NULL, 1, NULL, $2, 1)",
                    vec![
                        Parameter::Text(conflicting_tag.clone()),
                        Parameter::Blob(vec![2]),
                    ],
                ),
            ];
            conn.atomic(&ops).map(|_| StepOutcome::Write {
                affected: 0,
                keys: Vec::new(),
            })
        }
        HistoryStep::AtomicRollbackForeignKey {
            new_tag,
            missing_item_id,
        } => {
            let ops = vec![
                Operation::write(
                    "INSERT INTO items (tag, kind, score, ratio, active, note, payload, version) \
                     VALUES ($1, 'fk_rb', 1, NULL, 1, NULL, $2, 1)",
                    vec![Parameter::Text(new_tag.clone()), Parameter::Blob(vec![3])],
                ),
                Operation::write(
                    "INSERT INTO audits (item_id, event) VALUES ($1, 'bad_fk')",
                    vec![Parameter::Integer(*missing_item_id)],
                ),
            ];
            conn.atomic(&ops).map(|_| StepOutcome::Write {
                affected: 0,
                keys: Vec::new(),
            })
        }
        HistoryStep::AtomicRollbackNotNull { new_tag } => {
            let ops = vec![
                Operation::write(
                    "INSERT INTO items (tag, kind, score, ratio, active, note, payload, version) \
                     VALUES ($1, 'nn_rb', 1, NULL, 1, NULL, $2, 1)",
                    vec![Parameter::Text(new_tag.clone()), Parameter::Blob(vec![4])],
                ),
                Operation::write(
                    "INSERT INTO items (tag, kind, score, ratio, active, note, payload, version) \
                     VALUES ($1, NULL, 1, NULL, 1, NULL, $2, 1)",
                    vec![
                        Parameter::Text(format!("{new_tag}-null")),
                        Parameter::Blob(vec![5]),
                    ],
                ),
            ];
            conn.atomic(&ops).map(|_| StepOutcome::Write {
                affected: 0,
                keys: Vec::new(),
            })
        }
        HistoryStep::AtomicReadOnlyDisguisedWrite { tag } => conn
            .atomic(&[Operation::read(
                "WITH sneaky AS (SELECT 1) INSERT INTO items (tag, kind, score, ratio, active, note, payload, version) \
                 VALUES ($1, 'sneak', 1, NULL, 1, NULL, $2, 1)",
                vec![Parameter::Text(tag.clone()), Parameter::Blob(vec![9])],
            )])
            .map(|_| StepOutcome::Write {
                affected: 0,
                keys: Vec::new(),
            }),
    };
    match outcome {
        Ok(ok) => ok,
        Err(err) => StepOutcome::Error(err.adapter_kind().expect("adapter error kind")),
    }
}

fn generate_history(seed: u64, len: usize) -> Vec<HistoryStep> {
    let mut rng = Rng::new(seed);
    let unicode_samples = [
        "alpha",
        "🦀🚀rust-cloud",
        "資料列-漢字",
        "مرحبا-rtl",
        "e\u{0301}-combining",
        "sql'); DROP TABLE items; --",
        "quote\"and'slash\\line\nend",
    ];
    let boundary_scores = [0_i64, -1, 1, i64::MIN, i64::MAX, 42, -42];
    let mut steps = Vec::with_capacity(len);
    let mut known_tags = Vec::new();
    let mut known_ids = 0_i64;

    for idx in 0..len {
        let tag = format!(
            "tag-{idx}-{}",
            unicode_samples[rng.gen_range(unicode_samples.len())]
        );
        let choice = rng.gen_range(13);
        let step = match choice {
            0..=3 => {
                known_tags.push(tag.clone());
                known_ids += 1;
                let blob_len = rng.gen_range(12);
                let payload = (0..blob_len)
                    .map(|_| (rng.next_u64() & 0xFF) as u8)
                    .collect();
                HistoryStep::InsertItem {
                    tag,
                    kind: unicode_samples[rng.gen_range(unicode_samples.len())].to_owned(),
                    score: boundary_scores[rng.gen_range(boundary_scores.len())],
                    ratio: if rng.gen_bool() {
                        Some(((rng.next_u64() % 1000) as f64) / 8.0 - 32.0)
                    } else {
                        None
                    },
                    active: rng.gen_bool(),
                    note: if rng.gen_bool() {
                        Some(format!(
                            "note-{idx}-{}",
                            unicode_samples[rng.gen_range(unicode_samples.len())]
                        ))
                    } else {
                        None
                    },
                    payload,
                }
            }
            4 => {
                let conflict = if !known_tags.is_empty() && rng.gen_bool() {
                    known_tags[rng.gen_range(known_tags.len())].clone()
                } else {
                    known_tags.push(tag.clone());
                    known_ids += 1;
                    tag
                };
                HistoryStep::InsertIgnoreItem {
                    tag: conflict,
                    kind: "ignore".into(),
                    score: boundary_scores[rng.gen_range(boundary_scores.len())],
                }
            }
            5 => {
                let target_id = if known_ids > 0 {
                    1 + (rng.gen_range(known_ids as usize) as i64)
                } else {
                    1
                };
                HistoryStep::UpdateScoreCas {
                    id: target_id,
                    expected_version: 1 + (rng.gen_range(3) as i64),
                    new_score: boundary_scores[rng.gen_range(boundary_scores.len())],
                    new_note: if rng.gen_bool() {
                        Some(format!("cas-{idx}"))
                    } else {
                        None
                    },
                }
            }
            6 => {
                let item_id = if known_ids > 0 && rng.gen_bool() {
                    1 + (rng.gen_range(known_ids as usize) as i64)
                } else {
                    999_999
                };
                HistoryStep::InsertAudit {
                    item_id,
                    event: format!("evt-{idx}"),
                }
            }
            7 => {
                let id = if known_ids > 0 {
                    1 + (rng.gen_range(known_ids as usize) as i64)
                } else {
                    1
                };
                HistoryStep::DeleteItem { id }
            }
            8 => HistoryStep::ReadOrderedByScore {
                min_score: if rng.gen_bool() { i64::MIN } else { 0 },
            },
            9 => HistoryStep::ReadByNullNote,
            10 => HistoryStep::ReadKeysetPage {
                after_id: rng.gen_range(4) as i64,
                limit: 3,
            },
            11 => {
                known_tags.push(tag.clone());
                known_ids += 1;
                HistoryStep::AtomicSuccessMixed {
                    tag,
                    score: boundary_scores[rng.gen_range(boundary_scores.len())],
                    event: format!("batch-evt-{idx}"),
                }
            }
            _ => match rng.gen_range(4) {
                0 => {
                    let new_tag = format!("rb-uniq-{idx}");
                    HistoryStep::AtomicRollbackUniqueConflict {
                        conflicting_tag: new_tag.clone(),
                        new_tag,
                    }
                }
                1 => HistoryStep::AtomicRollbackForeignKey {
                    new_tag: format!("rb-fk-{idx}"),
                    missing_item_id: 888_888,
                },
                2 => HistoryStep::AtomicRollbackNotNull {
                    new_tag: format!("rb-nn-{idx}"),
                },
                _ => HistoryStep::AtomicReadOnlyDisguisedWrite {
                    tag: format!("sneak-{idx}"),
                },
            },
        };
        steps.push(step);
    }
    steps
}

fn shrink_counterexample<F>(steps: &[HistoryStep], fails: &F) -> Vec<HistoryStep>
where
    F: Fn(&[HistoryStep]) -> bool,
{
    let mut current = steps.to_vec();
    let mut chunk = (current.len() / 2).max(1);
    while chunk >= 1 && current.len() > 1 {
        let mut improved = false;
        let mut start = 0;
        while start < current.len() && current.len() > 1 {
            let end = (start + chunk).min(current.len());
            let mut candidate = Vec::with_capacity(current.len() - (end - start));
            candidate.extend_from_slice(&current[..start]);
            candidate.extend_from_slice(&current[end..]);
            if !candidate.is_empty() && fails(&candidate) {
                current = candidate;
                improved = true;
            } else {
                start += chunk;
            }
        }
        if !improved {
            if chunk == 1 {
                break;
            }
            chunk /= 2;
        }
    }
    current
}

fn run_history_on_all_models(
    steps: &[HistoryStep],
    mode: ServerMode,
) -> Result<(), (usize, StepOutcome, StepOutcome, StepOutcome)> {
    let dir = TempDir::new().unwrap();
    let sqlite_path = dir.path().join("local_sqlite.db");
    let sqlite_path_str = sqlite_path.to_string_lossy().into_owned();
    let sqlite_conn = Connection::open(DatastoreRoute::sqlite(&sqlite_path_str)).unwrap();
    sqlite_conn
        .atomic(&[Operation::schema(SCHEMA_SQL, vec![])])
        .unwrap();

    let (server, _cloud_dir) = CloudRpcServer::start(mode);
    struct StaticDpopSigner;
    impl RequestProofSigner for StaticDpopSigner {
        fn sign_request(&self, input: &RequestProofInput<'_>) -> Result<RequestProof, DactylError> {
            RequestProof::dpop(format!("dpop.{}.{}", input.method(), input.canonical_url()))
        }
    }
    let route = match mode {
        ServerMode::NativeRpc => DatastoreRoute::supabase_rpc(
            SupabaseRpcConfig::v1(&server.origin, KEY),
            Some(BEARER.into()),
        ),
        ServerMode::DeviceGateway => DatastoreRoute::supabase_rpc(
            SupabaseRpcConfig::v1(&server.origin, KEY).with_device_gateway(),
            Some(BEARER.into()),
        )
        .with_proof_signer(Arc::new(StaticDpopSigner)),
        ServerMode::LegacyService => DatastoreRoute::supabase(&server.origin, Some(BEARER.into())),
    };
    let cloud_conn = Connection::open_with_context(
        route,
        Some(StorageContext::new(1, json!({"org": "diff", "repo": "dactyl"})).unwrap()),
    )
    .unwrap();

    let mut reference = ReferenceModel::new();
    for (idx, step) in steps.iter().enumerate() {
        let exp = reference.apply(step);
        let got_sqlite = execute_step(&sqlite_conn, step);
        let got_cloud = execute_step(&cloud_conn, step);
        if exp != got_sqlite || exp != got_cloud {
            return Err((idx, exp, got_sqlite, got_cloud));
        }
    }

    // Independent readback verification from fresh read-only observers on both SQLite and Cloud store.
    let expected_all = reference.all_items_ordered();
    let sqlite_observer = Connection::open_with_options(
        DatastoreRoute::sqlite(&sqlite_path_str),
        OpenOptions {
            access_mode: AccessMode::ReadOnly,
            ..OpenOptions::default()
        },
    )
    .unwrap();
    let cloud_observer = server.observer();
    let select_all = "SELECT id, tag, kind, score, ratio, active, note, payload, version FROM items ORDER BY id ASC";
    let sqlite_all = canonical_rows(&sqlite_observer.read(select_all, &[]).unwrap());
    let cloud_direct_all = canonical_rows(&cloud_observer.read(select_all, &[]).unwrap());
    let cloud_rpc_all = canonical_rows(&cloud_conn.read(select_all, &[]).unwrap());
    assert_eq!(
        sqlite_all, expected_all,
        "SQLite independent readback mismatch"
    );
    assert_eq!(
        cloud_direct_all, expected_all,
        "Cloud backing independent readback mismatch"
    );
    assert_eq!(cloud_rpc_all, expected_all, "Cloud RPC readback mismatch");
    Ok(())
}

#[test]
fn differential_seeded_histories_match_reference_sqlite_and_cloud_rpc_and_legacy_envelopes() {
    let seeds: [u64; 12] = [
        0x1450_0001,
        0x1450_0002,
        0x1450_0003,
        0x1450_0004,
        0x1450_0005,
        0x1450_0006,
        0x1450_0007,
        0x1450_0008,
        0xDEAD_BEEF_CAFE_0001,
        0xDEAD_BEEF_CAFE_0002,
        0x9E37_79B9_7F4A_7C15,
        0xFFFF_FFFF_0000_0145,
    ];

    for (idx, seed) in seeds.iter().enumerate() {
        let mode = match idx % 3 {
            0 => ServerMode::NativeRpc,
            1 => ServerMode::DeviceGateway,
            _ => ServerMode::LegacyService,
        };
        let steps = generate_history(*seed, 48);
        if let Err((step_idx, exp, got_sq, got_cl)) = run_history_on_all_models(&steps, mode) {
            let shrunk = shrink_counterexample(&steps, &|cand| {
                run_history_on_all_models(cand, mode).is_err()
            });
            panic!(
                "differential mismatch on seed=0x{seed:016x} at step {step_idx} (shrunk {} -> {} steps):\nexpected={exp:?}\nsqlite={got_sq:?}\ncloud={got_cl:?}\nminimal={shrunk:#?}",
                steps.len(),
                shrunk.len()
            );
        }
    }
}

#[test]
fn shrinker_reduces_seeded_history_to_minimal_failing_counterexample() {
    let steps = generate_history(0x1450_FACE, 24);
    // Inject a synthetic oracle defect triggered only when a DeleteItem follows InsertAudit
    // or specifically on any InsertAudit step.
    let fails = |cand: &[HistoryStep]| -> bool {
        cand.iter().any(|s| {
            matches!(
                s,
                HistoryStep::InsertAudit { .. } | HistoryStep::ReadKeysetPage { .. }
            )
        })
    };
    assert!(fails(&steps));
    let minimal = shrink_counterexample(&steps, &fails);
    assert_eq!(minimal.len(), 1);
}

#[test]
fn non_normalizing_and_capability_refusal_semantics_are_preserved() {
    let dir = TempDir::new().unwrap();
    let sqlite_path = dir.path().join("cap_local.sqlite");
    let sqlite_conn =
        Connection::open(DatastoreRoute::sqlite(sqlite_path.to_string_lossy())).unwrap();
    sqlite_conn
        .atomic(&[Operation::schema(SCHEMA_SQL, vec![])])
        .unwrap();

    let (server, _cloud_dir) = CloudRpcServer::start(ServerMode::NativeRpc);
    let cloud_conn = Connection::open_with_context(
        DatastoreRoute::supabase_rpc(
            SupabaseRpcConfig::v1(&server.origin, KEY),
            Some(BEARER.into()),
        ),
        Some(StorageContext::new(1, json!({"org": "cap"})).unwrap()),
    )
    .unwrap();

    // 1. Bool representation difference is preserved in raw JSON (SQLite 0/1 vs Cloud bool),
    //    while `row.get_bool()` provides portable boolean access.
    let insert = HistoryStep::InsertItem {
        tag: "bool-diff".into(),
        kind: "type-check".into(),
        score: i64::MAX,
        ratio: Some(-0.25),
        active: true,
        note: None,
        payload: vec![0x00, 0x7F, 0x80, 0xFF],
    };
    assert_eq!(
        execute_step(&sqlite_conn, &insert),
        execute_step(&cloud_conn, &insert)
    );
    let sq_row = sqlite_conn
        .read(
            "SELECT active, note, score, payload FROM items WHERE tag = 'bool-diff'",
            &[],
        )
        .unwrap();
    let cl_row = cloud_conn
        .read(
            "SELECT active, note, score, payload FROM items WHERE tag = 'bool-diff'",
            &[],
        )
        .unwrap();
    assert_eq!(sq_row.as_slice()[0].get_json("active").unwrap(), json!(1));
    assert_eq!(
        cl_row.as_slice()[0].get_json("active").unwrap(),
        json!(true)
    );
    assert!(sq_row.as_slice()[0].get_bool("active").unwrap());
    assert!(cl_row.as_slice()[0].get_bool("active").unwrap());
    assert!(sq_row.as_slice()[0].is_null("note").unwrap());
    assert!(cl_row.as_slice()[0].is_null("note").unwrap());
    assert_eq!(sq_row.as_slice()[0].get_int("score").unwrap(), i64::MAX);
    assert_eq!(cl_row.as_slice()[0].get_int("score").unwrap(), i64::MAX);
    assert_eq!(
        sq_row.as_slice()[0].get_blob("payload").unwrap(),
        vec![0x00, 0x7F, 0x80, 0xFF]
    );
    assert_eq!(
        cl_row.as_slice()[0].get_blob("payload").unwrap(),
        vec![0x00, 0x7F, 0x80, 0xFF]
    );

    // 2. Ordering tie-breaking is never normalized away.
    let tie_a = HistoryStep::InsertItem {
        tag: "tie-a".into(),
        kind: "tie".into(),
        score: 100,
        ratio: None,
        active: false,
        note: Some("".into()),
        payload: vec![],
    };
    let tie_b = HistoryStep::InsertItem {
        tag: "tie-b".into(),
        kind: "tie".into(),
        score: 100,
        ratio: None,
        active: false,
        note: Some("".into()),
        payload: vec![],
    };
    execute_step(&sqlite_conn, &tie_a);
    execute_step(&sqlite_conn, &tie_b);
    execute_step(&cloud_conn, &tie_a);
    execute_step(&cloud_conn, &tie_b);
    let sq_tie = canonical_rows(
        &sqlite_conn
            .read(
                "SELECT id, tag, kind, score, ratio, active, note, payload, version \
                 FROM items WHERE kind = 'tie' ORDER BY score DESC, id ASC",
                &[],
            )
            .unwrap(),
    );
    let cl_tie = canonical_rows(
        &cloud_conn
            .read(
                "SELECT id, tag, kind, score, ratio, active, note, payload, version \
                 FROM items WHERE kind = 'tie' ORDER BY score DESC, id ASC",
                &[],
            )
            .unwrap(),
    );
    assert_eq!(sq_tie, cl_tie);
    assert_eq!(sq_tie[0].tag, "tie-a");
    assert_eq!(sq_tie[1].tag, "tie-b");
    assert_eq!(sq_tie[0].note.as_deref(), Some(""));

    // 3. 0-row INSERT OR IGNORE never leaks stale last_insert_rowid in SQLite or Cloud.
    let zero_res_sq = sqlite_conn
        .write_result(
            "INSERT OR IGNORE INTO items (tag, kind, score, ratio, active, note, payload, version) \
             VALUES ('tie-a', 'tie', 1, NULL, 1, NULL, $1, 1)",
            &[Parameter::Blob(vec![])],
        )
        .unwrap();
    let zero_res_cl = cloud_conn
        .write_result(
            "INSERT OR IGNORE INTO items (tag, kind, score, ratio, active, note, payload, version) \
             VALUES ('tie-a', 'tie', 1, NULL, 1, NULL, $1, 1)",
            &[Parameter::Blob(vec![])],
        )
        .unwrap();
    assert_eq!(zero_res_sq.affected_rows, 0);
    assert_eq!(zero_res_cl.affected_rows, 0);
    assert!(zero_res_sq.generated_keys.is_empty());
    assert!(zero_res_cl.generated_keys.is_empty());

    // 4. Keyset pagination with visit_rows + early stop on both backends.
    let mut sq_ids = Vec::new();
    let sq_scan = sqlite_conn
        .visit_rows(
            "SELECT id FROM items WHERE id > $1 ORDER BY id ASC LIMIT 2",
            &[Parameter::Integer(0)],
            |row| {
                sq_ids.push(row.get_int("id")?);
                Ok(sq_ids.is_empty())
            },
        )
        .unwrap();
    let mut cl_ids = Vec::new();
    let cl_scan = cloud_conn
        .visit_rows(
            "SELECT id FROM items WHERE id > $1 ORDER BY id ASC LIMIT 2",
            &[Parameter::Integer(0)],
            |row| {
                cl_ids.push(row.get_int("id")?);
                Ok(cl_ids.is_empty())
            },
        )
        .unwrap();
    assert_eq!(
        sq_scan,
        ScanOutcome {
            rows: 1,
            complete: false
        }
    );
    assert_eq!(cl_scan, sq_scan);
    assert_eq!(sq_ids, cl_ids);

    // 5. Explicit capability refusal differences:
    //    SQLite supports catalog/maintenance; Cloud refuses locally with zero HTTP requests.
    assert!(sqlite_conn.has_table("items").unwrap());
    assert!(sqlite_conn.has_column("items", "payload").unwrap());
    assert!(sqlite_conn.inspect_schema().is_ok());
    assert!(sqlite_conn.verify_integrity().is_ok());

    let before_reqs = server.requests.load(Ordering::SeqCst);
    for cloud_only_refusal in [
        cloud_conn.has_table("items").unwrap_err(),
        cloud_conn.has_column("items", "payload").unwrap_err(),
        cloud_conn.inspect_schema().unwrap_err(),
        cloud_conn.verify_integrity().unwrap_err(),
    ] {
        assert_eq!(
            cloud_only_refusal.adapter_kind(),
            Some(AdapterErrorKind::Capability)
        );
        assert_eq!(
            cloud_only_refusal.mutation_outcome(),
            MutationOutcome::NotAttempted
        );
    }
    assert_eq!(
        server.requests.load(Ordering::SeqCst),
        before_reqs,
        "Cloud catalog/maintenance refusals must not issue HTTP requests"
    );

    //    Native Cloud RPC refuses Operation::schema explicitly without SQLite fallback.
    let ddl_err = cloud_conn
        .atomic(&[Operation::schema("CREATE TABLE rogue(id INT)", vec![])])
        .unwrap_err();
    assert_eq!(ddl_err.adapter_kind(), Some(AdapterErrorKind::Capability));
    assert!(!sqlite_conn.has_table("rogue").unwrap());

    // 6. Disguised mutations on read() / visit_rows() / Operation::read() fail closed on BOTH backends.
    for bad_sql in [
        "INSERT INTO items (tag, kind, score, ratio, active, note, payload, version) VALUES ('evil', 'e', 1, NULL, 1, NULL, X'', 1)",
        "WITH cte AS (SELECT 1) INSERT INTO items (tag, kind, score, ratio, active, note, payload, version) VALUES ('evil2', 'e', 1, NULL, 1, NULL, X'', 1)",
    ] {
        let sq_err = sqlite_conn.read(bad_sql, &[]).unwrap_err();
        assert_eq!(sq_err.adapter_kind(), Some(AdapterErrorKind::ReadOnly));
        let cl_err = cloud_conn.read(bad_sql, &[]).unwrap_err();
        assert_eq!(cl_err.adapter_kind(), Some(AdapterErrorKind::ReadOnly));
    }
    assert_eq!(
        sqlite_conn
            .read(
                "SELECT COUNT(*) AS n FROM items WHERE tag LIKE 'evil%'",
                &[]
            )
            .unwrap()
            .as_slice()[0]
            .get_int("n")
            .unwrap(),
        0
    );
    assert_eq!(
        server
            .observer()
            .read(
                "SELECT COUNT(*) AS n FROM items WHERE tag LIKE 'evil%'",
                &[]
            )
            .unwrap()
            .as_slice()[0]
            .get_int("n")
            .unwrap(),
        0
    );

    // 7. Malformed / oversized / non-finite real inputs fail before mutating on BOTH backends.
    for bad_real in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let sq_err = sqlite_conn
            .write("INSERT INTO items (tag, kind, score, ratio, active, note, payload, version) VALUES ('nf', 'nf', 1, $1, 1, NULL, X'', 1)", &[Parameter::Real(bad_real)])
            .unwrap_err();
        let cl_err = cloud_conn
            .write("INSERT INTO items (tag, kind, score, ratio, active, note, payload, version) VALUES ('nf', 'nf', 1, $1, 1, NULL, X'', 1)", &[Parameter::Real(bad_real)])
            .unwrap_err();
        assert_eq!(sq_err.adapter_kind(), Some(AdapterErrorKind::Value));
        assert_eq!(sq_err.mutation_outcome(), MutationOutcome::NotAttempted);
        assert_eq!(cl_err.adapter_kind(), Some(AdapterErrorKind::Value));
        assert_eq!(cl_err.mutation_outcome(), MutationOutcome::NotAttempted);
    }

    let tight_budget = ResourceBudget::v1()
        .with_max_sql_bytes(16)
        .with_max_cell_bytes(8);
    let sq_tight = Connection::open_with_budget(
        DatastoreRoute::sqlite(sqlite_path.to_string_lossy()),
        OpenOptions::default(),
        None,
        tight_budget,
    )
    .unwrap();
    let cl_tight = Connection::open_with_budget(
        DatastoreRoute::supabase_rpc(
            SupabaseRpcConfig::v1(&server.origin, KEY),
            Some(BEARER.into()),
        ),
        OpenOptions::default(),
        Some(StorageContext::new(1, json!({"org": "cap"})).unwrap()),
        tight_budget,
    )
    .unwrap();
    for conn in [&sq_tight, &cl_tight] {
        let sql_err = conn.read("SELECT 12345678901234567890", &[]).unwrap_err();
        assert_eq!(sql_err.failure_origin(), FailureOrigin::LocalBudget);
        assert_eq!(sql_err.mutation_outcome(), MutationOutcome::NotAttempted);

        let cell_err = conn
            .write("INSERT", &[Parameter::Text("oversized-cell".into())])
            .unwrap_err();
        assert_eq!(cell_err.failure_origin(), FailureOrigin::LocalBudget);
        assert_eq!(cell_err.mutation_outcome(), MutationOutcome::NotAttempted);
    }
}
