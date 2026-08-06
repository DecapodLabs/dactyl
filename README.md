# dactyl-db

[![🦀 Decapod](https://img.shields.io/badge/🦀%20Decapod-v0.96.12-dc2626)](https://github.com/DecapodLabs/decapod)

`dactyl-db` is a single SQL-vendor-agnostic Rust persistence framework. One crate, one backend-neutral operation contract, and one ambient env-var selector — talk to SQLite, Neon/Postgres, and (planned) Redis, MySQL, Cassandra from one import, instead of pulling a per-database library for each backend.

The same SQL string produces the same logical rows regardless of the active backend. Parameters are always bound, never interpolated, so dynamic values cannot become SQL.

## Key Features

- **One import, many backends** — SQLite and Neon ship today; Redis, MySQL, and Cassandra are planned behind the same `query` surface.
- **Ambient selection** — `DATASTORE` env var picks the active backend at runtime. No `init()`, no per-call datastore argument, no global connection cache.
- **Safe parameter binding** — `query(sql, &[params])` binds typed values; SQL injection via parameter values is structurally impossible.
- **Atomic batches** — `transaction(&[Statement])` commits all-or-nothing on every backend (no nesting; caller-owned retry/idempotency; see contract below).
- **Connection-scoped integration** — `Connection` and `StorageOp` provide the thin waist needed by Decapod without exposing `rusqlite`, HTTP clients, or adapter types.
- **Caller-owned schema** — dactyl never silently creates tables. `execute("create table ...")` is the only way dactyl touches schema.

## Quick Start

Add `dactyl-db` to your `Cargo.toml`:

```toml
[dependencies]
dactyl-db = { version = "0.1.7", features = ["sqlite", "neon"] }
```

Set the active backend via env vars at runtime:

```text
DATASTORE=sqlite DATASTORE_ROUTE=/path/to/store.db
# or
DATASTORE=neon DATASTORE_ROUTE=https://propodus.example DATASTORE_TOKEN=...
```

### Usage Example

```rust
use dactyl_db::{self, query, execute, Parameter, Rows};

fn main() -> Result<(), dactyl_db::DactylError> {
    // The caller owns the schema. dactyl never bootstraps tables on its own.
    execute(
        "create table if not exists todos (id integer primary key, title text not null, status text not null)",
        &[],
    )?;
    execute(
        "insert into todos (id, title, status) values ($1, $2, $3)",
        &[
            Parameter::Integer(1),
            Parameter::Text("ship dactyl 0.1.7".into()),
            Parameter::Text("open".into()),
        ],
    )?;

    let sql = query!("select id, title, status from todos");
    for row in dactyl_db::query(&sql, &[])?.iter() {
        // Strict typed projection (owned). Prefer try_get if you like Result style.
        let id: i64 = row.try_get("id")?;
        let title: String = row.get("title")?;
        // Nullable columns use Option<T>; missing columns are ColumnNotFound.
        let status: Option<String> = row.get("status")?;
        println!("todo {id}: {title} [{status:?}]");
    }
    Ok(())
}
```

Run it:

```text
DATASTORE=sqlite DATASTORE_ROUTE=/tmp/dactyl-example.db \
  cargo run --features sqlite --example readme_example
```

## Named-column projections (`Row`)

This is the stable contract for typed and NULL-safe extraction (dactyl [#25](https://github.com/DecapodLabs/dactyl/issues/25), conformance [#2](https://github.com/DecapodLabs/dactyl/issues/2); also DecapodLabs/decapod#1111):

| Concern | Semantics |
|---|---|
| Integer / real / bool / text | `get_int`, `get_real`, `get_bool`, `get_str` (owned) or strict `get::<T>` / `try_get::<T>` via serde |
| Portable bool | `get_bool` accepts JSON `true`/`false` **or** integer `0`/`1` (SQLite stores bools as integers) |
| JSON / text | `get_json` / `get_json_ref` return the raw cell. Text payloads stay strings until the caller parses them; Neon may surface structured JSON objects. |
| SQL NULL | `get::<Option<T>>` → `None`; non-`Option` getters → `Conversion` mentioning NULL; `is_null` / `get_json` surface null without converting |
| Missing column | `DactylError::ColumnNotFound` |
| Duplicate aliases | **First match** left-to-right. `select a as x, b as x` → `get("x")` is `a`. Use a positional index for later duplicates. |
| Owned vs borrowed | `get` / `get_*` return owned values that outlive the row. `get_str_ref` / `get_json_ref` borrow from `&Row` for the row lifetime. A `Row` outlives the adapter connection. |
| Conversion failure | `DactylError::Conversion` with the column key and a reason |

## How dactyl selects the backend

The active backend is chosen by ambient environment variables — no `init()` call, no per-call datastore argument, no process-wide connection cache. Each `query` / `execute` / `transaction` call constructs a fresh short-lived adapter and drops it on return, so workspace and session isolation is automatic and the public surface is `Send + Sync` without any lock.

| Environment Variable | Allowed Values / Format | Required | Description |
|---|---|---|---|
| `DATASTORE` | `"sqlite"` or `"neon"` | yes | Selects the active backend. Any other value is a typed error. |
| `DATASTORE_ROUTE` | SQLite filepath OR Neon endpoint URL | yes | Connection route for the selected backend. |
| `DATASTORE_TOKEN` | Opaque string | no | Bearer token forwarded to Neon. Ignored by SQLite. |
| `DATASTORE_REWRITE` | `1`, `true`, `yes`, or `on` | no | Enables only dactyl's explicitly safe dialect rewrites. Disabled by default. |
| `DATASTORE_SQLITE_ROUTE` | SQLite filepath | no | Optional alternate route for a `-- dactyl: sqlite` inline directive when the active datastore is Neon. |
| `DATASTORE_NEON_ROUTE` | Neon endpoint URL | no | Optional alternate route for a `-- dactyl: neon` inline directive when the active datastore is SQLite. |

## Connection-scoped integration boundary

Use an explicit connection when several operations must share one configured
route, such as schema setup, validation, migrations, or a sequence of reads and
writes:

```rust
use dactyl_db::{Connection, DatastoreRoute, Parameter, Statement};

let db = Connection::open(DatastoreRoute::sqlite(".decapod/data/decapod.db"))?;
db.execute_batch("PRAGMA foreign_keys=ON; CREATE TABLE IF NOT EXISTS state (id INTEGER PRIMARY KEY, value TEXT);")?;
db.transaction(&[Statement::new(
    "INSERT INTO state(value) VALUES ($1)",
    vec![Parameter::Text("ready".into())],
)])?;
```

`Connection::open_with_options` additionally controls read-only SQLite access,
busy timeout, foreign-key enforcement, journal mode (WAL falls back to DELETE),
and safe dialect rewrites. The
operation-based `StorageOp` / `StorageResult` pair is the integration surface
for code that cannot accept a closure tied to a SQLite connection. Dactyl's
public API intentionally does not expose `rusqlite` types.

## Public surface

| Function | Purpose |
|---|---|
| `query(sql, params)` | One entry point for any SQL (read or write). Returns `Rows`. |
| `execute(sql, params)` | DDL / migration / affected-row operations. Returns affected count. |
| `execute_batch(sql)` | Caller-owned multi-statement DDL or migration script. |
| `transaction(&[Statement])` | Atomic batch; full rollback on any per-statement failure. |
| `query!("sql")` macro | Compile-time SQL-literal analysis. |
| `Connection` | Connection-scoped backend-neutral operations and configuration. |
| `StorageOp` | Operation-based thin waist for adapter-independent callers. |

`Parameter` enumerates the typed binding set: `Null`, `Bool`, `Integer`, `Real`, `Text`, and `Blob`. The adapter forwards the values verbatim — never as interpolated SQL.

`Row` provides `get` / `try_get`, lenient scalar getters, `is_null`, and borrowed `get_str_ref` / `get_json_ref` under the projection contract above.

## Atomic batches (`transaction`)

Stable contract for multi-statement units of work ([#24](https://github.com/DecapodLabs/dactyl/issues/24); prerequisite for DecapodLabs/decapod#1111 / #1120):

| Concern | Semantics |
|---|---|
| Atomicity | Any per-statement failure aborts the whole unit. SQLite uses a real transaction; Neon uses one `POST /batch` that the server accepts or rejects as a unit. Empty slice → `Ok([])`. |
| Nesting | **Not supported.** No SAVEPOINTs. Each call uses a fresh adapter; put every statement in one slice. |
| Retry | **dactyl does not retry.** Callers own retry policy. |
| Timeout | **No public deadline.** Neon uses reqwest defaults; SQLite is local. |
| Idempotency | **Not idempotent.** Replays may conflict or double-write. Design deterministic keys / upserts if retrying after ambiguous transport failures. |
| Proof | Conformance covers SQLite + Neon-mock failure injection and an event-plus-state fixture (state row + event row in one batch; mid-batch failure leaves neither). |

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
