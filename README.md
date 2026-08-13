# dactyl

[![🦀 Decapod](https://img.shields.io/badge/🦀%20Decapod-v0.98.3-dc2626)](https://github.com/DecapodLabs/decapod)

`dactyl-db` is a lightweight application-layer datastore for read/write-heavy
apps that need the same small Rust surface over a local store and Vercel Neon.
It binds values, normalizes returned rows, and exposes explicit physical
atomicity so the application does not need separate driver code for each
backend.

Dactyl is deliberately not database-administration tooling. Callers own schema
design, migration ids and ordering, grouping policy, retries, analytics, and
business intelligence. Dactyl owns only physical execution, local durability,
atomic batch boundaries, access mode, and result/error normalization.

## What is shared

SQLite and Neon use the same application contract:

- `read(sql, params)` returns owned `Rows`.
- `write(sql, params)` returns the affected-row count for compatibility;
  `write_result` also returns explicit generated keys.
- `atomic(&[Operation])` executes an opaque all-or-nothing batch and preserves
  result order. Empty batches are no-ops, and a failed batch persists nothing.
  It does not implement retry, nesting, or idempotency policy.
- `OpenOptions { access_mode: ReadOnly, .. }` opens a non-mutating handle.
- Values are bound as `Null`, `Bool`, `Integer`, `Real`, `Text`, or `Blob`.
- The Neon adapter forwards SQL to `/query` and atomic batches to `/batch`.
- `StorageContext` is a versioned opaque `{ version, payload }` envelope. It
  is ignored by local storage and forwarded unchanged by Neon; Dactyl does not
  interpret tenancy or authorization fields.
- Adapter failures use typed categories including busy/locked/timeout,
  constraint/conflict/version-conflict/transaction-aborted, read-only,
  capability, value, storage, transport, authentication/authorization, quota,
  rate-limit, and protocol failures. Remote stable error codes are available
  through `DactylError::adapter_code()` without parsing provider messages.

The local implementation is a thin private C-ABI connection behind the same
public contract. The optional `sqlite` feature dynamically loads the host's
shared SQLite library at runtime; Dactyl does not compile or bundle SQLite,
and does not duplicate SQLite's file format, parser, pager, journal, or query
planner. The route name and `DATASTORE=sqlite` setting therefore mean what
they say: the requested path is an ordinary SQLite database.

## Local SQLite route

`DATASTORE=sqlite DATASTORE_ROUTE=/path/to/app.db` opens the file directly.
Existing SQLite files remain readable and writable without conversion. A
read/write route creates a missing file and its parent directory; a read-only
route requires an existing file. SQLite supplies locking, journaling, crash
recovery, and its supported SQL surface. Dactyl applies the configured busy
timeout and maps SQLite busy, locked, constraint, read-only, corrupt, and
storage outcomes into its typed error categories.

The [SQLite connector report](docs/whitepapers/dactyl-sqlite-connector.md)
records the boundary and the compatibility proof. The same report is published
from `docs/` as GitHub Pages.

`Connection::inspect_schema()` returns a backend-neutral catalog containing
tables, columns, nullability/defaults, primary and unique keys, indexes,
foreign keys, delete actions, and row counts. Callers do not need to issue
SQLite-specific catalog queries. Blobs are normalized to JSON arrays of bytes
and are read back with `Row::get_blob`; NULL, text, integer, REAL, and blob
values remain distinct through the public row contract.

Schema versioning, migration ordering, backups, import from legacy stores,
retry/backoff, idempotency keys, and domain-level version/CAS policy remain
with Decapod or Propodus. The Neon adapter maps the stable Propodus v1 error
codes it receives, but it does not invent the resource-route translation or
claim live cloud parity when that service contract is unavailable.

## Local and mock conformance matrix

`tests/storage_fixtures.rs` is the backend-neutral fixture suite. The same
cases run against the local store and, when the `neon` feature is enabled, an
in-process executing mock that speaks the Neon `/query` and `/batch` envelope.

| Case | Local SQLite | Neon executing mock | Live Propodus / Vercel Neon |
|---|---|---|---|
| Parameterized read/write and result normalization | proved | proved | unavailable unless `DACTYL_LIVE_PROPODUS_ROUTE` is set; still not claimed here |
| Explicit caller-owned ids and affected-row counts | proved | proved | unavailable |
| Conditional `UPDATE` / CAS and zero-row stale writes | proved as `affected_rows = 0` | proved as `affected_rows = 0` | live `version_conflict` remains a service-side proof |
| Atomic state-plus-event commit and rollback | proved | proved | unavailable |
| Read-only handles | proved | covered by the Neon adapter tests | unavailable |
| Typed constraint / timeout errors | proved | proved for constraint | live transport and provider codes remain a service-side proof |
| Concurrent scoped writes and `DROP` cleanup | proved | not required of the HTTP mock | unavailable |
| Opaque `StorageContext` | ignored; tenancy fields are unnecessary | forwarded unchanged; missing/invalid context and `repository_not_authorized` are typed | live authorization directory remains a service-side proof |

A skipped live backend is recorded as `unavailable`, never `passed`. Local
CAS is a zero-row observation on a caller-owned `version` predicate. Dactyl
does not invent a version-conflict policy for the local store.

## Quick start

```toml
[dependencies]
dactyl-db = { version = "0.8.0", features = ["sqlite", "neon"] }
```

Select the backend with environment variables:

```text
DATASTORE=sqlite DATASTORE_ROUTE=/path/to/app.db
# or
DATASTORE=neon DATASTORE_ROUTE=https://propodus.example DATASTORE_TOKEN=...
```

Use the same calls for either backend:

```rust
use dactyl_db::{read, write, Parameter};

fn load_app_rows() -> Result<(), dactyl_db::DactylError> {
    write(
        "insert into app_events (name) values ($1)",
        &[Parameter::Text("opened".into())],
    )?;

    let rows = read("select name from app_events order by id", &[])?;
    for row in rows.iter() {
        println!("{}", row.get_str("name")?);
    }
    Ok(())
}
```

For an explicit route, use `Connection::open`:

```rust
use dactyl_db::{Connection, DatastoreRoute, Parameter};

let db = Connection::open(DatastoreRoute::sqlite("/tmp/app.db"))?;
db.write(
    "update accounts set last_seen = $1 where id = $2",
    &[Parameter::Integer(1_725_000_000), Parameter::Integer(7)],
)?;
```

Remote callers provide the Decapod-owned context separately from the physical
route. The context payload is application-owned and must be a JSON object; its
fields are opaque to Dactyl:

```rust
use dactyl_db::{Connection, DatastoreRoute, StorageContext};
use serde_json::json;

let context = StorageContext::new(
    1,
    json!({"opaque_target": "target", "opaque_session": "session"}),
)?;
let db = Connection::open_with_context(
    DatastoreRoute::neon("https://propodus.example", None),
    Some(context),
)?;
```

Neon requests without a valid context fail closed with a typed
`authentication_required` or `invalid_context` error before Dactyl sends SQL.

Use an explicit physical batch and generated-key result when those semantics
matter:

```rust
use dactyl_db::{Connection, DatastoreRoute, Operation, Parameter};

let db = Connection::open(DatastoreRoute::sqlite("/tmp/app.db"))?;
let result = db.atomic(&[
    Operation::schema("create table if not exists events (id integer primary key, name text)", Vec::new()),
    Operation::write("insert into events (name) values ($1)", vec![Parameter::Text("opened".into())]),
])?;
```

## Environment

| Variable | Meaning |
|---|---|
| `DATASTORE` | `sqlite` or `neon` |
| `DATASTORE_ROUTE` | Dactyl local-store path or Neon service endpoint |
| `DATASTORE_TOKEN` | Optional opaque bearer token for Neon |
| `DACTYL_SQLITE_LIBRARY` | Optional explicit host SQLite shared-library path for non-standard loader paths |

`DATASTORE` is the only ambient selector. Dactyl requires a non-empty
`DATASTORE_ROUTE` for whichever selector is chosen and fails before adapter
construction when either value is missing, empty, or unsupported. There is no
implicit SQLite fallback, so a deployment cannot silently write to a local
file when its Neon configuration is malformed. `DATASTORE_TOKEN` is read only
for `neon`; an unset or blank token is treated as absent and does not change
the context requirement. Explicit `Connection::open` routes bypass ambient
selection without changing the free-function signatures.

The database schema and backend endpoint contract are application-owned. Dactyl
executes caller-supplied schema statements but does not assign migration ids,
order migrations, create hidden tables, or administer recovery policy.

## License

MIT.
