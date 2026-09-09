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

Schema versioning, migration ordering, import from legacy stores,
retry/backoff, idempotency keys, and domain-level version/CAS policy remain
with Decapod or Propodus; Dactyl owns the physical local SQLite maintenance
contract described below. The Neon adapter maps the stable Propodus v1 error
codes it receives, but it does not invent the resource-route translation or
claim live cloud parity when that service contract is unavailable.

## Explicit SQLite maintenance and recovery

Local callers can use the additive `Connection` methods
`verify_integrity()`, `backup(destination)`, and
`recover_from_dump_reload(RecoveryOptions)`. These methods are deliberately
not part of ordinary connection startup: Dactyl never silently repairs,
renames, overwrites, or replaces a database during open or validation.
Neon returns typed capability errors for these local-only operations.

`verify_integrity()` runs SQLite's full `PRAGMA integrity_check`. A successful
call returns the observed journal mode, `user_version`, and `application_id`.
Malformed files and damaged indexes return `AdapterErrorKind::Corrupt` with a
stable code such as `malformed_database`, `corrupt_database`, or
`integrity_check_failed`; busy/locked, unavailable, and filesystem failures
remain separate typed outcomes.

`backup(destination)` uses SQLite's online backup API against the live
connection. It is the supported live snapshot operation: WAL and SHM are read
through SQLite and are not copied independently, so copying only the main
database file is not used as a live-backup strategy. Dactyl writes a temporary
destination, integrity-checks it, syncs it, and atomically publishes it. The
backup is durable only to the extent that the host filesystem honors the file
and directory sync operations. The destination is standalone and no partial
destination is published on a backup, validation, sync, or rename failure.

`recover_from_dump_reload(RecoveryOptions::new(archive_path,
RecoveryJournalMode::Delete))` is an explicit operator action for a file-backed
SQLite route. Dactyl starts a bounded exclusive SQLite transaction, rebuilds a
new database from schema and table data using full-table reads that bypass
secondary indexes, restores `user_version`, `application_id`, and
`sqlite_sequence`, validates the new database, syncs it, preserves the
original database plus any `-wal`/`-shm` sidecars at `archive_path`, and then
renames the verified replacement into place. The original is retained until
activation is ready; rename, sync, and reopen failures attempt rollback and
leave the original active. An existing archive path is a typed conflict.

Logical dump/reload intentionally does not preserve `PRAGMA journal_mode`.
Dactyl always activates recovered databases in DELETE rollback-journal mode,
which keeps replacement a single-file operation. The recovery result exposes
the selected mode. Re-enabling WAL is a separate, explicit caller operation
after reopening; it is not part of recovery atomicity.

Recovery requires every other Dactyl connection in the current process to be
closed and uses SQLite's configured busy timeout for cooperating clients. A
live backup may run while other Dactyl connections are open. Dactyl cannot
detect idle handles in another process, coordinate arbitrary external SQLite
writers, or guarantee correctness on mounted filesystems that do not reliably
propagate advisory locks. Operators must quiesce cooperating clients before
replacement; Decapod's canonical coordination layer remains responsible for
that higher-level process boundary.

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
