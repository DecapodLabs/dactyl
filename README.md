# dactyl

[![🦀 Decapod](https://img.shields.io/badge/🦀%20Decapod-v0.96.13-dc2626)](https://github.com/DecapodLabs/decapod)

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
  result order. It does not implement retry, nesting, or idempotency policy.
- `OpenOptions { access_mode: ReadOnly, .. }` opens a non-mutating handle.
- Values are bound as `Null`, `Bool`, `Integer`, `Real`, `Text`, or `Blob`.
- The Neon adapter forwards SQL to `/query` and atomic batches to `/batch`.
- Adapter failures use typed categories including busy/locked/timeout,
  constraint/conflict, read-only, capability, value, storage, transport, and
  protocol failures.

The local implementation is Dactyl-owned Rust. It has no `sqlite`, `rusqlite`,
`libsqlite3-sys`, or SQLite subprocess dependency. The `sqlite` feature and
route constructor remain as compatibility names for existing callers, but the
local file is a versioned Dactyl snapshot, not a SQLite file. A SQLite header is
rejected with a typed capability error; migration/import belongs to the caller.

The local SQL surface is intentionally bounded: caller-supplied `CREATE TABLE`,
`ALTER TABLE ... ADD`, `DROP TABLE`, `INSERT`, `UPDATE`, `DELETE`, and `SELECT`
with predicates and basic ordering/limits. Unsupported SQL fails with a typed
capability/query error rather than silently changing the request. This is a
storage primitive, not a planner or schema owner.

## Quick start

```toml
[dependencies]
dactyl-db = { version = "0.4.0", features = ["sqlite", "neon"] }
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

The database schema and backend endpoint contract are application-owned. Dactyl
executes caller-supplied schema statements but does not assign migration ids,
order migrations, create hidden tables, or administer recovery policy.

## License

MIT.
