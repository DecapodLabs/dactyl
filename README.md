# dactyl

[![🦀 Decapod](https://img.shields.io/badge/🦀%20Decapod-v0.96.12-dc2626)](https://github.com/DecapodLabs/decapod)

`dactyl-db` is the application-layer database provider for read/write-heavy
apps that need the same small Rust surface over local SQLite and Vercel Neon.
It forwards application SQL, binds values, and normalizes returned rows so the
application does not need separate driver code for each backend.

Dactyl is deliberately not database-administration tooling. It does not own
schema design, migrations, transactions, query planning, analytics, retries,
or business intelligence. Those belong to the backend and the database that
owns the application data.

## What is shared

SQLite and Neon use the same application contract:

- `read(sql, params)` returns owned `Rows`.
- `write(sql, params)` returns the backend-reported affected-row count.
- Values are bound as `Null`, `Bool`, `Integer`, `Real`, `Text`, or `Blob`.
- SQL is forwarded unchanged; Dactyl does not parse, rewrite, or optimize it.
- Adapter failures use the same coarse categories, including query,
  constraint, storage, transport, and protocol failures.

The SQLite implementation uses a small private C-API wrapper through
`libsqlite3-sys`; no `rusqlite` types or high-level database API are exposed.
The Neon implementation sends the same SQL and bound values to the configured
`/query` endpoint.

## Quick start

```toml
[dependencies]
dactyl-db = { version = "0.3.0", features = ["sqlite", "neon"] }
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

## Environment

| Variable | Meaning |
|---|---|
| `DATASTORE` | `sqlite` or `neon` |
| `DATASTORE_ROUTE` | SQLite file path or Neon `/query` service endpoint |
| `DATASTORE_TOKEN` | Optional opaque bearer token for Neon |

The database schema and backend endpoint contract are application-owned. Dactyl
expects the tables and query behavior to already exist; it does not bootstrap
or administer them.

## License

MIT.
