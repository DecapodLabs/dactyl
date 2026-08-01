# dactyl-db

`dactyl-db` is a single SQL-vendor-agnostic Rust persistence framework. One crate, one `query(sql, params)` API, one ambient env-var selector — talk to SQLite, Neon/Postgres, and (planned) Redis, MySQL, Cassandra from one import, instead of pulling a per-database library for each backend.

The same SQL string produces the same logical rows regardless of the active backend. Parameters are always bound, never interpolated, so dynamic values cannot become SQL.

## Key Features

- **One import, many backends** — SQLite and Neon ship today; Redis, MySQL, and Cassandra are planned behind the same `query` surface.
- **Ambient selection** — `DATASTORE` env var picks the active backend at runtime. No `init()`, no per-call datastore argument, no global connection cache.
- **Safe parameter binding** — `query(sql, &[params])` binds typed values; SQL injection via parameter values is structurally impossible.
- **Atomic batches** — `transaction(&[Statement])` commits all-or-nothing on every backend.
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
        let id: i64 = row.get("id")?;
        let title: String = row.get("title")?;
        let status: String = row.get("status")?;
        println!("todo {id}: {title} [{status}]");
    }
    Ok(())
}
```

Run it:

```text
DATASTORE=sqlite DATASTORE_ROUTE=/tmp/dactyl-example.db \
  cargo run --features sqlite --example readme_example
```

## How dactyl selects the backend

The active backend is chosen by ambient environment variables — no `init()` call, no per-call datastore argument, no process-wide connection cache. Each `query` / `execute` / `transaction` call constructs a fresh short-lived adapter and drops it on return, so workspace and session isolation is automatic and the public surface is `Send + Sync` without any lock.

| Environment Variable | Allowed Values / Format | Required | Description |
|---|---|---|---|
| `DATASTORE` | `"sqlite"` or `"neon"` | yes | Selects the active backend. Any other value is a typed error. |
| `DATASTORE_ROUTE` | SQLite filepath OR Neon endpoint URL | yes | Connection route for the selected backend. |
| `DATASTORE_TOKEN` | Opaque string | no | Bearer token forwarded to Neon. Ignored by SQLite. |

## Public surface

| Function | Purpose |
|---|---|
| `query(sql, params)` | One entry point for any SQL (read or write). Returns `Rows`. |
| `execute(sql, params)` | DDL / migration / affected-row operations. Returns affected count. |
| `transaction(&[Statement])` | Atomic batch; full rollback on any per-statement failure. |
| `query!("sql")` macro | Compile-time SQL-literal analysis. |

`Parameter` enumerates the typed binding set: `Null`, `Bool`, `Integer`, `Real`, `Text`. The adapter forwards the values verbatim — never as interpolated SQL.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
