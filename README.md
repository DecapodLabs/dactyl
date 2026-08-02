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

## Public surface

| Function | Purpose |
|---|---|
| `query(sql, params)` | One entry point for any SQL (read or write). Returns `Rows`. |
| `execute(sql, params)` | DDL / migration / affected-row operations. Returns affected count. |
| `transaction(&[Statement])` | Atomic batch; full rollback on any per-statement failure. |
| `query!("sql")` macro | Compile-time SQL-literal analysis. |

`Parameter` enumerates the typed binding set: `Null`, `Bool`, `Integer`, `Real`, `Text`. The adapter forwards the values verbatim — never as interpolated SQL.

`Row` provides `get` / `try_get`, lenient scalar getters, `is_null`, and borrowed `get_str_ref` / `get_json_ref` under the projection contract above.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
